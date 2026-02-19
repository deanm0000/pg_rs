#![allow(unsafe_op_in_unsafe_fn)]
#![feature(int_roundings)]

use native_tls::TlsConnector;
use polars::frame::row::AnyValueBuffer;
use polars::prelude::{Column as PlColumn, DataFrame, IntoColumn, Schema as PlSchema};
use postgres_native_tls::MakeTlsConnector;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PySequence, PyTuple};
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use pyo3_polars::PySchema;
use pyo3_polars::types::PyExpr;
use pyo3_polars::{PolarsAllocator, PyDataFrame};

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio::time::{Duration, timeout};
use tokio_postgres::Client;
use tokio_postgres::RowStream;

pub mod pgtopl;
use futures::TryStreamExt;
mod db_utils;
use crate::db_utils::py_tools::{fetchall, py_seq_to_rust_params};
use crate::pgtopl::row_to_av::first_row_to_avs;
use crate::pgtopl::row_to_av::next_row_to_avs;
#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

#[pymethods]
impl SourceGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyDataFrame>> {
        let pres = slf.presource.borrow_mut(py);
        let rows_read = &pres.rows_read.clone();
        let stream_is_done = &pres.stream_is_done.clone();
        let buffer = &pres.buffer.clone();
        let pause_clone = &pres.pause.clone();
        let schema = pres.schema.clone();
        let rt = get_runtime();
        let buffer_size = pres.buffer_size;
        let n_rows = slf.n_rows.clone();
        let consumer_is_done = pres.consumer_is_done.clone();

        let df = py.detach(|| -> PyResult<Option<PyDataFrame>> {
            rt.block_on(async move {
                let (rows_ready, stream_is_done_val) = loop {
                    // check to see how many rows are ready
                    // we wait until there are rows equal to buffer size or the query is done.
                    let rows_ready = rows_read.load(Ordering::SeqCst);

                    let stream_is_done_val = stream_is_done.load(Ordering::SeqCst);
                    if rows_ready >= buffer_size || stream_is_done_val {
                        pause_clone.swap(true, Ordering::SeqCst);
                        // TODO(maybe): verify the buffer has non-zero number of columns
                        break (rows_ready, stream_is_done_val);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                };
                if rows_ready == 0 && stream_is_done_val {
                    return Ok(None);
                }
                // take the existing row buffers replacing them with an empty vec so the reader task can get back to work immediately
                let old_buffer = {
                    let mut buffer_lock = buffer.lock().await;
                    rows_read.store(0, Ordering::SeqCst);
                    let old_buffer = std::mem::take(&mut *buffer_lock);
                    let _res = pause_clone.swap(false, Ordering::SeqCst);
                    old_buffer
                };
                let mut height: Option<usize> = None;
                let columns: Vec<PlColumn> = old_buffer
                    .into_iter()
                    .enumerate()
                    .map(|(i, avb)| {
                        let field = schema.get_at_index(i);
                        let name = match field {
                            Some((n, _)) => n,
                            None => &format!("col{i}").into(),
                        };
                        let s = avb.into_series();
                        let s_len = s.len();
                        match height {
                            Some(h) => match h == s_len {
                                true => {}
                                false => {
                                    return Err(format!(
                                        "column {name} has height {s_len}, previous columns has {h}"
                                    ));
                                }
                            },
                            None => height = Some(s_len),
                        };
                        let s = s.with_name(name.clone());
                        Ok(s.into_column())
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                let Some(height) = height else {
                    return Err(PyRuntimeError::new_err(
                        "height is 0 but buffer was non-zero".to_string(),
                    ));
                };

                let mut df = DataFrame::new(height, columns)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                if let Some(atomic_limit) = n_rows.as_ref() {
                    let n_rows_val = atomic_limit.load(Ordering::Relaxed);
                    if df.height() > n_rows_val {
                        df = df.slice(0, n_rows_val);
                        atomic_limit.store(0, Ordering::Relaxed);
                    } else {
                        atomic_limit.fetch_sub(df.height(), Ordering::Relaxed);
                    }
                    if atomic_limit.load(Ordering::SeqCst) <= 0 {
                        consumer_is_done.store(true, Ordering::SeqCst);
                    }
                }

                let df = PyDataFrame(df);

                Ok(Some(df))
            })
        });
        df
    }
}
#[pyclass]
struct SourceGenerator {
    with_columns: Option<Vec<String>>,
    predicate: Option<PyExpr>,
    n_rows: Arc<Option<AtomicUsize>>,
    batch_size: Option<i32>,
    presource: Py<PreSourceGenerator>,
}

#[pyclass]
struct PreSourceGenerator {
    schema: Arc<PlSchema>,
    buffer: Arc<Mutex<Vec<AnyValueBuffer<'static>>>>,
    buffer_size: usize,
    rows_read: Arc<AtomicUsize>,
    pause: Arc<AtomicBool>,
    stream_is_done: Arc<AtomicBool>,
    consumer_is_done: Arc<AtomicBool>,
}
#[pymethods]
impl PreSourceGenerator {
    fn __call__<'p>(
        slf: Py<Self>,
        with_columns: Option<Bound<'_, PySequence>>,
        predicate: Option<PyExpr>,
        n_rows: Option<i32>,
        batch_size: Option<i32>,
    ) -> PyResult<SourceGenerator> {
        let mut s_with_columns: Option<Vec<String>> = None;

        if let Some(v) = with_columns {
            let mut w_col = Vec::new();
            let v_len = v.len()?;
            for i in 0..v_len {
                let item = v.get_item(i)?;
                let vi = item.extract::<String>()?;
                w_col.push(vi);
            }
            s_with_columns = Some(w_col)
        };
        let s_n_rows = match n_rows {
            Some(n) => Arc::new(Some(AtomicUsize::new(n as usize))),
            None => Arc::new(None),
        };

        Ok(SourceGenerator {
            with_columns: s_with_columns,
            predicate,
            n_rows: s_n_rows,
            batch_size,
            presource: slf,
        })
    }
}

#[pyclass]
struct Connection {
    client: Arc<Client>,
    keep_alive: KeepAlive,
    active_stream: Arc<AtomicBool>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        match &self.keep_alive {
            KeepAlive::No => {}
            KeepAlive::Yes(keeping_alive) => keeping_alive.monitor_abort.abort(),
        }
    }
}
#[pymethods]
impl Connection {
    fn set_active_stream(&self, on_off: bool) {
        let active_stream = self.active_stream.clone();
        active_stream.store(on_off, Ordering::SeqCst);
    }
    #[pyo3(signature = (query, params=None))]
    fn scan_pq<'p>(
        &self,
        py: Python<'p>,
        query: String,
        params: Option<&Bound<'p, PyAny>>,
    ) -> Result<pyo3::Bound<'p, PyAny>, pyo3::PyErr> {
        let client = self.client.clone();
        let rust_params = py_seq_to_rust_params(params)?;
        let io_plugins = PyModule::import(py, "polars.io.plugins")?;
        let register_io_source = io_plugins.getattr("register_io_source")?;
        self.set_active_stream(true);
        let active_stream = self.active_stream.clone();
        let first_row_res = py.detach(|| {
            let rt = get_runtime();

            rt.block_on(async move {
                let rs = client
                    .query_raw(&query, rust_params.iter())
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                let mut rs_pin = Box::pin(rs);

                let first_row = rs_pin
                    .try_next()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                match first_row {
                    Some(row) => {
                        let (schema, avs) = first_row_to_avs(row)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                        Ok::<FirstRowRes, PyErr>(FirstRowRes::Exists((schema, avs, rs_pin)))
                    }
                    None => Ok(FirstRowRes::NoRows),
                }
            })
        })?;
        let FirstRowRes::Exists((schema, avs, mut rs_pin)) = first_row_res else {
            // if no rows returned, we don't know schema so return 0,0 lazyframe
            eprintln!("query returned 0 rows");
            active_stream.store(false, Ordering::SeqCst);
            let pl = PyModule::import(py, "polars")?;
            let df_constructor = pl.getattr("DataFrame")?;
            let df = df_constructor.call0()?;
            let df_to_lazy = df.getattr("lazy")?;
            return df_to_lazy.call0();
        };
        let avs = Arc::new(Mutex::new(avs));
        let schema_arc = Arc::new(schema);

        let avs_clone = avs.clone();
        let pause = Arc::new(AtomicBool::new(false));
        let pause_clone = pause.clone();
        let rows_read = Arc::new(AtomicUsize::new(1));
        let rows_read_clone = rows_read.clone();
        let buffer_size = 100;
        let buffer_size_clone = buffer_size.clone();

        let rt = get_runtime();
        let stream_is_done = Arc::new(AtomicBool::new(false));
        let stream_is_done_clone = stream_is_done.clone();
        let consumer_is_done = Arc::new(AtomicBool::new(false));
        let consumer_is_done_clone = consumer_is_done.clone();
        let active_stream = self.active_stream.clone();
        let _buffer_task = rt.spawn(async move {
            let result: Result<(), String> = async {
                let mut restart = false;
                while let Some(row) = rs_pin.try_next().await.map_err(|e| e.to_string())? {
                    let avs_clone1 = avs_clone.clone();
                    let rows_read_clone1 = rows_read_clone.clone();
                    let rows_read_clone2 = rows_read_clone.clone();
                    match restart {
                        true => {
                            let (_, mut avs) = first_row_to_avs(row)?;
                            let mut avs_guard = avs_clone1.lock().await;
                            std::mem::swap(&mut avs, &mut avs_guard);
                            rows_read_clone1.fetch_add(1, Ordering::SeqCst);
                            restart = false;
                        }
                        false => {
                            let mut avs_guard = avs_clone1.lock().await;
                            next_row_to_avs(row, &mut avs_guard, rows_read_clone1).await?;
                        }
                    }
                    let consumer_done = consumer_is_done_clone.load(Ordering::SeqCst);
                    if consumer_done {
                        break;
                    }

                    loop {
                        let need_pause = pause_clone.load(Ordering::Relaxed);
                        let rows_read = rows_read_clone2.load(Ordering::SeqCst);

                        if need_pause || rows_read >= buffer_size_clone {
                            if !restart {
                                restart = true;
                            }
                            tokio::time::sleep(Duration::from_millis(99)).await;
                        } else {
                            break;
                        }
                    }
                }
                Ok(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("buffer task error: {:?}", e);
            };
            stream_is_done_clone.store(true, Ordering::SeqCst);
            active_stream.store(false, Ordering::SeqCst);
        });
        let generator = PreSourceGenerator {
            schema: schema_arc.clone(),
            buffer: avs.clone(),
            buffer_size: 100,
            rows_read,
            pause,
            stream_is_done,
            consumer_is_done,
        };

        let pyschema = PySchema(schema_arc);

        let args = PyTuple::new(py, vec![generator])?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", pyschema)?;

        register_io_source.call(args, Some(&kwargs))
    }
    fn fetchall<'p>(
        &self,
        py: Python<'p>,
        query: String,
        params: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let client = self.client.clone();
        fetchall(py, client, self.keep_alive.get_notifier(), query, params)
    }
    fn check_connection<'p>(&self, py: Python<'p>) -> bool {
        let client = self.client.clone();
        let rt = get_runtime();
        let res = py.detach(|| {
            rt.block_on(async move {
                let timeout_res = timeout(Duration::from_secs(1), client.check_connection()).await;
                match timeout_res {
                    Ok(client_res) => match client_res {
                        Ok(_) => true,
                        Err(_) => false,
                    },
                    Err(_) => false,
                }
            })
        });
        res
    }
    fn __aenter__<'p>(slf: PyRef<'p, Self>, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let slf_obj: Py<Self> = slf.into();

        future_into_py(py, async move { Ok(slf_obj) })
    }

    fn __aexit__<'p>(
        &self,
        py: Python<'p>,
        _exc_type: Option<&Bound<'p, PyAny>>,
        _exc_value: Option<&Bound<'p, PyAny>>,
        _traceback: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        future_into_py(py, async move { Ok(()) })
    }
}
async fn _async_conn(dsn: String, keep_alive_seconds: Option<u64>) -> Result<Connection, String> {
    let mut tls_builder = TlsConnector::builder();
    tls_builder.danger_accept_invalid_certs(true);

    let connector = tls_builder.build().map_err(|e| e.to_string())?;

    let connector = MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(&dsn, connector)
        .await
        .map_err(|e| e.to_string())?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let client = Arc::new(client);
    let active_stream = Arc::new(AtomicBool::new(false));
    let keep_alive = KeepAlive::new(keep_alive_seconds, active_stream.clone(), client.clone());

    Ok(Connection {
        client,
        active_stream,
        keep_alive,
    })
}
#[pymodule]
fn pg_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfunction]
    #[pyo3(signature = (dsn, keep_alive_seconds=None))]
    fn async_connect(
        py: Python<'_>,
        dsn: String,
        keep_alive_seconds: Option<u64>,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            _async_conn(dsn, keep_alive_seconds)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("TLS Init Error: {}", e)))
        })
    }
    #[pyfunction]
    #[pyo3(signature = (dsn, keep_alive_seconds=None))]
    fn connect(
        py: Python<'_>,
        dsn: String,
        keep_alive_seconds: Option<u64>,
    ) -> PyResult<Connection> {
        let res = py
            .detach(|| {
                let rt = get_runtime();
                rt.block_on(async move { _async_conn(dsn, keep_alive_seconds).await })
            })
            .map_err(|e| PyRuntimeError::new_err(format!("TLS Init Error: {}", e)))?;
        Ok(res)
    }
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(async_connect, m)?)?;
    m.add_class::<Connection>()?;

    Ok(())
}

enum KeepAlive {
    Yes(KeepingAlive),
    No,
}

impl KeepAlive {
    fn new(
        keep_alive_seconds: Option<u64>,
        active_stream: Arc<AtomicBool>,
        client: Arc<Client>,
    ) -> KeepAlive {
        match keep_alive_seconds {
            None => KeepAlive::No,
            Some(keep_alive_seconds) => {
                let notify = Arc::new(Notify::new());
                let notify_clone = notify.clone();
                let monitor_task = tokio::spawn(async move {
                    loop {
                        // Wait seconds OR until notified
                        let sleep_timer = timeout(
                            Duration::from_secs(keep_alive_seconds),
                            notify_clone.notified(),
                        );

                        match sleep_timer.await {
                            Ok(_) => {
                                // We received a notification, reset loop
                                continue;
                            }
                            Err(_) => {
                                // Timeout elapsed (30s passed with no notification)
                                if client.is_closed() {
                                    break;
                                }
                                let active_stream = active_stream.load(Ordering::SeqCst);
                                // can't query postgres while active stream is going
                                if !active_stream {
                                    let _ = client.simple_query("SELECT 1").await;
                                }
                            }
                        }
                    }
                });

                let ka = KeepingAlive {
                    notify,
                    monitor_abort: monitor_task.abort_handle(),
                };
                KeepAlive::Yes(ka)
            }
        }
    }
    fn get_notifier(&self) -> Box<dyn Fn() + Send> {
        match self {
            KeepAlive::Yes(ka) => {
                let notifier = ka.notify.clone();
                Box::new(move || {
                    notifier.notify_one();
                    ()
                })
            }
            KeepAlive::No => Box::new(|| ()),
        }
    }
}
struct KeepingAlive {
    notify: Arc<Notify>,
    monitor_abort: AbortHandle,
}

enum FirstRowRes {
    Exists((PlSchema, Vec<AnyValueBuffer<'static>>, Pin<Box<RowStream>>)),
    NoRows,
}
