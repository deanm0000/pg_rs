use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres_types::{FromSql, Kind};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySequence, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;

use serde_json::Value as JsonValue;

use std::sync::Arc;

use crate::KeepAlive;
use crate::db_utils::QueryParam;

use tokio::sync::Notify;

use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Row};
use uuid::Uuid;

pub fn json_to_py(py: Python, val: JsonValue) -> PyResult<Py<PyAny>> {
    match val {
        JsonValue::Null => Ok(py.None()),

        // Use .into_py_any(py) for simple scalars
        JsonValue::Bool(b) => Ok(b.into_py_any(py)?),

        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py_any(py)?)
            } else {
                Ok(n.to_string().into_py_any(py)?)
            }
        }

        JsonValue::String(s) => Ok(s.into_py_any(py)?),

        JsonValue::Array(arr) => {
            // PyO3 0.23: "empty_bound" -> "empty"
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            // Convert Bound<'py, PyList> -> Py<PyAny>
            Ok(list.into_any().unbind())
        }

        JsonValue::Object(map) => {
            // PyO3 0.23: "new_bound" -> "new"
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            // Convert Bound<'py, PyDict> -> Py<PyAny>
            Ok(dict.into_any().unbind())
        }
    }
}
pub fn py_to_json(obj: pyo3::Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if obj.is_none() {
        return Ok(JsonValue::Null);
    }

    if let Ok(b) = obj.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }

    if let Ok(s) = obj.extract::<String>() {
        return Ok(JsonValue::String(s.to_string()));
    }

    if let Ok(i) = obj.extract::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(i)));
    }

    if let Ok(f) = obj.extract::<f64>() {
        // serde_json::Number::from_f64 returns Option (None if infinite/NaN)
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        } else {
            return Ok(JsonValue::Null);
        }
    }

    if let Ok(list) = obj.extract::<pyo3::Bound<'_, PySequence>>() {
        // Distinguish strings/bytes from generic sequences if necessary,
        // but PyString is caught above.
        let mut arr = Vec::new();
        for i in 0..list.len()? {
            let item = list.get_item(i)?;
            arr.push(py_to_json(item)?);
        }
        return Ok(JsonValue::Array(arr));
    }

    if let Ok(dict) = obj.extract::<pyo3::Bound<'_, PyDict>>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict {
            let key = k.extract::<String>()?; // JSON keys must be strings
            let val = py_to_json(v)?;
            map.insert(key, val);
        }
        return Ok(JsonValue::Object(map));
    }

    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "Cannot convert object to JSON: {}",
        obj
    )))
}

pub fn extract_py_value(obj: pyo3::Bound<'_, PyAny>) -> PyResult<QueryParam> {
    if obj.is_none() {
        return Ok(QueryParam::Null);
    }
    if obj.is_instance_of::<PyDict>() {
        let j = py_to_json(obj)?;
        return Ok(QueryParam::Json(j));
    }
    if obj.is_instance_of::<PyList>() {
        let j = py_to_json(obj)?;
        return Ok(QueryParam::Json(j));
    }
    if let Ok(dt) = obj.extract::<DateTime<Utc>>() {
        return Ok(QueryParam::DateTimeTz(dt));
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(QueryParam::Bool(b));
    }

    if let Ok(i) = obj.extract::<i64>() {
        return Ok(QueryParam::Int(i));
    }

    if let Ok(f) = obj.extract::<f64>() {
        return Ok(QueryParam::Float(f));
    }

    if let Ok(dt) = obj.extract::<NaiveDateTime>() {
        return Ok(QueryParam::DateTime(dt));
    }

    if let Ok(d) = obj.extract::<NaiveDate>() {
        return Ok(QueryParam::Date(d));
    }

    if let Ok(b) = obj.extract::<Vec<u8>>() {
        return Ok(QueryParam::Bytes(b));
    }

    if let Ok(hex_attr) = obj.getattr("hex") {
        if let Ok(hex_str) = hex_attr.extract::<String>() {
            if let Ok(u) = Uuid::parse_str(&hex_str) {
                return Ok(QueryParam::Uuid(u));
            }
        }
    }
    if let Ok(b) = obj.extract::<Vec<u8>>() {
        return Ok(QueryParam::Bytes(b));
    }
    let s: String = obj.extract()?;
    Ok(QueryParam::String(s))
}

pub fn py_seq_to_rust_params(params: Option<&pyo3::Bound<'_, PyAny>>) -> PyResult<Vec<QueryParam>> {
    let mut rust_params = Vec::new();
    if let Some(p_any) = params {
        if let Ok(seq) = p_any.extract::<Bound<'_, PySequence>>() {
            for i in 0..seq.len()? {
                let item = seq.get_item(i)?;
                rust_params.push(extract_py_value(item)?);
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Parameters must be a sequence",
            ));
        }
    }
    Ok(rust_params)
}

struct PgEnum(String);

impl<'a> FromSql<'a> for PgEnum {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // Enums are sent as simple UTF-8 strings
        let s = std::str::from_utf8(raw)?;
        Ok(PgEnum(s.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        // Crucial: We accept ANY type that Postgres declares as an Enum
        matches!(ty.kind(), Kind::Enum(_))
    }
}

#[derive(Debug)]
pub enum PgCell {
    Null,
    Bool(bool),
    Int(i32),
    BigInt(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    DateTimeTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(JsonValue),
}
pub fn row_to_rust(row: &Row) -> Result<Vec<PgCell>, String> {
    let mut result = Vec::with_capacity(row.len());

    for (i, col) in row.columns().iter().enumerate() {
        let cell = match col.type_() {
            &Type::BOOL => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Bool(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::INT2 | &Type::INT4 => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Int(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::INT8 => match row.try_get(i) {
                Ok(Some(v)) => PgCell::BigInt(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::FLOAT4 | &Type::FLOAT8 => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Float(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::VARCHAR | &Type::TEXT | &Type::NAME => match row.try_get(i) {
                Ok(Some(v)) => PgCell::String(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::BYTEA => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Bytes(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::DATE => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Date(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::TIMESTAMP => match row.try_get(i) {
                Ok(Some(v)) => PgCell::DateTime(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::TIMESTAMPTZ => match row.try_get(i) {
                Ok(Some(v)) => PgCell::DateTimeTz(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::UUID => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Uuid(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            &Type::JSON | &Type::JSONB => match row.try_get(i) {
                Ok(Some(v)) => PgCell::Json(v),
                Ok(None) => PgCell::Null,
                Err(e) => return Err(e.to_string()),
            },
            _ => {
                // 1. Check if it is an Enum
                if let Kind::Enum(_) = col.type_().kind() {
                    // Use our custom PgEnum wrapper
                    match row.try_get::<_, Option<PgEnum>>(i) {
                        Ok(Some(e)) => PgCell::String(e.0), // Extract inner string
                        Ok(None) => PgCell::Null,
                        Err(e) => return Err(format!("Enum Error: {}", e)),
                    }
                }
                // 2. Try generic String (Text, Unknowns)
                else {
                    match row.try_get::<_, Option<String>>(i) {
                        Ok(Some(s)) => PgCell::String(s),
                        Ok(None) => PgCell::Null,
                        Err(_e) => {
                            // 3. Last Resort: Return error string instead of crashing
                            // This handles Arrays, Numeric, Money, etc. gracefully
                            let type_name = col.type_().name();
                            PgCell::String(format!("<UnmappedType:{}>", type_name))
                        }
                    }
                }
            }
        };
        result.push(cell);
    }
    Ok(result)
}
pub fn rust_row_to_py(py: Python, row: Vec<PgCell>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::with_capacity(row.len());
    for cell in row {
        let obj = match cell {
            PgCell::Null => py.None(),
            PgCell::Bool(v) => v.into_py_any(py)?,
            PgCell::Int(v) => v.into_py_any(py)?,
            PgCell::BigInt(v) => v.into_py_any(py)?,
            PgCell::Float(v) => v.into_py_any(py)?,
            PgCell::String(v) => v.into_py_any(py)?,
            PgCell::Bytes(v) => v.into_py_any(py)?,
            PgCell::Date(v) => v.into_py_any(py)?,
            PgCell::DateTime(v) => v.into_py_any(py)?,
            PgCell::DateTimeTz(v) => v.into_py_any(py)?,
            PgCell::Json(v) => json_to_py(py, v)?, // Reuse your existing helper
            PgCell::Uuid(v) => {
                let uuid_mod = PyModule::import(py, "uuid")?;
                uuid_mod
                    .call_method1("UUID", (v.to_string(),))?
                    .into_py_any(py)?
            }
        };
        result.push(obj);
    }
    Ok(result)
}

pub fn fetchall<'p>(
    py: Python<'p>,
    client: Arc<Client>,
    notifier: Box<dyn Fn() + Send>,
    query: String,
    params: Option<&Bound<'p, PyAny>>,
) -> PyResult<Bound<'p, PyAny>> {
    let rust_params = py_seq_to_rust_params(params.map(|x| x.as_any()))?;

    future_into_py(py, async move {
        let param_refs: Vec<&(dyn ToSql + Sync)> = rust_params
            .iter()
            .map(|x| x as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(&query, &param_refs[..])
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        notifier();

        let parsed_rows: Result<Vec<Vec<PgCell>>, String> =
            rows.iter().map(|r| row_to_rust(r)).collect();

        let rust_data = parsed_rows.map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Row Parse Error: {}", e))
        })?;

        let py_result = Python::attach(|py| {
            let result = PyList::empty(py);

            for r_row in rust_data {
                let py_objs = rust_row_to_py(py, r_row)?;
                let tuple = PyTuple::new(py, py_objs)?;
                result.append(tuple)?;
            }

            Ok::<Py<PyList>, PyErr>(result.unbind())
        })?;

        Ok(py_result)
    })
}
