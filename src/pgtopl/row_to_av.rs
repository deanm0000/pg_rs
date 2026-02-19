use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::pgtopl::from_sql::AVWrap;
use chrono::{Timelike, Utc};
use polars::datatypes::DataType as PlDT;
use polars::frame::row::AnyValueBuffer;
use polars::prelude::{
    AnyValue, Field as PlField, FrozenCategories, PlSmallStr, Schema as PlSchema, TimeUnit,
    TimeZone as PlTimeZone,
};
use postgres_types::{Kind, Type};
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio_postgres::Row;
use tokio_postgres::types::Field as PGField;
const TIME_UNIT: TimeUnit = TimeUnit::Microseconds;
pub fn first_row_to_avs(row: Row) -> Result<(PlSchema, Vec<AnyValueBuffer<'static>>), String> {
    let num_columns = row.columns().len();
    let mut schema = PlSchema::with_capacity(num_columns);
    let mut buffers = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        let col = &row.columns()[i];
        let name = col.name();
        let ty = col.type_();
        let pl_dt = get_dtype_from_name_type(name, ty)?;
        schema.insert(name.into(), pl_dt.clone());
        let mut avb = AnyValueBuffer::new(&pl_dt, 100);
        let wrap: AVWrap = row.try_get(i).map_err(|e| e.to_string())?;
        avb.add(wrap.into());
        buffers.push(avb);
    }

    Ok((schema, buffers))
}

pub async fn next_row_to_avs(
    row: Row,
    avs: &mut Vec<AnyValueBuffer<'static>>,
    rows_read: Arc<AtomicUsize>,
) -> Result<(), String> {
    let num_columns = row.columns().len();

    rows_read.fetch_add(1, Ordering::SeqCst);
    for i in 0..num_columns {
        match avs.get_mut(i) {
            Some(avb) => {
                let wrap: AVWrap = row.try_get(i).map_err(|e| e.to_string())?;
                avb.add(wrap.into());
            }
            None => {
                dbg!(row, avs.len());
                return Err("imbalanced columns and buffers".to_string());
            }
        }
    }

    Ok(())
}

pub fn get_dtype_from_name_type(name: &str, type_: &Type) -> Result<PlDT, String> {
    match type_.kind() {
        Kind::Simple => get_inner_type(name, type_),
        Kind::Array(_type_) => todo!("no support for Postgres Arrays yet"),
        Kind::Enum(strings) => Ok(PlDT::String), //Ok(get_enum_type(strings)),
        Kind::Composite(fields) => get_struct_type(fields),
        _ => {
            let msg = format!("unsupported kind {:?} for col {}", type_.kind(), name);
            return Err(msg);
        }
    }
}

fn get_enum_type(strings: &Vec<String>) -> PlDT {
    // https://github.com/pola-rs/polars/blob/efe029e435818b83f3eb7e8d6dac9631f1f86d12/crates/polars-core/src/scalar/new.rs#L74
    let strs = strings.into_iter().map(|s| s.as_str());
    let categories = FrozenCategories::new(strs).unwrap();
    let mappings = categories.mapping();
    PlDT::Enum(categories.clone(), mappings.clone())
}

fn get_struct_type(fields: &Vec<PGField>) -> Result<PlDT, String> {
    let pl_fields: Vec<PlField> = fields
        .iter()
        .map(|field| {
            let name = field.name();
            let pl_dt = get_dtype_from_name_type(name, field.type_())?;
            Ok(PlField::new(name.into(), pl_dt))
        })
        .collect::<Result<Vec<PlField>, String>>()?;

    Ok(PlDT::Struct(pl_fields))
}
fn get_inner_type(name: &str, type_: &Type) -> Result<PlDT, String> {
    match type_ {
        &Type::BOOL => Ok(PlDT::Boolean),
        &Type::BYTEA => Ok(PlDT::Binary),
        &Type::CHAR | &Type::NAME | &Type::BPCHAR | &Type::TEXT | &Type::VARCHAR => {
            Ok(PlDT::String)
        }
        &Type::INT8 => Ok(PlDT::Int64),
        &Type::INT2 => Ok(PlDT::Int16),
        &Type::INT4 => Ok(PlDT::Int32),
        &Type::FLOAT4 => Ok(PlDT::Float32),
        &Type::FLOAT8 => Ok(PlDT::Float64),
        &Type::DATE => Ok(PlDT::Date),
        &Type::TIME => Ok(PlDT::Time),
        &Type::TIMESTAMP => Ok(PlDT::Datetime(TIME_UNIT, None)),
        &Type::TIMESTAMPTZ => Ok(PlDT::Datetime(TIME_UNIT, Some(PlTimeZone::UTC))),
        &Type::UUID => Ok(PlDT::Binary),
        _ => {
            let msg = format!(
                "don't know that one {} {} {} {:?}",
                name,
                type_.name(),
                type_.oid(),
                type_.kind()
            );
            Err(msg)
        }
    }
}
