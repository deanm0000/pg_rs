use std::sync::Arc;

use chrono::{Timelike, Utc};
use polars::datatypes::DataType as PlDT;
use polars::frame::row::AnyValueBuffer;
use polars::prelude::{
    AnyValue, CategoricalMapping, Field as PlField, FrozenCategories, PlSmallStr,
    Schema as PlSchema, TimeUnit, TimeZone as PlTimeZone,
};
use postgres_protocol::types::{
    bool_from_sql, bytea_from_sql, char_from_sql, float4_from_sql, float8_from_sql, int2_from_sql,
    int4_from_sql, int8_from_sql,
};
use postgres_types::{FromSql, Kind, Type};
use tokio_postgres::Row;
use tokio_postgres::types::Field as PGField;

pub struct AVWrap<'a>(AnyValue<'a>);
const TIME_UNIT: TimeUnit = TimeUnit::Microseconds;
fn date_i32_from_sql<'a>(raw: &'a [u8]) -> Result<i32, Box<dyn std::error::Error + Sync + Send>> {
    let bytes: [u8; 4] = raw.try_into()?;
    let pg_days = i32::from_be_bytes(bytes);
    let pg_epoch_offset_days = 10_957;
    Ok(pg_days + pg_epoch_offset_days)
}
fn json_from_sql(
    ty: &Type,
    raw: &[u8],
) -> Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if *ty == Type::JSONB {
        // Postgres JSONB binary format starts with a version byte (1)
        if raw.is_empty() {
            return Err("Empty JSONB buffer".into());
        }
        if raw[0] != 1 {
            return Err("Unsupported JSONB version".into());
        }
        Ok(std::str::from_utf8(&raw[1..])?.to_string())
    } else {
        // Type::JSON is just raw UTF-8
        Ok(std::str::from_utf8(raw)?.to_string())
    }
}
fn datetime_i64_from_sql<'a>(
    raw: &'a [u8],
) -> Result<i64, Box<dyn std::error::Error + Sync + Send>> {
    let bytes: [u8; 8] = raw.try_into()?;
    let pg_micros = i64::from_be_bytes(bytes);

    // Seconds between 1970-01-01 and 2000-01-01: 946,684,800
    const SECONDS_DIFF: i64 = 946_684_800;
    const MICROS_PER_SECOND: i64 = 1_000_000;

    let unix_micros = pg_micros + (SECONDS_DIFF * MICROS_PER_SECOND);

    Ok(unix_micros)
}

impl<'a> FromSql<'a> for AVWrap<'static> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty {
            &Type::BOOL => bool_from_sql(raw)
                .and_then(|x| Ok(AVWrap(AnyValue::Boolean(x))))
                .or_else(|_| Ok(AVWrap(AnyValue::Null))),
            &Type::INT2 => {
                let b = int2_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Int16(b)))
            }
            &Type::INT4 => {
                let b = int4_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Int32(b)))
            }
            &Type::INT8 => {
                let b = int8_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Int64(b)))
            }
            &Type::FLOAT4 => {
                let b = float4_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Float32(b)))
            }
            &Type::FLOAT8 => {
                let b = float8_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Float64(b)))
            }
            &Type::VARCHAR | &Type::TEXT | &Type::NAME => {
                let b = <&str as FromSql>::from_sql(ty, raw).map(ToString::to_string)?;
                Ok(AVWrap(AnyValue::StringOwned(b.into())))
            }
            &Type::BYTEA => {
                let b = bytea_from_sql(raw);
                Ok(AVWrap(AnyValue::BinaryOwned(b.to_vec())))
            }
            &Type::DATE => {
                let b = date_i32_from_sql(raw)?;
                Ok(AVWrap(AnyValue::Date(b)))
            }
            &Type::TIMESTAMP => {
                let b = datetime_i64_from_sql(raw)?;
                Ok(AVWrap(AnyValue::DatetimeOwned(b, TIME_UNIT, None)))
            }
            &Type::TIMESTAMPTZ => {
                let b = datetime_i64_from_sql(raw)?;
                Ok(AVWrap(AnyValue::DatetimeOwned(
                    b,
                    TIME_UNIT,
                    Some(Arc::new(PlTimeZone::UTC)),
                )))
            }
            &Type::UUID => Ok(AVWrap(AnyValue::BinaryOwned(raw.to_vec()))),
            &Type::JSON | &Type::JSONB => {
                let s = json_from_sql(ty, raw)?;
                Ok(AVWrap(AnyValue::StringOwned(s.into())))
            }

            _ => {
                if let Kind::Enum(_) = ty.kind() {
                    // let catmap = CategoricalMapping::new(&vars.len());
                    // TODO make this return actual Enum type
                    let s = <&str as FromSql>::from_sql(ty, raw).map(ToString::to_string)?;
                    return Ok(AVWrap(AnyValue::StringOwned(s.into())));
                }

                todo!("Type {} not implemented", ty)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        true
    }
    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(AVWrap(AnyValue::Null))
    }
}

impl<'a> From<AVWrap<'a>> for AnyValue<'a> {
    fn from(value: AVWrap<'a>) -> Self {
        value.0
    }
}
