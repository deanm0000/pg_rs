use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

use serde_json::Value as JsonValue;

use tokio_postgres::types::{ToSql, Type};

use uuid::Uuid;
#[derive(Debug, Clone)]
pub enum QueryParam {
    Null,
    Bool(bool),

    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    DateTimeTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(JsonValue),
}

impl ToSql for QueryParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            QueryParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            QueryParam::Bool(v) => v.to_sql(ty, out),
            QueryParam::Int(v) => match *ty {
                Type::INT8 => v.to_sql(ty, out),
                Type::INT4 => (*v as i32).to_sql(ty, out),
                Type::INT2 => (*v as i16).to_sql(ty, out),
                _ => v.to_sql(ty, out),
            },
            QueryParam::Float(v) => match *ty {
                Type::FLOAT8 => v.to_sql(ty, out),
                Type::FLOAT4 => (*v as f32).to_sql(ty, out),
                _ => v.to_sql(ty, out),
            },
            QueryParam::String(v) => match *ty {
                Type::UUID => {
                    if let Ok(u) = Uuid::parse_str(v) {
                        u.to_sql(ty, out)
                    } else {
                        v.to_sql(ty, out)
                    }
                }

                Type::JSON | Type::JSONB => v.to_sql(ty, out),

                _ => v.to_sql(ty, out),
            },
            QueryParam::Bytes(v) => v.to_sql(ty, out),
            QueryParam::Date(v) => v.to_sql(ty, out),
            QueryParam::DateTime(v) => match *ty {
                Type::TIMESTAMP => v.to_sql(ty, out),

                Type::TIMESTAMPTZ => {
                    let dt_utc = &Utc.from_utc_datetime(v);
                    dt_utc.to_sql(ty, out)
                }
                _ => v.to_sql(ty, out),
            },
            QueryParam::DateTimeTz(v) => v.to_sql(ty, out),
            QueryParam::Uuid(v) => v.to_sql(ty, out),
            QueryParam::Json(v) => tokio_postgres::types::Json(v).to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}
