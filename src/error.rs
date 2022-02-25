#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("Bad config: {0}")]
    BadConfig(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

impl std::convert::From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => Error::NotFound("row not found".into()),
            _ => Error::Internal(err.to_string()),
        }
    }
}
