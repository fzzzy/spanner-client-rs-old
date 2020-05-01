
#[derive(Debug)]
pub struct DbError {
    message: String,
}

impl DbError {
    pub fn new(message: String) -> Self {
        DbError {
            message
        }
    }
}

impl From<grpcio::Error> for DbError {
    fn from(inner: grpcio::Error) -> Self {
        DbError::new(format!("grpcio error {:?}", inner))
    }
}
