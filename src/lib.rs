
mod builder;
mod connection;
mod db;
mod errors;
mod metadata;

use connection::SpannerConnection;
use db::Db;
use errors::DbError;

pub fn connect(host: String, dbname: String) -> Result<SpannerConnection, DbError> {
    let db = Db::new(host);
    db.connect(dbname)
}

#[test]
fn test_connect() {
    connect("asdf".to_string(), "asdf".to_string()).unwrap();
}