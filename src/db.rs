
use std::cell::RefCell;
use std::sync::Arc;

use googleapis_raw::spanner::v1::{
    spanner::{CreateSessionRequest, Session},
    spanner_grpc::SpannerClient,
};

use grpcio::{
    CallOption, ChannelBuilder, ChannelCredentials, Environment, MetadataBuilder,
};

use crate::connection::SpannerConnection;
use crate::errors::DbError;
use crate::metadata::SpannerMetadata;

pub struct Db {
    address: String
}

impl Db {
    fn create_session(&self, client: &SpannerClient, dbname: String) -> Result<Session, grpcio::Error> {
        let mut req = CreateSessionRequest::new();
        req.database = dbname.to_owned();
        let mut meta = MetadataBuilder::new();
        meta.add_str("google-cloud-resource-prefix", &dbname)?;
        meta.add_str("x-goog-api-client", "gcp-grpc-rs")?;
        let opt = CallOption::default().headers(meta.build());
        client.create_session_opt(&req, opt)
    }
    
    pub fn connect(&self, dbname: String) -> Result<SpannerConnection, DbError> {
        let creds = ChannelCredentials::google_default_credentials()?;
    
        let arc = Arc::new(Environment::new(32));
        // Create a Spanner client.
        let chan = ChannelBuilder::new(arc)
            .max_send_message_len(100 << 20)
            .max_receive_message_len(100 << 20)
            .secure_connect(&self.address, creds);
        let client = SpannerClient::new(chan);
    
        // Connect to the instance and create a Spanner session.
        let session = self.create_session(&client, dbname)?;
    
        Ok(SpannerConnection {
            client,
            session,
            metadata: RefCell::new(SpannerMetadata {
                in_write_transaction: false,
                transaction: None,
                execute_sql_count: 0,
            })
        })
    }
    
    pub fn new(address: String) -> Self {
        Db {
            address
        }
    }
}
