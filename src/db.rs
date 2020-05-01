

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::{convert::TryInto, sync::Arc};

use futures::compat::{Compat01As03, Stream01CompatExt};
use futures::stream::{StreamExt, StreamFuture};

use googleapis_raw::spanner::v1::{
    result_set::{PartialResultSet, ResultSetMetadata, ResultSetStats},
    spanner::{BeginTransactionRequest, CommitRequest, CreateSessionRequest, ExecuteSqlRequest, GetSessionRequest, RollbackRequest, Session},
    spanner_grpc::SpannerClient,
    transaction::{
        TransactionOptions, TransactionOptions_ReadOnly, TransactionOptions_ReadWrite, TransactionSelector,
    },
    type_pb::{StructType_Field, Type, TypeCode},
};

use protobuf::{
    well_known_types::{ListValue, NullValue, Struct, Value},
    RepeatedField,
};

use grpcio::{
    CallOption, ChannelBuilder, ChannelCredentials, ClientSStreamReceiver, Environment, MetadataBuilder,
};

use crate::connection::SpannerConnection;
use crate::errors::DbError;
use crate::metadata::SpannerMetadata;

const SPANNER_ADDRESS: &str = "asdf";

pub struct Db {
    name: String
}

impl Db {
    fn create_session(&self, client: &SpannerClient) -> Result<Session, grpcio::Error> {
        let mut req = CreateSessionRequest::new();
        req.database = self.name.to_owned();
        let mut meta = MetadataBuilder::new();
        meta.add_str("google-cloud-resource-prefix", &self.name)?;
        meta.add_str("x-goog-api-client", "gcp-grpc-rs")?;
        let opt = CallOption::default().headers(meta.build());
        client.create_session_opt(&req, opt)
    }
    
    fn connect(&self, address: String) -> Result<SpannerConnection, DbError> {
        let creds = ChannelCredentials::google_default_credentials()?;
    
        let arc = Arc::new(Environment::new(32));
        // Create a Spanner client.
        let chan = ChannelBuilder::new(arc)
            .max_send_message_len(100 << 20)
            .max_receive_message_len(100 << 20)
            .secure_connect(SPANNER_ADDRESS, creds);
        let client = SpannerClient::new(chan);
    
        // Connect to the instance and create a Spanner session.
        let session = self.create_session(&client)?;
    
        Ok(SpannerConnection {
            client,
            session,
            metadata: SpannerMetadata {
                in_write_transaction: false,
                transaction: None,
                execute_sql_count: 0,
            }
        })
    }
    
    fn new(name: String) -> Self {
        Db {
            name
        }
    }
}
