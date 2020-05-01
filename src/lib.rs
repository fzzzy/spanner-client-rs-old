
mod errors;

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

use crate::errors::DbError;

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

pub struct SpannerMetadata {
    in_write_transaction: bool,
    transaction: Option<TransactionSelector>,
    execute_sql_count: u64,
}

pub struct SpannerConnection {
    pub client: SpannerClient,
    pub metadata: RefCell<SpannerMetadata>,
    pub session: Session,
}

impl SpannerConnection {
    pub async fn sql(&self, sql: &str) -> Result<ExecuteSqlRequestBuilder, DbError> {
        Ok(ExecuteSqlRequestBuilder::new(self.sql_request(sql).await?))    
    }

    async fn begin_async(&self, for_write: bool) -> Result<(), DbError> {
        let mut options = TransactionOptions::new();
        if for_write {
            options.set_read_write(TransactionOptions_ReadWrite::new());
            self.metadata.borrow_mut().in_write_transaction = true;
        } else {
            options.set_read_only(TransactionOptions_ReadOnly::new());
        }
        let mut req = BeginTransactionRequest::new();
        req.set_session(self.session.get_name().to_owned());
        req.set_options(options);
        let mut transaction = self
            .client
            .begin_transaction_async(&req)?
            .compat()
            .await?;

        let mut ts = TransactionSelector::new();
        ts.set_id(transaction.take_id());
        self.metadata.borrow_mut().transaction = Some(ts);
        Ok(())
    }

    /// Return the current transaction metadata (TransactionSelector) if one is active.
    async fn get_transaction_async(&self) -> Result<Option<TransactionSelector>, DbError> {
        Ok(if self.metadata.borrow().transaction.is_some() {
            self.metadata.borrow().transaction.clone()
        } else {
            self.begin_async(true).await?;
            self.metadata.borrow().transaction.clone()
        })
    }

    async fn sql_request(&self, sql: &str) -> Result<ExecuteSqlRequest, DbError> {
        let mut sqlr = ExecuteSqlRequest::new();
        sqlr.set_sql(sql.to_owned());
        if let Some(transaction) = self.get_transaction_async().await? {
            sqlr.set_transaction(transaction);
            let mut session = self.metadata.borrow_mut();
            sqlr.seqno = session
                .execute_sql_count
                .try_into()
                .map_err(|_| DbError::new("seqno overflow".to_string()))?;
            session.execute_sql_count += 1;
        }
        Ok(sqlr)
    }

    pub fn commit(&self) {
    }

    pub fn rollback(&self) {
    }
}

pub type Conn = SpannerConnection;

pub fn as_value(string_value: String) -> Value {
    let mut value = Value::new();
    value.set_string_value(string_value);
    value
}

pub fn as_type(v: TypeCode) -> Type {
    let mut t = Type::new();
    t.set_code(v);
    t
}

pub fn struct_type_field(name: &str, field_type: TypeCode) -> StructType_Field {
    let mut field = StructType_Field::new();
    field.set_name(name.to_owned());
    field.set_field_type(as_type(field_type));
    field
}

pub fn as_list_value(
    string_values: impl Iterator<Item = String>,
) -> protobuf::well_known_types::Value {
    let mut list = ListValue::new();
    list.set_values(RepeatedField::from_vec(
        string_values.map(as_value).collect(),
    ));
    let mut value = Value::new();
    value.set_list_value(list);
    value
}

pub fn null_value() -> Value {
    let mut value = Value::new();
    value.set_null_value(NullValue::NULL_VALUE);
    value
}

#[derive(Default)]
pub struct ExecuteSqlRequestBuilder {
    execute_sql: ExecuteSqlRequest,
    params: Option<HashMap<String, Value>>,
    param_types: Option<HashMap<String, Type>>,
}

impl ExecuteSqlRequestBuilder {
    pub fn new(execute_sql: ExecuteSqlRequest) -> Self {
        ExecuteSqlRequestBuilder {
            execute_sql,
            ..Default::default()
        }
    }

    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params = Some(params);
        self
    }

    pub fn param_types(mut self, param_types: HashMap<String, Type>) -> Self {
        self.param_types = Some(param_types);
        self
    }

    fn prepare_request(self, conn: &Conn) -> ExecuteSqlRequest {
        let mut request = self.execute_sql;
        request.set_session(conn.session.get_name().to_owned());
        if let Some(params) = self.params {
            let mut paramss = Struct::new();
            paramss.set_fields(params);
            request.set_params(paramss);
        }
        if let Some(param_types) = self.param_types {
            request.set_param_types(param_types);
        }
        request
    }

    /// Execute a SQL read statement but return a non-blocking streaming result
    pub async fn execute(self, conn: &Conn) -> Result<StreamedResultSetAsync, DbError> {
        let stream = conn
            .client
            .execute_streaming_sql(&self.prepare_request(conn))?;
        Ok(StreamedResultSetAsync::new(stream))
    }

    // Execute a DML statement, returning the exact count of modified rows
    // pub async fn execute_dml_async(self, conn: &Conn) -> Result<i64, DbError> {
    //     let rs = conn
    //         .client
    //         .execute_sql_async(&self.prepare_request(conn))?
    //         .compat()
    //         .await?;
    //     Ok(rs.get_stats().get_row_count_exact())
    // }
}

pub struct StreamedResultSetAsync {
    /// Stream from execute_streaming_sql
    stream: Option<StreamFuture<Compat01As03<ClientSStreamReceiver<PartialResultSet>>>>,

    metadata: Option<ResultSetMetadata>,
    stats: Option<ResultSetStats>,

    /// Fully-processed rows
    rows: VecDeque<Vec<Value>>,
    /// Accumulated values for incomplete row
    current_row: Vec<Value>,
    /// Incomplete value
    pending_chunk: Option<Value>,
}

impl StreamedResultSetAsync {
    pub fn new(stream: ClientSStreamReceiver<PartialResultSet>) -> Self {
        Self {
            stream: Some(stream.compat().into_future()),
            metadata: None,
            stats: None,
            rows: Default::default(),
            current_row: vec![],
            pending_chunk: None,
        }
    }

    #[allow(dead_code)]
    pub fn metadata(&self) -> Option<&ResultSetMetadata> {
        self.metadata.as_ref()
    }

    #[allow(dead_code)]
    pub fn stats(&self) -> Option<&ResultSetStats> {
        self.stats.as_ref()
    }

    pub fn fields(&self) -> &[StructType_Field] {
        match self.metadata {
            Some(ref metadata) => metadata.get_row_type().get_fields(),
            None => &[],
        }
    }

    pub async fn one(&mut self) -> Result<Vec<Value>, DbError> {
        if let Some(result) = self.one_or_none().await? {
            Ok(result)
        } else {
            Err(DbError::new("No rows matched the given query.".to_string()))?
        }
    }

    pub async fn one_or_none(&mut self) -> Result<Option<Vec<Value>>, DbError> {
        let result = self.next_async().await;
        if result.is_none() {
            Ok(None)
        } else if self.next_async().await.is_some() {
            Err(DbError::new("Expected one result; got more.".to_string()))?
        } else {
            result.transpose()
        }
    }

    /// Pull and process the next values from the Stream
    ///
    /// Returns false when the stream is finished
    async fn consume_next(&mut self) -> Result<bool, DbError> {
        let (result, stream) = self
            .stream
            .take()
            .expect("Could not get next stream element")
            .await;

        self.stream = Some(stream.into_future());
        let mut partial_rs = if let Some(result) = result {
            result?
        } else {
            // Stream finished
            return Ok(false);
        };

        if self.metadata.is_none() && partial_rs.has_metadata() {
            // first response
            self.metadata = Some(partial_rs.take_metadata());
        }
        if partial_rs.has_stats() {
            // last response
            self.stats = Some(partial_rs.take_stats());
        }

        let mut values = partial_rs.take_values().into_vec();
        if values.is_empty() {
            // sanity check
            return Ok(true);
        }

        if let Some(pending_chunk) = self.pending_chunk.take() {
            let fields = self.fields();
            let current_row_i = self.current_row.len();
            if fields.len() <= current_row_i {
                Err(DbError::new("Invalid PartialResultSet fields".to_string()))?;
            }
            let field = &fields[current_row_i];
            values[0] = merge_by_type(pending_chunk, &values[0], field.get_field_type())?;
        }
        if partial_rs.get_chunked_value() {
            self.pending_chunk = values.pop();
        }

        self.consume_values(values);
        Ok(true)
    }

    fn consume_values(&mut self, values: Vec<Value>) {
        let width = self.fields().len();
        for value in values {
            self.current_row.push(value);
            if self.current_row.len() == width {
                let current_row = mem::replace(&mut self.current_row, vec![]);
                self.rows.push_back(current_row);
            }
        }
    }

    // We could implement Stream::poll_next instead of this, but
    // this is easier for now and we can refactor into the trait later
    pub async fn next_async(&mut self) -> Option<Result<Vec<Value>, DbError>> {
        while self.rows.is_empty() {
            match self.consume_next().await {
                Ok(true) => (),
                Ok(false) => return None,
                // Note: Iteration may continue after an error. We may want to
                // stop afterwards instead for safety sake (it's not really
                // recoverable)
                Err(e) => return Some(Err(e)),
            }
        }
        Ok(self.rows.pop_front()).transpose()
    }
}

fn merge_by_type(lhs: Value, rhs: &Value, field_type: &Type) -> Result<Value, DbError> {
    // We only support merging basic string types as that's all we currently use.
    // The python client also supports: float64, array, struct. The go client
    // only additionally supports array (claiming structs are only returned as
    // arrays anyway)
    match field_type.get_code() {
        TypeCode::BYTES
        | TypeCode::DATE
        | TypeCode::INT64
        | TypeCode::STRING
        | TypeCode::TIMESTAMP => merge_string(lhs, rhs),
        TypeCode::ARRAY
        | TypeCode::FLOAT64
        | TypeCode::STRUCT
        | TypeCode::TYPE_CODE_UNSPECIFIED
        | TypeCode::BOOL => unsupported_merge(field_type),
    }
}

fn unsupported_merge(field_type: &Type) -> Result<Value, DbError> {
    Err(DbError::new(format!(
        "merge not supported, type: {:?}",
        field_type
    ).to_string()))
}

fn merge_string(mut lhs: Value, rhs: &Value) -> Result<Value, DbError> {
    if !lhs.has_string_value() || !rhs.has_string_value() {
        Err(DbError::new("merge_string has no string value".to_string()))?
    }
    let mut merged = lhs.take_string_value();
    merged.push_str(rhs.get_string_value());
    Ok(as_value(merged))
}


