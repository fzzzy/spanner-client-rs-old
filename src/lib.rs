
mod builder;
mod connection;
mod db;
mod errors;
mod metadata;

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



