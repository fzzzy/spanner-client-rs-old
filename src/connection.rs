

use std::cell::RefCell;
use std::{convert::TryInto};

use futures::compat::{Future01CompatExt};

use googleapis_raw::spanner::v1::{
    spanner::{BeginTransactionRequest, ExecuteSqlRequest, Session},
    spanner_grpc::SpannerClient,
    transaction::{
        TransactionOptions, TransactionOptions_ReadOnly, TransactionOptions_ReadWrite, TransactionSelector,
    },
};

use crate::metadata::SpannerMetadata;
use crate::builder::ExecuteSqlRequestBuilder;
use crate::errors::DbError;

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
