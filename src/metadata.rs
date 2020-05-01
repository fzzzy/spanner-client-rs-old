
use googleapis_raw::spanner::v1::transaction::TransactionSelector;

pub struct SpannerMetadata {
    pub in_write_transaction: bool,
    pub transaction: Option<TransactionSelector>,
    pub execute_sql_count: u64,
}
