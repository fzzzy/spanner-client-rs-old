

fn main() {
    spanner_client_rs::connect(
        "spanner.googleapis.com".to_string(),
        "projects/moz-fx-sync-nonprod-904c/instances/sync/databases/syncdb4".to_string()
    ).unwrap();
}