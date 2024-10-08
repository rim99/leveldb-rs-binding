use crate::utils::tmpdir;
use leveldb::database::cache::Cache;
use leveldb::database::Database;
use leveldb::options::Options;

#[test]
fn test_open_database_with_cache() {
    let mut opts = Options::new();
    opts.create_if_missing = true;
    opts.cache = Some(Cache::new(20));
    let tmp = tmpdir("create_if_missing");
    let res: Result<Database<i32>, _> = Database::open(tmp.path(), opts);
    assert!(res.is_ok());
}
