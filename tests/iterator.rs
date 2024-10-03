use crate::utils::{db_put_simple, open_database, tmpdir};
use leveldb::iterator::Iterable;
use leveldb::iterator::LevelDBIterator;
use leveldb::options::ReadOptions;

#[test]
fn test_iterator() {
    let tmp = tmpdir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts);

    let entry = iter.next();
    assert!(entry.is_some());
    assert_eq!(entry.unwrap(), (1, vec![1]));
    let entry2 = iter.next();
    assert!(entry2.is_some());
    assert_eq!(entry2.unwrap(), (2, vec![2]));
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_reverse() {
    let tmp = tmpdir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).reverse();

    let entry = iter.next();
    assert!(entry.is_some());
    assert_eq!(entry.unwrap(), (2, vec![2]));
    let entry2 = iter.next();
    assert!(entry2.is_some());
    assert_eq!(entry2.unwrap(), (1, vec![1]));
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_last() {
    let tmp = tmpdir("iter_last");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let read_opts = ReadOptions::new();
    let iter = database.iter(read_opts);

    assert!(iter.last().is_some());
}

#[test]
fn test_iterator_from_to() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 4, &[4]);
    db_put_simple(database, 5, &[5]);

    let from = 2;
    let to = 4;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&from).to(&to);

    assert_eq!(iter.next().unwrap(), (2, vec![2]));
    assert_eq!(iter.last().unwrap(), (4, vec![4]));
}

#[test]
fn test_iterator_bounded_next() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 4, &[4]);
    db_put_simple(database, 5, &[5]);

    let begin = 2;
    let end = 4;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![2, 3]);
}

#[test]
fn test_iterator_bounded_next_2() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 5, &[5]);
    db_put_simple(database, 7, &[7]);
    db_put_simple(database, 9, &[9]);

    let begin = 2;
    let end = 6;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![3, 5]);
}

#[test]
fn test_iterator_bounded_next_3() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 2, &[2]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 4, &[4]);
    db_put_simple(database, 5, &[5]);
    db_put_simple(database, 6, &[6]);

    let begin = 1;
    let end = 7;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![2, 3, 4, 5, 6]);
}

#[test]
fn test_iterator_bounded_next_4() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 2, &[2]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 4, &[4]);
    db_put_simple(database, 5, &[5]);
    db_put_simple(database, 6, &[6]);

    let begin = 1;
    let end = 5;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![2, 3, 4]);
}

#[test]
fn test_iterator_bounded_next_5() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 2, &[2]);
    db_put_simple(database, 3, &[3]);
    db_put_simple(database, 4, &[4]);
    db_put_simple(database, 5, &[5]);
    db_put_simple(database, 6, &[6]);

    let begin = 4;
    let end = 7;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![4, 5, 6]);
}

#[test]
fn test_iterator_bounded_next_6() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);

    let begin = 2;
    let end = 3;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![]);
}

#[test]
fn test_iterator_bounded_next_7() {
    let tmp = tmpdir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 3, &[3]);

    let begin = 1;
    let end = 2;
    let read_opts = ReadOptions::new();
    let mut iter = database.iter(read_opts).from(&begin).to(&end);

    let mut keys = vec![];
    while let Some((k, _)) = iter.next() {
        keys.push(k)
    }
    assert_eq!(keys, vec![]);
}

#[test]
fn test_key_iterator() {
    let tmp = tmpdir("key_iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let iterable: &mut dyn Iterable<i32> = database;

    let read_opts = ReadOptions::new();
    let mut iter = iterable.keys_iter(read_opts);
    let value = iter.next().unwrap();
    assert_eq!(value, 1);
}

#[test]
fn test_value_iterator() {
    let tmp = tmpdir("value_iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let iterable: &mut dyn Iterable<i32> = database;

    let read_opts = ReadOptions::new();
    let mut iter = iterable.value_iter(read_opts);
    let value = iter.next().unwrap();
    assert_eq!(value, vec![1]);
}

#[test]
fn test_seek_before_inserted() {
    let tmp = tmpdir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let read_opts = ReadOptions::new();
    let iter = database.iter(read_opts);

    iter.seek(&0);
    assert!(iter.valid())
}

#[test]
fn test_seek_after_inserted() {
    let tmp = tmpdir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, 1, &[1]);
    db_put_simple(database, 2, &[2]);

    let read_opts = ReadOptions::new();
    let iter = database.iter(read_opts);

    iter.seek(&3);
    assert!(!iter.valid())
}
