//! leveldb snapshots
//!
//! Snapshots give you a reference to the database at a certain
//! point in time and won't change while you work with them.
use crate::binding::{leveldb_create_snapshot, leveldb_release_snapshot};
use crate::binding::{leveldb_snapshot_t, leveldb_t};

use crate::database::kv::KV;
use crate::database::serializable::Serializable;
use crate::database::Database;

use crate::database::error::Error;
use crate::database::iterator::{Iterable, Iterator, KeyIterator, ValueIterator};
use crate::database::options::ReadOptions;

use std::borrow::Borrow;
use std::cmp::Ord;

#[allow(missing_docs)]
struct RawSnapshot {
    db_ptr: *mut leveldb_t,
    ptr: *mut leveldb_snapshot_t,
}

impl Drop for RawSnapshot {
    fn drop(&mut self) {
        unsafe { leveldb_release_snapshot(self.db_ptr, self.ptr) };
    }
}

/// A database snapshot
///
/// Represents a database at a certain point in time,
/// and allows for all read operations (get and iteration).
pub struct Snapshot<'a, K: Serializable + 'a> {
    raw: RawSnapshot,
    database: &'a Database<K>,
}

/// Structs implementing the Snapshots trait can be
/// snapshotted.
pub trait Snapshots<K: Serializable> {
    /// Creates a snapshot and returns a struct
    /// representing it.
    fn snapshot<'a>(&'a self) -> Snapshot<'a, K>;
}

impl<K: Serializable> Snapshots<K> for Database<K> {
    fn snapshot<'a>(&'a self) -> Snapshot<'a, K> {
        let db_ptr = self.database.ptr;
        let snap = unsafe { leveldb_create_snapshot(db_ptr) };

        let raw = RawSnapshot {
            db_ptr: db_ptr,
            ptr: snap,
        };
        Snapshot {
            raw: raw,
            database: self,
        }
    }
}

impl<'a, K: Serializable> Snapshot<'a, K> {
    /// fetches a key from the database
    ///
    /// Inserts this snapshot into ReadOptions before reading
    pub fn get<BK: Borrow<K>>(
        &'a self,
        mut options: ReadOptions<'a, K>,
        key: BK,
    ) -> Result<Option<Vec<u8>>, Error> {
        options.snapshot = Some(self);
        self.database.get(options, key)
    }

    #[inline]
    #[allow(missing_docs)]
    pub fn raw_ptr(&self) -> *mut leveldb_snapshot_t {
        self.raw.ptr
    }
}

impl<'a, K: Serializable + Ord + 'a> Iterable<'a, K> for Snapshot<'a, K> {
    fn iter(&'a self, mut options: ReadOptions<'a, K>) -> Iterator<K> {
        options.snapshot = Some(self);
        self.database.iter(options)
    }
    fn keys_iter(&'a self, mut options: ReadOptions<'a, K>) -> KeyIterator<K> {
        options.snapshot = Some(self);
        self.database.keys_iter(options)
    }
    fn value_iter(&'a self, mut options: ReadOptions<'a, K>) -> ValueIterator<K> {
        options.snapshot = Some(self);
        self.database.value_iter(options)
    }
}
