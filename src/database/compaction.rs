//! Compaction
use super::serializable::Serializable;
use super::Database;
use crate::binding::leveldb_compact_range;
use libc::{c_char, size_t};

pub trait Compaction<'a, K: Serializable + 'a> {
    fn compact(&self, start: &'a K, limit: &'a K);
}

impl<'a, K: Serializable + 'a> Compaction<'a, K> for Database<K> {
    fn compact(&self, start: &'a K, limit: &'a K) {
        unsafe {
            start.as_slice(|s| {
                limit.as_slice(|l| {
                    leveldb_compact_range(
                        self.database.ptr,
                        s.as_ptr() as *mut c_char,
                        s.len() as size_t,
                        l.as_ptr() as *mut c_char,
                        l.len() as size_t,
                    );
                });
            });
        }
    }
}
