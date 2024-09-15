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
            let start = &start.as_u8();
            let limit = &limit.as_u8();
            leveldb_compact_range(
                self.database.ptr,
                start.as_ptr() as *mut c_char,
                start.len() as size_t,
                limit.as_ptr() as *mut c_char,
                limit.len() as size_t,
            )
        }
    }
}
