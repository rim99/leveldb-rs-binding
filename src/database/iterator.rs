//! leveldb iterators
//!
//! Iteration is one of the most important parts of leveldb. This module provides
//! Iterators to iterate over key, values and pairs of both.
use super::options::{c_readoptions, ReadOptions};
use super::serializable::{from_u8, Serializable};
use super::Database;
use crate::binding::{
    leveldb_create_iterator, leveldb_iter_destroy, leveldb_iter_key, leveldb_iter_next,
    leveldb_iter_prev, leveldb_iter_seek, leveldb_iter_seek_to_first, leveldb_iter_seek_to_last,
    leveldb_iter_valid, leveldb_iter_value, leveldb_iterator_t, leveldb_readoptions_destroy,
};
use libc::{c_char, size_t};
use std::iter;
use std::marker::PhantomData;
use std::slice::from_raw_parts;
use std::cmp::Ord;

#[allow(missing_docs)]
struct RawIterator {
    ptr: *mut leveldb_iterator_t,
}

#[allow(missing_docs)]
impl Drop for RawIterator {
    fn drop(&mut self) {
        unsafe { leveldb_iter_destroy(self.ptr) }
    }
}

/// An iterator over the leveldb keyspace.
///
/// Returns key and value as a tuple.
pub struct Iterator<'a, K: Serializable + 'a> {
    started: bool,
    stopped: bool,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database<K>>,
    iter: RawIterator,
    from: Option<&'a K>,
    to: Option<&'a K>,
}

/// An iterator over the leveldb keyspace that browses the keys backwards.
///
/// Returns key and value as a tuple.
pub struct RevIterator<'a, K: Serializable + 'a> {
    started: bool,
    stopped: bool,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database<K>>,
    iter: RawIterator,
    from: Option<&'a K>,
    to: Option<&'a K>,
}

/// An iterator over the leveldb keyspace.
///
/// Returns just the keys.
pub struct KeyIterator<'a, K: Serializable + 'a> {
    inner: Iterator<'a, K>,
}

/// An iterator over the leveldb keyspace that browses the keys backwards.
///
/// Returns just the keys.
pub struct RevKeyIterator<'a, K: Serializable + 'a> {
    inner: RevIterator<'a, K>,
}

/// An iterator over the leveldb keyspace.
///
/// Returns just the value.
pub struct ValueIterator<'a, K: Serializable + 'a> {
    inner: Iterator<'a, K>,
}

/// An iterator over the leveldb keyspace that browses the keys backwards.
///
/// Returns just the value.
pub struct RevValueIterator<'a, K: Serializable + 'a> {
    inner: RevIterator<'a, K>,
}

/// A trait to allow access to the three main iteration styles of leveldb.
pub trait Iterable<'a, K: Serializable + 'a> {
    /// Return an Iterator iterating over (Key,Value) pairs
    fn iter(&'a self, options: ReadOptions<'a, K>) -> Iterator<K>;
    /// Returns an Iterator iterating over Keys only.
    fn keys_iter(&'a self, options: ReadOptions<'a, K>) -> KeyIterator<K>;
    /// Returns an Iterator iterating over Values only.
    fn value_iter(&'a self, options: ReadOptions<'a, K>) -> ValueIterator<K>;
}

impl<'a, K: Serializable + Ord + 'a> Iterable<'a, K> for Database<K> {
    fn iter(&'a self, options: ReadOptions<'a, K>) -> Iterator<K> {
        Iterator::new(self, options)
    }

    fn keys_iter(&'a self, options: ReadOptions<'a, K>) -> KeyIterator<K> {
        KeyIterator::new(self, options)
    }

    fn value_iter(&'a self, options: ReadOptions<'a, K>) -> ValueIterator<K> {
        ValueIterator::new(self, options)
    }
}

#[allow(missing_docs)]
#[allow(unused_attributes)]
pub trait LevelDBIterator<'a, K: Serializable + Ord> {
    type RevIter: LevelDBIterator<'a, K>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t;

    #[inline]
    fn start(&mut self);

    #[inline]
    fn started(&self) -> bool;

    #[inline]
    fn stop(&mut self);

    #[inline]
    fn stopped(&self) -> bool;

    fn reverse(self) -> Self::RevIter;

    fn from(self, key: &'a K) -> Self;
    fn to(self, key: &'a K) -> Self;

    fn from_key(&self) -> Option<&K>;
    fn to_key(&self) -> Option<&K>;

    fn valid(&self) -> bool {
        unsafe { leveldb_iter_valid(self.raw_iterator()) != 0 }
    }

    #[doc(hidden)]
    unsafe fn advance_raw(&mut self);

    fn advance(&mut self) -> bool {
        unsafe {
            if self.started() && !self.stopped() {
                self.advance_raw();
                if let Some(end) = self.to_key() {
                    if end <= &self.key() {
                        self.stop();
                    }
                }
            } else {
                if let Some(begin) = self.from_key() {
                    self.seek(begin)
                }
                self.start();
            }
        }
        self.valid()
    }

    fn key(&self) -> K {
        unsafe {
            let length: size_t = 0;
            let value = leveldb_iter_key(self.raw_iterator(), &length) as *const u8;
            from_u8(from_raw_parts(value, length as usize))
        }
    }

    fn value(&self) -> Vec<u8> {
        unsafe {
            let length: size_t = 0;
            let value = leveldb_iter_value(self.raw_iterator(), &length) as *const u8;
            from_raw_parts(value, length as usize).to_vec()
        }
    }

    fn entry(&self) -> (K, Vec<u8>) {
        (self.key(), self.value())
    }

    fn seek_to_first(&self) {
        unsafe { leveldb_iter_seek_to_first(self.raw_iterator()) }
    }

    fn seek_to_last(&self) {
        if let Some(k) = self.to_key() {
            self.seek(k);
        } else {
            unsafe {
                leveldb_iter_seek_to_last(self.raw_iterator());
            }
        }
    }

    fn seek(&self, key: &K) {
        unsafe {
            let key = &key.as_u8();

            leveldb_iter_seek(
                self.raw_iterator(),
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
            );
        }
    }
}

impl<'a, K: Serializable + Ord> Iterator<'a, K> {
    fn new(database: &'a Database<K>, options: ReadOptions<'a, K>) -> Iterator<'a, K> {
        unsafe {
            let c_readoptions = c_readoptions(&options);
            let ptr = leveldb_create_iterator(database.database.ptr, c_readoptions);
            leveldb_readoptions_destroy(c_readoptions);
            leveldb_iter_seek_to_first(ptr);
            Iterator {
                started: false,
                stopped: false,
                iter: RawIterator { ptr: ptr },
                database: PhantomData,
                from: None,
                to: None,
            }
        }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<(K, Vec<u8>)> {
        self.seek_to_last();
        Some((self.key(), self.value()))
    }
}

impl<'a, K: Serializable + Ord> LevelDBIterator<'a, K> for Iterator<'a, K> {
    type RevIter = RevIterator<'a, K>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t {
        self.iter.ptr
    }

    #[inline]
    fn start(&mut self) {
        self.started = true
    }

    #[inline]
    fn started(&self) -> bool {
        self.started
    }

    #[inline]
    fn stopped(&self) -> bool {
        self.stopped
    }

    #[inline]
    fn stop(&mut self) {
        self.stopped = true
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_next(self.raw_iterator());
    }

    #[inline]
    fn reverse(self) -> Self::RevIter {
        if !self.started {
            unsafe {
                leveldb_iter_seek_to_last(self.iter.ptr);
            }
        }
        RevIterator {
            started: self.started,
            stopped: self.stopped,
            database: self.database,
            iter: self.iter,
            from: self.from,
            to: self.to,
        }
    }

    fn from(mut self, key: &'a K) -> Self {
        self.from = Some(key);
        self
    }

    fn to(mut self, key: &'a K) -> Self {
        self.to = Some(key);
        self
    }

    fn from_key(&self) -> Option<&K> {
        self.from
    }

    fn to_key(&self) -> Option<&K> {
        self.to
    }
}

impl<'a, K: Serializable + Ord> LevelDBIterator<'a, K> for RevIterator<'a, K> {
    type RevIter = Iterator<'a, K>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t {
        self.iter.ptr
    }

    #[inline]
    fn start(&mut self) {
        self.started = true
    }

    #[inline]
    fn started(&self) -> bool {
        self.started
    }

    #[inline]
    fn stop(&mut self) {
        self.stopped = true
    }

    #[inline]
    fn stopped(&self) -> bool {
        self.stopped
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_prev(self.raw_iterator());
    }

    #[inline]
    fn reverse(self) -> Self::RevIter {
        if !self.started {
            unsafe {
                leveldb_iter_seek_to_first(self.iter.ptr);
            }
        }
        Iterator {
            started: self.started,
            stopped: self.stopped,
            database: self.database,
            iter: self.iter,
            from: self.from,
            to: self.to,
        }
    }

    fn from(mut self, key: &'a K) -> Self {
        self.from = Some(key);
        self
    }

    fn to(mut self, key: &'a K) -> Self {
        self.to = Some(key);
        self
    }

    fn from_key(&self) -> Option<&K> {
        self.from
    }

    fn to_key(&self) -> Option<&K> {
        self.to
    }
}

impl<'a, K: Serializable + Ord> KeyIterator<'a, K> {
    fn new(database: &'a Database<K>, options: ReadOptions<'a, K>) -> KeyIterator<'a, K> {
        KeyIterator {
            inner: Iterator::new(database, options),
        }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<K> {
        self.seek_to_last();
        Some(self.key())
    }
}

impl<'a, K: Serializable + Ord> ValueIterator<'a, K> {
    fn new(database: &'a Database<K>, options: ReadOptions<'a, K>) -> ValueIterator<'a, K> {
        ValueIterator {
            inner: Iterator::new(database, options),
        }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<Vec<u8>> {
        self.seek_to_last();
        Some(self.value())
    }
}

macro_rules! impl_leveldb_iterator {
    ($T:ty, $RevT:ty) => {
        impl<'a, K: Serializable + Ord> LevelDBIterator<'a, K> for $T {
            type RevIter = $RevT;

            #[inline]
            fn raw_iterator(&self) -> *mut leveldb_iterator_t {
                self.inner.iter.ptr
            }

            #[inline]
            fn start(&mut self) {
                self.inner.started = true
            }

            #[inline]
            fn started(&self) -> bool {
                self.inner.started
            }

            #[inline]
            fn stop(&mut self) {
                self.inner.stopped = true
            }

            #[inline]
            fn stopped(&self) -> bool {
                self.inner.stopped
            }

            #[inline]
            unsafe fn advance_raw(&mut self) {
                self.inner.advance_raw();
            }

            #[inline]
            fn reverse(self) -> Self::RevIter {
                Self::RevIter {
                    inner: self.inner.reverse(),
                }
            }

            fn from(mut self, key: &'a K) -> Self {
                self.inner.from = Some(key);
                self
            }

            fn to(mut self, key: &'a K) -> Self {
                self.inner.to = Some(key);
                self
            }

            fn from_key(&self) -> Option<&K> {
                self.inner.from
            }

            fn to_key(&self) -> Option<&K> {
                self.inner.to
            }
        }
    };
}

impl_leveldb_iterator!(KeyIterator<'a, K>, RevKeyIterator<'a, K>);
impl_leveldb_iterator!(RevKeyIterator<'a, K>, KeyIterator<'a, K>);
impl_leveldb_iterator!(ValueIterator<'a, K>, RevValueIterator<'a, K>);
impl_leveldb_iterator!(RevValueIterator<'a, K>, ValueIterator<'a, K>);

macro_rules! impl_iterator {
    ($T:ty, $Item:ty, $ItemMethod:ident) => {
        impl<'a, K: Serializable + Ord> iter::Iterator for $T {
            type Item = $Item;

            fn next(&mut self) -> Option<Self::Item> {
                if self.advance() && !self.stopped() {
                    Some(self.$ItemMethod())
                } else {
                    None
                }
            }
        }
    };
}

impl_iterator!(Iterator<'a, K>, (K, Vec<u8>), entry);
impl_iterator!(RevIterator<'a, K>, (K, Vec<u8>), entry);
impl_iterator!(KeyIterator<'a, K>, K, key);
impl_iterator!(RevKeyIterator<'a, K>, K, key);
impl_iterator!(ValueIterator<'a, K>, Vec<u8>, value);
impl_iterator!(RevValueIterator<'a, K>, K, key);
