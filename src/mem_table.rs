use std::iter::Skip;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use anyhow::{Result};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    id: usize,
    // Arc<AtomicUsize> is the substitute for Arc<Mutex<usize>>
    // since it provide better performance. Also, the reason for using Arc is that we have to share
    // this between threads, and AtomicUsize does not implement Copy or Clone trait, so we cannot
    // move it into other thread. So, we use Arc<AtomicUsize> instead to have multiple ownerships
    pub(crate) approximate_size: Arc<AtomicUsize>
}

impl MemTable {
    pub(crate) fn create() {

    }

    pub(crate) fn get(self: &Self, key: Bytes) -> Option<Bytes> {
        self.map.get(&key).map(|pair| pair.value().clone())
    }

    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map.insert(Bytes::from(key), Bytes::from(value));
        self.approximate_size.fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn approximate_size(&self) -> usize {
        self.approximate_size.load(std::sync::atomic::Ordering::Relaxed)
    }
}