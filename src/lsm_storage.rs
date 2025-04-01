use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;
use anyhow::{Result};
use std::sync::{Arc};
use parking_lot::{RwLock, Mutex};
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use crate::block::Block;
use crate::compact::{CompactionController, CompactionOption};
use crate::iterator::merge_iterator::MergeIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::SsTable;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageState {
    // I use Arc here since it can offer fast read by just cloning it without occupy the RwLock
    // for a long time, optimizing the concurrency efficiency.
    memtable: Arc<MemTable>,
    immut_memtable: Vec<Arc<MemTable>>,
    l0_sstables: Vec<usize>,
    levels: Vec<(usize, Vec<usize>)>,
    sstables: HashMap<usize, SsTable>
}

pub struct LsmStorageConfig {
    // a SsTable is consist of a lot of blocks
    block_size: usize,
    // the target size for a MemTable to reach to become a SsTable
    target_sst_size: usize,
    // the number of maximum MemTable that can exist, otherwise it will be converted into SsTable
    num_memtable_limit: usize,
    compaction_option: CompactionOption,
    enable_wal: bool,
    // something related to MVCC, I do not know yet
    serializable: bool,
}

pub struct LsmStorageInner {
    // the current state of the storage engine
    state: Arc<RwLock<Arc<LsmStorageState>>>,
    // global lock
    state_lock: Mutex<()>,
    // block cache that can store the closest saved block
    block_cache: BlockCache,
    next_sstable_id: AtomicUsize,
    path: PathBuf,
    config: LsmStorageConfig,
    compaction_controller: CompactionController,
    manifest: Option<Manifest>,
    mvcc: Option<LsmMvccInner>
}

impl LsmStorageInner {
    // it is only currently getting from the memtables
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self.state.read();
        let snapshot = guard;

        if let Some(value) = snapshot.memtable.get(Bytes::copy_from_slice(key)) {
            if value.is_empty() {
                return Ok(None)
            }
            return Ok(Some(value))
        }

        for memtable in snapshot.immut_memtable.iter() {
            if let Some(value) = memtable.get(Bytes::copy_from_slice(key)) {
                if value.is_empty() {
                    return Ok(None)
                }
                return Ok(Some(value))
            }
        }
        Ok(None)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "value cannot be empty");

        let guard = self.state.read();
        guard.memtable.put(key, value)?;
        let size = guard.memtable.approximate_size();

        self.try_freeze_memtable(size)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, b"")?;
            size = guard.memtable.approximate_size();
        }
        self.try_freeze_memtable(size)?;
        Ok(())
    }

    fn try_freeze_memtable(&self, size: usize) -> Result<()> {
        if size > self.config.target_sst_size {
            let _state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the reason for recheck is that is the case that there are two threads already executing
            // the try_freeze_memtable function in put function, and the first thread may lock the state_lock
            // first and the second thread will wait until the state_lock is unlock, and execute,
            // in this case, the size parameter it passes is the size of the old memtable, so we have to
            // get the approximate_size again.
            if guard.memtable.approximate_size() > self.config.target_sst_size {
                drop(guard);
                self.freeze_memtable()?
            }
        }
        Ok(())
    }

    fn freeze_memtable(&self) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = Arc::new(MemTable::create(new_memtable_id));
        {
            let mut guard = self.state.write();
            // guard itself is a pointer that points to the Arc pointer that points to the real data
            // on heap
            // the reason I want to use guard.as_ref() is to get the reference of the underlying data T
            // inside of Arc<T>, and .clone() is to get the underlying data that T points to on the heap
            // to pass it into the std::mem::replace() function
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
            snapshot.immut_memtable.insert(0, old_memtable);
            *guard = Arc::new(snapshot);
        }
        Ok(())
    }

    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sstable_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.immut_memtable.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.immut_memtable.iter() {
            memtable_iters.push(Box::new(memtable .scan(lower, upper)));
        }
        let iter = MergeIterator::create(memtable_iters);
        Ok(FusedIterator::new(LsmIterator::new(iter)?))
    }
}

pub struct MiniLsm {}

