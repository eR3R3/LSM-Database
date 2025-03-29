use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use anyhow::{Result};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use crate::wal::Wal;
use ouroboros::self_referencing;
use crate::iterator::StorageIterator;

fn map_bound(original: Bound<&[u8]>) -> Bound<Bytes> {
    match original {
        Bound::Included(data) => Bound::Included(Bytes::copy_from_slice(data)),
        Bound::Excluded(data) => Bound::Excluded(Bytes::copy_from_slice(data)),
        Bound::Unbounded => Bound::Unbounded
    }
}

pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    id: usize,
    // Arc<AtomicUsize> is the substitute for Arc<Mutex<usize>>
    // since it provide better performance. Also, the reason for using Arc is that we have to share
    // this between threads, and AtomicUsize does not implement Copy or Clone trait, so we cannot
    // move it into other thread. So, we use Arc<AtomicUsize> instead to have multiple ownerships
    pub(crate) approximate_size: Arc<AtomicUsize>,
    wal: Option<Wal>
}

impl MemTable {
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn scan(&self, low_bound: Bound<&[u8]>, upper_bound: Bound<&[u8]>) -> MemTableIterator {
        let range = (map_bound(low_bound), map_bound(upper_bound));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            // since iter rely on map, I need to take map as a parameter, and .range just returns
            // an iterator
            iter_builder: |map| map.range(range),
            item: (Bytes::new(), Bytes::new())
        }.build();
        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    pub(crate) fn get(self: &Self, key: Bytes) -> Option<Bytes> {
        self.map.get(&key).map(|pair| pair.value().clone())
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map.insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size.fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn approximate_size(&self) -> usize {
        self.approximate_size.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// I want to make three points clear here:
// 1. the reason I include the original skipmap is that we want to make sure that the iterator is
// not pointing to nothing in the multi-thread scenario. But for normal iterators like the iterator
// for vectors, they just return the iterator with a lifetime 'a and it is easy to use since it
// does not need to cross the threads so it is ok to have no ownership.
// 2. the reason I use Arc to wrap the original skipmap is that we need the ownership to move one thing
// into another thread, also, it is efficient to clone Arc.
// 3. the reason I use self-reference is that the Rust compiler cannot make sure the skipmap always
// exist when I try to use the iterator that points to it. (Note we can also use 'a here, but it can be
// become quite complicated)
type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry.map(|each| { return (each.key().clone(), each.value().clone()) })
             .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    fn next(&mut self) -> Result<()> {
        let entry = self
            .with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_item_mut(|item| {*item = entry});
        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }
}




















