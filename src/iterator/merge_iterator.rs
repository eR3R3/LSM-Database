use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use crate::iterator::StorageIterator;

// these four traits right here are needed for the BinaryHeap data structure
// Box prevent the copy of big data
pub struct HeapWrapper<T: StorageIterator>(pub usize, Box<T>);

impl<T: StorageIterator> PartialEq for HeapWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == Ordering::Equal
    }
}

// this is a marker trait that do not have sany function to implement
impl<T: StorageIterator> Eq for HeapWrapper<T> {}

impl<T: StorageIterator> PartialOrd for HeapWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.1.key().cmp(other.1.key()) {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => self.0.partial_cmp(&other.0)
        // we use x.reverse() right here since the default behaviour of the BinaryHeap is
        // the greater iterator will be on top, now we reverse it so the less iterator
        // is on top
        }.map(|x| x.reverse())
    }
}

impl<T: StorageIterator> Ord for HeapWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct MergeIterator<T: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<T>>,
    current: Option<HeapWrapper<T>>,
}

impl<T: StorageIterator> MergeIterator<T> {
    pub(crate) fn create(iters: Vec<Box<T>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<T: StorageIterator> StorageIterator for MergeIterator<T> {
    // the right way to think about this is just:
    // keys in iter1: a, c, d;
    // keys in iter2: a, b, c;
    // keys in iter3: b, c ,d;
    // since iter1 is the most recent mem_table iterator, we take a from iter1, detect that the next iter
    // has the equal, then call .next() for iter2, then call .next() for the current iterator
    // the situation will turn into this:
    // keys in iter1: c, d;
    // keys in iter2: b, c;
    // keys in iter3: b, c ,d;
    // but the binary heap will keep the order for you, so the real one is like this:
    // keys in iter2: b, c;
    // keys in iter3: b, c ,d;
    // keys in iter1: c, d;
    // then we can take b
    fn next(&mut self) -> anyhow::Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value using while.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // call next in the mem_table iterator that is coming up since they have the
                // same key
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // deal with it when iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                // break the loop when something with bigger key appears
                // it maybe from a different mem_table iterator
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }
}