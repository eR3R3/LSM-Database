use std::cmp::Ordering;
use std::collections::BinaryHeap;
use crate::iterator::StorageIterator;

pub struct HeapWrapper<T: StorageIterator>(pub usize, Box<T>);

impl<T: StorageIterator> PartialEq for HeapWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == Ordering::Equal
    }
}

// this is a marker trait that do not have any function to implement
impl<T: StorageIterator> Eq for HeapWrapper<T> {}

impl<T: StorageIterator> PartialOrd for HeapWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.1.key().cmp(other.1.key()) {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => self.0.partial_cmp(&other.0)
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