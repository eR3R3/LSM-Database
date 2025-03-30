use crate::iterator::merge_iterator::MergeIterator;
use crate::mem_table::MemTableIterator;
use anyhow::{bail, Result};
use crate::iterator::StorageIterator;

type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let mut iter = Self { inner: iter };
        iter.move_to_non_delete()?;
        Ok(iter)
    }
}

impl LsmIterator {
    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_non_delete()?;
        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}

pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn next(&mut self) -> Result<()> {
        // only move when the iterator is valid and not errored
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn key(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }
}