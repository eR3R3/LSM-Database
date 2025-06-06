use crate::iterator::StorageIterator;
use anyhow::Result;
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    fn choose_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        a.key() < b.key()
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            if self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            choose_a: false,
            a,
            b,
        };
        iter.skip_b()?;
        iter.choose_a = Self::choose_a(&iter.a, &iter.b);
        Ok(iter)
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.choose_a = Self::choose_a(&self.a, &self.b);
        Ok(())
    }
}