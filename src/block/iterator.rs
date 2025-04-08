use std::borrow::Borrow;
use std::sync::Arc;
use bytes::Buf;
use crate::block::{Block, SIZEOF_U16};

pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value_range: (usize, usize),
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> BlockIterator {
        BlockIterator {
            block,
            value_range: (0, 0),
            idx: 0,
            key: Vec::new(),
        }
    }

    fn seek_to_offset(&mut self, offset: usize) {
        // getting the key_len and key
        let mut data_from_start = &self.block.data[offset..];
        let key_len = data_from_start.get_u16() as usize;
        let key = data_from_start[..key_len].to_vec();
        data_from_start.advance(key_len);
        self.key.clear();
        self.key.extend(key);
        // getting the value_len and the value
        let value_len = data_from_start.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        data_from_start.advance(value_len);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
    }

    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }

    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    pub fn key(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }
}