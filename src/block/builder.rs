use bytes::BufMut;
use super::{Block, SIZEOF_U16};

pub struct BlockBuilder {
    offsets: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        // Encode key length.
        self.data.put_u16(key.len() as u16);
        // Encode key content.
        self.data.put(key);
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);
        true
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
