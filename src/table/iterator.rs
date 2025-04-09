use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::block::{Block, BlockIterator};
use crate::iterator::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    pub fn create_first_block_iterator_and_seek_to_first_pair(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        let first_block = table.read_block_cache(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(first_block);
        Ok((0, block_iterator))
    }

    pub fn create_block_iterator_and_seek_to_key(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        // find which block is the key located, returns the index
        let mut block_index = table.find_block_idx(key);
        let block = table.read_block_cache(block_index)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        //    如果当前 block 的迭代器无效：
        //         尝试读取下一个 block
        //         如果还有 block：
        //             创建新迭代器
        //         否则：
        //             结束迭代
        if !block_iterator.is_valid() {
            block_index += 1;
            if block_index < table.num_of_blocks() {
                block_iterator =
                    BlockIterator::create_and_seek_to_first(table.read_block_cache(block_index)?);
            }
        }
        Ok((block_index, block_iterator))
    }

    pub fn create_and_seek_to_first(&self, table: Arc<SsTable>) -> Result<Self> {
        let(block_idx, block_iterator) =
            Self::create_first_block_iterator_and_seek_to_first_pair(&table)?;
        Ok(Self {
            table,
            block_iter: block_iterator,
            block_idx,
        })
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        let(block_idx, block_iterator) =
            Self::create_first_block_iterator_and_seek_to_first_pair(&self.table)?;
        self.block_idx = block_idx;
        self.block_iter = block_iterator;
        Ok(())
    }

    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let(block_idx, block_iterator) = Self::create_block_iterator_and_seek_to_key(&table, key)?;
        Ok(Self {
            block_idx,
            block_iter: block_iterator,
            table
        })
    }

    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (blk_idx, blk_iter) = Self::create_block_iterator_and_seek_to_key(&self.table, key)?;
        self.block_iter = blk_iter;
        self.block_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        unimplemented!()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        unimplemented!()
    }
}