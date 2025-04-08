use std::path::Path;
use std::sync::Arc;
use crate::block::BlockBuilder;
use crate::lsm_storage::BlockCache;
use crate::table::{BlockMeta, FileObject, SsTable};
use anyhow::Result;
use bytes::BufMut;

pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) block_meta: Vec<BlockMeta>,
    target_block_size: usize,
}

impl SsTableBuilder {
    fn new(target_block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(target_block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            block_meta: Vec::new(),
            target_block_size
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.clear();
            // everything that implements IntoIterator<Item = u8> can be used in .extend()
            self.first_key.extend(key);
        }

        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key);
            return
        }

        self.finish_block();
        assert!(self.builder.add(key, value));
        self.first_key.clear();
        self.first_key.extend(key);
        self.last_key.clear();
        self.last_key.extend(key);
    }

    fn finish_block(&mut self) {
        let old_block_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.target_block_size));
        let encoded_block = old_block_builder.build().encode();
        self.block_meta.push(
            BlockMeta {
                offset: self.data.len(),
                first_key: std::mem::take(&mut self.first_key).into(),
                last_key: std::mem::take(&mut self.last_key).into(),
            }
        );
        self.data.extend(encoded_block);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        // encode the block meta, it will format the block_meta and put it after the block data section
        BlockMeta::encode_block_meta(&self.block_meta, &mut buf);
        // the length of the offset section(the length of the block data section), should occupy the last four bytes
        buf.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.block_meta.first().unwrap().first_key.clone(),
            last_key: self.block_meta.last().unwrap().last_key.clone(),
            block_meta: self.block_meta,
            block_meta_offset: meta_offset,
            block_cache,
        })
    }
}