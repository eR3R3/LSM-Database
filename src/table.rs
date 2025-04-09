mod builder;
mod iterator;

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes};
use anyhow::{anyhow, Result};
use crate::block::Block;
use crate::lsm_storage::BlockCache;

pub struct BlockMeta {
    pub offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

impl BlockMeta {
    // the block_meta are the slice of all the meta infos for all the block data, and the buf
    // right here should already include the block data section, this function is used in SsTableBuilder::build
    // function
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = 0;
        for meta in block_meta {
            // The size of offset
            estimated_size += size_of::<u32>();
            // The size of key length
            estimated_size += size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }
        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        buf.reserve(estimated_size);
        let original_len = buf.len();
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(&meta.last_key);
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

pub struct FileObject(Option<File>, u64);

impl FileObject {
    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data, offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options()
                .read(true)
                .write(false)
                .open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: Bytes,
    last_key: Bytes,
}

impl SsTable {
    fn open(file_object: FileObject, block_cache: Option<Arc<BlockCache>>, id: usize) -> Result<Self> {
        let block_meta_offset_raw = file_object.read(file_object.size() - 4, 4)?;
        // the reason why I use get_u32 is that it only actually occupies 4 bytes.
        let block_meta_offset = (&block_meta_offset_raw[..]).get_u32() as u64;
        let block_metas_raw = file_object.read(block_meta_offset, (file_object.size() - 4 - block_meta_offset) as u32)?;
        let block_meta = BlockMeta::decode_block_meta(&block_metas_raw[..]);
        Ok(Self {
            file: file_object,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(id: usize, file_size: u64, first_key: Bytes, last_key: Bytes) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
        }
    }

    // the right way to think about this is
    // to get the raw data from the file then decode it
    // to make sure that it does not decode the whole thing and make the whole thing on memory
    fn read_block(&self, idx: usize) -> Result<Arc<Block>> {
        // the idx HAVE to be usize
        let offset = self.block_meta[idx].offset;
        let next_block_offset = self.block_meta.get(idx + 1)
            // self.block_meta_offset is the first index of the block meta section
            .map_or(self.block_meta_offset, |x| x.offset);;
        let length = next_block_offset - offset;
        let block_data = self.file.read(offset as u64, length as u32)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    fn read_block_cache(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = self.block_cache.clone() {
            let cached_data = block_cache
                // the reason it takes in a closure
                // 如果直接传入普通函数，而不是闭包，普通函数就没有捕获外部变量的能力
                // ，也就无法像闭包那样消耗 key，也不会在缓存缺失时控制重复读取
                // and the reason I do not use ? but use .map_err()? is that
                // the Error type .try_get_with() returns is not anyhow::Error type
                // so we need to use anyhow!() macro to convert it
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err( |err| anyhow!("{}", err))?;
            Ok(cached_data)
        } else {
            self.read_block(block_idx)
        }
    }
}
