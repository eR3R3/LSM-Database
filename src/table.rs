use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes};
use anyhow::Result;
use crate::lsm_storage::BlockCache;

pub struct BlockMeta {
    pub offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes,
}

impl BlockMeta {
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
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: Bytes,
    last_key: Bytes,
}
