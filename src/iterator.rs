mod merge_iterator;

pub trait StorageIterator {
    // type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord where Self: 'a;
    fn next(&mut self) -> anyhow::Result<()>;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn is_valid(&self) -> bool;
    fn num_active_iterators(&self) -> usize {
        1
    }
}