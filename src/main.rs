use std::rc::Rc;
use std::sync::Arc;
use bytes::Buf;
// Send trait allows to move the ownership, Sync trait allows share reference between different threads
// for example, Arc has Sync trait since Arc<T> itself is a pointer, and it
// can be passed into different threads

// the reason why we always use Arc<Mutex<T>> or Arc<RwLock<T>> is that we need the ownership of
// the lock to move them into different threads
fn main() {
    let mut x: &[u8] = &[1,2,3,4,5];
    let y = x.get_u16();
    println!("{:?}, {:?}", x, y);

    print!("{}", size_of::<Rc<u16>>());
    println!("Hello, world!");
}
