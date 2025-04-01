use std::rc::Rc;
use std::sync::Arc;
use bytes::Buf;
// Send trait allows to move the ownership, Sync trait allows share reference
// for example, Arc has Sync trait since Arc<T> itself is a pointer, and it
// can be passed into different threads

struct Country {
    city: Vec<String>,
}

fn main() {
    let mut x: &[u8] = &[1,2,3,4,5];
    let y = x.get_u16();
    println!("{:?}, {:?}", x, y);

    print!("{}", size_of::<Rc<u16>>());
    println!("Hello, world!");
}
