// Send trait allows to move the ownership, Sync trait allows share reference
// for example, Arc has Sync trait since Arc<T> itself is a pointer, and it
// can be passed into different threads
fn main() {
    println!("Hello, world!");
}
