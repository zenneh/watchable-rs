//! Basic usage of Watchable types.

use watchable::{Watchable, WatchableFast, WatchableLite, Watcher};

fn main() {
    // -- Watchable<T>: general purpose --
    let w = Watchable::new(42);
    let mut watcher = w.watch();

    println!("initial: {}", watcher.get());

    w.set(100);
    println!("has_changed: {}", watcher.has_changed());
    println!("updated: {}", watcher.get());

    // set_if_changed only notifies when the value actually differs
    let result = w.set_if_changed(100);
    println!("set same value: {:?} (Err = unchanged)", result);

    // modify in place
    w.modify(|v| *v += 1);
    println!("after modify: {}", watcher.get());

    // -- WatchableLite<T>: minimal overhead, polling only --
    println!("\n--- WatchableLite ---");
    let lite = WatchableLite::new("hello".to_string());
    let mut lite_watcher = lite.watch();

    lite.set("world".to_string());
    println!("lite changed: {}", lite_watcher.has_changed());
    println!("lite value: {}", lite_watcher.get());

    // -- WatchableFast<T>: optimized for high-contention writes --
    println!("\n--- WatchableFast ---");
    let fast = WatchableFast::new(0);
    let mut fast_watcher = fast.watch();

    for i in 0..10 {
        fast.set(i);
    }
    // watcher only sees the latest value
    println!("fast latest: {}", fast_watcher.get());
}
