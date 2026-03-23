//! WatchableMap and WatchableVec with change tracking.

use watchable::{WatchableMap, WatchableVec, WatchableVecLite};

fn main() {
    // -- WatchableMap: observable HashMap --
    let map = WatchableMap::<String, i32>::new();
    let mut watcher = map.watch_changes();

    map.insert("x".into(), 10);
    map.insert("y".into(), 20);
    map.insert("z".into(), 30);

    if let Some((changes, missed)) = watcher.poll_changes() {
        println!("map changes ({} total, missed={}): ", changes.len(), missed);
        for change in &changes {
            println!("  {:?}", change);
        }
    }

    println!("map snapshot: {:?}", map.snapshot());
    map.remove(&"y".into());
    println!("after remove 'y': {:?}", map.snapshot());

    // -- WatchableVec: observable Vec --
    println!("\n--- WatchableVec ---");
    let vec = WatchableVec::<String>::new();
    let mut vec_watcher = vec.watch_changes();

    vec.push("first".into());
    vec.push("second".into());
    vec.push("third".into());

    println!("vec: {:?}", vec.snapshot());
    println!("vec[1]: {:?}", vec.get(1));

    vec.set(1, "replaced".into());
    println!("after set(1): {:?}", vec.snapshot());

    if let Some((changes, _)) = vec_watcher.poll_changes() {
        println!("vec changes: {}", changes.len());
    }

    // -- WatchableVecLite: epoch-only, no change details --
    println!("\n--- WatchableVecLite ---");
    let lite = WatchableVecLite::<i32>::new();
    let mut epoch = lite.watch();

    lite.push(1);
    lite.push(2);
    lite.push(3);

    println!("lite vec: {:?}", lite.snapshot());
    println!("epoch changed: {:?}", epoch.poll_changed());
    println!("epoch changed again (no new writes): {:?}", epoch.poll_changed());
}
