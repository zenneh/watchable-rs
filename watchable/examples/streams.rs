//! Async streams: convert watchers into Stream types.

use futures_lite::StreamExt;
use watchable_rs::{Watchable, WatchableMap, Watcher};

#[tokio::main]
async fn main() {
    // -- Watcher::stream() yields current value + all future changes --
    println!("--- value stream ---");
    let counter = Watchable::new(0);
    let mut stream = counter.watch().stream();

    tokio::spawn(async move {
        for i in 1..=3 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            counter.set(i);
        }
        // small delay so the consumer sees the last value before disconnect
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });

    // stream yields the initial value, then each update, then None on disconnect
    while let Some(value) = stream.next().await {
        println!("  stream got: {value}");
    }
    println!("  stream ended (producer dropped)");

    // -- stream_updates_only() skips the initial value --
    println!("\n--- updates only stream ---");
    let name = Watchable::new("initial".to_string());
    let mut stream = name.watch().stream_updates_only();

    tokio::spawn(async move {
        for s in ["alpha", "beta", "gamma"] {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            name.set(s.to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });

    while let Some(value) = stream.next().await {
        println!("  update: {value}");
    }
    println!("  updates stream ended");

    // -- ChangeStream from WatchableMap --
    // Note: ChangeStream works best consumed with take() or in a select!,
    // since collection types don't wake watchers on drop.
    println!("\n--- change stream (WatchableMap) ---");
    let map = WatchableMap::<String, i32>::new();
    let mut change_stream = map.watch_changes().into_stream();

    let m = map.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        m.insert("x".into(), 1);
        m.insert("y".into(), 2);
        m.remove(&"x".into());
    });

    // take one batch of changes
    if let Some((changes, missed)) = change_stream.next().await {
        println!("  batch ({} changes, missed={})", changes.len(), missed);
        for change in &changes {
            println!("    {:?}", change);
        }
    }
}
