//! Watcher combinators: map, and, join.

use watchable_rs::{Join, Watchable, Watcher};

fn main() {
    // -- map: transform watched values --
    let counter = Watchable::new(5);
    let mut doubled = counter.watch().map(|v| v * 2);

    println!("doubled: {}", doubled.get());
    counter.set(10);
    doubled.update();
    println!("doubled after set(10): {}", doubled.peek());

    // -- and: combine two watchers into a tuple --
    println!("\n--- and ---");
    let name = Watchable::new("Alice".to_string());
    let age = Watchable::new(30u32);

    let mut combined = name.watch().and(age.watch());
    println!("combined: {:?}", combined.get());

    name.set("Bob".to_string());
    combined.update();
    println!("after name change: {:?}", combined.peek());

    // -- join: combine many watchers of the same type --
    println!("\n--- join ---");
    let sensors: Vec<_> = (0..4).map(|i| Watchable::new(i as f64 * 1.5)).collect();
    let mut readings = Join::new(sensors.iter().map(|s| s.watch()));

    println!("readings: {:?}", readings.get());

    sensors[2].set(99.9);
    readings.update();
    println!("after sensor[2] update: {:?}", readings.peek());
}
