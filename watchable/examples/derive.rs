//! Derive macro: per-field observable structs.

use watchable_rs::{Watchable, Watcher};

#[derive(Clone, Debug, Watchable)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub debug: bool,
}

fn main() {
    let config = WatchableAppConfig::new(AppConfig {
        host: "localhost".into(),
        port: 8080,
        debug: false,
    });

    let mut watcher = config.watch();

    // read individual fields
    println!("host: {}", watcher.host());
    println!("port: {}", watcher.port());
    println!("debug: {}", watcher.debug());

    // update a single field
    config.set_port(9090);
    println!("\nafter set_port(9090):");
    println!("  has_changed: {}", watcher.has_changed());
    watcher.update();
    println!("  port: {}", watcher.port());

    // get a full snapshot
    let snapshot = watcher.peek();
    println!("\nsnapshot: {:?}", snapshot);

    // set all fields at once
    config.set(AppConfig {
        host: "0.0.0.0".into(),
        port: 443,
        debug: true,
    });
    let snapshot = watcher.get();
    println!("after full set: {:?}", snapshot);

    // you can also watch individual fields directly
    let mut host_watcher = config.host.watch();
    config.set_host("example.com".into());
    println!("\nhost watcher: {}", host_watcher.get());
}
