//! Async watching with tokio.

use watchable_rs::{Watchable, Watcher};

#[tokio::main]
async fn main() {
    // -- updated(): wait for next change --
    let counter = Watchable::new(0);
    let mut watcher = counter.watch();

    tokio::spawn(async move {
        for i in 1..=5 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            counter.set(i);
            println!("[producer] set {i}");
        }
        // counter is dropped here, disconnecting watchers
    });

    println!("[consumer] waiting for updates...");
    loop {
        match watcher.updated().await {
            Ok(value) => println!("[consumer] got: {value}"),
            Err(_) => {
                println!("[consumer] disconnected");
                break;
            }
        }
    }

    // -- initialized(): wait for Option<T> to become Some --
    println!("\n--- initialized ---");
    let config = Watchable::new(None::<String>);
    let mut config_watcher = config.watch();

    let c = config.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        c.set(Some("ready".to_string()));
    });

    match config_watcher.initialized().await {
        Ok(value) => println!("config initialized: {value}"),
        Err(_) => println!("config source dropped"),
    }
}
