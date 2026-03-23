# watchable

Observable values with change tracking and async support for Rust.

A `Watchable` wraps a value that may change over time, allowing observers to be notified of changes. The design prioritizes the **latest** value over observing every intermediate change.

## Variants

| Type | Use case |
|------|----------|
| `Watchable<T>` | General purpose, full async support |
| `WatchableLite<T>` | Minimal overhead, polling only |
| `WatchableAtomic<T>` | Lock-free for small `Copy` types (up to 8 bytes) |
| `WatchableFast<T>` | Optimized for high-contention writes |
| `WatchableWithHistory<T>` | Tracks old/new value pairs |

## Collections

| Type | Use case |
|------|----------|
| `WatchableMap<K, V>` | HashMap with change tracking |
| `WatchableVec<T>` | Vec with change tracking |
| `WatchableMapLite<K, V>` | HashMap, epoch-only |
| `WatchableVecLite<T>` | Vec, epoch-only |

## Usage

```rust
use watchable::{Watchable, Watcher};

let watchable = Watchable::new(42);
let mut watcher = watchable.watch();

assert_eq!(watcher.get(), 42);

watchable.set(100);
assert!(watcher.has_changed());
assert_eq!(watcher.get(), 100);
```

### Derive macro

The `Watchable` derive macro generates per-field observable wrappers:

```rust
use watchable::Watchable;

#[derive(Clone, Watchable)]
struct Config {
    pub name: String,
    pub count: u32,
}

let w = WatchableConfig::new(Config { name: "hello".into(), count: 42 });
let mut watcher = w.watch();

assert_eq!(watcher.name(), "hello");

w.set_name("world".into());
assert!(watcher.has_changed());
```

### Async

```rust
use watchable::{Watchable, Watcher};

let w = Watchable::new(0);
let mut watcher = w.watch();

tokio::spawn(async move {
    let value = watcher.updated().await.unwrap();
    println!("got: {value}");
});

w.set(42);
```

### Combinators

```rust
use watchable::{Watchable, Watcher};

let a = Watchable::new(1);
let b = Watchable::new("hello".to_string());

// Combine watchers
let mut combined = a.watch().and(b.watch());

// Map values
let mut doubled = a.watch().map(|v| v * 2);
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
