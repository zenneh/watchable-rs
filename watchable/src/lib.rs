//! Observable values with change tracking.
//!
//! A [`Watchable`] wraps a value that may change over time, allowing observers
//! to be notified of changes. The design prioritizes the **latest** value over
//! observing every intermediate change.
//!
//! # Variants
//!
//! - [`Watchable<T>`] - General purpose, supports async waiting
//! - [`WatchableLite<T>`] - Minimal overhead, polling only
//! - [`WatchableAtomic<T>`] - Lock-free for small `Copy` types
//! - [`WatchableWithHistory<T>`] - Tracks old/new value pairs
//! - [`WatchableFast<T>`] - Optimized for high-contention writes
//!
//! # Collections
//!
//! - [`WatchableMap<K, V>`] - HashMap with change tracking
//! - [`WatchableVec<T>`] - Vec with change tracking
//! - [`WatchableMapLite<K, V>`] - HashMap, epoch-only
//! - [`WatchableVecLite<T>`] - Vec, epoch-only
//!
//! # Example
//!
//! ```
//! use watchable_rs::{Watchable, Watcher};
//!
//! let watchable = Watchable::new(42);
//! let mut watcher = watchable.watch();
//!
//! assert_eq!(watcher.get(), 42);
//!
//! watchable.set(100);
//! assert!(watcher.has_changed());
//! assert_eq!(watcher.get(), 100);
//! ```

use std::{
    collections::HashMap,
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

use crossbeam::queue::ArrayQueue;
use parking_lot::{MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard};

#[cfg(feature = "derive")]
pub use watchable_rs_derive::Watchable;

// ============================================================================
// Error Type
// ============================================================================

/// Error returned when the underlying [`Watchable`] has been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disconnected;

impl std::fmt::Display for Disconnected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "watchable disconnected")
    }
}

impl std::error::Error for Disconnected {}

// ============================================================================
// Change Types
// ============================================================================

/// Change event for map operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapChange<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Clear,
}

/// Change event for vec operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VecChange<T> {
    Push { value: T },
    Pop,
    Insert { index: usize, value: T },
    Remove { index: usize },
    Set { index: usize, value: T },
    Clear,
}

/// Change record with old and new values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueChange<T> {
    pub old: T,
    pub new: T,
}

// ============================================================================
// Watcher Trait
// ============================================================================

/// A handle to observe a value that may change over time.
///
/// Watchers track the "latest known" value and can detect when updates occur.
/// Only the most recent value is accessible - intermediate values may be skipped
/// if the producer updates faster than the consumer polls.
pub trait Watcher: Clone {
    /// The type of value being watched.
    type Value: Clone;

    /// Updates internal state and returns the latest value.
    fn get(&mut self) -> Self::Value {
        self.update();
        self.peek().clone()
    }

    /// Updates internal state, returns `true` if the value changed.
    fn update(&mut self) -> bool;

    /// Returns a reference to the cached value without updating.
    fn peek(&self) -> &Self::Value;

    /// Returns `true` if connected to the underlying watchable.
    fn is_connected(&self) -> bool;

    /// Returns `true` if there's a pending change since last `update()` or `get()`.
    fn has_changed(&self) -> bool;

    /// Polls for the next update.
    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>>;

    /// Returns a future that completes when the value changes.
    fn updated(&mut self) -> UpdatedFut<'_, Self> {
        UpdatedFut { watcher: self }
    }

    /// Returns a future that completes when the value becomes `Some`.
    fn initialized<T>(&mut self) -> InitializedFut<'_, T, Self>
    where
        Self: Watcher<Value = Option<T>>,
        T: Clone,
    {
        InitializedFut {
            initial: self.get(),
            watcher: self,
        }
    }

    /// Converts this watcher into a stream yielding values on change.
    /// The first item is the current value.
    fn stream(mut self) -> WatchStream<Self>
    where
        Self: Sized + Unpin,
    {
        WatchStream {
            initial: Some(self.get()),
            watcher: self,
        }
    }

    /// Converts this watcher into a stream yielding only future updates.
    fn stream_updates_only(self) -> WatchStream<Self>
    where
        Self: Sized + Unpin,
    {
        WatchStream {
            initial: None,
            watcher: self,
        }
    }

    /// Maps this watcher's values through a function.
    fn map<U, F>(self, f: F) -> Map<Self, U, F>
    where
        Self: Sized,
        U: Clone + PartialEq,
        F: Fn(&Self::Value) -> U,
    {
        let current = f(self.peek());
        Map {
            watcher: self,
            map_fn: f,
            current,
        }
    }

    /// Combines this watcher with another, yielding tuples.
    fn and<W: Watcher>(self, other: W) -> And<Self, W>
    where
        Self: Sized,
    {
        And::new(self, other)
    }
}

// ============================================================================
// Watcher Futures and Streams
// ============================================================================

/// Future returned by [`Watcher::updated`].
pub struct UpdatedFut<'a, W: Watcher> {
    watcher: &'a mut W,
}

impl<W: Watcher> Future for UpdatedFut<'_, W> {
    type Output = Result<W::Value, Disconnected>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.watcher.poll_updated(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(self.watcher.peek().clone())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Future returned by [`Watcher::initialized`].
pub struct InitializedFut<'a, T, W: Watcher<Value = Option<T>>> {
    initial: Option<T>,
    watcher: &'a mut W,
}

impl<T: Clone + Unpin, W: Watcher<Value = Option<T>> + Unpin> Future
    for InitializedFut<'_, T, W>
{
    type Output = Result<T, Disconnected>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(value) = self.initial.take() {
            return Poll::Ready(Ok(value));
        }
        loop {
            match self.watcher.poll_updated(cx) {
                Poll::Ready(Ok(())) => {
                    if let Some(value) = self.watcher.peek().clone() {
                        return Poll::Ready(Ok(value));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Stream returned by [`Watcher::stream`] and [`Watcher::stream_updates_only`].
pub struct WatchStream<W: Watcher + Unpin> {
    initial: Option<W::Value>,
    watcher: W,
}

impl<W: Watcher + Unpin> futures_lite::Stream for WatchStream<W>
where
    W::Value: Unpin,
{
    type Item = W::Value;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(value) = self.initial.take() {
            return Poll::Ready(Some(value));
        }
        match self.watcher.poll_updated(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Some(self.watcher.peek().clone())),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ============================================================================
// Watcher Combinators
// ============================================================================

/// Watcher combinator that maps values through a function.
pub struct Map<W: Watcher, U, F> {
    watcher: W,
    map_fn: F,
    current: U,
}

impl<W: Watcher, U: Clone, F: Clone> Clone for Map<W, U, F> {
    fn clone(&self) -> Self {
        Self {
            watcher: self.watcher.clone(),
            map_fn: self.map_fn.clone(),
            current: self.current.clone(),
        }
    }
}

impl<W, U, F> Watcher for Map<W, U, F>
where
    W: Watcher,
    U: Clone + PartialEq,
    F: Fn(&W::Value) -> U + Clone,
{
    type Value = U;

    fn update(&mut self) -> bool {
        if self.watcher.update() {
            let new = (self.map_fn)(self.watcher.peek());
            if new != self.current {
                self.current = new;
                return true;
            }
        }
        false
    }

    fn peek(&self) -> &Self::Value {
        &self.current
    }

    fn is_connected(&self) -> bool {
        self.watcher.is_connected()
    }

    fn has_changed(&self) -> bool {
        self.watcher.has_changed()
    }

    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        loop {
            match self.watcher.poll_updated(cx) {
                Poll::Ready(Ok(())) => {
                    let new = (self.map_fn)(self.watcher.peek());
                    if new != self.current {
                        self.current = new;
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Watcher combinator that combines two watchers into a tuple.
pub struct And<A: Watcher, B: Watcher> {
    a: A,
    b: B,
    current: (A::Value, B::Value),
}

impl<A: Watcher, B: Watcher> Clone for And<A, B> {
    fn clone(&self) -> Self {
        Self {
            a: self.a.clone(),
            b: self.b.clone(),
            current: self.current.clone(),
        }
    }
}

impl<A: Watcher, B: Watcher> And<A, B> {
    fn new(mut a: A, mut b: B) -> Self {
        let current = (a.get(), b.get());
        Self { a, b, current }
    }
}

impl<A: Watcher, B: Watcher> Watcher for And<A, B> {
    type Value = (A::Value, B::Value);

    fn update(&mut self) -> bool {
        let a_updated = self.a.update();
        let b_updated = self.b.update();
        if a_updated || b_updated {
            self.current = (self.a.peek().clone(), self.b.peek().clone());
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &Self::Value {
        &self.current
    }

    fn is_connected(&self) -> bool {
        self.a.is_connected() && self.b.is_connected()
    }

    fn has_changed(&self) -> bool {
        self.a.has_changed() || self.b.has_changed()
    }

    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        let poll_a = self.a.poll_updated(cx);
        let poll_b = self.b.poll_updated(cx);

        match (&poll_a, &poll_b) {
            (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => {
                return Poll::Ready(Err(*e));
            }
            (Poll::Pending, Poll::Pending) => return Poll::Pending,
            _ => {}
        }

        if matches!(poll_a, Poll::Ready(Ok(()))) {
            self.current.0 = self.a.peek().clone();
        }
        if matches!(poll_b, Poll::Ready(Ok(()))) {
            self.current.1 = self.b.peek().clone();
        }
        Poll::Ready(Ok(()))
    }
}

/// Watcher combinator for joining multiple watchers of the same type.
pub struct Join<W: Watcher> {
    watchers: Vec<W>,
    current: Vec<W::Value>,
}

impl<W: Watcher> Clone for Join<W> {
    fn clone(&self) -> Self {
        Self {
            watchers: self.watchers.clone(),
            current: self.current.clone(),
        }
    }
}

impl<W: Watcher> Join<W> {
    /// Creates a new `Join` from an iterator of watchers.
    pub fn new(watchers: impl IntoIterator<Item = W>) -> Self {
        let mut watchers: Vec<W> = watchers.into_iter().collect();
        let current: Vec<W::Value> = watchers.iter_mut().map(|w| w.get()).collect();
        Self { watchers, current }
    }
}

impl<W: Watcher> Watcher for Join<W> {
    type Value = Vec<W::Value>;

    fn update(&mut self) -> bool {
        let mut any_updated = false;
        for (watcher, value) in self.watchers.iter_mut().zip(self.current.iter_mut()) {
            if watcher.update() {
                *value = watcher.peek().clone();
                any_updated = true;
            }
        }
        any_updated
    }

    fn peek(&self) -> &Self::Value {
        &self.current
    }

    fn is_connected(&self) -> bool {
        self.watchers.iter().all(|w| w.is_connected())
    }

    fn has_changed(&self) -> bool {
        self.watchers.iter().any(|w| w.has_changed())
    }

    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        let mut any_ready = false;
        for (watcher, value) in self.watchers.iter_mut().zip(self.current.iter_mut()) {
            match watcher.poll_updated(cx) {
                Poll::Ready(Ok(())) => {
                    *value = watcher.peek().clone();
                    any_ready = true;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }
        if any_ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

// ============================================================================
// Watchable<T> - General Purpose
// ============================================================================

const DEFAULT_BUFFER_SIZE: usize = 1024;

/// A wrapper around a value that notifies watchers when modified.
///
/// This is the general-purpose variant with full async support.
#[derive(Debug)]
pub struct Watchable<T> {
    shared: Arc<WatchableShared<T>>,
}

#[derive(Debug)]
struct WatchableShared<T> {
    state: RwLock<WatchableState<T>>,
    watchers: Mutex<Vec<Waker>>,
}

#[derive(Debug, Clone)]
struct WatchableState<T> {
    value: T,
    epoch: u64,
}

impl<T> Clone for Watchable<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Clone + Default> Default for Watchable<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> Watchable<T> {
    /// Creates a new watchable with the given initial value.
    pub fn new(value: T) -> Self {
        Self {
            shared: Arc::new(WatchableShared {
                state: RwLock::new(WatchableState { value, epoch: 1 }),
                watchers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Sets a new value and notifies all watchers.
    #[inline]
    pub fn set(&self, value: T) {
        {
            let mut state = self.shared.state.write();
            state.value = value;
            state.epoch += 1;
        }
        self.wake_all();
    }

    /// Sets a new value only if it differs from the current value.
    /// Returns `Ok(old_value)` if changed, `Err(value)` if unchanged.
    #[inline]
    pub fn set_if_changed(&self, value: T) -> Result<T, T>
    where
        T: PartialEq,
    {
        let mut state = self.shared.state.write();
        if state.value != value {
            let old = std::mem::replace(&mut state.value, value);
            state.epoch += 1;
            drop(state);
            self.wake_all();
            Ok(old)
        } else {
            Err(value)
        }
    }

    /// Modifies the value in place and notifies watchers.
    #[inline]
    pub fn modify<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = {
            let mut state = self.shared.state.write();
            let result = f(&mut state.value);
            state.epoch += 1;
            result
        };
        self.wake_all();
        result
    }

    /// Returns a clone of the current value.
    #[inline]
    pub fn get(&self) -> T {
        self.shared.state.read().value.clone()
    }

    /// Returns a read guard to the value.
    #[inline]
    pub fn read(&self) -> MappedRwLockReadGuard<'_, T> {
        RwLockReadGuard::map(self.shared.state.read(), |s| &s.value)
    }

    /// Creates a watcher for this value.
    pub fn watch(&self) -> Direct<T> {
        let state = self.shared.state.read();
        Direct {
            shared: Arc::downgrade(&self.shared),
            state: state.clone(),
        }
    }

    /// Returns `true` if there are any active watchers.
    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.shared) > 0
    }

    fn wake_all(&self) {
        if let Some(mut watchers) = self.shared.watchers.try_lock() {
            for waker in watchers.drain(..) {
                waker.wake();
            }
        }
    }
}

impl<T> Drop for Watchable<T> {
    fn drop(&mut self) {
        // Wake all watchers so they can detect disconnection
        if let Some(mut watchers) = self.shared.watchers.try_lock() {
            for waker in watchers.drain(..) {
                waker.wake();
            }
        }
    }
}

/// Direct watcher for a [`Watchable`].
#[derive(Debug)]
pub struct Direct<T> {
    shared: Weak<WatchableShared<T>>,
    state: WatchableState<T>,
}

impl<T: Clone> Clone for Direct<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T: Clone> Watcher for Direct<T> {
    type Value = T;

    fn update(&mut self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        let state = shared.state.read();
        if state.epoch > self.state.epoch {
            self.state = state.clone();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &Self::Value {
        &self.state.value
    }

    fn is_connected(&self) -> bool {
        self.shared.upgrade().is_some()
    }

    fn has_changed(&self) -> bool {
        self.shared
            .upgrade()
            .map(|s| s.state.read().epoch > self.state.epoch)
            .unwrap_or(false)
    }

    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        let Some(shared) = self.shared.upgrade() else {
            return Poll::Ready(Err(Disconnected));
        };

        {
            let state = shared.state.read();
            if state.epoch > self.state.epoch {
                self.state = state.clone();
                return Poll::Ready(Ok(()));
            }
        }

        shared.watchers.lock().push(cx.waker().clone());

        // Re-check after registering
        {
            let state = shared.state.read();
            if state.epoch > self.state.epoch {
                self.state = state.clone();
                return Poll::Ready(Ok(()));
            }
        }

        Poll::Pending
    }
}

// ============================================================================
// WatchableLite<T> - Minimal Overhead
// ============================================================================

/// Lightweight watchable with minimal overhead.
///
/// Only tracks that something changed, not async-wake capable.
/// Use polling for change detection.
#[derive(Debug)]
pub struct WatchableLite<T> {
    data: Arc<RwLock<T>>,
    epoch: Arc<AtomicU64>,
}

impl<T> Clone for WatchableLite<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
        }
    }
}

impl<T: Clone + Default> Default for WatchableLite<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> WatchableLite<T> {
    pub fn new(value: T) -> Self {
        Self {
            data: Arc::new(RwLock::new(value)),
            epoch: Arc::new(AtomicU64::new(1)),
        }
    }

    #[inline]
    pub fn set(&self, value: T) {
        *self.data.write() = value;
        self.epoch.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn get(&self) -> T {
        self.data.read().clone()
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.data.read()
    }

    #[inline]
    pub fn modify<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = f(&mut *self.data.write());
        self.epoch.fetch_add(1, Ordering::Release);
        result
    }

    pub fn watch(&self) -> DirectLite<T> {
        DirectLite {
            data: Arc::downgrade(&self.data),
            epoch: Arc::downgrade(&self.epoch),
            last_epoch: self.epoch.load(Ordering::Acquire),
            cached: self.data.read().clone(),
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.epoch) > 0
    }
}

/// Direct watcher for [`WatchableLite`].
pub struct DirectLite<T> {
    data: Weak<RwLock<T>>,
    epoch: Weak<AtomicU64>,
    last_epoch: u64,
    cached: T,
}

impl<T: Clone> Clone for DirectLite<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
            last_epoch: self.last_epoch,
            cached: self.cached.clone(),
        }
    }
}

impl<T: Clone> Watcher for DirectLite<T> {
    type Value = T;

    fn update(&mut self) -> bool {
        let (Some(data), Some(epoch)) = (self.data.upgrade(), self.epoch.upgrade()) else {
            return false;
        };
        let current = epoch.load(Ordering::Acquire);
        if current > self.last_epoch {
            self.last_epoch = current;
            self.cached = data.read().clone();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &Self::Value {
        &self.cached
    }

    fn is_connected(&self) -> bool {
        self.epoch.upgrade().is_some()
    }

    fn has_changed(&self) -> bool {
        self.epoch
            .upgrade()
            .map(|e| e.load(Ordering::Acquire) > self.last_epoch)
            .unwrap_or(false)
    }

    fn poll_updated(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        if !self.is_connected() {
            return Poll::Ready(Err(Disconnected));
        }
        if self.update() {
            Poll::Ready(Ok(()))
        } else {
            // Lite variant doesn't support async waiting
            Poll::Pending
        }
    }
}

// ============================================================================
// WatchableAtomic<T> - Lock-free for Copy types
// ============================================================================

/// Lock-free watchable for small `Copy` types (up to 8 bytes).
#[derive(Debug)]
pub struct WatchableAtomic<T: Copy> {
    data: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Copy> Clone for WatchableAtomic<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Copy + Default> Default for WatchableAtomic<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Copy> WatchableAtomic<T> {
    pub fn new(value: T) -> Self {
        assert!(
            std::mem::size_of::<T>() <= std::mem::size_of::<u64>(),
            "WatchableAtomic only supports types up to 8 bytes"
        );
        Self {
            data: Arc::new(AtomicU64::new(Self::to_bits(value))),
            epoch: Arc::new(AtomicU64::new(1)),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    fn to_bits(value: T) -> u64 {
        let mut bits: u64 = 0;
        // SAFETY: T is Copy and guaranteed to be <= 8 bytes by the constructor assertion.
        // We copy exactly size_of::<T>() bytes from a valid T into a zeroed u64.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &value as *const T as *const u8,
                &mut bits as *mut u64 as *mut u8,
                std::mem::size_of::<T>(),
            );
        }
        bits
    }

    #[inline]
    fn from_bits(bits: u64) -> T {
        let mut value = std::mem::MaybeUninit::<T>::uninit();
        // SAFETY: T is Copy and guaranteed to be <= 8 bytes by the constructor assertion.
        // We copy exactly size_of::<T>() bytes from a valid u64 into a properly aligned
        // MaybeUninit<T>. The remaining bytes (if T < 8 bytes) are unused padding.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &bits as *const u64 as *const u8,
                value.as_mut_ptr() as *mut u8,
                std::mem::size_of::<T>(),
            );
            value.assume_init()
        }
    }

    #[inline]
    pub fn set(&self, value: T) {
        self.data.store(Self::to_bits(value), Ordering::Release);
        self.epoch.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn get(&self) -> T {
        Self::from_bits(self.data.load(Ordering::Acquire))
    }

    pub fn watch(&self) -> DirectAtomic<T> {
        DirectAtomic {
            data: Arc::downgrade(&self.data),
            epoch: Arc::downgrade(&self.epoch),
            last_epoch: self.epoch.load(Ordering::Acquire),
            cached: self.get(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.epoch) > 0
    }
}

/// Direct watcher for [`WatchableAtomic`].
pub struct DirectAtomic<T: Copy> {
    data: Weak<AtomicU64>,
    epoch: Weak<AtomicU64>,
    last_epoch: u64,
    cached: T,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Copy> Clone for DirectAtomic<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
            last_epoch: self.last_epoch,
            cached: self.cached,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: Copy> Watcher for DirectAtomic<T> {
    type Value = T;

    fn update(&mut self) -> bool {
        let (Some(data), Some(epoch)) = (self.data.upgrade(), self.epoch.upgrade()) else {
            return false;
        };
        let current = epoch.load(Ordering::Acquire);
        if current > self.last_epoch {
            self.last_epoch = current;
            self.cached = WatchableAtomic::<T>::from_bits(data.load(Ordering::Acquire));
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &Self::Value {
        &self.cached
    }

    fn is_connected(&self) -> bool {
        self.epoch.upgrade().is_some()
    }

    fn has_changed(&self) -> bool {
        self.epoch
            .upgrade()
            .map(|e| e.load(Ordering::Acquire) > self.last_epoch)
            .unwrap_or(false)
    }

    fn poll_updated(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        if !self.is_connected() {
            return Poll::Ready(Err(Disconnected));
        }
        if self.update() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

// ============================================================================
// WatchableFast<T> - Optimized for concurrent writes
// ============================================================================

/// Watchable optimized for high-contention concurrent writes.
///
/// Uses a single lock for value+epoch with parking_lot.
#[derive(Debug)]
pub struct WatchableFast<T> {
    shared: Arc<FastShared<T>>,
}

struct FastShared<T> {
    state: RwLock<FastState<T>>,
    watchers: Mutex<Vec<Waker>>,
}

impl<T> std::fmt::Debug for FastShared<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastShared").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct FastState<T> {
    value: T,
    epoch: u64,
}

impl<T> Clone for WatchableFast<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Clone + Default> Default for WatchableFast<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> WatchableFast<T> {
    pub fn new(value: T) -> Self {
        Self {
            shared: Arc::new(FastShared {
                state: RwLock::new(FastState { value, epoch: 1 }),
                watchers: Mutex::new(Vec::new()),
            }),
        }
    }

    #[inline]
    pub fn set(&self, value: T) {
        {
            let mut state = self.shared.state.write();
            state.value = value;
            state.epoch += 1;
        }
        if let Some(mut watchers) = self.shared.watchers.try_lock() {
            for waker in watchers.drain(..) {
                waker.wake();
            }
        }
    }

    #[inline]
    pub fn set_if_changed(&self, value: T) -> bool
    where
        T: PartialEq,
    {
        let changed = {
            let mut state = self.shared.state.write();
            if state.value != value {
                state.value = value;
                state.epoch += 1;
                true
            } else {
                false
            }
        };
        if changed {
            if let Some(mut watchers) = self.shared.watchers.try_lock() {
                for waker in watchers.drain(..) {
                    waker.wake();
                }
            }
        }
        changed
    }

    #[inline]
    pub fn get(&self) -> T {
        self.shared.state.read().value.clone()
    }

    #[inline]
    pub fn read(&self) -> MappedRwLockReadGuard<'_, T> {
        RwLockReadGuard::map(self.shared.state.read(), |s| &s.value)
    }

    #[inline]
    pub fn modify<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let result = {
            let mut state = self.shared.state.write();
            let result = f(&mut state.value);
            state.epoch += 1;
            result
        };
        if let Some(mut watchers) = self.shared.watchers.try_lock() {
            for waker in watchers.drain(..) {
                waker.wake();
            }
        }
        result
    }

    pub fn watch(&self) -> DirectFast<T> {
        let state = self.shared.state.read();
        DirectFast {
            shared: Arc::downgrade(&self.shared),
            last_epoch: state.epoch,
            cached: state.value.clone(),
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.shared) > 0
    }
}

/// Direct watcher for [`WatchableFast`].
pub struct DirectFast<T> {
    shared: Weak<FastShared<T>>,
    last_epoch: u64,
    cached: T,
}

impl<T: Clone> Clone for DirectFast<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            last_epoch: self.last_epoch,
            cached: self.cached.clone(),
        }
    }
}

impl<T: Clone> Watcher for DirectFast<T> {
    type Value = T;

    fn update(&mut self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        let state = shared.state.read();
        if state.epoch > self.last_epoch {
            self.last_epoch = state.epoch;
            self.cached = state.value.clone();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> &Self::Value {
        &self.cached
    }

    fn is_connected(&self) -> bool {
        self.shared.upgrade().is_some()
    }

    fn has_changed(&self) -> bool {
        self.shared
            .upgrade()
            .map(|s| s.state.read().epoch > self.last_epoch)
            .unwrap_or(false)
    }

    fn poll_updated(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Disconnected>> {
        let Some(shared) = self.shared.upgrade() else {
            return Poll::Ready(Err(Disconnected));
        };

        {
            let state = shared.state.read();
            if state.epoch > self.last_epoch {
                self.last_epoch = state.epoch;
                self.cached = state.value.clone();
                return Poll::Ready(Ok(()));
            }
        }

        shared.watchers.lock().push(cx.waker().clone());

        {
            let state = shared.state.read();
            if state.epoch > self.last_epoch {
                self.last_epoch = state.epoch;
                self.cached = state.value.clone();
                return Poll::Ready(Ok(()));
            }
        }

        Poll::Pending
    }
}

// ============================================================================
// WatchableWithHistory<T> - Tracks changes
// ============================================================================

/// Watchable that tracks the history of changes (old to new).
pub struct WatchableWithHistory<T> {
    shared: Arc<HistoryShared<T, ValueChange<T>>>,
}

struct HistoryShared<T, C> {
    data: RwLock<T>,
    changes: ArrayQueue<C>,
    epoch: AtomicU64,
    waker_epoch: AtomicU64,
    watchers: Mutex<Vec<Waker>>,
    has_watchers: AtomicBool,
}

impl<T> Clone for WatchableWithHistory<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static + Default> Default for WatchableWithHistory<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone + Send + Sync + 'static> WatchableWithHistory<T> {
    pub fn new(value: T) -> Self {
        Self::with_capacity(value, DEFAULT_BUFFER_SIZE)
    }

    pub fn with_capacity(value: T, buffer_size: usize) -> Self {
        Self {
            shared: Arc::new(HistoryShared {
                data: RwLock::new(value),
                changes: ArrayQueue::new(buffer_size),
                epoch: AtomicU64::new(1),
                waker_epoch: AtomicU64::new(0),
                watchers: Mutex::new(Vec::new()),
                has_watchers: AtomicBool::new(false),
            }),
        }
    }

    #[inline]
    pub fn set(&self, value: T) {
        let old = {
            let mut guard = self.shared.data.write();
            std::mem::replace(&mut *guard, value.clone())
        };
        self.push_change(ValueChange { old, new: value });
    }

    #[inline]
    pub fn get(&self) -> T {
        self.shared.data.read().clone()
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.shared.data.read()
    }

    #[inline]
    pub fn modify<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let (result, old, new) = {
            let mut guard = self.shared.data.write();
            let old = guard.clone();
            let result = f(&mut *guard);
            let new = guard.clone();
            (result, old, new)
        };
        self.push_change(ValueChange { old, new });
        result
    }

    /// Creates a watcher that receives change events.
    pub fn watch_changes(&self) -> ChangeWatcher<ValueChange<T>> {
        ChangeWatcher {
            shared: Arc::downgrade(&self.shared)
                as Weak<dyn ChangeSource<ValueChange<T>> + Send + Sync>,
            last_epoch: self.shared.epoch.load(Ordering::Acquire),
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.shared) > 0
    }

    fn push_change(&self, change: ValueChange<T>) {
        if self.shared.changes.push(change).is_err() {
            let _ = self.shared.changes.pop();
        }
        let epoch = self.shared.epoch.fetch_add(1, Ordering::Release);
        if self.shared.has_watchers.load(Ordering::Acquire) {
            self.wake_all(epoch + 1);
        }
    }

    fn wake_all(&self, current_epoch: u64) {
        let last = self
            .shared
            .waker_epoch
            .swap(current_epoch, Ordering::AcqRel);
        if last >= current_epoch {
            return;
        }
        let mut watchers = self.shared.watchers.lock();
        for waker in watchers.drain(..) {
            waker.wake();
        }
    }
}

// ============================================================================
// Change Watcher (for history-tracking types)
// ============================================================================

trait ChangeSource<C> {
    fn current_epoch(&self) -> u64;
    fn drain_changes(&self, since: u64) -> (Vec<C>, u64, bool);
    fn register_waker(&self, waker: &Waker);
}

/// Watcher for change events from history-tracking watchables.
pub struct ChangeWatcher<C> {
    shared: Weak<dyn ChangeSource<C> + Send + Sync>,
    last_epoch: u64,
}

impl<C> Clone for ChangeWatcher<C> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            last_epoch: self.last_epoch,
        }
    }
}

impl<C> ChangeWatcher<C> {
    /// Returns `true` if connected to the source.
    pub fn is_connected(&self) -> bool {
        self.shared.upgrade().is_some()
    }

    /// Polls for changes, returning `(changes, missed)`.
    pub fn poll_changes(&mut self) -> Option<(Vec<C>, bool)> {
        let shared = self.shared.upgrade()?;
        let (changes, new_epoch, missed) = shared.drain_changes(self.last_epoch);
        if !changes.is_empty() || missed {
            self.last_epoch = new_epoch;
            Some((changes, missed))
        } else {
            None
        }
    }

    /// Returns a future that resolves with the next batch of changes.
    pub fn next_changes(&mut self) -> NextChangesFut<'_, C> {
        NextChangesFut { watcher: self }
    }

    /// Converts into a stream of change batches.
    pub fn into_stream(self) -> ChangeStream<C> {
        ChangeStream { watcher: self }
    }

    /// Resets the watcher to the current epoch.
    pub fn reset(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            self.last_epoch = shared.current_epoch();
        }
    }
}

/// Future for [`ChangeWatcher::next_changes`].
pub struct NextChangesFut<'a, C> {
    watcher: &'a mut ChangeWatcher<C>,
}

impl<C> Future for NextChangesFut<'_, C> {
    type Output = Result<(Vec<C>, bool), Disconnected>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(shared) = self.watcher.shared.upgrade() else {
            return Poll::Ready(Err(Disconnected));
        };

        let (changes, new_epoch, missed) = shared.drain_changes(self.watcher.last_epoch);
        if !changes.is_empty() || missed {
            self.watcher.last_epoch = new_epoch;
            return Poll::Ready(Ok((changes, missed)));
        }

        shared.register_waker(cx.waker());

        let (changes, new_epoch, missed) = shared.drain_changes(self.watcher.last_epoch);
        if !changes.is_empty() || missed {
            self.watcher.last_epoch = new_epoch;
            return Poll::Ready(Ok((changes, missed)));
        }

        Poll::Pending
    }
}

/// Stream for [`ChangeWatcher::into_stream`].
pub struct ChangeStream<C> {
    watcher: ChangeWatcher<C>,
}

impl<C: Unpin> futures_lite::Stream for ChangeStream<C> {
    type Item = (Vec<C>, bool);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(shared) = self.watcher.shared.upgrade() else {
            return Poll::Ready(None);
        };

        let (changes, new_epoch, missed) = shared.drain_changes(self.watcher.last_epoch);
        if !changes.is_empty() || missed {
            self.watcher.last_epoch = new_epoch;
            return Poll::Ready(Some((changes, missed)));
        }

        shared.register_waker(cx.waker());

        let (changes, new_epoch, missed) = shared.drain_changes(self.watcher.last_epoch);
        if !changes.is_empty() || missed {
            self.watcher.last_epoch = new_epoch;
            return Poll::Ready(Some((changes, missed)));
        }

        Poll::Pending
    }
}

// ============================================================================
// WatchableMap<K, V> - HashMap with change tracking
// ============================================================================

/// Observable HashMap with change tracking.
pub struct WatchableMap<K, V> {
    shared: Arc<HistoryShared<HashMap<K, V>, MapChange<K, V>>>,
}

impl<K, V> Clone for WatchableMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<K, V> Default for WatchableMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> WatchableMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_SIZE)
    }

    pub fn with_capacity(buffer_size: usize) -> Self {
        Self {
            shared: Arc::new(HistoryShared {
                data: RwLock::new(HashMap::new()),
                changes: ArrayQueue::new(buffer_size),
                epoch: AtomicU64::new(1),
                waker_epoch: AtomicU64::new(0),
                watchers: Mutex::new(Vec::new()),
                has_watchers: AtomicBool::new(false),
            }),
        }
    }

    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let old = self.shared.data.write().insert(key.clone(), value.clone());
        self.push_change(MapChange::Insert { key, value });
        old
    }

    #[inline]
    pub fn remove(&self, key: &K) -> Option<V> {
        let removed = self.shared.data.write().remove(key);
        if removed.is_some() {
            self.push_change(MapChange::Remove { key: key.clone() });
        }
        removed
    }

    pub fn clear(&self) {
        let mut data = self.shared.data.write();
        if !data.is_empty() {
            data.clear();
            drop(data);
            while self.shared.changes.pop().is_some() {}
            self.push_change(MapChange::Clear);
        }
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.shared.data.read().get(key).cloned()
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.shared.data.read().contains_key(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared.data.read().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.data.read().is_empty()
    }

    pub fn snapshot(&self) -> HashMap<K, V> {
        self.shared.data.read().clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, HashMap<K, V>> {
        self.shared.data.read()
    }

    pub fn watch_changes(&self) -> ChangeWatcher<MapChange<K, V>> {
        ChangeWatcher {
            shared: Arc::downgrade(&self.shared)
                as Weak<dyn ChangeSource<MapChange<K, V>> + Send + Sync>,
            last_epoch: self.shared.epoch.load(Ordering::Acquire),
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.shared) > 0
    }

    fn push_change(&self, change: MapChange<K, V>) {
        if self.shared.changes.push(change).is_err() {
            let _ = self.shared.changes.pop();
        }
        let epoch = self.shared.epoch.fetch_add(1, Ordering::Release);
        if self.shared.has_watchers.load(Ordering::Acquire) {
            let current = epoch + 1;
            let last = self.shared.waker_epoch.swap(current, Ordering::AcqRel);
            if last < current {
                for waker in self.shared.watchers.lock().drain(..) {
                    waker.wake();
                }
            }
        }
    }
}

// ============================================================================
// WatchableVec<T> - Vec with change tracking
// ============================================================================

/// Observable Vec with change tracking.
pub struct WatchableVec<T> {
    shared: Arc<HistoryShared<Vec<T>, VecChange<T>>>,
}

impl<T> Clone for WatchableVec<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Default for WatchableVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + Sync + 'static> WatchableVec<T> {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_SIZE)
    }

    pub fn with_capacity(buffer_size: usize) -> Self {
        Self {
            shared: Arc::new(HistoryShared {
                data: RwLock::new(Vec::new()),
                changes: ArrayQueue::new(buffer_size),
                epoch: AtomicU64::new(1),
                waker_epoch: AtomicU64::new(0),
                watchers: Mutex::new(Vec::new()),
                has_watchers: AtomicBool::new(false),
            }),
        }
    }

    #[inline]
    pub fn push(&self, value: T) {
        self.shared.data.write().push(value.clone());
        self.push_change(VecChange::Push { value });
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let popped = self.shared.data.write().pop();
        if popped.is_some() {
            self.push_change(VecChange::Pop);
        }
        popped
    }

    #[inline]
    pub fn insert(&self, index: usize, value: T) {
        self.shared.data.write().insert(index, value.clone());
        self.push_change(VecChange::Insert { index, value });
    }

    #[inline]
    pub fn remove(&self, index: usize) -> T {
        let removed = self.shared.data.write().remove(index);
        self.push_change(VecChange::Remove { index });
        removed
    }

    #[inline]
    pub fn set(&self, index: usize, value: T) {
        self.shared.data.write()[index] = value.clone();
        self.push_change(VecChange::Set { index, value });
    }

    pub fn clear(&self) {
        let mut data = self.shared.data.write();
        if !data.is_empty() {
            data.clear();
            drop(data);
            while self.shared.changes.pop().is_some() {}
            self.push_change(VecChange::Clear);
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<T> {
        self.shared.data.read().get(index).cloned()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared.data.read().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.data.read().is_empty()
    }

    pub fn snapshot(&self) -> Vec<T> {
        self.shared.data.read().clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Vec<T>> {
        self.shared.data.read()
    }

    pub fn watch_changes(&self) -> ChangeWatcher<VecChange<T>> {
        ChangeWatcher {
            shared: Arc::downgrade(&self.shared)
                as Weak<dyn ChangeSource<VecChange<T>> + Send + Sync>,
            last_epoch: self.shared.epoch.load(Ordering::Acquire),
        }
    }

    pub fn has_watchers(&self) -> bool {
        Arc::weak_count(&self.shared) > 0
    }

    fn push_change(&self, change: VecChange<T>) {
        if self.shared.changes.push(change).is_err() {
            let _ = self.shared.changes.pop();
        }
        let epoch = self.shared.epoch.fetch_add(1, Ordering::Release);
        if self.shared.has_watchers.load(Ordering::Acquire) {
            let current = epoch + 1;
            let last = self.shared.waker_epoch.swap(current, Ordering::AcqRel);
            if last < current {
                for waker in self.shared.watchers.lock().drain(..) {
                    waker.wake();
                }
            }
        }
    }
}

impl<D, C: Send + Sync> ChangeSource<C> for HistoryShared<D, C> {
    fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn drain_changes(&self, since: u64) -> (Vec<C>, u64, bool) {
        let current = self.current_epoch();
        if since >= current {
            return (Vec::new(), current, false);
        }
        let expected = (current - since) as usize;
        let mut changes = Vec::with_capacity(expected.min(self.changes.capacity()));
        while let Some(change) = self.changes.pop() {
            changes.push(change);
        }
        let missed = changes.len() < expected;
        (changes, current, missed)
    }

    fn register_waker(&self, waker: &Waker) {
        self.has_watchers.store(true, Ordering::Release);
        self.watchers.lock().push(waker.clone());
    }
}

// ============================================================================
// WatchableMapLite / WatchableVecLite - Epoch-only collections
// ============================================================================

/// Lightweight observable HashMap (epoch-only, no change tracking).
#[derive(Debug)]
pub struct WatchableMapLite<K, V> {
    data: Arc<RwLock<HashMap<K, V>>>,
    epoch: Arc<AtomicU64>,
    watchers: Arc<Mutex<Vec<Waker>>>,
    has_watchers: Arc<AtomicBool>,
}

impl<K, V> Clone for WatchableMapLite<K, V> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
            watchers: self.watchers.clone(),
            has_watchers: self.has_watchers.clone(),
        }
    }
}

impl<K: Eq + Hash, V> Default for WatchableMapLite<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> WatchableMapLite<K, V> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            epoch: Arc::new(AtomicU64::new(1)),
            watchers: Arc::new(Mutex::new(Vec::new())),
            has_watchers: Arc::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    fn notify(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        if !self.has_watchers.load(Ordering::Acquire) {
            return;
        }
        if let Some(mut watchers) = self.watchers.try_lock() {
            if watchers.is_empty() {
                self.has_watchers.store(false, Ordering::Release);
            } else {
                for waker in watchers.drain(..) {
                    waker.wake();
                }
            }
        }
    }

    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let old = self.data.write().insert(key, value);
        self.notify();
        old
    }

    #[inline]
    pub fn remove(&self, key: &K) -> Option<V> {
        let removed = self.data.write().remove(key);
        if removed.is_some() {
            self.notify();
        }
        removed
    }

    pub fn clear(&self) {
        let mut data = self.data.write();
        if !data.is_empty() {
            data.clear();
            drop(data);
            self.notify();
        }
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.data.read().get(key).cloned()
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.data.read().contains_key(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    pub fn snapshot(&self) -> HashMap<K, V>
    where
        K: Clone,
        V: Clone,
    {
        self.data.read().clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, HashMap<K, V>> {
        self.data.read()
    }

    pub fn watch(&self) -> EpochWatcher {
        EpochWatcher {
            epoch: Arc::downgrade(&self.epoch),
            watchers: Arc::downgrade(&self.watchers),
            has_watchers: Arc::downgrade(&self.has_watchers),
            last_epoch: self.epoch.load(Ordering::Acquire),
        }
    }
}

/// Lightweight observable Vec (epoch-only, no change tracking).
#[derive(Debug)]
pub struct WatchableVecLite<T> {
    data: Arc<RwLock<Vec<T>>>,
    epoch: Arc<AtomicU64>,
    watchers: Arc<Mutex<Vec<Waker>>>,
    has_watchers: Arc<AtomicBool>,
}

impl<T> Clone for WatchableVecLite<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            epoch: self.epoch.clone(),
            watchers: self.watchers.clone(),
            has_watchers: self.has_watchers.clone(),
        }
    }
}

impl<T> Default for WatchableVecLite<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WatchableVecLite<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(Vec::new())),
            epoch: Arc::new(AtomicU64::new(1)),
            watchers: Arc::new(Mutex::new(Vec::new())),
            has_watchers: Arc::new(AtomicBool::new(false)),
        }
    }

    #[inline]
    fn notify(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        if !self.has_watchers.load(Ordering::Acquire) {
            return;
        }
        if let Some(mut watchers) = self.watchers.try_lock() {
            if watchers.is_empty() {
                self.has_watchers.store(false, Ordering::Release);
            } else {
                for waker in watchers.drain(..) {
                    waker.wake();
                }
            }
        }
    }

    #[inline]
    pub fn push(&self, value: T) {
        self.data.write().push(value);
        self.notify();
    }

    #[inline]
    pub fn pop(&self) -> Option<T> {
        let popped = self.data.write().pop();
        if popped.is_some() {
            self.notify();
        }
        popped
    }

    #[inline]
    pub fn insert(&self, index: usize, value: T) {
        self.data.write().insert(index, value);
        self.notify();
    }

    #[inline]
    pub fn remove(&self, index: usize) -> T {
        let removed = self.data.write().remove(index);
        self.notify();
        removed
    }

    #[inline]
    pub fn set(&self, index: usize, value: T) {
        self.data.write()[index] = value;
        self.notify();
    }

    pub fn clear(&self) {
        let mut data = self.data.write();
        if !data.is_empty() {
            data.clear();
            drop(data);
            self.notify();
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<T>
    where
        T: Clone,
    {
        self.data.read().get(index).cloned()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.data.read().clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Vec<T>> {
        self.data.read()
    }

    pub fn watch(&self) -> EpochWatcher {
        EpochWatcher {
            epoch: Arc::downgrade(&self.epoch),
            watchers: Arc::downgrade(&self.watchers),
            has_watchers: Arc::downgrade(&self.has_watchers),
            last_epoch: self.epoch.load(Ordering::Acquire),
        }
    }
}

// ============================================================================
// EpochWatcher - For Lite variants
// ============================================================================

/// Watcher that only tracks *that* something changed, not *what*.
#[derive(Clone)]
pub struct EpochWatcher {
    epoch: Weak<AtomicU64>,
    watchers: Weak<Mutex<Vec<Waker>>>,
    has_watchers: Weak<AtomicBool>,
    last_epoch: u64,
}

impl EpochWatcher {
    pub fn is_connected(&self) -> bool {
        self.epoch.upgrade().is_some()
    }

    pub fn has_changed(&self) -> bool {
        self.epoch
            .upgrade()
            .map(|e| e.load(Ordering::Acquire) > self.last_epoch)
            .unwrap_or(false)
    }

    /// Returns `Some(true)` if changed, `Some(false)` if not, `None` if disconnected.
    pub fn poll_changed(&mut self) -> Option<bool> {
        let epoch = self.epoch.upgrade()?;
        let current = epoch.load(Ordering::Acquire);
        if current > self.last_epoch {
            self.last_epoch = current;
            Some(true)
        } else {
            Some(false)
        }
    }

    /// Async wait for next change.
    pub fn changed(&mut self) -> EpochChangedFut<'_> {
        EpochChangedFut { watcher: self }
    }

    pub fn reset(&mut self) {
        if let Some(epoch) = self.epoch.upgrade() {
            self.last_epoch = epoch.load(Ordering::Acquire);
        }
    }
}

/// Future for [`EpochWatcher::changed`].
pub struct EpochChangedFut<'a> {
    watcher: &'a mut EpochWatcher,
}

impl Future for EpochChangedFut<'_> {
    type Output = Result<(), Disconnected>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(epoch) = self.watcher.epoch.upgrade() else {
            return Poll::Ready(Err(Disconnected));
        };

        let current = epoch.load(Ordering::Acquire);
        if current > self.watcher.last_epoch {
            self.watcher.last_epoch = current;
            return Poll::Ready(Ok(()));
        }

        if let (Some(watchers), Some(has_watchers)) = (
            self.watcher.watchers.upgrade(),
            self.watcher.has_watchers.upgrade(),
        ) {
            has_watchers.store(true, Ordering::Release);
            watchers.lock().push(cx.waker().clone());
        }

        let current = epoch.load(Ordering::Acquire);
        if current > self.watcher.last_epoch {
            self.watcher.last_epoch = current;
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

// ============================================================================
// Arc-wrapped variants (avoid cloning large values)
// ============================================================================

/// Observable map with Arc-wrapped values.
pub type WatchableMapArc<K, V> = WatchableMap<K, Arc<V>>;

/// Observable vec with Arc-wrapped values.
pub type WatchableVecArc<T> = WatchableVec<Arc<T>>;

/// Helper trait for inserting non-Arc values into Arc-wrapped collections.
pub trait InsertArc<K, V> {
    fn insert_arc(&self, key: K, value: V) -> Option<Arc<V>>;
}

impl<K, V> InsertArc<K, V> for WatchableMap<K, Arc<V>>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn insert_arc(&self, key: K, value: V) -> Option<Arc<V>> {
        self.insert(key, Arc::new(value))
    }
}

/// Helper trait for pushing non-Arc values into Arc-wrapped vecs.
pub trait PushArc<T> {
    fn push_arc(&self, value: T);
}

impl<T: Send + Sync + 'static> PushArc<T> for WatchableVec<Arc<T>> {
    fn push_arc(&self, value: T) {
        self.push(Arc::new(value));
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watchable_basic() {
        let w = Watchable::new(42);
        assert_eq!(w.get(), 42);

        w.set(100);
        assert_eq!(w.get(), 100);
    }

    #[test]
    fn test_watcher_trait() {
        let w = Watchable::new(42);
        let mut watcher = w.watch();

        assert_eq!(watcher.get(), 42);
        assert!(!watcher.has_changed());

        w.set(100);
        assert!(watcher.has_changed());
        assert_eq!(watcher.get(), 100);
        assert!(!watcher.has_changed());
    }

    #[test]
    fn test_watcher_map() {
        let w = Watchable::new(10);
        let mut mapped = w.watch().map(|v| v * 2);

        assert_eq!(mapped.get(), 20);

        w.set(25);
        assert!(mapped.update());
        assert_eq!(mapped.peek(), &50);
    }

    #[test]
    fn test_watcher_and() {
        let a = Watchable::new(1);
        let b = Watchable::new("hello".to_string());

        let mut combined = a.watch().and(b.watch());
        assert_eq!(combined.get(), (1, "hello".to_string()));

        a.set(2);
        assert!(combined.update());
        assert_eq!(combined.peek(), &(2, "hello".to_string()));
    }

    #[test]
    fn test_watcher_join() {
        let watchables: Vec<_> = (0..3).map(Watchable::new).collect();
        let mut joined = Join::new(watchables.iter().map(|w| w.watch()));

        assert_eq!(joined.get(), vec![0, 1, 2]);

        watchables[1].set(10);
        assert!(joined.update());
        assert_eq!(joined.peek(), &vec![0, 10, 2]);
    }

    #[test]
    fn test_set_if_changed() {
        let w = Watchable::new(42);
        let watcher = w.watch();

        assert!(w.set_if_changed(100).is_ok());
        assert!(watcher.has_changed());

        assert!(w.set_if_changed(100).is_err());
    }

    #[test]
    fn test_watchable_lite() {
        let w = WatchableLite::new(42);
        let mut watcher = w.watch();

        assert_eq!(watcher.get(), 42);
        assert!(!watcher.has_changed());

        w.set(100);
        assert!(watcher.has_changed());
        assert!(watcher.update());
        assert_eq!(watcher.peek(), &100);
    }

    #[test]
    fn test_watchable_atomic() {
        let w = WatchableAtomic::new(42i32);
        let mut watcher = w.watch();

        assert_eq!(*watcher.peek(), 42);

        w.set(100);
        assert!(watcher.update());
        assert_eq!(*watcher.peek(), 100);
    }

    #[test]
    fn test_watchable_fast() {
        let w = WatchableFast::new(42);
        let mut watcher = w.watch();

        assert_eq!(watcher.get(), 42);

        w.set(100);
        assert!(watcher.has_changed());
        assert_eq!(watcher.get(), 100);
    }

    #[test]
    fn test_watchable_map_collection() {
        let map = WatchableMap::<String, i32>::new();
        let mut watcher = map.watch_changes();

        map.insert("a".into(), 1);
        map.insert("b".into(), 2);

        let (changes, missed) = watcher.poll_changes().unwrap();
        assert!(!missed);
        assert_eq!(changes.len(), 2);
    }

    #[test]
    fn test_watchable_vec_collection() {
        let vec = WatchableVec::<i32>::new();
        vec.push(1);
        vec.push(2);
        vec.push(3);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec.get(1), Some(2));
    }

    #[test]
    fn test_disconnection() {
        let w = Watchable::new(42);
        let watcher = w.watch();

        assert!(watcher.is_connected());
        drop(w);
        assert!(!watcher.is_connected());
    }

    #[tokio::test]
    async fn test_async_updated() {
        let w = Watchable::new(0);
        let mut watcher = w.watch();

        let handle = tokio::spawn(async move { watcher.updated().await });

        tokio::task::yield_now().await;
        w.set(42);

        let result = handle.await.unwrap();
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_initialized() {
        let w = Watchable::new(None::<i32>);
        let mut watcher = w.watch();

        let handle = tokio::spawn(async move { watcher.initialized().await });

        tokio::task::yield_now().await;
        w.set(Some(42));

        let result = handle.await.unwrap();
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_concurrent_writes() {
        use std::thread;

        let w = WatchableFast::new(0i32);
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let w = w.clone();
                thread::spawn(move || {
                    for i in 0..1000 {
                        w.set(t * 1000 + i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
