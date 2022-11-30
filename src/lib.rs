//! A singleflight implementation for tokio.
//!
//! Inspired by [singleflight](https://crates.io/crates/singleflight).
//!
//! # Examples
//!
//! ```no_run
//! use futures::future::join_all;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use async_singleflight::Group;
//!
//! const RES: usize = 7;
//!
//! async fn expensive_fn() -> Result<usize, ()> {
//!     tokio::time::sleep(Duration::new(1, 500)).await;
//!     Ok(RES)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let g = Arc::new(Group::<_, ()>::new());
//!     let mut handlers = Vec::new();
//!     for _ in 0..10 {
//!         let g = g.clone();
//!         handlers.push(tokio::spawn(async move {
//!             let res = g.work("key", expensive_fn()).await.0;
//!             let r = res.unwrap();
//!             println!("{}", r);
//!         }));
//!     }
//!
//!     join_all(handlers).await;
//! }
//! ```
//!

use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use hashbrown::HashMap;
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use tokio::sync::watch;

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct Group<T, E>
where
    T: Clone,
{
    m: Mutex<HashMap<String, watch::Receiver<State<T>>>>,
    _marker: PhantomData<fn(E)>,
}

impl<T, E> Debug for Group<T, E>
where
    T: Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").finish()
    }
}

impl<T, E> Default for Group<T, E>
where
    T: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
enum State<T: Clone> {
    Starting,
    LeaderDropped,
    Done(Option<T>),
}

impl<T, E> Group<T, E>
where
    T: Clone,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> Group<T, E> {
        Self {
            m: Mutex::new(HashMap::new()),
            _marker: PhantomData,
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    /// Only owner call returns error if exists.
    /// The third return value indicates whether the call is the owner.
    pub async fn work(
        &self,
        key: &str,
        fut: impl Future<Output = Result<T, E>>,
    ) -> (Option<T>, Option<E>, bool) {
        use hashbrown::hash_map::EntryRef;

        let tx_or_rx = match self.m.lock().entry_ref(key) {
            EntryRef::Occupied(mut entry) => {
                let state = entry.get().borrow().clone();
                match state {
                    State::Starting => Err(entry.get().clone()),
                    State::LeaderDropped => {
                        // switch into leader if leader dropped
                        let (tx, rx) = watch::channel(State::Starting);
                        entry.insert(rx);
                        Ok(tx)
                    }
                    State::Done(val) => return (val, None, false),
                }
            }
            EntryRef::Vacant(entry) => {
                let (tx, rx) = watch::channel(State::Starting);
                entry.insert(rx);
                Ok(tx)
            }
        };

        match tx_or_rx {
            Ok(tx) => {
                let fut = Leader { fut, tx };
                let result = fut.await;
                self.m.lock().remove(key);
                match result {
                    Ok(val) => (Some(val), None, true),
                    Err(err) => (None, Some(err), true),
                }
            }
            Err(mut rx) => {
                let mut state = rx.borrow_and_update().clone();
                if matches!(state, State::Starting) {
                    let _changed = rx.changed().await;
                    state = rx.borrow().clone();
                }
                match state {
                    State::Starting => (None, None, false), // unreachable
                    State::LeaderDropped => {
                        self.m.lock().remove(key);
                        (None, None, false)
                    }
                    State::Done(val) => (val, None, false),
                }
            }
        }
    }
}

#[pin_project(PinnedDrop)]
struct Leader<T: Clone, F> {
    #[pin]
    fut: F,
    tx: watch::Sender<State<T>>,
}

impl<T, E, F> Future for Leader<T, F>
where
    T: Clone,
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            let _send = this.tx.send(State::Done(val.as_ref().ok().cloned()));
        }
        result
    }
}

#[pinned_drop]
impl<T, F> PinnedDrop for Leader<T, F>
where
    T: Clone,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let _ = this.tx.send_if_modified(|s| {
            if matches!(s, State::Starting) {
                *s = State::LeaderDropped;
                true
            } else {
                false
            }
        });
    }
}

/// UnaryGroup represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct UnaryGroup<T>
where
    T: Clone,
{
    m: Mutex<HashMap<String, watch::Receiver<UnaryState<T>>>>,
}

impl<T> Debug for UnaryGroup<T>
where
    T: Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryGroup").finish()
    }
}

impl<T> Default for UnaryGroup<T>
where
    T: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
enum UnaryState<T: Clone> {
    Starting,
    LeaderDropped,
    Done(T),
}

impl<T> UnaryGroup<T>
where
    T: Clone + Send + Sync,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> UnaryGroup<T> {
        Self {
            m: Mutex::new(HashMap::new()),
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    ///
    /// The third return value indicates whether the call is the owner.
    pub fn work<'s>(
        &'s self,
        key: &'s str,
        fut: impl Future<Output = T> + Send + 's,
    ) -> BoxFuture<'s, (T, bool)> {
        use hashbrown::hash_map::EntryRef;
        Box::pin(async move {
            let tx_or_rx = match self.m.lock().entry_ref(key) {
                EntryRef::Occupied(mut entry) => {
                    let state = entry.get().borrow().clone();
                    match state {
                        UnaryState::Starting => Err(entry.get().clone()),
                        UnaryState::LeaderDropped => {
                            // switch into leader if leader dropped
                            let (tx, rx) = watch::channel(UnaryState::Starting);
                            entry.insert(rx);
                            Ok(tx)
                        }
                        UnaryState::Done(val) => return (val, false),
                    }
                }
                EntryRef::Vacant(entry) => {
                    let (tx, rx) = watch::channel(UnaryState::Starting);
                    entry.insert(rx);
                    Ok(tx)
                }
            };

            match tx_or_rx {
                Ok(tx) => {
                    let fut = UnaryLeader { fut, tx };
                    let result = fut.await;
                    self.m.lock().remove(key);
                    (result, true)
                }
                Err(mut rx) => {
                    let mut state = rx.borrow_and_update().clone();
                    if matches!(state, UnaryState::Starting) {
                        let _changed = rx.changed().await;
                        state = rx.borrow().clone();
                    }
                    match state {
                        UnaryState::Starting => unreachable!(), // unreachable
                        UnaryState::LeaderDropped => {
                            self.m.lock().remove(key);
                            // the leader dropped, so we need to retry
                            self.work(key, fut).await
                        }
                        UnaryState::Done(val) => (val, false),
                    }
                }
            }
        })
    }
}

#[pin_project(PinnedDrop)]
struct UnaryLeader<T: Clone, F> {
    #[pin]
    fut: F,
    tx: watch::Sender<UnaryState<T>>,
}

impl<T, F> Future for UnaryLeader<T, F>
where
    T: Clone + Send + Sync,
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            let _send = this.tx.send(UnaryState::Done(val.clone()));
        }
        result
    }
}

#[pinned_drop]
impl<T, F> PinnedDrop for UnaryLeader<T, F>
where
    T: Clone,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let _ = this.tx.send_if_modified(|s| {
            if matches!(s, UnaryState::Starting) {
                *s = UnaryState::LeaderDropped;
                true
            } else {
                false
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Group;

    const RES: usize = 7;

    async fn return_res() -> Result<usize, ()> {
        Ok(7)
    }

    async fn expensive_fn() -> Result<usize, ()> {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(RES)
    }

    #[tokio::test]
    async fn test_simple() {
        let g = Group::new();
        let res = g.work("key", return_res()).await.0;
        let r = res.unwrap();
        assert_eq!(r, RES);
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(Group::new());
        let mut handlers = Vec::new();
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work("key", expensive_fn()).await.0;
                let r = res.unwrap();
                println!("{}", r);
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_drop_leader() {
        use std::time::Duration;

        let g = Group::new();
        {
            tokio::time::timeout(Duration::from_millis(50), g.work("key", expensive_fn()))
                .await
                .expect_err("owner should be running and cancelled");
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), g.work("key", expensive_fn())).await,
            Ok((Some(RES), None, true)),
        );
    }
}
