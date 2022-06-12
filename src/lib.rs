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

use hashbrown::HashMap;
use parking_lot::Mutex;
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
            EntryRef::Occupied(entry) => {
                let rx = entry.get();
                Err(rx.clone())
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

struct Leader<T: Clone, F> {
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
        let this = unsafe { Pin::get_unchecked_mut(self) };
        let fut = unsafe { Pin::new_unchecked(&mut this.fut) };
        let result = fut.poll(cx);
        if let Poll::Ready(val) = &result {
            let _send = this.tx.send(State::Done(val.as_ref().ok().cloned()));
        }
        result
    }
}

impl<T, F> Drop for Leader<T, F>
where
    T: Clone,
{
    fn drop(&mut self) {
        let _ = self.tx.send_if_modified(|s| {
            if matches!(s, State::Starting) {
                *s = State::LeaderDropped;
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
            Ok((None, None, false)),
            "following should be returned in time"
        );
    }
}
