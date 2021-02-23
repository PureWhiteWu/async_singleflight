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
//! use anyhow::Result;
//! use async_singleflight::Group;
//!
//! const RES: usize = 7;
//!
//! async fn expensive_fn() -> Result<usize> {
//!     tokio::time::sleep(Duration::new(1, 500)).await;
//!     Ok(RES)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let g = Arc::new(Group::new());
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

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use hashbrown::HashMap;
use tokio::sync::{Mutex, Notify};

// Call is an in-flight or completed call to work.
#[derive(Clone)]
struct Call<T>
where
    T: Clone,
{
    nt: Arc<Notify>,
    // TODO: how to share res through threads without lock?
    res: Arc<parking_lot::RwLock<Option<T>>>,
}

impl<T> Call<T>
where
    T: Clone,
{
    fn new() -> Call<T> {
        Call {
            nt: Arc::new(Notify::new()),
            res: Arc::new(parking_lot::RwLock::new(None)),
        }
    }
}

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Default)]
pub struct Group<T>
where
    T: Clone,
{
    m: Mutex<HashMap<String, Arc<Call<T>>>>,
}

impl<T> Group<T>
where
    T: Clone,
{
    /// Create a new Group to do work with.
    pub fn new() -> Group<T> {
        Group {
            m: Mutex::new(HashMap::new()),
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
        fut: impl Future<Output = Result<T>>,
    ) -> (Option<T>, Option<anyhow::Error>, bool) {
        // grab lock
        let mut m = self.m.lock().await;

        // key already exists
        if let Some(c) = m.get(key) {
            let c = c.clone();
            // need to create Notify first before drop lock
            let nt = c.nt.notified();
            drop(m);
            // wait for notify
            nt.await;
            let res = c.res.read();
            return (res.clone(), None, false);
        }

        // insert call into map and start call
        let c = Arc::new(Call::new());
        m.insert(key.to_owned(), c);
        drop(m);
        let res = fut.await;

        // grab lock before set result and notify waiters
        let mut m = self.m.lock().await;
        let c = m.get(key).unwrap();
        if res.is_ok() {
            let mut m2 = c.res.write();
            *m2 = Some(res.as_ref().unwrap().clone());
            drop(m2);
        }
        c.nt.notify_waiters();
        m.remove(key).unwrap();
        drop(m);

        if res.is_ok() {
            return (Some(res.unwrap()), None, true);
        }
        (None, Some(res.err().unwrap()), true)
    }
}

#[cfg(test)]
mod tests {
    use super::Group;
    use anyhow::Result;

    const RES: usize = 7;

    async fn return_res() -> Result<usize> {
        Ok(7)
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
        use futures::future::join_all;
        use std::sync::Arc;
        use std::time::Duration;

        async fn expensive_fn() -> Result<usize> {
            tokio::time::sleep(Duration::new(1, 500)).await;
            Ok(RES)
        }

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
}
