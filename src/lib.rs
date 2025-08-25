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
//!             let res = g.work("key", expensive_fn()).await;
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

mod group;
mod unary;

pub use group::*;
pub use unary::*;

use dashmap::DashMap;
use pin_project::{pin_project, pinned_drop};
use std::hash::Hash;
use tokio::sync::watch;

#[derive(Clone)]
enum State<T: Clone> {
    Starting,
    LeaderDropped,
    LeaderFailed,
    Success(T),
}

enum ChannelHandler<T: Clone> {
    Sender(watch::Sender<State<T>>),
    Receiver(watch::Receiver<State<T>>),
}

#[pin_project(PinnedDrop)]
struct Leader<T: Clone, F, Output> {
    #[pin]
    fut: F,
    tx: watch::Sender<State<T>>,
    _marker: PhantomData<Output>,
}

impl<T, F, Output> Leader<T, F, Output>
where
    T: Clone,
{
    fn new(fut: F, tx: watch::Sender<State<T>>) -> Self {
        Self {
            fut,
            tx,
            _marker: PhantomData,
        }
    }
}

#[pinned_drop]
impl<T, F, Output> PinnedDrop for Leader<T, F, Output>
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

impl<T, E, F> Future for Leader<T, F, Result<T, E>>
where
    T: Clone,
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            let _send = match val {
                Ok(v) => this.tx.send(State::Success(v.clone())),
                Err(_) => this.tx.send(State::LeaderFailed),
            };
        }
        result
    }
}

impl<T, F> Future for Leader<T, F, T>
where
    T: Clone + Send + Sync,
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = this.fut.poll(cx);
        if let Poll::Ready(val) = &result {
            let _send = this.tx.send(State::Success(val.clone()));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn return_res() -> Result<usize, ()> {
        Ok(7)
    }

    async fn expensive_fn<const RES: usize>() -> Result<usize, ()> {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(RES)
    }

    async fn expensive_unary_fn<const RES: usize>() -> usize {
        tokio::time::sleep(Duration::from_millis(500)).await;
        RES
    }

    #[tokio::test]
    async fn test_simple() {
        let g = Group::new();
        let res = g.work("key", return_res()).await;
        let r = res.unwrap();
        assert_eq!(r, 7);
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(Group::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work("key", expensive_fn::<7>()).await;
                let r = res.unwrap();
                println!("{}", r);
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_multiple_threads_custom_type() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(Group::<_, _, u64>::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work(&42, expensive_fn::<8>()).await;
                let r = res.unwrap();
                println!("{}", r);
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_multiple_threads_unary() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(UnaryGroup::<_, u64>::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work(&42, expensive_unary_fn::<8>()).await;
                assert_eq!(res, 8);
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_drop_leader() {
        use std::time::Duration;

        let g = Group::new();
        {
            tokio::time::timeout(
                Duration::from_millis(50),
                g.work("key", expensive_fn::<2>()),
            )
            .await
            .expect_err("owner should be running and cancelled");
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), g.work("key", expensive_fn::<7>())).await,
            Ok(Ok(7))
        );
    }
}
