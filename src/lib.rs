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
//! use async_singleflight::DefaultGroup;
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
//!     let g = Arc::new(DefaultGroup::<usize>::new());
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

use pin_project::{pin_project, pinned_drop};
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::watch;
use tokio::sync::Mutex;

#[derive(Clone)]
enum State<T> {
    Starting,
    LeaderDropped,
    LeaderFailed,
    Success(T),
}

enum ChannelHandler<T> {
    Sender(watch::Sender<State<T>>),
    Receiver(watch::Receiver<State<T>>),
}

#[pin_project(PinnedDrop)]
struct Leader<T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
{
    #[pin]
    fut: F,
    tx: watch::Sender<State<T>>,
}

impl<T, F, Output> Leader<T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
{
    fn new(fut: F, tx: watch::Sender<State<T>>) -> Self {
        Self { fut, tx }
    }
}

#[pinned_drop]
impl<T, F, Output> PinnedDrop for Leader<T, F, Output>
where
    T: Clone,
    F: Future<Output = Output>,
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
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;

    async fn return_res() -> Result<usize, ()> {
        Ok(7)
    }

    async fn expensive_fn<const RES: usize>(delay: u64) -> Result<usize, ()> {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        Ok(RES)
    }

    async fn expensive_unary_fn<const RES: usize>(delay: u64) -> usize {
        tokio::time::sleep(Duration::from_millis(delay)).await;
        RES
    }

    #[tokio::test]
    async fn test_simple() {
        let g = DefaultGroup::new();
        let res = g.work("key", return_res()).await;
        let r = res.unwrap();
        assert_eq!(r, 7);
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        use std::sync::Arc;

        use futures::future::join_all;

        let g = Arc::new(DefaultGroup::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work("key", expensive_fn::<7>(300)).await;
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

        let g = Arc::new(Group::<u64, usize, ()>::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work(&42, expensive_fn::<8>(300)).await;
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

        let g = Arc::new(UnaryGroup::<u64, usize>::new());
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let g = g.clone();
            handlers.push(tokio::spawn(async move {
                let res = g.work(&42, expensive_unary_fn::<8>(300)).await;
                assert_eq!(res, 8);
            }));
        }

        join_all(handlers).await;
    }

    #[tokio::test]
    async fn test_drop_leader() {
        let group = Arc::new(Group::new());

        // Signal when the leader's inner future gets polled (implies map entry inserted).
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let leader_owned = group.clone();
        let leader = tokio::spawn(async move {
            // The inner future signals on first poll, then sleeps long.
            let fut = async move {
                let _ = ready_tx.send(());
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<usize, ()>(7)
            };
            // We expect this task to be aborted before completion.
            let _ = leader_owned.work("key", fut).await;
        });

        // Wait until the leader's future has been polled once (map entry is in place).
        let _ = ready_rx.await;

        // Spawn a follower that will wait on the existing key and should observe LeaderDropped.
        let follower_owned = group.clone();
        let follower = tokio::spawn(async move {
            follower_owned
                .work("key", async { Ok::<usize, ()>(42) })
                .await
        });

        // Give the follower a chance to attach to the receiver.
        tokio::task::yield_now().await;

        // Abort the leader to trigger LeaderDropped notification to all followers.
        leader.abort();

        // The follower should return LeaderDropped.
        let res = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should finish in time")
            .expect("follower task should not panic");

        assert_eq!(res, Ok(42));
    }

    #[tokio::test]
    async fn test_drop_leader_no_retry() {
        let group = Arc::new(DefaultGroup::<usize>::new());

        // Signal when the leader's inner future gets polled (implies map entry inserted).
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let leader_owned = group.clone();
        let leader = tokio::spawn(async move {
            // The inner future signals on first poll, then sleeps long.
            let fut = async move {
                let _ = ready_tx.send(());
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<usize, ()>(7)
            };
            // We expect this task to be aborted before completion.
            let _ = leader_owned.work("key", fut).await;
        });

        // Wait until the leader's future has been polled once (map entry is in place).
        let _ = ready_rx.await;

        // Spawn a follower that will wait on the existing key and should observe LeaderDropped.
        let follower_owned = group.clone();
        let follower = tokio::spawn(async move {
            follower_owned
                .work_no_retry("key", async { Ok::<usize, ()>(42) })
                .await
        });

        // Give the follower a chance to attach to the receiver.
        tokio::task::yield_now().await;

        // Abort the leader to trigger LeaderDropped notification to all followers.
        leader.abort();

        // The follower should return LeaderDropped.
        let res = tokio::time::timeout(Duration::from_secs(1), follower)
            .await
            .expect("follower should finish in time")
            .expect("follower task should not panic");

        assert_eq!(res, Err(GroupWorkError::LeaderDropped));
    }
}
