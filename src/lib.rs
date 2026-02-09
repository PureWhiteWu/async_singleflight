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
use std::hash::BuildHasher;
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
use std::hash::RandomState;
use tokio::sync::{watch, Mutex};

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
        let group = Arc::new(DefaultGroup::new());

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

    /// Regression test for issue #12: when the leader is dropped, only ONE follower
    /// should become the new leader. Due to a race condition in the LeaderDropped
    /// handler (which calls `map.remove(key)` and can delete a newly-inserted entry
    /// from a follower that already became the new leader), multiple followers can
    /// each independently become leaders and execute the work function.
    ///
    /// The race scenario:
    /// 1. Leader is dropped; followers A and B both see LeaderDropped
    /// 2. Follower A acquires the lock and calls remove(key), then loops back
    ///    into work_inner where it inserts a NEW entry and becomes the new leader
    /// 3. Follower B acquires the lock for its remove(key) call -- but by now
    ///    the map entry belongs to the new leader (A). B removes it anyway!
    /// 4. Follower B loops back into work_inner, sees no entry, inserts its own,
    ///    and becomes a SECOND independent leader.
    ///
    /// We use a multi-threaded runtime and many iterations to trigger this race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_drop_single_new_leader() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Barrier;

        const NUM_FOLLOWERS: usize = 5;

        // Run the test many times to increase the chance of hitting the race.
        for iteration in 0..200 {
            let group = Arc::new(DefaultGroup::new());

            // Counts how many times the actual work function body executes.
            let execute_count = Arc::new(AtomicUsize::new(0));

            // Signal when the leader's inner future gets polled (map entry inserted).
            let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();

            // Barrier: all followers + main thread wait here to sync up before
            // we abort the leader, ensuring followers are subscribed.
            let barrier = Arc::new(Barrier::new(NUM_FOLLOWERS + 1));

            // Spawn the leader task.
            let leader_group = group.clone();
            let leader = tokio::spawn(async move {
                let fut = async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok::<usize, ()>(999)
                };
                let _ = leader_group.work("key", fut).await;
            });

            // Wait for the leader's future to be polled (entry in the map).
            let _ = leader_ready_rx.await;

            let mut follower_handles = Vec::with_capacity(NUM_FOLLOWERS);

            for _ in 0..NUM_FOLLOWERS {
                let g = group.clone();
                let cnt = execute_count.clone();
                let b = barrier.clone();
                follower_handles.push(tokio::spawn(async move {
                    // Strategy: each follower signals readiness via the barrier,
                    // then calls work() which subscribes to the leader's channel.
                    // When the leader is aborted, followers see LeaderDropped and
                    // retry via the work() loop. Only one should become the new
                    // leader; the rest should subscribe to the new leader's channel.
                    b.wait().await;

                    g.work("key", async move {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        // Yield to give other followers a chance to also become
                        // leaders if the race condition is triggered.
                        tokio::task::yield_now().await;
                        Ok::<usize, ()>(42)
                    })
                    .await
                }));
            }

            // Wait for all followers to be ready to enter work.
            barrier.wait().await;

            // Give followers time to actually enter work_inner and subscribe
            // as receivers on the watch channel.
            tokio::time::sleep(Duration::from_millis(5)).await;

            // Abort the leader. This triggers LeaderDropped to all followers.
            leader.abort();

            // Wait for all followers to complete.
            for handle in follower_handles {
                let res = tokio::time::timeout(Duration::from_secs(5), handle)
                    .await
                    .expect("follower should finish in time")
                    .expect("follower task should not panic");
                assert_eq!(res, Ok(42), "follower should get the correct result");
            }

            // The critical assertion: the work function should have executed exactly
            // once (by the single new leader). If the bug is present, multiple
            // followers become independent leaders and execute_count will be > 1.
            let count = execute_count.load(Ordering::SeqCst);
            assert_eq!(
                count, 1,
                "Iteration {}: Expected exactly 1 work execution after leader drop, \
                 but got {}. This indicates multiple followers became leaders (issue #12).",
                iteration, count
            );
        }
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

    /// Same as test_leader_drop_single_new_leader but for UnaryGroup.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_drop_single_new_leader_unary() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Barrier;

        const NUM_FOLLOWERS: usize = 5;

        for iteration in 0..200 {
            let group = Arc::new(DefaultUnaryGroup::new());
            let execute_count = Arc::new(AtomicUsize::new(0));
            let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();
            let barrier = Arc::new(Barrier::new(NUM_FOLLOWERS + 1));

            let leader_group = group.clone();
            let leader = tokio::spawn(async move {
                let fut = async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    999_usize
                };
                leader_group.work("key", fut).await
            });

            let _ = leader_ready_rx.await;

            let mut follower_handles = Vec::with_capacity(NUM_FOLLOWERS);
            for _ in 0..NUM_FOLLOWERS {
                let g = group.clone();
                let cnt = execute_count.clone();
                let b = barrier.clone();
                follower_handles.push(tokio::spawn(async move {
                    b.wait().await;
                    g.work("key", async move {
                        cnt.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        42_usize
                    })
                    .await
                }));
            }

            barrier.wait().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            leader.abort();

            for handle in follower_handles {
                let res = tokio::time::timeout(Duration::from_secs(5), handle)
                    .await
                    .expect("follower should finish in time")
                    .expect("follower task should not panic");
                assert_eq!(res, 42, "follower should get the correct result");
            }

            let count = execute_count.load(Ordering::SeqCst);
            assert_eq!(
                count, 1,
                "Iteration {}: Expected exactly 1 work execution after leader drop, \
                 but got {}. This indicates multiple followers became leaders (issue #12).",
                iteration, count
            );
        }
    }

    /// After a promoted leader completes, its result is cached in the map.
    /// A fresh caller (not a retrier) should start new work, not return
    /// the stale cached result.
    #[tokio::test]
    async fn test_fresh_caller_replaces_stale_entry() {
        let group = Arc::new(DefaultGroup::new());

        let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();
        let leader_group = group.clone();
        let leader = tokio::spawn(async move {
            let _ = leader_group
                .work("key", async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok::<usize, ()>(999)
                })
                .await;
        });
        let _ = leader_ready_rx.await;

        // Spawn a follower that will recover after leader drop.
        let follower_group = group.clone();
        let follower = tokio::spawn(async move {
            follower_group
                .work("key", async { Ok::<usize, ()>(42) })
                .await
        });
        tokio::task::yield_now().await;

        leader.abort();
        let res = follower.await.unwrap();
        assert_eq!(res, Ok(42));

        // Now the map has a stale Success(42) entry from the promoted leader.
        // A fresh caller should start new work and get 99, not the stale 42.
        let res = group.work("key", async { Ok::<usize, ()>(99) }).await;
        assert_eq!(res, Ok(99));
    }

    /// Verify that purge_stale removes completed entries and that
    /// subsequent calls create fresh leaders.
    #[tokio::test]
    async fn test_purge_stale() {
        let group = Arc::new(DefaultGroup::new());

        let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();
        let leader_group = group.clone();
        let leader = tokio::spawn(async move {
            let _ = leader_group
                .work("key", async move {
                    let _ = leader_ready_tx.send(());
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok::<usize, ()>(999)
                })
                .await;
        });
        let _ = leader_ready_rx.await;

        let follower_group = group.clone();
        let follower = tokio::spawn(async move {
            follower_group
                .work("key", async { Ok::<usize, ()>(42) })
                .await
        });
        tokio::task::yield_now().await;

        leader.abort();
        let res = follower.await.unwrap();
        assert_eq!(res, Ok(42));

        // Stale entry exists; purge it.
        group.purge_stale().await;

        // After purge, a new call should work normally.
        let res = group.work("key", async { Ok::<usize, ()>(77) }).await;
        assert_eq!(res, Ok(77));
    }

    /// Verify purge_stale works for UnaryGroup.
    #[tokio::test]
    async fn test_purge_stale_unary() {
        let group = Arc::new(DefaultUnaryGroup::new());

        let (leader_ready_tx, leader_ready_rx) = oneshot::channel::<()>();
        let leader_group = group.clone();
        let leader = tokio::spawn(async move {
            let fut = async move {
                let _ = leader_ready_tx.send(());
                tokio::time::sleep(Duration::from_secs(60)).await;
                999_usize
            };
            leader_group.work("key", fut).await
        });
        let _ = leader_ready_rx.await;

        let follower_group = group.clone();
        let follower =
            tokio::spawn(async move { follower_group.work("key", async { 42_usize }).await });
        tokio::task::yield_now().await;

        leader.abort();
        let res = follower.await.unwrap();
        assert_eq!(res, 42);

        group.purge_stale().await;

        let res = group.work("key", async { 77_usize }).await;
        assert_eq!(res, 77);
    }
}
