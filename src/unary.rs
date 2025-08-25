use super::*;

/// UnaryGroup represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct UnaryGroup<T, K = String>
where
    T: Clone,
    K: Hash + Eq,
{
    map: DashMap<K, watch::Receiver<State<T>>>,
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

impl<T, K> UnaryGroup<T, K>
where
    T: Clone + Send + Sync,
    K: Hash + Eq + Send + Sync,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> UnaryGroup<T, K> {
        Self {
            map: DashMap::new(),
        }
    }

    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>) -> Option<T>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = T> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let handler = if let Some(state_ref) = self.map.get(key) {
            let state = state_ref.borrow().clone();
            match state {
                State::Starting => ChannelHandler::Receiver(state_ref.clone()),
                State::LeaderDropped => {
                    drop(state_ref);
                    // switch into leader if leader dropped
                    let (tx, rx) = watch::channel(State::Starting);
                    self.map.insert(key.to_owned(), rx);
                    ChannelHandler::Sender(tx)
                }
                State::Success(val) => return Some(val),
                State::LeaderFailed => unreachable!(),
            }
        } else {
            let (tx, rx) = watch::channel(State::Starting);
            self.map.insert(key.to_owned(), rx);
            ChannelHandler::Sender(tx)
        };

        match handler {
            ChannelHandler::Sender(tx) => {
                let leader = Leader::new(
                    fut.take()
                        .expect("future should be available when becoming leader"),
                    tx,
                );
                let result = leader.await;
                let _ = self.map.remove(key);
                Some(result)
            }
            ChannelHandler::Receiver(mut rx) => {
                let mut state = rx.borrow_and_update().clone();
                if matches!(state, State::Starting) {
                    let _changed = rx.changed().await;
                    state = rx.borrow().clone();
                }
                match state {
                    State::LeaderDropped => {
                        let _ = self.map.remove(key);
                        // the leader dropped
                        None
                    }
                    State::Success(val) => Some(val),
                    _ => unreachable!(), // unreachable
                }
            }
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment.
    ///
    /// - If a duplicate call comes in, that caller will wait until the original
    ///   call completes and return the same value.
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> T
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = T> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);

        // Use a loop to avoid async tail recursion on leader dropped
        loop {
            if let Some(result) = self.work_inner(key, &mut fut_opt).await {
                break result;
            }
            // Retry the loop, potentially becoming leader, and consuming the future
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment.
    ///
    /// - If a duplicate call comes in, that caller will wait until the original
    ///   call completes and return the same value.
    /// - If the leader drops, the call will return `None`.
    pub async fn work_no_retry<Q, F>(&self, key: &Q, fut: F) -> Option<T>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = T> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        self.work_inner(key, &mut fut_opt).await
    }
}
