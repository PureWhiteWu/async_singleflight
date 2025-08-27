use super::*;

/// UnaryGroup represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct UnaryGroup<K, T, S = RandomState> {
    map: Mutex<HashMap<K, watch::Receiver<State<T>>, S>>,
}

pub type DefaultUnaryGroup<T> = UnaryGroup<String, T>;

impl<K, T, S> Debug for UnaryGroup<K, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryGroup").finish()
    }
}

impl<K, T, S> Default for UnaryGroup<K, T, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            map: Mutex::new(HashMap::<K, watch::Receiver<State<T>>, S>::default()),
        }
    }
}

impl<K, T, S> UnaryGroup<K, T, S>
where
    S: Default,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> UnaryGroup<K, T, S> {
        Self::default()
    }
}

impl<K, T, S> UnaryGroup<K, T, S>
where
    T: Clone + Send + Sync,
    K: Hash + Eq + Send + Sync,
    S: BuildHasher,
{
    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>) -> Option<T>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = T> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let handler = {
            let mut locked_map = self.map.lock().await;
            match locked_map.get_mut(key) {
                Some(state_ref) => {
                    let state = state_ref.borrow().clone();
                    match state {
                        State::Starting => ChannelHandler::Receiver(state_ref.clone()),
                        State::LeaderDropped => {
                            // switch into leader if leader dropped
                            let (tx, rx) = watch::channel(State::Starting);
                            *state_ref = rx;
                            ChannelHandler::Sender(tx)
                        }
                        State::Success(val) => return Some(val),
                        State::LeaderFailed => unreachable!(),
                    }
                }
                None => {
                    let (tx, rx) = watch::channel(State::Starting);
                    locked_map.insert(key.to_owned(), rx);
                    ChannelHandler::Sender(tx)
                }
            }
        };

        match handler {
            ChannelHandler::Sender(tx) => {
                let leader = Leader::new(
                    fut.take()
                        .expect("future should be available when becoming leader"),
                    tx,
                );
                let result = leader.await;
                self.map.lock().await.remove(key);
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
                        self.map.lock().await.remove(key);
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
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
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
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = T> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        self.work_inner(key, &mut fut_opt).await
    }
}
