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
    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>, is_retry: bool) -> Option<T>
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
                        State::Success(val) => {
                            if is_retry {
                                // A promoted leader already completed; return cached result.
                                return Some(val);
                            }
                            // Stale entry from a completed promoted leader; start fresh.
                            let (tx, rx) = watch::channel(State::Starting);
                            *state_ref = rx;
                            ChannelHandler::Sender(tx)
                        }
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
                // Only the original leader removes the entry. Promoted leaders
                // (is_retry=true) leave the entry so late-arriving retrying
                // followers can read the cached result instead of becoming
                // spurious independent leaders.
                if !is_retry {
                    self.map.lock().await.remove(key);
                }
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
        let mut is_retry = false;

        // Use a loop to avoid async tail recursion on leader dropped
        loop {
            if let Some(result) = self.work_inner(key, &mut fut_opt, is_retry).await {
                break result;
            }
            // Retry the loop, potentially becoming leader, and consuming the future
            is_retry = true;
        }
    }

    /// Remove completed entries left by promoted leaders after leader-drop recovery.
    ///
    /// When a leader is dropped and a follower takes over, the promoted leader
    /// leaves its result cached in the map so that late-arriving retriers can read
    /// it. These entries are automatically replaced by the next fresh [`work`] call
    /// for the same key, but if no new call arrives, they persist.
    ///
    /// This method removes all such completed entries. It is safe to call at any
    /// time, though calling it while leader-drop recovery is actively in progress
    /// for a key may cause a late retrier to re-execute the work function for that
    /// key (a benign but redundant execution).
    ///
    /// [`work`]: Self::work
    pub async fn purge_stale(&self) {
        self.map.lock().await.retain(|_, rx| {
            let state = rx.borrow();
            matches!(&*state, State::Starting)
        });
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
        self.work_inner(key, &mut fut_opt, false).await
    }
}
