use super::*;

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct Group<K, T, E, S = RandomState> {
    map: Mutex<HashMap<K, watch::Receiver<State<T>>, S>>,
    _marker: PhantomData<fn(E)>,
}

pub type DefaultGroup<T, E = ()> = Group<String, T, E>;

impl<K, T, E, S> Debug for Group<K, T, E, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").finish()
    }
}

impl<K, T, E, S> Default for Group<K, T, E, S>
where
    S: Default,
{
    fn default() -> Self {
        Self {
            map: Mutex::new(HashMap::<K, watch::Receiver<State<T>>, S>::default()),
            _marker: PhantomData,
        }
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    S: Default,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> Group<K, T, E, S> {
        Self::default()
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum GroupWorkError<E> {
    #[error("Leader returned an error")]
    LeaderFailed,
    #[error("Leader has been dropped")]
    LeaderDropped,
    #[error("Error returned by the leader")]
    Error(E),
}

impl<E> GroupWorkError<E> {
    #[inline(always)]
    pub fn err(self) -> Option<E> {
        match self {
            GroupWorkError::Error(e) => Some(e),
            _ => None,
        }
    }
}

impl<K, T, E, S> Group<K, T, E, S>
where
    T: Clone,
    K: Hash + Eq,
    S: BuildHasher,
{
    async fn work_inner<Q, F>(
        &self,
        key: &Q,
        fut: &mut Option<F>,
        is_retry: bool,
    ) -> Result<T, GroupWorkError<E>>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
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
                                return Ok(val);
                            }
                            // Stale entry from a completed promoted leader; start fresh.
                            let (tx, rx) = watch::channel(State::Starting);
                            *state_ref = rx;
                            ChannelHandler::Sender(tx)
                        }
                        State::LeaderFailed => {
                            if is_retry {
                                return Err(GroupWorkError::LeaderFailed);
                            }
                            let (tx, rx) = watch::channel(State::Starting);
                            *state_ref = rx;
                            ChannelHandler::Sender(tx)
                        }
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
                match result {
                    Ok(val) => Ok(val),
                    Err(err) => Err(GroupWorkError::Error(err)),
                }
            }
            ChannelHandler::Receiver(mut rx) => {
                let mut state = rx.borrow_and_update().clone();
                if matches!(state, State::Starting) {
                    let _changed = rx.changed().await;
                    state = rx.borrow().clone();
                }
                match state {
                    State::Starting => unreachable!(), // unreachable
                    State::LeaderDropped => {
                        // the leader dropped
                        Err(GroupWorkError::LeaderDropped)
                    }
                    State::Success(val) => Ok(val),
                    State::LeaderFailed => Err(GroupWorkError::LeaderFailed),
                }
            }
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment.
    ///
    /// - If a duplicate call comes in, that caller will wait until the original
    ///   call completes and return the same value.
    /// - If the leader returns an error, owner call returns error,
    ///   others will return `Err(None)`.
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> Result<T, Option<E>>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        let mut is_retry = false;

        // Use a loop to avoid async tail recursion on leader dropped
        loop {
            match self.work_inner(key, &mut fut_opt, is_retry).await {
                Err(GroupWorkError::LeaderDropped) => {
                    // Retry the loop, potentially becoming leader, and consuming the future
                    is_retry = true;
                    continue;
                }
                Ok(val) => return Ok(val),
                Err(GroupWorkError::Error(err)) => return Err(Some(err)),
                Err(GroupWorkError::LeaderFailed) => return Err(None),
            }
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
    /// - If the leader returns an error, owner call returns error,
    ///   others will return `Err(GroupWorkError::LeaderFailed)`.
    /// - If the leader drops, the call will return `Err(GroupWorkError::LeaderDropped)`.
    pub async fn work_no_retry<Q, F>(&self, key: &Q, fut: F) -> Result<T, GroupWorkError<E>>
    where
        Q: Hash + Eq + ?Sized + Send + Sync + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        self.work_inner(key, &mut fut_opt, false).await
    }
}
