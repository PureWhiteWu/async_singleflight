use super::*;

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
pub struct Group<T, E, K = String>
where
    T: Clone,
    K: Hash + Eq,
{
    map: DashMap<K, watch::Receiver<State<T>>>,
    _marker: PhantomData<fn(E)>,
}

impl<T, E, K> Debug for Group<T, E, K>
where
    T: Clone,
    K: Hash + Eq,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Group").finish()
    }
}

impl<T, E, K> Default for Group<T, E, K>
where
    T: Clone,
    K: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
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
    pub fn unwrap_err(self) -> E {
        match self {
            GroupWorkError::Error(err) => err,
            _ => panic!("called `unwrap_err` on a non-error variant"),
        }
    }

    pub fn as_ref(&self) -> GroupWorkError<&E> {
        match self {
            GroupWorkError::Error(err) => GroupWorkError::Error(err),
            GroupWorkError::LeaderFailed => GroupWorkError::LeaderFailed,
            GroupWorkError::LeaderDropped => GroupWorkError::LeaderDropped,
        }
    }

    pub fn into_inner(self) -> E {
        match self {
            GroupWorkError::Error(err) => err,
            _ => panic!("called `into_inner` on a non-error variant"),
        }
    }
}

impl<E> From<GroupWorkError<E>> for Option<E> {
    fn from(err: GroupWorkError<E>) -> Self {
        match err {
            GroupWorkError::Error(e) => Some(e),
            _ => None,
        }
    }
}

impl<T, E, K> Group<T, E, K>
where
    T: Clone,
    K: Hash + Eq,
{
    /// Create a new Group to do work with.
    #[must_use]
    pub fn new() -> Group<T, E, K> {
        Self {
            map: DashMap::new(),
            _marker: PhantomData,
        }
    }

    async fn work_inner<Q, F>(&self, key: &Q, fut: &mut Option<F>) -> Result<T, GroupWorkError<E>>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = Result<T, E>> + Send,
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
                State::Success(val) => return Ok(val),
                State::LeaderFailed => return Err(GroupWorkError::LeaderFailed),
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
                        let _ = self.map.remove(key);
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
    /// - Only owner call returns error if exists.
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> Result<T, Option<E>>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);

        // Use a loop to avoid async tail recursion on leader dropped
        loop {
            match self.work_inner(key, &mut fut_opt).await {
                Err(GroupWorkError::LeaderDropped) => {
                    // Retry the loop, potentially becoming leader, and consuming the future
                    continue;
                }
                Ok(val) => return Ok(val),
                Err(GroupWorkError::Error(err)) => return Err(Some(err)),
                Err(GroupWorkError::LeaderFailed) => return Err(None),
            }
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment.
    ///
    /// - If a duplicate call comes in, that caller will wait until the original
    ///   call completes and return the same value.
    /// - Only owner call returns error if exists.
    /// - If the leader drops, the call will return `None`.
    pub async fn work_no_retry<Q, F>(&self, key: &Q, fut: F) -> Result<T, GroupWorkError<E>>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K> + Send + Sync,
        F: Future<Output = Result<T, E>> + Send,
        K: std::borrow::Borrow<Q>,
    {
        let mut fut_opt = Some(fut);
        self.work_inner(key, &mut fut_opt).await
    }
}
