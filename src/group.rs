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

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment.
    ///
    /// - If a duplicate call comes in, that caller will wait until the original
    ///   call completes and return the same value.
    ///
    /// - Only owner call returns error if exists.
    pub async fn work<Q, F>(&self, key: &Q, fut: F) -> Result<T, Option<E>>
    where
        Q: Hash + Eq + ?Sized + ToOwned<Owned = K>,
        F: Future<Output = Result<T, E>>,
        K: std::borrow::Borrow<Q>,
    {
        // Use a loop to avoid async tail recursion on leader dropped
        let mut fut_opt = Some(fut);
        loop {
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
                    State::LeaderFailed => return Err(None),
                }
            } else {
                let (tx, rx) = watch::channel(State::Starting);
                self.map.insert(key.to_owned(), rx);
                ChannelHandler::Sender(tx)
            };

            match handler {
                ChannelHandler::Sender(tx) => {
                    let fut = Leader::new(
                        fut_opt
                            .take()
                            .expect("future should be available when becoming leader"),
                        tx,
                    );
                    let result = fut.await;
                    let _ = self.map.remove(key);
                    match result {
                        Ok(val) => return Ok(val),
                        Err(err) => return Err(Some(err)),
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
                            // the leader dropped, so we retry the loop, potentially becoming leader
                            continue;
                        }
                        State::Success(val) => return Ok(val),
                        State::LeaderFailed => return Err(None),
                    }
                }
            }
        }
    }
}
