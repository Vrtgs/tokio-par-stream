use futures::stream::{FusedStream, FuturesUnordered};
use futures::{Stream, StreamExt};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle as TokioHandle;
use tokio::task::JoinHandle;

// don't use JoinSet: https://github.com/tokio-rs/tokio/issues/5564
#[must_use = "streams do nothing unless polled"]
pub struct FuturesParallelUnordered<F: Future> {
    futures: FuturesUnordered<JoinHandle<F::Output>>,
    handle: TokioHandle,
}

impl<F: Future> Unpin for FuturesParallelUnordered<F> {}

impl<F: Future> Debug for FuturesParallelUnordered<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuturesParallelUnordered")
            .finish_non_exhaustive()
    }
}

impl<Fut: Future> FuturesParallelUnordered<Fut> {
    /// Constructs a new, empty [`FuturesParallelUnordered`].
    ///
    /// The returned [`FuturesParallelUnordered`] does not contain any futures.
    /// In this state, [`FuturesParallelUnordered::poll_next`](Stream::poll_next) will
    /// return [`Poll::Ready(None)`](Poll::Ready).
    ///
    /// must be called within a tokio runtime
    pub fn new() -> Self {
        Self {
            futures: FuturesUnordered::new(),
            handle: TokioHandle::current(),
        }
    }
    /// Returns the number of futures contained in the set.
    ///
    /// This represents the total number of in-flight futures.
    pub fn len(&self) -> usize {
        self.futures.len()
    }

    /// Returns `true` if the set contains no futures.
    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }
}

impl<Fut: Future + Send + 'static> FuturesParallelUnordered<Fut>
where
    Fut::Output: Send,
{
    /// Push a future into the set.
    ///
    /// This method adds the given future to the set. This method may
    /// call [`poll`](Future::poll) on the submitted future.
    /// and will spawn in it on the current executor
    ///
    /// tasks will be scheduled on the same runtime that the [`FuturesParallelUnordered`]
    /// was originally made in
    pub fn push(&self, future: Fut) {
        self.add_join_handle(self.handle().spawn(future));
    }

    /// Returns a handle to this stream's spawner.
    pub const fn handle(&self) -> &TokioHandle {
        &self.handle
    }

    /// adds any JoinHandle to the executor
    pub fn add_join_handle(&self, jh: JoinHandle<Fut::Output>) {
        self.futures.push(jh)
    }
}

impl<Fut: Future> Default for FuturesParallelUnordered<Fut> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Fut: Future> Stream for FuturesParallelUnordered<Fut>
where
    Fut::Output: 'static,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.futures.poll_next_unpin(cx) {
            Poll::Ready(x) => Poll::Ready(x.map(|res| {
                res.unwrap_or_else(|join_err| {
                    match join_err.try_into_panic() {
                        // we forward the panic onto the other thread
                        Ok(panic) => std::panic::resume_unwind(panic),
                        // since we never give out the abort handle so its unexpected that a task is canceled
                        Err(other) => panic!("could not get the next future due to {other}"),
                    }
                })
            })),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.futures.size_hint()
    }
}

impl<Fut: Future> FusedStream for FuturesParallelUnordered<Fut>
where
    Fut::Output: 'static,
{
    fn is_terminated(&self) -> bool {
        self.futures.is_terminated()
    }
}

impl<F: Future> Drop for FuturesParallelUnordered<F> {
    fn drop(&mut self) {
        for orphan in self.futures.iter_mut() {
            orphan.abort()
        }
    }
}

impl<Fut: Future + Send + 'static> Extend<Fut> for FuturesParallelUnordered<Fut>
where
    Fut::Output: Send,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Fut>,
    {
        for item in iter {
            self.push(item);
        }
    }
}

impl<Fut: Future + Send + 'static> FromIterator<Fut> for FuturesParallelUnordered<Fut>
where
    Fut::Output: Send,
{
    fn from_iter<T: IntoIterator<Item = Fut>>(iter: T) -> Self {
        let mut ret = FuturesParallelUnordered::new();
        ret.extend(iter);
        ret
    }
}
