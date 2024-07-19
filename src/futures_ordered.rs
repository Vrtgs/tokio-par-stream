use crate::futures_unordered::FuturesParallelUnordered;
use crate::order::OrderWrapper;
use core::fmt;
use futures::stream::FusedStream;
use futures::{ready, Stream, StreamExt};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// we don't use FuturesOrdered as it doesn't give us any way to iterate over existing futures
// and, so we can't abort them on drop
#[must_use = "streams do nothing unless polled"]
pub struct FuturesParallelOrdered<T: Future> {
    in_progress_queue: FuturesParallelUnordered<OrderWrapper<T>>,
    queued_outputs: BinaryHeap<OrderWrapper<T::Output>>,
    next_incoming_index: i64,
    next_outgoing_index: i64,
}

impl<T: Future> Unpin for FuturesParallelOrdered<T> {}

impl<F: Future> Debug for FuturesParallelOrdered<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuturesParallelOrdered")
            .finish_non_exhaustive()
    }
}

impl<Fut: Future> FuturesParallelOrdered<Fut> {
    /// Constructs a new, empty `FuturesOrdered`
    ///
    /// The returned [`FuturesParallelOrdered`] does not contain any futures and, in
    /// this state, [`FuturesParallelOrdered::poll_next`] will return
    /// [`Poll::Ready(None)`](Poll::Ready).
    pub fn new() -> Self {
        Self {
            in_progress_queue: FuturesParallelUnordered::new(),
            queued_outputs: BinaryHeap::new(),
            next_incoming_index: 0,
            next_outgoing_index: 0,
        }
    }

    /// Returns the number of futures contained in the queue.
    ///
    /// This represents the total number of in-flight futures, both
    /// those currently processing and those that have completed but
    /// which are waiting for earlier futures to complete.
    pub fn len(&self) -> usize {
        self.in_progress_queue.len() + self.queued_outputs.len()
    }

    /// Returns `true` if the queue contains no futures
    pub fn is_empty(&self) -> bool {
        self.in_progress_queue.is_empty() && self.queued_outputs.is_empty()
    }
}

impl<Fut: Future + Send + 'static> FuturesParallelOrdered<Fut>
where
    Fut::Output: Send,
{
    /// Pushes a future to the back of the queue.
    ///
    /// This method adds the given future to the set. This method may
    /// call [`poll`](Future::poll) on the submitted future.
    /// and will spawn in it on the current executor
    ///
    /// tasks will be scheduled on the same runtime that the [`FuturesParallelOrdered`]
    /// was originally made in
    pub fn push_back(&mut self, future: Fut) {
        let wrapped = OrderWrapper {
            data: future,
            index: self.next_incoming_index,
        };
        self.next_incoming_index += 1;
        self.in_progress_queue.push(wrapped);
    }

    /// Pushes a future to the front of the queue.
    ///
    /// This method adds the given future to the set. This method may
    /// call [`poll`](Future::poll) on the submitted future.
    /// and will spawn in it on the current executor
    ///
    /// tasks will be scheduled on the same runtime that the [`FuturesParallelOrdered`]
    /// was originally made in
    pub fn push_front(&mut self, future: Fut) {
        let wrapped = OrderWrapper {
            data: future,
            index: self.next_outgoing_index - 1,
        };
        self.next_outgoing_index -= 1;
        self.in_progress_queue.push(wrapped);
    }
}

impl<Fut: Future> Default for FuturesParallelOrdered<Fut> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Fut: Future + Send + 'static> Stream for FuturesParallelOrdered<Fut>
where
    Fut::Output: Send,
{
    type Item = Fut::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);

        // Check to see if we've already received the next value
        if let Some(next_output) = this.queued_outputs.peek_mut() {
            if next_output.index == this.next_outgoing_index {
                this.next_outgoing_index += 1;
                return Poll::Ready(Some(PeekMut::pop(next_output).data));
            }
        }

        loop {
            match ready!(this.in_progress_queue.poll_next_unpin(cx)) {
                Some(output) => {
                    if output.index == this.next_outgoing_index {
                        this.next_outgoing_index += 1;
                        return Poll::Ready(Some(output.data));
                    } else {
                        this.queued_outputs.push(output)
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<Fut: Future + Send + 'static> FromIterator<Fut> for FuturesParallelOrdered<Fut>
where
    Fut::Output: Send,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Fut>,
    {
        let mut ret = FuturesParallelOrdered::new();
        ret.extend(iter);
        ret
    }
}

impl<Fut: Future + Send + 'static> FusedStream for FuturesParallelOrdered<Fut>
where
    Fut::Output: Send,
{
    fn is_terminated(&self) -> bool {
        self.in_progress_queue.is_terminated() && self.queued_outputs.is_empty()
    }
}

impl<Fut: Future + Send + 'static> Extend<Fut> for FuturesParallelOrdered<Fut>
where
    Fut::Output: Send,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Fut>,
    {
        for item in iter {
            self.push_back(item);
        }
    }
}
