mod buffered;
mod buffered_unordered;

mod futures_ordered;
mod futures_unordered;

pub use self::{
    futures_ordered::FuturesParallelOrdered, futures_unordered::FuturesParallelUnordered,
};
use futures::Stream;
use std::future::Future;

pub trait TokioParStream: Stream {
    fn par_buffered(self, n: usize) -> BufferedParallel<Self>
    where
        Self: Sized,
        Self::Item: Future;

    fn par_buffered_unordered(self, n: usize) -> BufferedParallelUnordered<Self>
    where
        Self: Sized,
        Self::Item: Future;
}

impl<St: Stream> TokioParStream for St {
    fn par_buffered(self, n: usize) -> BufferedParallel<Self>
    where
        Self: Sized,
        Self::Item: Future,
    {
        BufferedParallel::new(self, Some(n))
    }

    fn par_buffered_unordered(self, n: usize) -> BufferedParallelUnordered<Self>
    where
        Self: Sized,
        Self::Item: Future,
    {
        BufferedParallelUnordered::new(self, Some(n))
    }
}

pub(crate) mod order {
    use pin_project_lite::pin_project;
    use std::cmp::Ordering;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    pin_project! {
        #[must_use = "futures do nothing unless you `.await` or poll them"]
        #[derive(Debug)]
        pub(crate) struct OrderWrapper<T> {
            #[pin]
            pub(crate) data: T, // A future or a future's output
            // Use i64 for index since isize may overflow in 32-bit targets.
            pub(crate) index: i64,
        }
    }

    impl<T> PartialEq for OrderWrapper<T> {
        fn eq(&self, other: &Self) -> bool {
            self.index == other.index
        }
    }

    impl<T> Eq for OrderWrapper<T> {}

    impl<T> PartialOrd for OrderWrapper<T> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<T> Ord for OrderWrapper<T> {
        fn cmp(&self, other: &Self) -> Ordering {
            // BinaryHeap is a max heap, so compare backwards here.
            other.index.cmp(&self.index)
        }
    }

    impl<T> Future for OrderWrapper<T>
    where
        T: Future,
    {
        type Output = OrderWrapper<T::Output>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let index = self.index;
            self.project().data.poll(cx).map(|output| OrderWrapper {
                data: output,
                index,
            })
        }
    }
}

macro_rules! buffered_stream {
    ($ty: ident, $backing:ident, $push: ident) => {
        use futures::stream::{Fuse, FusedStream};
        use futures::{Stream, StreamExt};
        use pin_project_lite::pin_project;
        use std::fmt::{Debug, Formatter};
        use std::future::Future;
        use std::num::NonZeroUsize;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        pin_project! {
        #[must_use = "streams do nothing unless polled"]
        pub struct $ty<St>
        where
            St: Stream,
            St::Item: Future
        {
                #[pin]
                stream: Fuse<St>,
                in_progress_queue: $backing<St::Item>,
                limit: Option<NonZeroUsize>,
            }
        }

        impl<St> Debug for $ty<St>
        where
            St: Stream,
            St::Item: Future,
        {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("BufferedParallelUnordered")
                    .finish_non_exhaustive()
            }
        }

        impl<St> $ty<St>
        where
            St: Stream,
            St::Item: Future,
        {
            pub(super) fn new(stream: St, limit: Option<usize>) -> Self {
                Self {
                    stream: stream.fuse(),
                    in_progress_queue: $backing::new(),
                    // limit = 0 => no limit
                    limit: limit.and_then(NonZeroUsize::new),
                }
            }
        }

        impl<St> Stream for $ty<St>
        where
            St: Stream,
            St::Item: Future + Send + 'static,
            <St::Item as Future>::Output: Send,
        {
            type Item = <St::Item as Future>::Output;

            fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                let mut this = self.project();

                let limit = *this.limit;
                while limit.map_or(true, |limit| this.in_progress_queue.len() < limit.get()) {
                    match this.stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(fut)) => this.in_progress_queue.$push(fut),
                        Poll::Ready(None) | Poll::Pending => break,
                    }
                }

                // attempt to pull from the queue in progress
                if let x @ (Poll::Pending | Poll::Ready(Some(_))) =
                    this.in_progress_queue.poll_next_unpin(cx)
                {
                    return x;
                }

                // If more values are still coming from the stream, we're not done yet
                if this.stream.is_done() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                let queue_len = self.in_progress_queue.len();
                let (lower, upper) = self.stream.size_hint();
                (
                    lower.saturating_add(queue_len),
                    upper.and_then(|x| x.checked_add(queue_len)),
                )
            }
        }

        impl<St> FusedStream for $ty<St>
        where
            St: Stream,
            St::Item: Future + Send + 'static,
            <St::Item as Future>::Output: Send,
        {
            fn is_terminated(&self) -> bool {
                self.in_progress_queue.is_empty() && self.stream.is_terminated()
            }
        }
    };
}

use crate::buffered::BufferedParallel;
use crate::buffered_unordered::BufferedParallelUnordered;
pub(crate) use buffered_stream;


#[cfg(test)]
mod tests {
    use std::ops::Range;
    use super::*;
    use futures::stream::iter;
    use futures::StreamExt;

    const TEST_RANGE: Range<u64> = 0..256;

    fn transform(i: u64) -> u64 {
        i * 2
    }
    
    fn test_stream() -> impl Stream<Item: Future<Output=u64> + Send + 'static> {
        iter(TEST_RANGE).map(|i| async move {
            for _ in i..TEST_RANGE.end {
                tokio::task::yield_now().await
            }
            transform(i)
        })
    }
    
    #[tokio::test]
    async fn buffered() {
        let stream = test_stream().par_buffered(8);
        assert!(stream.zip(iter(TEST_RANGE).map(transform)).all(|(x, y)| async move { x == y }).await);
    }

    #[tokio::test]
    async fn buffered_unordered() {
        let mut items = test_stream().buffer_unordered(8).collect::<Vec<_>>().await;
        items.sort();
        assert_eq!(items, TEST_RANGE.map(transform).collect::<Vec<_>>());
    }
}
