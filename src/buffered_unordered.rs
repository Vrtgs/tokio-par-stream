use crate::buffered_stream;
use crate::futures_unordered::FuturesParallelUnordered;

buffered_stream! {
    BufferedParallelUnordered,
    FuturesParallelUnordered,
    push
}
