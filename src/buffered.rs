use crate::buffered_stream;
use crate::futures_ordered::FuturesParallelOrdered;

buffered_stream! {
    BufferedParallel,
    FuturesParallelOrdered,
    push_back
}
