//! A channel from the fetch workers to the coordinator that limits how far the workers read
//! ahead.
//!
//! Before a worker sends a block, it reserves the block's estimated encoded bytes in a shared
//! budget. It waits until the reserved bytes and the block fit in the read-ahead limit. The
//! bytes stay reserved after the coordinator receives the block, while the block waits in the
//! builder. The coordinator releases them when it puts the block in a batch.
//!
//! The block at `first_missing_height` always fits. Only this block lets the builder write more
//! blocks, and so release bytes. If this block waits, a deadlock occurs.

use std::{
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel},
    },
    time::Duration,
};

use crate::{codec::CompactBlockRecord, ingest::IngestError, types::ParsedCompactBlock};

use super::PipelineError;

/// Makes a channel. Workers can reserve at most `read_ahead_limit` estimated encoded bytes,
/// except for the block at `first_missing_height`.
pub(crate) fn channel(
    read_ahead_limit: usize,
    first_missing_height: u64,
) -> (ReadAheadSender, ReadAheadReceiver) {
    let (tx, rx) = sync_channel(0);
    let budget = Arc::new(ReadAheadBudget {
        state: Mutex::new(BudgetState {
            reserved_bytes: 0,
            first_missing_height,
            cancelled: false,
        }),
        changed: Condvar::new(),
        read_ahead_limit,
    });
    (ReadAheadSender { tx, budget: Arc::clone(&budget) }, ReadAheadReceiver { rx, budget })
}

/// The read-ahead budget that the senders and the receiver share. The condition variable wakes
/// the senders that wait when the state changes.
struct ReadAheadBudget {
    state: Mutex<BudgetState>,
    changed: Condvar,
    /// The most estimated encoded bytes that workers can reserve. The block at
    /// `first_missing_height` can go above it.
    read_ahead_limit: usize,
}

struct BudgetState {
    /// The estimated encoded bytes ([`CompactBlockRecord::encoded_size_bound`]) of the blocks
    /// that workers reserved and that the coordinator did not put in a batch yet. This includes
    /// a block that a worker reserved but did not give to the channel yet.
    reserved_bytes: usize,
    /// The first height that the coordinator does not have.
    first_missing_height: u64,
    /// When this flag is set, the senders that wait, and all later reservations, stop with an
    /// error.
    cancelled: bool,
}

impl ReadAheadBudget {
    fn lock(&self) -> MutexGuard<'_, BudgetState> {
        self.state.lock().expect("read-ahead budget lock is not poisoned")
    }

    /// Waits until `bytes` fit in the limit, then reserves them. The block at
    /// `first_missing_height` does not wait.
    fn reserve(&self, height: u64, bytes: usize) -> Result<(), PipelineError> {
        let mut state = self
            .changed
            .wait_while(self.lock(), |state| {
                !state.cancelled
                    && height != state.first_missing_height
                    && state.reserved_bytes + bytes > self.read_ahead_limit
            })
            .expect("read-ahead budget lock is not poisoned");
        if state.cancelled {
            return Err(PipelineError::Worker);
        }
        state.reserved_bytes += bytes;
        Ok(())
    }

    /// Gives back the bytes of the blocks that the coordinator put in a batch.
    fn release_bytes(&self, bytes: usize) {
        if bytes > 0 {
            let mut state = self.lock();
            state.reserved_bytes = state
                .reserved_bytes
                .checked_sub(bytes)
                .expect("released more bytes than were reserved");
            self.changed.notify_all();
        }
    }

    /// The worker with the block at `height` can then send it without a wait.
    fn set_first_missing_height(&self, height: u64) {
        let mut state = self.lock();
        if state.first_missing_height != height {
            state.first_missing_height = height;
            self.changed.notify_all();
        }
    }

    fn cancel(&self) {
        self.lock().cancelled = true;
        self.changed.notify_all();
    }
}

/// The sender of a worker. It sends the blocks of one segment at a time, through
/// [`ReadAheadSender::segment`].
#[derive(Clone)]
pub(crate) struct ReadAheadSender {
    tx: SyncSender<ParsedCompactBlock>,
    budget: Arc<ReadAheadBudget>,
}

impl ReadAheadSender {
    /// Starts a segment. The worker must send each height from `start` to `end`, in ascending
    /// order, and then call [`SegmentSender::finish`].
    pub(crate) fn segment(&mut self, start: u32, end: u32) -> SegmentSender<'_> {
        SegmentSender { sender: self, next_height: u64::from(start), end_height: u64::from(end) }
    }

    /// Stops all senders that wait. Use this when a worker fails.
    /// The failed worker can have the block at `first_missing_height`, and no other worker can
    /// send it.
    pub(crate) fn cancel(&self) {
        self.budget.cancel();
    }

    /// Reserves the block's bytes, then gives the block to the coordinator. The budget lock is
    /// not held during the send.
    fn send(&self, block: ParsedCompactBlock) -> Result<(), PipelineError> {
        let bytes = CompactBlockRecord::encoded_size_bound(&block.transactions)
            .map_err(IngestError::from)?;
        self.budget.reserve(u64::from(block.height), bytes)?;
        self.tx.send(block).map_err(|_| PipelineError::Worker)
    }
}

impl Drop for ReadAheadSender {
    fn drop(&mut self) {
        // A panic in a worker also stops the other workers.
        if std::thread::panicking() {
            self.cancel();
        }
    }
}

/// The sender for one segment. It checks that the worker sends the heights of the segment in
/// ascending order. A missing height must not cause a wait: if the block at
/// `first_missing_height` never comes, all workers wait for all time.
pub(crate) struct SegmentSender<'a> {
    sender: &'a mut ReadAheadSender,
    /// The height that the worker must send next.
    next_height: u64,
    /// The last height of the segment.
    end_height: u64,
}

impl SegmentSender<'_> {
    /// Sends the next block of the segment. The result is an error if the block is not the next
    /// height of the segment, or if the pipeline stopped.
    pub(crate) fn send(&mut self, block: ParsedCompactBlock) -> Result<(), PipelineError> {
        let height = u64::from(block.height);
        if height != self.next_height || height > self.end_height {
            return Err(PipelineError::SourceGap {
                expected: self.next_height as u32,
                actual: Some(block.height),
            });
        }
        self.next_height += 1;
        self.sender.send(block)
    }

    /// Ends the segment. The result is an error if the worker did not send all its heights.
    pub(crate) const fn finish(self) -> Result<(), PipelineError> {
        if self.next_height <= self.end_height {
            return Err(PipelineError::SourceGap {
                expected: self.next_height as u32,
                actual: None,
            });
        }
        Ok(())
    }
}

/// The receiver of the coordinator. When it is dropped, also after an error, all senders that
/// wait stop.
pub(crate) struct ReadAheadReceiver {
    rx: Receiver<ParsedCompactBlock>,
    budget: Arc<ReadAheadBudget>,
}

impl ReadAheadReceiver {
    /// Waits for a block for at most `timeout`. After all senders are dropped, the result is
    /// [`RecvTimeoutError::Disconnected`]. Receiving a block does not release its bytes.
    pub(crate) fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ParsedCompactBlock, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    /// Stops all senders: a sender that waits, or that sends later, gets an error. Blocks that
    /// were already sent can still be received.
    pub(crate) fn cancel(&self) {
        self.budget.cancel();
    }

    /// Sets the first height that the coordinator does not have. The worker with the block at
    /// this height can then send it without a wait.
    pub(crate) fn set_first_missing_height(&self, height: u64) {
        self.budget.set_first_missing_height(height);
    }

    /// Gives back the bytes of the blocks that the coordinator put in a batch.
    pub(crate) fn release_bytes(&self, bytes: usize) {
        self.budget.release_bytes(bytes);
    }
}

impl Drop for ReadAheadReceiver {
    fn drop(&mut self) {
        self.budget.cancel();
    }
}
