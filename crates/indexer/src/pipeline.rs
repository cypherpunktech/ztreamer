//! Bounded parallel historical fetch-and-parse pipeline over Zakura's finalized database.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};
use tracing::info;

mod flow_control;
mod remote;
mod telemetry;
mod zakura;
pub use flow_control::SegmentSender;
use flow_control::{ReadAheadReceiver, ReadAheadSender};
pub use remote::RemoteTailSource;
use telemetry::{PipelineMetrics, SegmentMetrics};

use zakura_chain::block::{self, Height};
use ztreamer_remote::RpcError;

use crate::{
    index::{Index, IndexError, IndexState},
    ingest::{
        EncodedBatch, IngestError, OrderedBuilder, WriteBatch, partial_range_flush_threshold,
    },
    parser::CompactParseError,
    types::RawBlockError,
};

const MIB: usize = 1024 * 1024;
pub(crate) const MAX_SOURCE_BLOCKS: u32 = 4096;
/// How often the coordinator checks the stop flag while it waits for a block.
const STOP_POLL: Duration = Duration::from_millis(50);

/// Per-worker timing and volume counters. A [`HistoricalSource`] fills these in
/// [`HistoricalSource::process_segment`]; the pipeline sums them over all workers and logs the
/// totals at the end of a pass.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerStats {
    /// Total time spent reading from the source. Sources that split their reads keep this equal
    /// to the sum of the three `*_read` fields.
    pub read: Duration,
    /// Time spent reading block headers.
    pub header_read: Duration,
    /// Time spent reading raw transactions.
    pub transaction_read: Duration,
    /// Time spent reading transaction ids.
    pub txid_read: Duration,
    /// Time spent in [`parse_stored_block`](crate::parser::parse_stored_block) or an equivalent.
    pub parse: Duration,
    /// Time spent waiting in [`SegmentSender::send`] for read-ahead budget.
    pub send_wait: Duration,
    /// Segments processed. The source sets this to 1 for one segment.
    pub ranges: u64,
    /// Blocks sent.
    pub blocks: u64,
    /// Source bytes read, as the serialized block size.
    pub bytes: u64,
    /// Transactions sent.
    pub transactions: u64,
}

impl WorkerStats {
    fn add(&mut self, other: Self) {
        self.read += other.read;
        self.header_read += other.header_read;
        self.transaction_read += other.transaction_read;
        self.txid_read += other.txid_read;
        self.parse += other.parse;
        self.send_wait += other.send_wait;
        self.ranges += other.ranges;
        self.blocks += other.blocks;
        self.bytes += other.bytes;
        self.transactions += other.transactions;
    }
}

#[derive(Default)]
struct WriteStats {
    write: Duration,
    batches: u64,
    blocks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    pub workers: usize,
    pub source_segment_blocks: u32,
    pub max_source_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_batch_bytes: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        let workers = thread::available_parallelism().map_or(1, usize::from);
        Self {
            workers: workers.min(8),
            source_segment_blocks: 256,
            max_source_bytes: 64 * MIB,
            max_pending_bytes: 256 * MIB,
            max_batch_bytes: 16 * MIB,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Parse(#[from] CompactParseError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error("pipeline configuration has a zero or out-of-range limit")]
    Config,
    #[error("Zakura has no finalized source tip")]
    NoSourceTip,
    #[error("Zakura has pruned the required block body at height {height}")]
    MissingBody { height: u32 },
    #[error("Zakura block at height {height} needs {required} source bytes, limit is {limit}")]
    BlockExceedsByteLimit { height: u32, required: usize, limit: usize },
    /// The source could not decode the serialized block at `height`.
    #[error("Zakura block at height {height} could not be decoded: {source}")]
    BlockDecode {
        /// The height of the block.
        height: u32,
        /// Why the block was refused.
        source: RawBlockError,
    },
    /// A request to a remote zakura node failed.
    #[error("remote Zakura request failed: {0}")]
    Remote(#[from] RpcError),
    #[error("Zakura source size overflowed at height {height}")]
    SourceSize { height: u32 },
    #[error("Zakura source changed during historical ingestion")]
    SourceChanged,
    #[error("LMDB durable tip at height {height} does not match Zakura")]
    DurableTipMismatch { height: u32 },
    #[error("Zakura returned a gap or wrong height: expected {expected}, got {actual:?}")]
    SourceGap { expected: u32, actual: Option<u32> },
    #[error("pipeline channel closed after a worker failed")]
    Worker,
    #[error("pipeline worker panicked")]
    Panic,
}

/// Where the historical pass reads finalized blocks from.
///
/// The pipeline runs [`PipelineConfig::workers`] threads. Each one claims segments of
/// [`PipelineConfig::source_segment_blocks`] consecutive heights and calls
/// [`Self::process_segment`] for each. The production source is Zakura's finalized database,
/// read in-process with its native range iterators. [`RemoteTailSource`] reads a remote node
/// over JSON-RPC.
///
/// The methods are called from blocking worker threads, never from an async context. A source
/// that talks to the network blocks on its own runtime handle.
///
/// # Contract
///
/// - [`Self::tip`], [`Self::block_hash`] and [`Self::prune_height`] describe the finalized chain
///   as the source sees it when called. The pass is planned from one `tip` call and ends with an
///   error if the source changes under it.
/// - These three methods return an error when the source cannot read the chain, for example
///   when a request to a remote node fails. `None` means only that the source has no such
///   block. Thus the pipeline does not report a read failure as a mismatch.
/// - [`Self::process_segment`] must send every height from `start` to `end` inclusive, in
///   ascending order, through `parsed_tx`, or return an error. A missing height or a height out
///   of order ends the pass; the sender refuses it with [`PipelineError::SourceGap`].
/// - When `expected_end_hash` is set and the block at `end` has a different hash, the source
///   returns [`PipelineError::SourceChanged`].
/// - A block whose serialized size plus 32 bytes per transaction id exceeds `max_source_bytes`
///   is reported as [`PipelineError::BlockExceedsByteLimit`], not sent.
/// - A block body the source no longer has is [`PipelineError::MissingBody`].
pub trait HistoricalSource: Sync {
    /// The highest finalized block, or `None` when the source has no blocks yet.
    fn tip(&self) -> Result<Option<(Height, block::Hash)>, PipelineError>;
    /// The hash of the finalized block at `height`, or `None` when the source has no such block.
    fn block_hash(&self, height: u32) -> Result<Option<block::Hash>, PipelineError>;
    /// The lowest height whose body the source still has. Zero when nothing is pruned.
    fn prune_height(&self) -> Result<u32, PipelineError>;
    /// Reads and parses the blocks from `start` to `end` inclusive and sends each one through
    /// `parsed_tx` in ascending order. See the trait documentation for the contract.
    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        max_source_bytes: usize,
        parsed_tx: &mut SegmentSender<'_>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError>;
}

/// The historical pass over one [`HistoricalSource`]: fetch-and-parse workers feed one
/// [`OrderedBuilder`], which feeds one LMDB writer.
///
/// `ztreamerd` builds one over the embedded `ZakuraDb` or over [`RemoteTailSource`]. A source in
/// another crate builds one the same way:
///
/// ```no_run
/// # use std::sync::atomic::AtomicBool;
/// # use ztreamer_indexer::{index::Index, pipeline::{HistoricalPipeline, HistoricalSource, PipelineConfig, PipelineError}};
/// # fn run(index: &Index, source: &impl HistoricalSource, stop: &AtomicBool) -> Result<(), PipelineError> {
/// let state = HistoricalPipeline::new(index, source, PipelineConfig::default())?
///     .with_stop(stop)
///     .sync_until_stable()?;
/// # let _ = state;
/// # Ok(())
/// # }
/// ```
pub struct HistoricalPipeline<'a, S> {
    index: &'a Index,
    db: &'a S,
    config: PipelineConfig,
    metrics: PipelineMetrics,
    /// When this flag is set, the pass stops early but completes normally (see [`Self::with_stop`]).
    stop: Option<&'a AtomicBool>,
}

impl<S> std::fmt::Debug for HistoricalPipeline<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalPipeline").field("config", &self.config).finish_non_exhaustive()
    }
}

impl<'a, S: HistoricalSource> HistoricalPipeline<'a, S> {
    /// Builds a pipeline over `db` that writes into `index`. Fails with
    /// [`PipelineError::Config`] when a limit in `config` is zero or out of range.
    pub fn new(index: &'a Index, db: &'a S, config: PipelineConfig) -> Result<Self, PipelineError> {
        let pipeline = Self { index, db, config, metrics: PipelineMetrics::new(), stop: None };
        pipeline.validate_config()?;
        Ok(pipeline)
    }

    /// Lets `stop` end the pass early, for example at shutdown. When `stop` is set, each worker
    /// completes its segment and starts no new segment. The writer writes the blocks that it
    /// has, and the pass returns a state with a lower tip.
    pub const fn with_stop(mut self, stop: &'a AtomicBool) -> Self {
        self.stop = Some(stop);
        self
    }

    fn stopped(&self) -> bool {
        self.stop.is_some_and(|stop| stop.load(Ordering::Relaxed))
    }

    /// Repeats [`Self::sync`] until two consecutive passes see the same durable tip, or until
    /// the stop flag is set, and returns the index state at the end.
    pub fn sync_until_stable(&self) -> Result<IndexState, PipelineError> {
        let mut previous_tip = None;
        loop {
            let state = self.sync()?;
            if self.stopped() || previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }

    /// Ingests the finalized durable blocks available when this call starts.
    pub fn sync(&self) -> Result<IndexState, PipelineError> {
        let mut pass = self.metrics.pass();
        let result = self.sync_inner();
        pass.finish(result.is_ok());
        if result.is_ok() && self.stopped() {
            pass.stopped();
        }
        result
    }

    fn sync_inner(&self) -> Result<IndexState, PipelineError> {
        // read local resume point
        let initial_state = self.index.state()?;
        let durable_tip = initial_state.durable_tip();
        let start = durable_tip
            .map_or(Ok(0), |tip| tip.height.checked_add(1).ok_or(IngestError::Overflow))?;

        let (tip_height, tip_hash) = self.db.tip()?.ok_or(PipelineError::NoSourceTip)?;
        // The live-head reconciler validates durable records newer than Zakura's finalized tip.
        if durable_tip.is_some_and(|tip| tip.height > tip_height.0) {
            return Ok(initial_state);
        }
        if let Some(durable_tip) = durable_tip
            && self.db.block_hash(durable_tip.height)?.is_none_or(|hash| hash != durable_tip.hash)
        {
            return Err(PipelineError::DurableTipMismatch { height: durable_tip.height });
        }
        if start < self.db.prune_height()? {
            return Err(PipelineError::MissingBody { height: start });
        }
        let target = tip_height.0;
        self.metrics.target.set(f64::from(target));
        if start > target {
            return Ok(initial_state);
        }

        thread::scope(|scope| {
            let next = Arc::new(AtomicU64::new(u64::from(start)));

            // `workers` send ParsedCompactBlocks over parsed_tx, `coordinator` below recieves parsed_rx and writes to batch_tx
            // `writer` reads batch_rx and populates index
            // Workers can reserve bytes until the reserved bytes are at the partial-range
            // threshold. Then only the block at `first_missing_height` can go through (see
            // `read_ahead_limit` in `build_batches`).
            let (parsed_tx, parsed_rx) = flow_control::channel(
                partial_range_flush_threshold(
                    self.config.max_pending_bytes,
                    self.config.max_batch_bytes,
                ),
                u64::from(start),
            );
            let (batch_tx, batch_rx) = sync_channel::<EncodedBatch>(1);

            // single threaded writer receives WriteBatches and writes to Index
            let writer = scope.spawn(move || self.write_batches(initial_state, batch_rx));

            // concurrent parser workers
            let workers: Vec<_> = (0..self.config.workers)
                .map(|_| {
                    let parsed_tx = parsed_tx.clone();
                    let next = Arc::clone(&next);
                    // all workers add the segment size atomically to `next` to split work
                    //
                    // todo(@distractedm1nd): all ranges are not equal. optimize by having more workers in sandblasting range?
                    scope.spawn(move || self.process_segments(next, target, tip_hash, parsed_tx))
                })
                .collect();
            drop(parsed_tx);

            // single threaded ordered builder job gives batches to `writer` job
            let built = self.build_batches(initial_state, target, parsed_rx, batch_tx);

            // `build_batches` dropped its ends of both channels: the workers that wait stop, and
            // the writer ends after its last batch. Join all threads before returning, so that
            // the pass gives the error that caused the failure, and `Panic` for a panic.
            // One failure stops the other workers with `Worker`. Do not keep those errors.
            let mut worker_stats = WorkerStats::default();
            let mut worker_error = None;
            let mut worker_stopped = false;
            let mut panicked = false;
            for worker in workers {
                match worker.join() {
                    Ok(Ok(stats)) => worker_stats.add(stats),
                    Ok(Err(PipelineError::Worker)) => worker_stopped = true,
                    Ok(Err(error)) => {
                        worker_error.get_or_insert(error);
                    }
                    Err(_) => panicked = true,
                }
            }
            let written = writer.join();
            if panicked || written.is_err() {
                return Err(PipelineError::Panic);
            }
            let written = written.expect("a writer panic returned above");
            let (coordinator_receive_wait, batch_send_wait) = match built {
                Ok(waits) => waits,
                // A send fails only when the writer stopped. Give the error of the writer.
                Err(PipelineError::Worker) => {
                    written?;
                    return Err(worker_error.unwrap_or(PipelineError::Worker));
                }
                Err(error) => return Err(error),
            };
            if let Some(error) = worker_error {
                return Err(error);
            }
            // A stop cancels the channel, so the workers also end with `Worker` then.
            if worker_stopped && !self.stopped() {
                return Err(PipelineError::Worker);
            }
            let (state, write_stats) = written?;
            // After a stop, the pass can end below the target.
            if state.durable_tip().is_none_or(|tip| tip.height != target) && !self.stopped() {
                return Err(PipelineError::SourceGap {
                    expected: state.durable_tip().map_or(start, |tip| tip.height + 1),
                    actual: None,
                });
            }
            info!(
                start_height = start,
                target_height = target,
                target_hash = %tip_hash,
                fetch_read_seconds = worker_stats.read.as_secs_f64(),
                fetch_header_read_seconds = worker_stats.header_read.as_secs_f64(),
                fetch_transaction_read_seconds = worker_stats.transaction_read.as_secs_f64(),
                fetch_txid_read_seconds = worker_stats.txid_read.as_secs_f64(),
                parse_seconds = worker_stats.parse.as_secs_f64(),
                worker_send_wait_seconds = worker_stats.send_wait.as_secs_f64(),
                processed_ranges = worker_stats.ranges,
                processed_blocks = worker_stats.blocks,
                processed_bytes = worker_stats.bytes,
                coordinator_receive_wait_seconds = coordinator_receive_wait.as_secs_f64(),
                batch_send_wait_seconds = batch_send_wait.as_secs_f64(),
                write_seconds = write_stats.write.as_secs_f64(),
                write_batches = write_stats.batches,
                written_blocks = write_stats.blocks,
                "historical pipeline stage totals"
            );
            Ok(state)
        })
    }

    // receives WriteBatches from OrderedBuilder to write them to Index
    fn write_batches(
        &self,
        initial_state: IndexState,
        batch_rx: Receiver<EncodedBatch>,
    ) -> Result<(IndexState, WriteStats), PipelineError> {
        let mut state = initial_state;
        let mut stats = WriteStats::default();
        while let Ok(batch) = batch_rx.recv() {
            stats.batches += 1;
            let blocks = batch.blocks.len() as u64;
            stats.blocks += blocks;
            let started = Instant::now();
            let written = self.index.write_encoded(batch);
            let elapsed = started.elapsed();
            self.metrics.commit(elapsed, written.is_ok(), blocks);
            stats.write += elapsed;
            // Stop at the first error. The coordinator then stops when its next send fails.
            state = written?;
        }
        Ok((state, stats))
    }

    /// main parser worker job
    fn process_segments(
        &self,
        next: Arc<AtomicU64>,
        target: u32,
        tip_hash: block::Hash,
        mut parsed_tx: ReadAheadSender,
    ) -> Result<WorkerStats, PipelineError> {
        let _active = self.metrics.worker();
        let mut stats = WorkerStats::default();
        loop {
            if self.stopped() {
                return Ok(stats);
            }
            let segment_start =
                next.fetch_add(u64::from(self.config.source_segment_blocks), Ordering::Relaxed);
            if segment_start > u64::from(target) {
                return Ok(stats);
            }
            let segment_end = segment_start
                .saturating_add(u64::from(self.config.source_segment_blocks - 1))
                .min(u64::from(target)) as u32;
            let mut segment = SegmentMetrics::new(&self.metrics);
            let mut segment_tx = parsed_tx.segment(segment_start as u32, segment_end);
            let processed = self
                .db
                .process_segment(
                    segment_start as u32,
                    segment_end,
                    (segment_end == target).then_some(tip_hash),
                    self.config.max_source_bytes,
                    &mut segment_tx,
                    &mut segment.stats,
                )
                .and_then(|()| segment_tx.finish());
            if let Err(error) = processed {
                // `Worker` means that another thread failed or the pass stopped. This segment
                // did not fail, so it records no outcome.
                if matches!(error, PipelineError::Worker) {
                    segment.cancelled = true;
                }
                parsed_tx.cancel();
                return Err(error);
            }
            segment.success = true;
            stats.add(segment.stats);
        }
    }

    // job handling the OrderedBuilder
    fn build_batches(
        &self,
        initial_state: IndexState,
        target: u32,
        parsed_rx: ReadAheadReceiver,
        batch_tx: SyncSender<EncodedBatch>,
    ) -> Result<(Duration, Duration), PipelineError> {
        let mut builder = OrderedBuilder::new(initial_state, self.config.max_pending_bytes)?;
        // Write a batch when `max_batch_bytes` are ready. Also write batches while `pending` is
        // above the channel limit, so that the next block, which the channel always lets
        // through, cannot make `pending` more than `max_pending_bytes`.
        let read_ahead_limit = partial_range_flush_threshold(
            self.config.max_pending_bytes,
            self.config.max_batch_bytes,
        );
        let mut receive_wait = Duration::ZERO;
        let mut send_wait = Duration::ZERO;
        let mut cancelled = false;
        loop {
            // After a stop, cancel the channel: each worker stops after its current block and
            // does not complete its segment. The blocks already sent are still received.
            if !cancelled && self.stopped() {
                parsed_rx.cancel();
                cancelled = true;
            }
            let started = Instant::now();
            let block = match parsed_rx.recv_timeout(STOP_POLL) {
                Ok(block) => block,
                Err(RecvTimeoutError::Timeout) => {
                    receive_wait += started.elapsed();
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };
            let elapsed = started.elapsed();
            receive_wait += elapsed;
            self.metrics.receive_wait.record(elapsed.as_secs_f64());
            builder.push(block)?;
            parsed_rx.set_first_missing_height(builder.first_missing_height());
            self.metrics.buffer(&builder);
            if builder.ready_bytes() >= self.config.max_batch_bytes
                || builder.pending_bytes() > read_ahead_limit
            {
                loop {
                    let pending = builder.pending_bytes();
                    let Some(batch) = builder.build_batch(
                        Some(target),
                        Some(target),
                        self.config.max_batch_bytes,
                    )?
                    else {
                        break;
                    };
                    parsed_rx.release_bytes(pending - builder.pending_bytes());
                    send_wait += self.send_batch(batch, &batch_tx)?;
                    self.metrics.buffer(&builder);
                    if builder.pending_bytes() <= read_ahead_limit {
                        break;
                    }
                }
            }
        }
        // After a stop, the workers sent only part of the heights. Write every contiguous block
        // that arrived: the last range is not complete, so its blocks go to mutable rows and the
        // next pass does not fetch them again.
        let drain_through = if self.stopped() {
            u32::try_from(builder.first_missing_height())
                .ok()
                .and_then(|height| height.checked_sub(1))
                .map(|height| height.min(target))
        } else {
            Some(target)
        };
        while let Some(batch) =
            builder.build_batch(drain_through, drain_through, self.config.max_batch_bytes)?
        {
            send_wait += self.send_batch(batch, &batch_tx)?;
            self.metrics.buffer(&builder);
        }
        Ok((receive_wait, send_wait))
    }

    /// Encodes the batch on this thread, not on the writer thread. Then sends it to the writer.
    /// Returns the time that the send waited.
    fn send_batch(
        &self,
        batch: WriteBatch,
        batch_tx: &SyncSender<EncodedBatch>,
    ) -> Result<Duration, PipelineError> {
        let started = Instant::now();
        let batch = batch.encode(self.config.workers).map_err(IngestError::from)?;
        self.metrics.encode.record(started.elapsed().as_secs_f64());
        let started = Instant::now();
        let sent = batch_tx.send(batch);
        let elapsed = started.elapsed();
        self.metrics.batch_wait.record(elapsed.as_secs_f64());
        sent.map_err(|_| PipelineError::Worker)?;
        Ok(elapsed)
    }

    fn validate_config(&self) -> Result<(), PipelineError> {
        if self.config.workers == 0
            || !(1..=MAX_SOURCE_BLOCKS).contains(&self.config.source_segment_blocks)
            || self.config.max_source_bytes == 0
            || self.config.max_pending_bytes == 0
            || self.config.max_batch_bytes == 0
            || self.config.max_batch_bytes > self.config.max_pending_bytes
        {
            return Err(PipelineError::Config);
        }
        Ok(())
    }
}

#[cfg(test)]
mod coordinator_tests;
