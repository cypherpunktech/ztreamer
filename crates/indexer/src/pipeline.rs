//! Bounded parallel historical fetch-and-parse pipeline over Zakura's finalized database.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};
use tracing::info;

mod telemetry;
use telemetry::{PipelineMetrics, SegmentMetrics};

use zakura_chain::{
    block::{self, Height},
    serialization::{CompactSizeMessage, ZcashSerialize},
};
use zakura_state::{TransactionLocation, ZakuraDb};

use crate::{
    index::{Index, IndexError, IndexState},
    ingest::{IngestError, OrderedBuilder, WriteBatch},
    parser::{CompactParseError, ParsedCompactBlock, parse_stored_block},
};

const MIB: usize = 1024 * 1024;
pub(crate) const MAX_SOURCE_BLOCKS: u32 = 4096;

#[derive(Clone, Default)]
pub(crate) struct WorkerStats {
    read: Duration,
    header_read: Duration,
    transaction_read: Duration,
    txid_read: Duration,
    parse: Duration,
    send_wait: Duration,
    ranges: u64,
    blocks: u64,
    bytes: u64,
    transactions: u64,
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
    BlockExceedsByteLimit {
        height: u32,
        required: usize,
        limit: usize,
    },
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

/// A segment-oriented source seam: production scans raw transactions by range.
/// Implementations must send every height in the inclusive range, or return an error.
pub(crate) trait HistoricalSource: Sync {
    fn tip(&self) -> Option<(Height, block::Hash)>;
    fn block_hash(&self, height: u32) -> Option<block::Hash>;
    fn prune_height(&self) -> u32;
    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        max_source_bytes: usize,
        parsed_tx: &SyncSender<ParsedCompactBlock>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError>;
}

impl HistoricalSource for ZakuraDb {
    fn tip(&self) -> Option<(Height, block::Hash)> {
        ZakuraDb::tip(self)
    }

    fn block_hash(&self, height: u32) -> Option<block::Hash> {
        self.block_header(Height(height).into())
            .map(|header| header.hash())
    }

    fn prune_height(&self) -> u32 {
        ZakuraDb::prune_height(self).unwrap_or(Height::MIN).0
    }

    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        max_source_bytes: usize,
        parsed_tx: &SyncSender<ParsedCompactBlock>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError> {
        let transaction_range = TransactionLocation::min_for_height(Height(start))
            ..=TransactionLocation::max_for_height(Height(end));
        let mut transactions = self
            .raw_transactions_by_location_range(transaction_range)
            .peekable();

        for raw_height in start..=end {
            let height = Height(raw_height);
            let started = Instant::now();
            let header = self.block_header(height.into());
            stats.header_read += started.elapsed();
            let header = header.ok_or(PipelineError::SourceGap {
                expected: raw_height,
                actual: None,
            })?;

            let started = Instant::now();
            let txids = self.transaction_hashes_for_block(height.into());
            stats.txid_read += started.elapsed();
            let txids = txids.ok_or(PipelineError::MissingBody { height: raw_height })?;

            let mut block_transactions = Vec::new();
            loop {
                let started = Instant::now();
                let transaction = transactions.next_if(|(location, _)| location.height == height);
                stats.transaction_read += started.elapsed();
                let Some((transaction_location, transaction)) = transaction else {
                    break;
                };
                // Hashes are returned in block order; reject gaps in the raw body
                // before pairing them with transactions.
                if transaction_location
                    != TransactionLocation::from_usize(height, block_transactions.len())
                {
                    return Err(PipelineError::SourceChanged);
                }
                block_transactions.push(transaction);
            }
            if block_transactions.is_empty() {
                return Err(PipelineError::MissingBody { height: raw_height });
            }
            if block_transactions.len() != txids.len() {
                return Err(PipelineError::SourceChanged);
            }

            let hash = header.hash();
            if height.0 == end
                && expected_end_hash.is_some_and(|expected_hash| hash != expected_hash)
            {
                return Err(PipelineError::SourceChanged);
            }
            let tx_count = CompactSizeMessage::try_from(block_transactions.len())
                .expect("stored block transaction count is valid");
            let size = header
                .zcash_serialized_size()
                .checked_add(tx_count.zcash_serialized_size())
                .and_then(|size| {
                    block_transactions
                        .iter()
                        .try_fold(size, |size, transaction| {
                            size.checked_add(transaction.raw_bytes().len())
                        })
                })
                .ok_or(PipelineError::SourceSize { height: raw_height })?;
            let block_bytes = size
                .checked_add(
                    txids
                        .len()
                        .checked_mul(32)
                        .ok_or(PipelineError::SourceSize { height: raw_height })?,
                )
                .ok_or(PipelineError::SourceSize { height: raw_height })?;
            if block_bytes > max_source_bytes {
                return Err(PipelineError::BlockExceedsByteLimit {
                    height: raw_height,
                    required: block_bytes,
                    limit: max_source_bytes,
                });
            }
            stats.read = stats.header_read + stats.transaction_read + stats.txid_read;
            stats.ranges = 1;
            stats.blocks += 1;
            stats.transactions += block_transactions.len() as u64;
            stats.bytes += size as u64;

            let started = Instant::now();
            let parsed = parse_stored_block(
                raw_height,
                hash.0,
                header.previous_block_hash.0,
                u32::try_from(header.time.timestamp()).expect("stored block time is a u32"),
                block_transactions
                    .iter()
                    .zip(txids.iter().copied())
                    .map(|(transaction, txid)| (transaction.raw_bytes().as_slice(), txid)),
            );
            stats.parse += started.elapsed();
            let parsed = parsed?;

            let started = Instant::now();
            let sent = parsed_tx.send(parsed);
            stats.send_wait += started.elapsed();
            sent.map_err(|_| PipelineError::Worker)?;
        }

        Ok(())
    }
}

/// Fetch-and-parse workers feed one [`OrderedBuilder`], which feeds one LMDB writer.
pub(crate) struct HistoricalPipeline<'a, S = ZakuraDb> {
    index: &'a Index,
    db: &'a S,
    config: PipelineConfig,
    metrics: PipelineMetrics,
}

impl<'a, S: HistoricalSource> HistoricalPipeline<'a, S> {
    pub(crate) fn new(
        index: &'a Index,
        db: &'a S,
        config: PipelineConfig,
    ) -> Result<Self, PipelineError> {
        let pipeline = Self {
            index,
            db,
            config,
            metrics: PipelineMetrics::new(),
        };
        pipeline.validate_config()?;
        Ok(pipeline)
    }

    /// Ingests the finalized durable blocks available when this call starts.
    pub(crate) fn sync(&self) -> Result<IndexState, PipelineError> {
        let mut pass = self.metrics.pass();
        let result = self.sync_inner();
        pass.finish(result.is_ok());
        result
    }

    fn sync_inner(&self) -> Result<IndexState, PipelineError> {
        // read local resume point
        let initial_state = self.index.state()?;
        let durable_tip = initial_state.durable_tip();
        let start = durable_tip.map_or(Ok(0), |tip| {
            tip.height.checked_add(1).ok_or(IngestError::Overflow)
        })?;

        let (tip_height, tip_hash) = self.db.tip().ok_or(PipelineError::NoSourceTip)?;
        // The live-head reconciler validates durable records newer than Zakura's finalized tip.
        if durable_tip.is_some_and(|tip| tip.height > tip_height.0) {
            return Ok(initial_state);
        }
        if let Some(durable_tip) = durable_tip
            && self
                .db
                .block_hash(durable_tip.height)
                .is_none_or(|hash| hash.0 != durable_tip.hash)
        {
            return Err(PipelineError::DurableTipMismatch {
                height: durable_tip.height,
            });
        }
        if start < self.db.prune_height() {
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
            let (parsed_tx, parsed_rx) = sync_channel::<ParsedCompactBlock>(0);
            let (batch_tx, batch_rx) = sync_channel::<WriteBatch>(1);

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
            let (coordinator_receive_wait, batch_send_wait) =
                self.build_batches(initial_state, target, parsed_rx, batch_tx)?;

            let mut worker_stats = WorkerStats::default();
            for worker in workers {
                worker_stats.add(worker.join().map_err(|_| PipelineError::Panic)??);
            }
            let (state, write_stats) = writer.join().map_err(|_| PipelineError::Panic)??;
            if state.durable_tip().is_none_or(|tip| tip.height != target) {
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
        batch_rx: Receiver<WriteBatch>,
    ) -> Result<(IndexState, WriteStats), PipelineError> {
        let mut result = Ok(initial_state);
        let mut stats = WriteStats::default();
        while let Ok(batch) = batch_rx.recv() {
            if let Ok(state) = &mut result {
                stats.batches += 1;
                stats.blocks += batch.records.len() as u64;
                let started = Instant::now();
                let blocks = batch.records.len() as u64;
                let written = self.index.write(batch);
                let elapsed = started.elapsed();
                self.metrics.commit(elapsed, written.is_ok(), blocks);
                match written {
                    Ok(next) => *state = next,
                    Err(error) => result = Err(error.into()),
                }
                stats.write += elapsed;
            }
        }
        result.map(|state| (state, stats))
    }

    /// main parser worker job
    fn process_segments(
        &self,
        next: Arc<AtomicU64>,
        target: u32,
        tip_hash: block::Hash,
        parsed_tx: SyncSender<ParsedCompactBlock>,
    ) -> Result<WorkerStats, PipelineError> {
        let _active = self.metrics.worker();
        let mut stats = WorkerStats::default();
        loop {
            let segment_start = next.fetch_add(
                u64::from(self.config.source_segment_blocks),
                Ordering::Relaxed,
            );
            if segment_start > u64::from(target) {
                return Ok(stats);
            }
            let segment_end = segment_start
                .saturating_add(u64::from(self.config.source_segment_blocks - 1))
                .min(u64::from(target)) as u32;
            let mut segment = SegmentMetrics::new(&self.metrics);
            self.db.process_segment(
                segment_start as u32,
                segment_end,
                (segment_end == target).then_some(tip_hash),
                self.config.max_source_bytes,
                &parsed_tx,
                &mut segment.stats,
            )?;
            segment.success = true;
            stats.add(segment.stats.clone());
        }
    }

    // job handling the OrderedBuilder
    fn build_batches(
        &self,
        initial_state: IndexState,
        target: u32,
        parsed_rx: Receiver<ParsedCompactBlock>,
        batch_tx: SyncSender<WriteBatch>,
    ) -> Result<(Duration, Duration), PipelineError> {
        let mut builder = OrderedBuilder::new(initial_state, self.config.max_pending_bytes)?;
        let mut receive_wait = Duration::ZERO;
        let mut send_wait = Duration::ZERO;
        loop {
            let started = Instant::now();
            let Ok(block) = parsed_rx.recv() else {
                break;
            };
            let elapsed = started.elapsed();
            receive_wait += elapsed;
            self.metrics.receive_wait.record(elapsed.as_secs_f64());
            builder.push(block)?;
            self.metrics.buffer(&builder);
            if builder.ready_bytes() >= self.config.max_batch_bytes
                && let Some(batch) =
                    builder.build_batch(Some(target), Some(target), self.config.max_batch_bytes)?
            {
                let started = Instant::now();
                let sent = batch_tx.send(batch);
                let elapsed = started.elapsed();
                self.metrics.batch_wait.record(elapsed.as_secs_f64());
                send_wait += elapsed;
                self.metrics.buffer(&builder);
                sent.map_err(|_| PipelineError::Worker)?;
            }
        }
        while let Some(batch) =
            builder.build_batch(Some(target), Some(target), self.config.max_batch_bytes)?
        {
            let started = Instant::now();
            let sent = batch_tx.send(batch);
            let elapsed = started.elapsed();
            self.metrics.batch_wait.record(elapsed.as_secs_f64());
            send_wait += elapsed;
            self.metrics.buffer(&builder);
            sent.map_err(|_| PipelineError::Worker)?;
        }
        Ok((receive_wait, send_wait))
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
mod tests {
    use std::sync::mpsc::{TrySendError, sync_channel};

    use super::*;
    use crate::parser::ParsedCompactBlock;
    use crate::{Digest, codec::CompactBlockRecord};

    #[test]
    fn builder_stays_one_batch_ahead_of_a_blocked_writer() {
        let mut builder = OrderedBuilder::new(IndexState::default(), MIB).unwrap();
        for height in 0..3 {
            builder.push(prepared(height)).unwrap();
        }
        let batch_bytes = CompactBlockRecord::encoded_size_bound(&[]).unwrap();
        let first = builder
            .build_batch(Some(20), Some(20), batch_bytes)
            .unwrap()
            .unwrap();
        let (batch_tx, batch_rx) = sync_channel::<WriteBatch>(1);
        let (started_tx, started_rx) = sync_channel(0);
        let (release_tx, release_rx) = sync_channel(0);

        thread::scope(|scope| {
            let writer = scope.spawn(move || {
                let batch = batch_rx.recv().unwrap();
                assert_eq!(batch.records[0].height, 0);
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });

            batch_tx.send(first).unwrap();
            started_rx.recv().unwrap();
            let second = builder
                .build_batch(Some(20), Some(20), batch_bytes)
                .unwrap()
                .unwrap();
            assert_eq!(second.records[0].height, 1);
            batch_tx.send(second).unwrap();
            let third = builder
                .build_batch(Some(20), Some(20), batch_bytes)
                .unwrap()
                .unwrap();
            assert!(matches!(
                batch_tx.try_send(third),
                Err(TrySendError::Full(_))
            ));

            release_tx.send(()).unwrap();
            writer.join().unwrap();
        });
    }

    fn prepared(height: u32) -> ParsedCompactBlock {
        ParsedCompactBlock {
            height,
            hash: hash(height),
            previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
            time: height,
            transactions: Vec::new(),
            sapling_additions: 0,
            orchard_additions: 0,
            ironwood_additions: 0,
        }
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}

#[cfg(test)]
mod integration_tests;
