//! Real coordinator/writer tests with a deterministic segment source.

use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use super::*;
use crate::{
    codec::{CompactBlockRecord, TreeSizes},
    ingest::OrderedBuilder,
    test_utils::hash,
    types::{CompactSaplingOutput, CompactShieldedAction, CompactTransaction, ParsedCompactBlock},
};
use zakura_chain::{parameters::GENESIS_PREVIOUS_BLOCK_HASH, transaction};

struct Fixture {
    count: u32,
    fail_at: Option<u32>,
    panic_at: Option<u32>,
    omit: Option<u32>,
    /// Sends the heights of each segment from the last to the first.
    descending: bool,
    reorder: bool,
    later_sent: (Mutex<bool>, Condvar),
    /// Sets `stop` when the source sends this height.
    stop_at: Option<u32>,
    stop: AtomicBool,
}

impl Fixture {
    fn new(count: u32) -> Self {
        Self {
            count,
            fail_at: None,
            panic_at: None,
            omit: None,
            descending: false,
            reorder: false,
            later_sent: (Mutex::new(false), Condvar::new()),
            stop_at: None,
            stop: AtomicBool::new(false),
        }
    }
}

impl HistoricalSource for Fixture {
    fn tip(&self) -> Option<(Height, block::Hash)> {
        self.count.checked_sub(1).map(|height| (Height(height), hash(height)))
    }

    fn block_hash(&self, height: u32) -> Option<block::Hash> {
        (height < self.count).then(|| hash(height))
    }

    fn prune_height(&self) -> u32 {
        0
    }

    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        _max_source_bytes: usize,
        tx: &mut SegmentSender<'_>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError> {
        if self.reorder && start == 0 {
            let (lock, ready) = &self.later_sent;
            let (sent, timeout) = ready
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |sent| !*sent)
                .unwrap();
            assert!(*sent && !timeout.timed_out(), "a later block must arrive before block zero");
        }
        let mut heights: Vec<_> = (start..=end).collect();
        if self.descending {
            heights.reverse();
        }
        for height in heights {
            assert_ne!(self.panic_at, Some(height), "injected worker panic");
            if self.fail_at == Some(height) {
                return Err(PipelineError::MissingBody { height });
            }
            if self.omit == Some(height) {
                continue;
            }
            let block = prepared(height);
            stats.blocks += 1;
            stats.transactions += block.transactions.len() as u64;
            stats.bytes +=
                CompactBlockRecord::encoded_size_bound(&block.transactions).unwrap() as u64;
            tx.send(block)?;
            if self.stop_at == Some(height) {
                self.stop.store(true, Ordering::Relaxed);
            }
            if start > 0 {
                let (lock, ready) = &self.later_sent;
                *lock.lock().unwrap() = true;
                ready.notify_one();
            }
        }
        if expected_end_hash.is_some_and(|expected| expected != hash(end)) {
            return Err(PipelineError::SourceChanged);
        }
        Ok(())
    }
}

fn prepared(height: u32) -> ParsedCompactBlock {
    let count = height % 3; // Empty, light, and heavier blocks; all three pools.
    let action = CompactShieldedAction {
        nullifier: hash(height).0,
        commitment: [3; 32],
        ephemeral_key: [4; 32],
        ciphertext: [5; 52],
    };
    let transactions = if count == 0 {
        Vec::new()
    } else {
        vec![CompactTransaction {
            index: 1,
            txid: transaction::Hash(hash(height).0),
            sapling_spends: vec![hash(height).0],
            sapling_outputs: vec![
                CompactSaplingOutput {
                    cmu: [2; 32],
                    ephemeral_key: [4; 32],
                    ciphertext: [5; 52],
                };
                count as usize
            ],
            orchard_actions: vec![action; count as usize],
            ironwood_actions: vec![action; count as usize],
        }]
    };
    ParsedCompactBlock {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH),
        time: height,
        transactions,
        sapling_additions: count,
        orchard_additions: count,
        ironwood_additions: count,
    }
}

fn config(workers: usize) -> PipelineConfig {
    PipelineConfig {
        workers,
        source_segment_blocks: 37,
        max_batch_bytes: 32 * 1024,
        ..PipelineConfig::default()
    }
}

fn open(dir: &tempfile::TempDir, size: usize) -> Index {
    Index::open(dir.path(), size, "Mainnet", hash(0)).unwrap()
}

/// Independent expected records: no OrderedBuilder in the oracle.
fn verify(index: &Index, count: u32) -> Vec<CompactBlockRecord> {
    let state = index.state().unwrap();
    assert_eq!(state.durable_tip().map(|tip| tip.height), count.checked_sub(1));
    let mut sizes = TreeSizes::default();
    let records = (0..count)
        .map(|height| {
            let parsed = prepared(height);
            sizes.sapling += height % 3;
            sizes.orchard += height % 3;
            sizes.ironwood += height % 3;
            let expected = CompactBlockRecord {
                height,
                hash: parsed.hash,
                previous_hash: parsed.previous_hash,
                time: parsed.time,
                transactions: parsed.transactions,
                end_tree_sizes: sizes,
            };
            let actual = index.read_block(state.generation(), height).unwrap();
            assert_eq!(actual, expected);
            actual
        })
        .collect();
    assert_eq!(state.tree_sizes(), sizes);
    assert!(index.read_block(state.generation(), count).is_err());
    index.verify_continuity().unwrap();
    records
}

#[test]
fn sequential_parallel_and_skewed_ingestion_match_and_reopen() {
    let mut baseline = None;
    for workers in [1, 2, 8] {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir, 32 * MIB);
        let mut source = Fixture::new(1_107); // Cross sealed/mutable boundary.
        source.reorder = workers > 1;
        HistoricalPipeline::new(&index, &source, config(workers)).unwrap().sync().unwrap();
        let records = verify(&index, source.count);
        if let Some(baseline) = &baseline {
            assert_eq!(&records, baseline);
        } else {
            baseline = Some(records);
        }
        drop(index);
        verify(&open(&dir, 32 * MIB), source.count);
    }
}

/// A channel limit of a few blocks (4 KiB). Workers must wait for the block at `next_height`.
/// `pending` is then above the channel limit, so the coordinator writes a batch after each
/// push.
fn tight(workers: usize) -> PipelineConfig {
    let config = config(workers);
    PipelineConfig { max_pending_bytes: crate::codec::MAX_RECORD_BYTES + 4 * 1024, ..config }
}

/// Stops the test process if the pass does not complete in 30 seconds.
/// A scoped join cannot stop a thread that is in a deadlock.
fn sync_within(
    index: &Index,
    source: &Fixture,
    config: PipelineConfig,
) -> Result<IndexState, PipelineError> {
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let watchdog = thread::spawn(move || {
        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
            done_rx.recv_timeout(Duration::from_secs(30))
        {
            eprintln!("historical pass did not complete in 30 seconds");
            std::process::abort();
        }
    });
    let result = HistoricalPipeline::new(index, source, config).unwrap().sync();
    drop(done_tx);
    watchdog.join().unwrap();
    result
}

#[test]
fn workers_wait_at_a_small_channel_limit() {
    for workers in [1, 8] {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir, 32 * MIB);
        let mut source = Fixture::new(1_107);
        source.reorder = workers > 1;
        sync_within(&index, &source, tight(workers)).unwrap();
        verify(&index, source.count);
    }
}

#[test]
fn zero_channel_limit_completes() {
    // When `max_pending_bytes` is equal to `max_batch_bytes`, validation passes and the
    // channel limit is zero. Only the block at `next_height` can go through.
    // The pending bytes must not become more than `max_pending_bytes`.
    let base = config(8);
    let config = PipelineConfig { max_pending_bytes: base.max_batch_bytes, ..base };
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let source = Fixture::new(1_107);
    sync_within(&index, &source, config).unwrap();
    verify(&index, source.count);
}

#[test]
fn missing_or_unordered_height_fails_instead_of_waiting() {
    // With a small channel limit, a missing height makes the other workers wait for it.
    for omit in [0, 10, 1_050] {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir, 32 * MIB);
        let mut source = Fixture::new(1_107);
        source.omit = Some(omit);
        assert!(matches!(
            sync_within(&index, &source, tight(4)),
            Err(PipelineError::SourceGap { .. })
        ));
    }
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(1_107);
    source.descending = true;
    assert!(matches!(sync_within(&index, &source, tight(4)), Err(PipelineError::SourceGap { .. })));
}

#[test]
fn failed_worker_stops_workers_that_wait() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(1_107);
    source.fail_at = Some(500);
    let result = metrics::with_local_recorder(&recorder, || sync_within(&index, &source, tight(8)));
    assert!(matches!(result, Err(PipelineError::MissingBody { height: 500 })));
    // Only the segment that failed records the outcome "error". The segments that the failure
    // stopped record no outcome.
    let segment_errors: usize = snapshotter
        .snapshot()
        .into_vec()
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "ztreamer.pipeline.segment.seconds"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "error")
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => samples.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(segment_errors, 1);
    let count = index.state().unwrap().durable_tip().map_or(0, |tip| tip.height + 1);
    assert!(count <= 500);
    verify(&index, count);
}

#[test]
fn historical_pass_seals_a_range_that_the_head_follower_left_in_mutable_rows() {
    // The head follower writes blocks at 10 blocks deep and seals a range at 100 blocks deep.
    // At tip 1,098 it has written blocks 0 to 1,088: range 0..=999 is complete in mutable rows,
    // and it is not sealed. A restart at this point runs the historical pass on this state.
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut builder = OrderedBuilder::new(IndexState::default(), 32 * MIB).unwrap();
    for height in 0..=1_088 {
        builder.push(prepared(height)).unwrap();
    }
    let batch = builder.build_batch(Some(1_088), Some(998), 32 * MIB).unwrap().unwrap();
    let state = index.write(batch).unwrap();
    assert_eq!(state.sealed_through(), None);

    // The historical pass continues from 1,089. Its seal height is its target, so it seals
    // range 0..=999, although its first batch starts after that range.
    let source = Fixture::new(1_200);
    let state = HistoricalPipeline::new(&index, &source, config(4))
        .unwrap()
        .sync()
        .unwrap_or_else(|error| panic!("historical pass failed: {error}"));
    assert_eq!(state.sealed_through(), Some(999));
    verify(&index, source.count);
}

#[test]
fn stop_ends_the_pass_early_with_a_valid_prefix() {
    // With one worker, the stop is set after the worker sends 5,000. The coordinator cancels
    // the channel at its next receive. Up to two more blocks still go through: one that it
    // receives before it sees the stop, and one that the worker already gives to the channel.
    // The pass writes those blocks, also from the incomplete range 5,000 to 5,999.
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(20_000);
    source.stop_at = Some(5_000);
    let state = HistoricalPipeline::new(&index, &source, config(1))
        .unwrap()
        .with_stop(&source.stop)
        .sync()
        .unwrap();
    let count = state.durable_tip().map_or(0, |tip| tip.height + 1);
    assert!((5_001..=5_003).contains(&count), "{count}");
    verify(&index, count);

    // With more workers, the workers stop in the middle of their segments. The pass writes the
    // contiguous blocks that it has.
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(20_000);
    source.stop_at = Some(5_000);
    let state = HistoricalPipeline::new(&index, &source, config(4))
        .unwrap()
        .with_stop(&source.stop)
        .sync()
        .unwrap();
    let count = state.durable_tip().map_or(0, |tip| tip.height + 1);
    assert!(count < source.count, "{count}");
    verify(&index, count);
}

#[test]
fn source_failure_leaves_a_resumable_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(1_107);
    source.fail_at = Some(1_000);
    assert!(HistoricalPipeline::new(&index, &source, config(1)).unwrap().sync().is_err());
    let count = index.state().unwrap().durable_tip().map_or(0, |tip| tip.height + 1);
    assert!(count > 0 && count <= 1_000);
    verify(&index, count);
    drop(index);
    let index = open(&dir, 32 * MIB);
    source.fail_at = None;
    HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync().unwrap();
    verify(&index, source.count);
}

#[test]
fn missing_block_cannot_report_a_successful_pass() {
    for omitted in [0, 80, 119] {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir, 32 * MIB);
        let mut source = Fixture::new(120);
        source.omit = Some(omitted);
        assert!(matches!(
            HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync(),
            Err(PipelineError::SourceGap { .. })
        ));
        let count = index.state().unwrap().durable_tip().map_or(0, |tip| tip.height + 1);
        assert!(count <= omitted);
        verify(&index, count);
    }
}

#[test]
fn writer_failure_stops_the_encode_of_later_batches() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 256 * 1024);
    let source = Fixture::new(20_000);
    let result = metrics::with_local_recorder(&recorder, || {
        HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync()
    });
    assert!(matches!(
        result,
        Err(PipelineError::Index(IndexError::Heed(heed::Error::Mdb(heed::MdbError::MapFull))))
    ));
    let values = snapshotter.snapshot().into_vec();
    let samples = |name: &str| -> usize {
        values
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .map(|(_, _, _, value)| match value {
                DebugValue::Histogram(samples) => samples.len(),
                _ => 0,
            })
            .sum()
    };
    let writes = samples("ztreamer.pipeline.commit.seconds");
    let encodes = samples("ztreamer.pipeline.encode.seconds");
    // After the failed write, at most one batch waits in the channel and one is in encode.
    assert!(encodes <= writes + 2, "{encodes} encodes for {writes} writes");
}

#[test]
fn failed_lmdb_transaction_never_advances_checkpoint_and_can_resume() {
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 256 * 1024);
    let prefix = Fixture::new(12);
    HistoricalPipeline::new(&index, &prefix, config(1)).unwrap().sync().unwrap();
    let source = Fixture::new(1_107);
    assert!(matches!(
        HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync(),
        Err(PipelineError::Index(IndexError::Heed(heed::Error::Mdb(heed::MdbError::MapFull))))
    ));
    let count = index.state().unwrap().durable_tip().unwrap().height + 1;
    assert!(count >= prefix.count && count < source.count);
    verify(&index, count);
    drop(index);
    let index = open(&dir, 32 * MIB);
    verify(&index, count);
    HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync().unwrap();
    verify(&index, source.count);
}

#[test]
fn failed_pass_exports_partial_work_and_resets_live_gauges() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(120);
    source.fail_at = Some(10);
    metrics::with_local_recorder(&recorder, || {
        assert!(HistoricalPipeline::new(&index, &source, config(1)).unwrap().sync().is_err());
    });
    let values = snapshotter.snapshot().into_vec();
    let metric =
        |name: &str| &values.iter().find(|(key, _, _, _)| key.key().name() == name).unwrap().3;
    assert_eq!(metric("ztreamer.pipeline.source.blocks"), &DebugValue::Counter(10));
    let committed = index.state().unwrap().durable_tip().map_or(0, |tip| u64::from(tip.height) + 1);
    assert_eq!(metric("ztreamer.pipeline.committed.blocks"), &DebugValue::Counter(committed));
    for name in ["workers.active", "pending.bytes", "ready.bytes"] {
        assert_eq!(metric(&format!("ztreamer.pipeline.{name}")), &DebugValue::Gauge(0.0.into()));
    }
    for name in ["pass", "segment"] {
        let outcome = values
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == format!("ztreamer.pipeline.{name}.seconds")
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "error")
            })
            .unwrap();
        assert!(matches!(&outcome.3, DebugValue::Histogram(samples) if samples.len() == 1));
    }
}

#[test]
fn worker_panic_returns_an_error_and_keeps_a_valid_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(120);
    source.panic_at = Some(10);
    assert!(matches!(
        HistoricalPipeline::new(&index, &source, config(1)).unwrap().sync(),
        Err(PipelineError::Panic)
    ));
    let count = index.state().unwrap().durable_tip().map_or(0, |tip| tip.height + 1);
    assert!(count <= 10);
    verify(&index, count);
}

#[tokio::test]
async fn historical_to_live_handoff_keeps_tree_sizes_and_contiguous_heights() {
    use crate::{
        head::{CanonicalBlockSource, HeadError, sync_head_once},
        index::BlockId,
        types::RawIndexBlock,
    };
    use zakura_chain::transaction;

    // The live source is asked only for the historical anchor and new empty blocks.
    struct Live {
        anchor: u32,
        tip: u32,
    }
    #[tonic::async_trait]
    impl CanonicalBlockSource for Live {
        async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
            Ok(Some(BlockId::new(self.tip, hash(self.tip))))
        }
        async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            assert!(height >= self.anchor);
            if height > self.tip {
                return Ok(None);
            }
            let mut bytes = vec![0; 140];
            bytes[4..36].copy_from_slice(
                &height.checked_sub(1).map(hash).unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH).0,
            );
            bytes[100..104].copy_from_slice(&height.to_le_bytes());
            bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            Ok(Some(RawIndexBlock {
                height: Height(height),
                hash: hash(height),
                bytes,
                txids: vec![transaction::Hash(hash(height).0)],
            }))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let source = Fixture::new(120);
    let historical = HistoricalPipeline::new(&index, &source, config(4)).unwrap().sync().unwrap();
    let mut live = Live { anchor: 119, tip: 124 };
    let (state, head) = sync_head_once(&index, &mut live, &[], config(4)).await.unwrap();
    assert_eq!(state, historical);
    assert_eq!(
        head.iter().map(|block| block.height).collect::<Vec<_>>(),
        (120..=124).collect::<Vec<_>>()
    );
    for (offset, block) in head.iter().enumerate() {
        assert_eq!(block.previous_hash, hash(119 + offset as u32));
        assert_eq!(block.hash, hash(120 + offset as u32));
        assert_eq!(block.end_tree_sizes, state.tree_sizes());
    }
    verify(&index, source.count);
}
