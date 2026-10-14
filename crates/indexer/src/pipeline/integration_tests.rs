//! Real coordinator/writer tests with a deterministic segment source.

use std::sync::{Condvar, Mutex};

use super::*;
use crate::{
    Digest,
    codec::{CompactBlockRecord, TreeSizes},
    parser::{CompactSaplingOutput, CompactShieldedAction, CompactTransaction},
};

#[tokio::test]
async fn zakura_segments_match_block_parsing_and_enforce_limits() {
    use tower::ServiceExt;
    use zakura_chain::{parameters::Network, serialization::ZcashDeserialize};
    use zakura_state::{CheckpointVerifiedBlock, Config, Request};
    use zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS;

    let (mut state, read, _tip, _change) =
        zakura_state::init(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
            .await
            .unwrap();
    let mut expected = Vec::new();
    for (&height, &bytes) in CONTINUOUS_MAINNET_BLOCKS.iter() {
        let block = Arc::new(block::Block::zcash_deserialize(bytes).unwrap());
        expected.push(
            crate::parser::parse_block(&crate::parser::RawIndexBlock {
                height: Height(height),
                hash: block.hash(),
                bytes: bytes.to_vec(),
                txids: block.transactions.iter().map(|tx| tx.hash()).collect(),
            })
            .unwrap(),
        );
        (&mut state)
            .oneshot(Request::CommitCheckpointVerifiedBlock(
                CheckpointVerifiedBlock::from(block),
            ))
            .await
            .unwrap();
    }
    let db = read.db();
    for blocks in expected.chunks(3) {
        let start = blocks.first().unwrap().height;
        let end = blocks.last().unwrap();
        let (send, receive) = sync_channel(blocks.len());
        let mut stats = WorkerStats::default();
        db.process_segment(
            start,
            end.height,
            Some(block::Hash(end.hash)),
            MIB,
            &send,
            &mut stats,
        )
        .unwrap();
        drop(send);
        assert_eq!(receive.into_iter().collect::<Vec<_>>(), blocks);
        assert_eq!(stats.blocks, blocks.len() as u64);
        assert_eq!(stats.transactions, blocks.len() as u64);
    }
    let (send, _receive) = sync_channel(1);
    assert!(matches!(
        db.process_segment(1, 1, None, 1, &send, &mut WorkerStats::default()),
        Err(PipelineError::BlockExceedsByteLimit { height: 1, .. })
    ));
    assert!(matches!(
        db.process_segment(
            1,
            1,
            Some(block::Hash([0; 32])),
            MIB,
            &send,
            &mut WorkerStats::default(),
        ),
        Err(PipelineError::SourceChanged)
    ));
}

struct Fixture {
    count: u32,
    fail_at: Option<u32>,
    panic_at: Option<u32>,
    omit: Option<u32>,
    reorder: bool,
    later_sent: (Mutex<bool>, Condvar),
}

impl Fixture {
    fn new(count: u32) -> Self {
        Self {
            count,
            fail_at: None,
            panic_at: None,
            omit: None,
            reorder: false,
            later_sent: (Mutex::new(false), Condvar::new()),
        }
    }
}

impl HistoricalSource for Fixture {
    fn tip(&self) -> Option<(Height, block::Hash)> {
        self.count
            .checked_sub(1)
            .map(|height| (Height(height), block::Hash(hash(height))))
    }

    fn block_hash(&self, height: u32) -> Option<block::Hash> {
        (height < self.count).then(|| block::Hash(hash(height)))
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
        tx: &SyncSender<ParsedCompactBlock>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError> {
        if self.reorder && start == 0 {
            let (lock, ready) = &self.later_sent;
            let (sent, timeout) = ready
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |sent| !*sent)
                .unwrap();
            assert!(
                *sent && !timeout.timed_out(),
                "a later block must arrive before block zero"
            );
        }
        for height in start..=end {
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
            tx.send(block).map_err(|_| PipelineError::Worker)?;
            if start > 0 {
                let (lock, ready) = &self.later_sent;
                *lock.lock().unwrap() = true;
                ready.notify_one();
            }
        }
        if expected_end_hash.is_some_and(|expected| expected.0 != hash(end)) {
            return Err(PipelineError::SourceChanged);
        }
        Ok(())
    }
}

fn hash(height: u32) -> Digest {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}

fn prepared(height: u32) -> ParsedCompactBlock {
    let count = height % 3; // Empty, light, and heavier blocks; all three pools.
    let action = CompactShieldedAction {
        nullifier: hash(height),
        commitment: [3; 32],
        ephemeral_key: [4; 32],
        ciphertext: [5; 52],
    };
    let transactions = if count == 0 {
        Vec::new()
    } else {
        vec![CompactTransaction {
            index: 1,
            txid: hash(height),
            sapling_spends: vec![hash(height)],
            sapling_outputs: vec![
                CompactSaplingOutput {
                    cmu: [2; 32],
                    ephemeral_key: [4; 32],
                    ciphertext: [5; 52],
                };
                count as usize
            ],
            orchard_actions: vec![action.clone(); count as usize],
            ironwood_actions: vec![action; count as usize],
        }]
    };
    ParsedCompactBlock {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or_default(),
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
    assert_eq!(
        state.durable_tip().map(|tip| tip.height),
        count.checked_sub(1)
    );
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
        HistoricalPipeline::new(&index, &source, config(workers))
            .unwrap()
            .sync()
            .unwrap();
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

#[test]
fn source_failure_leaves_a_resumable_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let mut source = Fixture::new(1_107);
    source.fail_at = Some(1_000);
    assert!(
        HistoricalPipeline::new(&index, &source, config(1))
            .unwrap()
            .sync()
            .is_err()
    );
    let count = index
        .state()
        .unwrap()
        .durable_tip()
        .map_or(0, |tip| tip.height + 1);
    assert!(count > 0 && count <= 1_000);
    verify(&index, count);
    drop(index);
    let index = open(&dir, 32 * MIB);
    source.fail_at = None;
    HistoricalPipeline::new(&index, &source, config(4))
        .unwrap()
        .sync()
        .unwrap();
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
            HistoricalPipeline::new(&index, &source, config(4))
                .unwrap()
                .sync(),
            Err(PipelineError::SourceGap { .. })
        ));
        let count = index
            .state()
            .unwrap()
            .durable_tip()
            .map_or(0, |tip| tip.height + 1);
        assert!(count <= omitted);
        verify(&index, count);
    }
}

#[test]
fn failed_lmdb_transaction_never_advances_checkpoint_and_can_resume() {
    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 256 * 1024);
    let prefix = Fixture::new(12);
    HistoricalPipeline::new(&index, &prefix, config(1))
        .unwrap()
        .sync()
        .unwrap();
    let source = Fixture::new(1_107);
    assert!(matches!(
        HistoricalPipeline::new(&index, &source, config(4))
            .unwrap()
            .sync(),
        Err(PipelineError::Index(IndexError::Heed(heed::Error::Mdb(
            heed::MdbError::MapFull
        ))))
    ));
    let count = index.state().unwrap().durable_tip().unwrap().height + 1;
    assert!(count >= prefix.count && count < source.count);
    verify(&index, count);
    drop(index);
    let index = open(&dir, 32 * MIB);
    verify(&index, count);
    HistoricalPipeline::new(&index, &source, config(4))
        .unwrap()
        .sync()
        .unwrap();
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
        assert!(
            HistoricalPipeline::new(&index, &source, config(1))
                .unwrap()
                .sync()
                .is_err()
        );
    });
    let values = snapshotter.snapshot().into_vec();
    let metric = |name: &str| {
        &values
            .iter()
            .find(|(key, _, _, _)| key.key().name() == name)
            .unwrap()
            .3
    };
    assert_eq!(
        metric("ztreamer.pipeline.source.blocks"),
        &DebugValue::Counter(10)
    );
    let committed = index
        .state()
        .unwrap()
        .durable_tip()
        .map_or(0, |tip| u64::from(tip.height) + 1);
    assert_eq!(
        metric("ztreamer.pipeline.committed.blocks"),
        &DebugValue::Counter(committed)
    );
    for name in ["workers.active", "pending.bytes", "ready.bytes"] {
        assert_eq!(
            metric(&format!("ztreamer.pipeline.{name}")),
            &DebugValue::Gauge(0.0.into())
        );
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
        HistoricalPipeline::new(&index, &source, config(1))
            .unwrap()
            .sync(),
        Err(PipelineError::Panic)
    ));
    let count = index
        .state()
        .unwrap()
        .durable_tip()
        .map_or(0, |tip| tip.height + 1);
    assert!(count <= 10);
    verify(&index, count);
}

#[tokio::test]
async fn historical_to_live_handoff_keeps_tree_sizes_and_contiguous_heights() {
    use crate::{
        head::{CanonicalBlockSource, HeadError, sync_head_once},
        index::BlockId,
        parser::RawIndexBlock,
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
            bytes[4..36].copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or_default());
            bytes[100..104].copy_from_slice(&height.to_le_bytes());
            bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            Ok(Some(RawIndexBlock {
                height: Height(height),
                hash: block::Hash(hash(height)),
                bytes,
                txids: vec![transaction::Hash(hash(height))],
            }))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir, 32 * MIB);
    let source = Fixture::new(120);
    let historical = HistoricalPipeline::new(&index, &source, config(4))
        .unwrap()
        .sync()
        .unwrap();
    let mut live = Live {
        anchor: 119,
        tip: 124,
    };
    let (state, head) = sync_head_once(&index, &mut live, &[], config(4))
        .await
        .unwrap();
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
