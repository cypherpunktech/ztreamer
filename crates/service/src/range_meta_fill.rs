//! A background task that writes a `range_meta` row for each sealed range that has none.
//!
//! A ztreamer can be a snapshot source only when each sealed range has a [`RangeMeta`] row. The
//! row holds the manifest entry of the range and the node's answers for the range end. The index
//! writer seals ranges, but it does not write these rows. [`RangeMetaFiller`] writes them from the
//! index and from the node of a [`StateBackend`]. It writes one range at a time, in ascending
//! order. ztreamerd runs it in both modes: over the embedded node, and in the remote mode over the
//! remote node, for the ranges that the replica seals after its import.
//!
//! The filler does not write blocks and does not change the generation. It reads LMDB on a
//! blocking thread and yields between ranges. The writer and the serving path do not wait for it.
//!
//! # Epochs
//!
//! An epoch is a group of consecutive ranges. A replica asks the node for the tree state once
//! for each epoch. An epoch closes at [`MAX_RANGES`] ranges, or when its ranges add
//! [`MAX_COMMITMENTS`] note commitments. The first limit that a range reaches closes the epoch.
//! The filler reads the rows that exist in height order. Thus a restart continues the same
//! grouping.

use std::{
    ops::RangeInclusive,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::watch;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};
use zakura_chain::{
    block,
    subtree::{NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};
use ztreamer_indexer::{
    blocking::{BlockingTask, BlockingTaskCancelled},
    codec::{CodecError, CompactBlockRecord, RANGE_FORMAT_VERSION, TreeSizes, decode_range_record},
    index::{
        Index, IndexError, RANGE_SIZE,
        range_meta::{
            Attested, BlobDigest, Frontiers, MAX_COMMITMENTS, MAX_RANGES, ManifestEntry, NodeId,
            PoolRoots, RangeMeta, SubtreeNode, SubtreeRoot, VerifyState,
        },
    },
};

use crate::{
    ServingSnapshot,
    backend::{BackendError, StateBackend, Subtrees, TreeState},
};

/// The number of filled ranges between two progress lines in the log.
const LOG_EVERY: u64 = 100;

/// Why a fill pass stopped.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FillError {
    /// An index read or write failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// A range blob did not decode.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The node did not answer.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// The blocking pool stopped before an index read ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
    /// A sealed range is not in the index now. A deep reorg replaced it.
    #[error("sealed range at {start} changed during the fill")]
    Replaced {
        /// The first height of the range.
        start: u32,
    },
}

/// Writes a [`RangeMeta`] row for each sealed range that has none.
///
/// ```ignore
/// let filler = RangeMetaFiller::embedded(index, compact.backend());
/// tokio::spawn(filler.run(compact.subscribe(), shutdown));
/// ```
pub struct RangeMetaFiller {
    index: Arc<Index>,
    backend: Arc<dyn StateBackend>,
    verified_by: NodeId,
    /// The index reads on blocking threads. Each holds a clone of `index`.
    reads: TaskTracker,
}

impl std::fmt::Debug for RangeMetaFiller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeMetaFiller")
            .field("verified_by", &self.verified_by)
            .finish_non_exhaustive()
    }
}

impl RangeMetaFiller {
    /// A filler that asks the node behind `backend`. Each row names `verified_by` as the node
    /// that gave its answers.
    pub fn new(index: Arc<Index>, backend: Arc<dyn StateBackend>, verified_by: NodeId) -> Self {
        Self { index, backend, verified_by, reads: TaskTracker::new() }
    }

    /// A filler that asks the embedded Zakura node through `backend`. The rows name
    /// [`RangeMetaFiller::embedded_node`].
    pub fn embedded(index: Arc<Index>, backend: Arc<dyn StateBackend>) -> Self {
        Self::new(index, backend, Self::embedded_node())
    }

    /// The node that the rows of the embedded Zakura node name: `embedded`, with Zakura's
    /// version.
    pub fn embedded_node() -> NodeId {
        NodeId {
            url: "embedded".into(),
            version: zakurad::application::build_version().to_string(),
        }
    }

    /// Fills the missing rows once. Then fills them again each time a new snapshot shows that
    /// `sealed_through` changed. Stops when the caller drops the snapshot sender, or when `stop`
    /// completes.
    ///
    /// A stop cancels the pass, but not an index read that runs on a blocking thread. `run`
    /// waits for that read. Thus when `run` returns, no task of the filler holds the index, and
    /// the caller can close it.
    ///
    /// A failed pass logs a warning. The next snapshot starts the pass again. The future is
    /// `Send + 'static`. The caller spawns it.
    pub async fn run(
        self,
        snapshots: watch::Receiver<ServingSnapshot>,
        stop: impl Future<Output = ()>,
    ) {
        tokio::select! {
            biased;
            () = stop => {}
            () = self.fill_on_each_snapshot(snapshots) => {}
        }
        self.reads.close();
        self.reads.wait().await;
    }

    /// The loop of [`Self::run`]. Returns when the caller drops the snapshot sender.
    async fn fill_on_each_snapshot(&self, mut snapshots: watch::Receiver<ServingSnapshot>) {
        // `None` until a pass completes. Then the `sealed_through` that the pass covered.
        let mut covered = None;
        loop {
            match self.pass(covered).await {
                Ok(sealed_through) => covered = Some(sealed_through),
                Err(error) => {
                    warn!(%error, "range metadata fill failed; the next snapshot starts it again")
                }
            }
            if snapshots.changed().await.is_err() {
                return;
            }
        }
    }

    /// Fills the missing rows if `sealed_through` is not `covered`. Returns the `sealed_through`
    /// that the pass covered.
    async fn pass(&self, covered: Option<Option<u32>>) -> Result<Option<u32>, FillError> {
        let sealed_through = self.blocking(|index| Ok(index.state()?.sealed_through())).await?;
        if covered == Some(sealed_through) {
            return Ok(sealed_through);
        }
        let slots = self.blocking(move |index| Slot::scan(index, sealed_through)).await?;
        let mut pending = slots.iter().filter(|slot| slot.row.is_none()).count() as u64;
        if covered.is_none() {
            info!(pending, ?sealed_through, "range metadata filler started");
        }

        let mut epoch = Epoch::default();
        let mut filled = 0;
        for slot in slots {
            if let Some((epoch_end, end_tree_sizes)) = slot.row {
                epoch.advance(epoch_end, end_tree_sizes);
                continue;
            }
            let meta = self.fill(slot.start, epoch).await?;
            epoch.advance(meta.entry.epoch_end, meta.entry.end_tree_sizes);
            pending -= 1;
            filled += 1;
            if filled % LOG_EVERY == 0 {
                info!(filled, pending, end_height = meta.entry.end_height, "filled range metadata");
            }
            tokio::task::yield_now().await;
        }
        Ok(sealed_through)
    }

    /// Builds and writes the row of the range at `start`. `epoch` is the epoch state before it.
    #[tracing::instrument(level = "debug", skip(self, epoch))]
    async fn fill(&self, start: u32, epoch: Epoch) -> Result<RangeMeta, FillError> {
        let read = self.blocking(move |index| RangeRead::read(index, start)).await?;
        let end = &read.last;
        let trees = self.backend.tree_state(end.hash).await?;
        let subtree_roots = self.subtree_roots(&read).await?;
        let entry = ManifestEntry {
            range_version: RANGE_FORMAT_VERSION,
            start_height: start,
            end_height: end.height,
            end_hash: end.hash,
            end_tree_sizes: end.end_tree_sizes,
            frontiers: Frontiers::from(&trees),
            subtree_roots: subtree_roots.clone(),
            blob_digest: read.blob_digest,
            blob_len: read.blob_len,
            epoch_end: epoch.closes_at(end.end_tree_sizes),
        };
        let meta = RangeMeta {
            entry,
            state: VerifyState::Verified,
            verified_by: Some(self.verified_by.clone()),
            verified_at: Some(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            ),
            attested: Some(Attested {
                end_hash: end.hash,
                roots: PoolRoots::from(&trees),
                subtree_roots,
            }),
            sampled_heights: Vec::new(),
        };
        self.blocking(move |index| {
            // A deep reorg can replace the range after the read. Do not write a row for the old
            // blocks.
            if RangeRead::record(index, meta.entry.end_height)?.hash != meta.entry.end_hash {
                return Err(FillError::Replaced { start });
            }
            index.put_range_meta(start, &meta)?;
            Ok(meta)
        })
        .await
    }

    /// The roots of the subtrees that end inside the range, Sapling first, then Orchard, then
    /// Ironwood.
    async fn subtree_roots(&self, read: &RangeRead) -> Result<Vec<SubtreeRoot>, FillError> {
        let heights = block::Height(read.start)..=block::Height(read.last.height);
        let (before, after) = (read.start_tree_sizes, read.last.end_tree_sizes);
        let mut roots = Vec::new();
        if let Some(span) = SubtreeSpan::between(before.sapling, after.sapling) {
            let subtrees = self.backend.sapling_subtrees(span.first, Some(span.count)).await?;
            roots.extend(span.roots(subtrees, &heights, SubtreeNode::Sapling));
        }
        if let Some(span) = SubtreeSpan::between(before.orchard, after.orchard) {
            let subtrees = self.backend.orchard_subtrees(span.first, Some(span.count)).await?;
            roots.extend(span.roots(subtrees, &heights, SubtreeNode::Orchard));
        }
        if let Some(span) = SubtreeSpan::between(before.ironwood, after.ironwood) {
            let subtrees = self.backend.ironwood_subtrees(span.first, Some(span.count)).await?;
            roots.extend(span.roots(subtrees, &heights, SubtreeNode::Ironwood));
        }
        Ok(roots)
    }

    /// Runs an index read on a blocking thread. A cancel of the await does not stop the read.
    /// Thus [`Self::run`] waits for it before it returns.
    async fn blocking<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Index) -> Result<T, FillError> + Send + 'static,
    ) -> Result<T, FillError> {
        let (index, token) = (Arc::clone(&self.index), self.reads.token());
        BlockingTask::spawn(move || {
            let _token = token;
            read(&index)
        })
        .join_flattened()
        .await
    }
}

/// Each frontier in the `zcashd` RPC encoding, or empty when the pool is not active.
impl From<&TreeState> for Frontiers {
    fn from(trees: &TreeState) -> Self {
        Self {
            sapling: trees.sapling.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
            orchard: trees.orchard.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
            ironwood: trees.ironwood.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
        }
    }
}

/// Each tree root, or `None` when the pool is not active.
impl From<&TreeState> for PoolRoots {
    fn from(trees: &TreeState) -> Self {
        Self {
            sapling: trees.sapling.as_ref().map(|tree| tree.root()),
            orchard: trees.orchard.as_ref().map(|tree| tree.root()),
            ironwood: trees.ironwood.as_ref().map(|tree| tree.root()),
        }
    }
}

/// A sealed range as a scan finds it.
#[derive(Clone, Copy, Debug)]
struct Slot {
    /// The first height of the range.
    start: u32,
    /// `epoch_end` and `end_tree_sizes` of the range's row, or `None` when it has no row.
    row: Option<(bool, TreeSizes)>,
}

impl Slot {
    /// Each sealed range through `sealed_through`, in ascending order.
    fn scan(index: &Index, sealed_through: Option<u32>) -> Result<Vec<Self>, FillError> {
        let Some(sealed_through) = sealed_through else {
            return Ok(Vec::new());
        };
        (0..=sealed_through)
            .step_by(RANGE_SIZE as usize)
            .map(|start| {
                let row = index
                    .range_meta(start)?
                    .map(|meta| (meta.entry.epoch_end, meta.entry.end_tree_sizes));
                Ok(Self { start, row })
            })
            .collect()
    }
}

/// What the index gives for the row of one range. The last record and the digest come from the
/// same blob read.
struct RangeRead {
    start: u32,
    /// The tree sizes before the first block of the range.
    start_tree_sizes: TreeSizes,
    last: CompactBlockRecord,
    blob_digest: BlobDigest,
    blob_len: u64,
}

impl RangeRead {
    fn read(index: &Index, start: u32) -> Result<Self, FillError> {
        let blob = index.read_sealed_range_bytes(start)?.ok_or(FillError::Replaced { start })?;
        let last = decode_range_record(&blob, RANGE_SIZE as usize - 1)?;
        let start_tree_sizes = start
            .checked_sub(1)
            .map(|height| Self::record(index, height))
            .transpose()?
            .map_or(TreeSizes::default(), |record| record.end_tree_sizes);
        Ok(Self {
            start,
            start_tree_sizes,
            last,
            blob_digest: BlobDigest::hash(&blob),
            blob_len: blob.len() as u64,
        })
    }

    /// The record at `height` in the current generation.
    fn record(index: &Index, height: u32) -> Result<CompactBlockRecord, FillError> {
        let mut found = None;
        index.read_range_latest(height, height, |record| {
            found = Some(record);
            false
        })?;
        Ok(found.ok_or(IndexError::Coverage { height })?)
    }
}

/// The subtrees of one pool that complete between two tree sizes.
#[derive(Clone, Copy, Debug)]
struct SubtreeSpan {
    first: NoteCommitmentSubtreeIndex,
    count: NoteCommitmentSubtreeIndex,
}

impl SubtreeSpan {
    /// The subtrees that complete when the tree grows from `before` to `after` commitments.
    /// `None` when no subtree completes.
    fn between(before: u32, after: u32) -> Option<Self> {
        // A tree holds at most 2^32 commitments. Thus a subtree index fits in 16 bits.
        let index = |size: u32| (size >> TRACKED_SUBTREE_HEIGHT) as u16;
        let (first, end) = (index(before), index(after));
        (end > first).then(|| Self {
            first: NoteCommitmentSubtreeIndex(first),
            count: NoteCommitmentSubtreeIndex(end - first),
        })
    }

    /// The node's answer for this span. Keeps only the subtrees that end inside `heights`.
    fn roots<N>(
        self,
        subtrees: Subtrees<N>,
        heights: &RangeInclusive<block::Height>,
        node: fn(N) -> SubtreeNode,
    ) -> impl Iterator<Item = SubtreeRoot> {
        subtrees
            .into_iter()
            .filter(|(_, subtree)| heights.contains(&subtree.end_height))
            .map(move |(index, subtree)| SubtreeRoot { index, root: node(subtree.root) })
    }
}

/// The ranges and commitments of the open epoch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Epoch {
    /// The ranges in the epoch so far.
    ranges: usize,
    /// The commitments of all pools before the first range of the epoch.
    start_commitments: u64,
}

impl Epoch {
    /// Returns `true` if the next range closes the epoch. `end_tree_sizes` are the tree sizes at
    /// the end of that range.
    const fn closes_at(self, end_tree_sizes: TreeSizes) -> bool {
        self.ranges + 1 >= MAX_RANGES
            || end_tree_sizes.total().saturating_sub(self.start_commitments) >= MAX_COMMITMENTS
    }

    /// Adds the next range, with the `epoch_end` and `end_tree_sizes` of its row.
    const fn advance(&mut self, epoch_end: bool, end_tree_sizes: TreeSizes) {
        *self = if epoch_end {
            Self { ranges: 0, start_commitments: end_tree_sizes.total() }
        } else {
            Self { ranges: self.ranges + 1, ..*self }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InProcessBackend, test_utils::fixtures};
    use zakura_chain::parameters::Network;

    fn open(dir: &tempfile::TempDir) -> Arc<Index> {
        Arc::new(
            Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap(),
        )
    }

    async fn filler(index: &Arc<Index>) -> RangeMetaFiller {
        let (_state, read, _tip, _change) = zakura_state::init(
            zakura_state::Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .unwrap();
        RangeMetaFiller::embedded(Arc::clone(index), Arc::new(InProcessBackend::new(read)))
    }

    fn empty_records(count: u32) -> Vec<CompactBlockRecord> {
        (0..count).map(|height| fixtures::record(height, Vec::new())).collect()
    }

    #[tokio::test]
    async fn fills_a_row_for_each_sealed_range() {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir);
        let records = fixtures::mixed_records(3_010);
        let state = fixtures::populate(&index, &records, Some(2_999));
        let (sender, snapshots) = watch::channel(ServingSnapshot::from(state));
        drop(sender);
        filler(&index).await.run(snapshots, std::future::pending()).await;

        for start in [0, 1_000, 2_000] {
            let meta = index.range_meta(start).unwrap().unwrap();
            let blob = index.read_sealed_range_bytes(start).unwrap().unwrap();
            let last = &records[start as usize + 999];
            let entry = &meta.entry;
            assert_eq!((entry.start_height, entry.end_height), (start, start + 999));
            assert_eq!((entry.end_hash, entry.end_tree_sizes), (last.hash, last.end_tree_sizes));
            assert_eq!(entry.blob_digest, BlobDigest::hash(&blob));
            assert_eq!(entry.blob_len, blob.len() as u64);
            assert!(!entry.epoch_end);
            assert_eq!(meta.state, VerifyState::Verified);
            assert_eq!(meta.verified_by.unwrap().url, "embedded");
            assert_eq!(meta.attested.unwrap().end_hash, last.hash);
        }
        assert_eq!(index.range_meta(3_000).unwrap(), None);
    }

    #[tokio::test]
    async fn run_returns_after_the_index_read_that_a_stop_cancels() {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir);
        let filler = filler(&index).await;
        // A stop cancels the await of this read. The read goes on, on its blocking thread.
        let read = filler.blocking(|_| {
            std::thread::sleep(std::time::Duration::from_millis(300));
            Ok(())
        });
        let cancelled = tokio::time::timeout(std::time::Duration::from_millis(50), read).await;
        assert!(cancelled.is_err());
        let (_sender, snapshots) = watch::channel(ServingSnapshot::from(index.state().unwrap()));

        filler.run(snapshots, std::future::ready(())).await;

        assert_eq!(Arc::strong_count(&index), 1);
    }

    #[tokio::test]
    async fn a_restart_fills_only_the_missing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir);
        let records = fixtures::mixed_records(3_010);
        fixtures::populate(&index, &records[..2_010], Some(1_999));
        assert_eq!(filler(&index).await.pass(None).await.unwrap(), Some(1_999));

        // Mark the row of range 1,000. A filler that writes it again removes the mark.
        let mut marked = index.range_meta(1_000).unwrap().unwrap();
        marked.verified_by = Some(NodeId { url: "marked".into(), version: String::new() });
        index.put_range_meta(1_000, &marked).unwrap();
        let first = index.range_meta(0).unwrap().unwrap();

        fixtures::populate(&index, &records[2_010..], Some(2_999));
        assert_eq!(filler(&index).await.pass(None).await.unwrap(), Some(2_999));
        assert_eq!(index.range_meta(0).unwrap().unwrap(), first);
        assert_eq!(index.range_meta(1_000).unwrap().unwrap(), marked);
        assert_eq!(index.range_meta(2_000).unwrap().unwrap().entry.end_hash, records[2_999].hash);
    }

    #[tokio::test]
    async fn epoch_end_marks_every_tenth_range_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let index = open(&dir);
        let records = empty_records(11_010);
        fixtures::populate(&index, &records[..5_010], Some(4_999));
        filler(&index).await.pass(None).await.unwrap();
        fixtures::populate(&index, &records[5_010..], Some(10_999));
        filler(&index).await.pass(None).await.unwrap();

        let epoch_ends: Vec<_> = (0..11)
            .filter(|range| index.range_meta(range * RANGE_SIZE).unwrap().unwrap().entry.epoch_end)
            .collect();
        assert_eq!(epoch_ends, [9]);
    }

    #[test]
    fn an_epoch_closes_at_one_million_commitments() {
        let sizes = |sapling, orchard| TreeSizes { sapling, orchard, ironwood: 0 };
        let mut epoch = Epoch::default();
        assert!(!epoch.closes_at(sizes(999_999, 0)));
        assert!(epoch.closes_at(sizes(500_000, 500_000)));
        epoch.advance(true, sizes(1_000_000, 0));
        assert_eq!(epoch, Epoch { ranges: 0, start_commitments: 1_000_000 });
        epoch.advance(false, sizes(1_000_010, 0));
        assert!(!epoch.closes_at(sizes(1_999_999, 0)));
        assert!(epoch.closes_at(sizes(2_000_000, 0)));
    }

    #[test]
    fn subtree_span_covers_the_subtrees_that_complete() {
        let span = |before, after| {
            SubtreeSpan::between(before, after).map(|span| (span.first.0, span.count.0))
        };
        assert_eq!(span(0, 65_535), None);
        assert_eq!(span(65_535, 65_536), Some((0, 1)));
        assert_eq!(span(65_536, 131_071), None);
        assert_eq!(span(10, 200_000), Some((0, 3)));
    }
}
