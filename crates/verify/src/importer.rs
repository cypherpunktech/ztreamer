//! The importer: installs the sealed ranges of one or more sources in an index, in verified
//! epochs.
//!
//! [`Importer::run`] reads the manifest of each source. It refuses a manifest for another
//! network or genesis block, and an index that is verified at or above the first manifest, the
//! reference. Before it fetches a blob, it compares the blob digests of each epoch of the
//! reference with the other manifests. Then it starts the epochs after the verified tip of the
//! index in height order, at most two for each of the [`ImportConfig::import_workers`] at a
//! time. The work on one epoch is:
//!
//! 1. Take its part of a byte budget of [`MAX_PENDING_BYTES`], in height order. The epoch
//!    keeps it until its install.
//! 2. Fetch its blobs. The blob fetches rotate across the sources that agree on the digests of
//!    the epoch. After a wrong digest, fetch the blob from the next source. A wrong digest is a
//!    strike against the source, and [`Strikes::LIMIT`] strikes drop the source for the
//!    import. An epoch at or below the sealed tip is installed already, with
//!    [`VerifyState::ChainOk`] rows: read its blobs from the index, not from a source.
//! 3. Run [`Epoch::verify`] on the blocking thread pool. At most `import_workers` epochs do
//!    this at the same time.
//! 4. Run [`Epoch::attest`] and [`Epoch::sample`] against the [`TrustedNode`] at the same
//!    time. Thus the node calls of an epoch overlap the rebuild of the next epoch.
//!
//! The importer takes the verified epochs in height order, and installs each one before it
//! takes the next. It installs each range of a fetched epoch with
//! [`Index::install_sealed_range`] and a [`VerifyState::Verified`] row. It marks each range of
//! an installed epoch verified with [`Index::advance_verified_through`].
//!
//! The start frontiers of the first epoch come from the `range_meta` row of the last verified
//! range. The importer does not trust them: wrong start frontiers give rebuilt roots that the
//! node does not attest. The start frontiers of each other epoch are the frontiers of the
//! manifest entry before it. [`Epoch::verify`] of the epoch before rebuilds them byte for byte,
//! and the writer installs an epoch only after all epochs below it.
//!
//! The manifests disagree about an epoch when two of them give other blob digests for its
//! heights. Then the work on the epoch fetches, verifies and attests the version of each
//! manifest in turn, the reference first, until one passes. The sources of each rejected
//! version take a strike.
//!
//! A failed epoch stops the import. The importer first installs the epochs below it, thus the
//! index keeps them. A restart continues after the last installed epoch, and fetches no range
//! that is installed.
//!
//! [`Epoch::verify`]: crate::epoch::Epoch::verify
//! [`Strikes::LIMIT`]: crate::fetch::Strikes::LIMIT
//! [`Epoch::attest`]: crate::epoch::Epoch::attest
//! [`Epoch::sample`]: crate::epoch::Epoch::sample
//! [`VerifyState::ChainOk`]: ztreamer_indexer::index::range_meta::VerifyState::ChainOk
//! [`VerifyState::Verified`]: ztreamer_indexer::index::range_meta::VerifyState::Verified

mod pipeline;
mod sources;
mod telemetry;

use std::{future, num::NonZeroUsize, panic, pin, sync::Arc, thread};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::{Semaphore, watch};
use tracing::{Instrument as _, info};
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::index::{
    Index, IndexError, IndexState, RANGE_SIZE,
    range_meta::{Frontiers, NodeId},
};

use self::{pipeline::Run, sources::Sources};
use crate::{
    attest::{Attestor, EpochError, NodeError},
    epoch::Epoch,
    fetch::{FetchError, RangeSource, Source},
    manifest::Manifest,
    range::Reject,
    sample::{SamplePlan, Sampler},
};

/// The byte budget of the blobs from their fetch to their install. Each epoch takes the bytes
/// of its blobs before its fetch, in height order, and keeps them until its install. The
/// largest mainnet epoch holds 258 MB of blobs. An epoch above the budget takes the full budget.
pub const MAX_PENDING_BYTES: u64 = 2 << 30;

/// The node that attests and samples the imported epochs.
#[derive(Clone)]
pub struct TrustedNode {
    /// Attests the end hashes, the tree roots and the subtree roots of each epoch.
    pub attestor: Arc<dyn Attestor>,
    /// Gives the full blocks that [`Epoch::sample`](crate::epoch::Epoch::sample) compares.
    pub sampler: Arc<dyn Sampler>,
    /// The node in `verified_by` of each installed row.
    pub id: NodeId,
}

impl std::fmt::Debug for TrustedNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrustedNode").field("id", &self.id).finish_non_exhaustive()
    }
}

/// How the importer checks the ranges.
#[derive(Clone, Debug)]
pub struct ImportConfig {
    /// The network that the index was opened for. The manifest must have its name and its
    /// genesis hash.
    pub network: Network,
    /// The heights of each range that [`Epoch::sample`](crate::epoch::Epoch::sample) compares, and the secret seed.
    pub verify_sample: SamplePlan,
    /// The epochs that fetch at the same time, and the epochs that verify at the same time.
    pub import_workers: NonZeroUsize,
}

impl ImportConfig {
    /// The available cores minus two, for the writer and the runtime. At least one.
    pub fn default_import_workers() -> NonZeroUsize {
        thread::available_parallelism()
            .ok()
            .and_then(|cores| NonZeroUsize::new(cores.get().saturating_sub(2)))
            .unwrap_or(NonZeroUsize::MIN)
    }
}

/// What an import installed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ImportReport {
    /// The epochs installed.
    pub epochs: usize,
    /// The ranges installed.
    pub ranges: usize,
    /// The sealed tip of the index after the import.
    pub sealed_through: Option<u32>,
    /// The verified tip of the index after the import.
    pub verified_through: Option<u32>,
    /// `true` if the shutdown signal stopped the import before the end of the manifest.
    pub stopped: bool,
}

impl ImportReport {
    /// Adds the install of `epoch`. `fetched` is `false` for an epoch that was installed with
    /// `ChainOk` rows before the import.
    fn add_install(&mut self, epoch: &Epoch<'_>, fetched: bool, state: &IndexState) {
        if fetched {
            self.epochs += 1;
            self.ranges += epoch.entries().len();
        }
        self.sealed_through = state.sealed_through();
        self.verified_through = state.verified_through();
        info!(end_height = epoch.end_height(), ranges = self.ranges, "imported epoch");
    }
}

/// Why an import stopped.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// No source gave a manifest, or a source gave no blob.
    #[error(transparent)]
    Fetch(#[from] FetchError),
    /// An index read or install failed.
    #[error(transparent)]
    Index(#[from] IndexError),
    /// The fetchers dropped all sources that have a blob, or the importer has no source.
    #[error("no source is left")]
    NoSources,
    /// The manifest is for another network.
    #[error("manifest is for network {manifest}, the index for {index}")]
    Network {
        /// The network of the manifest.
        manifest: String,
        /// The network of the index.
        index: String,
    },
    /// The manifest has another genesis block.
    #[error("manifest has genesis {manifest}, the index {index}")]
    Genesis {
        /// The genesis hash of the manifest.
        manifest: block::Hash,
        /// The genesis hash of the index.
        index: block::Hash,
    },
    /// The index is verified at or above the manifest.
    #[error("index is verified through {index}, at or above the manifest ({manifest})")]
    Ahead {
        /// The verified tip of the index.
        index: u32,
        /// The sealed tip of the manifest.
        manifest: u32,
    },
    /// The sealed or the verified tip of the index is not the end of an epoch of the manifest.
    #[error("index tip {height} is inside an epoch of the manifest")]
    EpochBoundary {
        /// The sealed or the verified tip of the index.
        height: u32,
    },
    /// The last verified range of the index has no `range_meta` row, thus no frontiers.
    #[error("sealed range {start} has no range_meta row")]
    MissingRow {
        /// The first height of the range.
        start: u32,
    },
    /// A sealed range of the index has no blob.
    #[error("sealed range {start} has no blob")]
    MissingBlob {
        /// The first height of the range.
        start: u32,
    },
    /// A range of the epoch failed a check. The index keeps the epochs before it.
    #[error("epoch through {epoch} rejected")]
    Rejected {
        /// The end height of the epoch.
        epoch: u32,
        /// The check that failed.
        #[source]
        reject: Reject,
    },
    /// The node gave no answer for the epoch. Try again later.
    #[error("node gave no answer for height {height} of the epoch through {epoch}")]
    Node {
        /// The end height of the epoch.
        epoch: u32,
        /// The height that the failed call asked about.
        height: u32,
        /// Why the node gave no answer.
        #[source]
        source: NodeError,
    },
}

impl ImportError {
    /// The error for the epoch through `epoch` that `error` stopped.
    fn in_epoch(epoch: u32, error: EpochError) -> Self {
        match error {
            EpochError::Reject(reject) => Self::Rejected { epoch, reject },
            EpochError::Node { height, source } => Self::Node { epoch, height, source },
        }
    }
}

/// Installs the sealed ranges of one or more sources in an index, in verified epochs.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use tokio::sync::watch;
/// use ztreamer_indexer::index::Index;
/// use ztreamer_verify::{
///     fetch::Source,
///     importer::{ImportConfig, ImportError, ImportReport, Importer, TrustedNode},
/// };
///
/// # async fn run(
/// #     sources: Vec<Source>,
/// #     node: TrustedNode,
/// #     index: Arc<Index>,
/// #     config: ImportConfig,
/// # ) -> Result<ImportReport, ImportError> {
/// let (_stop, shutdown) = watch::channel(false);
/// let import = Importer::new(sources, node, index, config).run(shutdown);
/// tokio::spawn(import).await.unwrap()
/// # }
/// ```
#[derive(Debug)]
pub struct Importer<S = Source> {
    sources: Sources<S>,
    node: TrustedNode,
    index: Arc<Index>,
    config: ImportConfig,
}

impl<S: RangeSource + 'static> Importer<S> {
    /// An importer of the ranges of `sources` into `index`, checked against `node`.
    pub fn new(
        sources: Vec<S>,
        node: TrustedNode,
        index: Arc<Index>,
        config: ImportConfig,
    ) -> Self {
        Self { sources: Sources::new(sources), node, index, config }
    }

    /// Imports the epochs of the manifest after the verified tip of the index, as the module
    /// docs give.
    ///
    /// When `shutdown` becomes `true`, the importer completes the epoch that it installs, and
    /// the import stops. The import drops the other epochs. The future is `Send + 'static`. The
    /// caller spawns it.
    pub async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ImportReport, ImportError> {
        let manifests = self.sources.manifests().await?;
        for (_, manifest) in &manifests {
            self.check_identity(manifest)?;
        }
        let manifest = &manifests[0].1;
        let state = self.blocking(|index| index.state()).await?;
        let (sealed, verified) = (state.sealed_through(), state.verified_through());
        let tip = manifest.sealed_through();
        if let Some(index) = verified.filter(|&verified| verified >= tip) {
            return Err(ImportError::Ahead { index, manifest: tip });
        }
        let after = |height: Option<u32>| {
            manifest.epochs().position(|epoch| epoch.previous().map(|e| e.end_height) == height)
        };
        let first = after(verified)
            .ok_or(ImportError::EpochBoundary { height: verified.unwrap_or_default() })?;
        if let Some(sealed) = sealed.filter(|&sealed| sealed < tip && after(Some(sealed)).is_none())
        {
            return Err(ImportError::EpochBoundary { height: sealed });
        }
        let start = self.start_frontiers(verified).await?;
        let mut report = ImportReport {
            sealed_through: sealed,
            verified_through: verified,
            ..ImportReport::default()
        };
        let run = Arc::new(Run::new(self, manifests, first, start, sealed));
        let budget = Arc::new(Semaphore::new(MAX_PENDING_BYTES as usize));
        let stop = async move {
            // A dropped sender never stops the import.
            if shutdown.wait_for(|&stop| stop).await.is_err() {
                future::pending::<()>().await;
            }
        };
        let epochs = stream::iter(run.epochs_to_import())
            .then(|at| {
                let (budget, bytes) = (Arc::clone(&budget), run.budget_bytes(at));
                async move {
                    let held = budget.acquire_many_owned(bytes).await;
                    (at, held.expect("the budget is never closed"))
                }
            })
            .map(|(at, held)| {
                let (check, span) = (Arc::clone(&run).check(at), run.span(at));
                async move { Ok::<_, ImportError>((at, check.await?, held)) }.instrument(span)
            })
            .buffered(run.window())
            .take_until(stop);
        let mut epochs = pin::pin!(epochs);
        // `buffered` gives the epochs in height order. Thus the error of an epoch comes after
        // the install of each epoch below it.
        let mut next = run.epochs_to_import().start;
        while let Some((at, rows, _held)) = epochs.try_next().await? {
            let fetched = !run.is_installed(at);
            let state = run.install(at, rows).instrument(run.span(at)).await?;
            report.add_install(&run.epoch(at), fetched, &state);
            next = at + 1;
        }
        // Only the shutdown signal ends the epochs early: an error returns above.
        report.stopped = next < run.epochs_to_import().end;
        Ok(report)
    }

    /// Refuses a manifest for another network or genesis block than the index.
    fn check_identity(&self, manifest: &Manifest) -> Result<(), ImportError> {
        let network = &self.config.network;
        if manifest.network() != network.to_string() {
            let manifest = manifest.network().to_owned();
            return Err(ImportError::Network { manifest, index: network.to_string() });
        }
        if manifest.genesis_hash() != network.genesis_hash() {
            let (manifest, index) = (manifest.genesis_hash(), network.genesis_hash());
            return Err(ImportError::Genesis { manifest, index });
        }
        Ok(())
    }

    /// The frontiers after the verified tip: the frontiers of the row of the last verified
    /// range, or empty trees for an index with no verified range.
    async fn start_frontiers(&self, verified: Option<u32>) -> Result<Frontiers, ImportError> {
        let Some(verified) = verified else {
            return Ok(Frontiers::default());
        };
        let start = verified + 1 - RANGE_SIZE;
        self.blocking(move |index| index.range_meta(start))
            .await?
            .map(|meta| meta.entry.frontiers)
            .ok_or(ImportError::MissingRow { start })
    }

    /// Runs an index call on the blocking thread pool.
    async fn blocking<T: Send + 'static>(
        &self,
        call: impl FnOnce(&Index) -> Result<T, IndexError> + Send + 'static,
    ) -> Result<T, ImportError> {
        let index = Arc::clone(&self.index);
        Ok(tokio::task::spawn_blocking(move || call(&index))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))?)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex, time::Duration};

    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use zakura_chain::subtree::NoteCommitmentSubtreeIndex;
    use ztreamer_indexer::{
        codec::{RangeDecoder, encode_range},
        index::range_meta::{
            Attested, ManifestEntry, PoolRoots, RangeMeta, SubtreeNode, VerifyState,
        },
        test_utils::raw_block,
        types::RawIndexBlock,
    };

    use super::*;
    use crate::{
        fetch::{HttpSource, Strikes},
        manifest::Pool,
        range::Check,
        test_utils::{FileServer, SEED, Snapshot, SourceIndex},
    };

    /// Ranges of empty blocks: the epochs end at 9,999, 19,999 and 24,999.
    const RANGES: u32 = 25;

    /// The `tree_roots` call at the first height waits until the call at the second height
    /// answers. With no second height, it waits forever.
    type Hold = (u32, Option<u32>);

    /// The answers in the `attested` records that the filler wrote in the source index.
    struct FakeAttestor {
        answers: BTreeMap<u32, Attested>,
        hold: Option<Hold>,
        released: Notify,
        /// The heights of the answered `tree_roots` calls, in order.
        answered: Mutex<Vec<u32>>,
    }

    impl FakeAttestor {
        fn answer(&self, height: u32) -> Result<&Attested, NodeError> {
            self.answers.get(&height).ok_or(NodeError::Height { expected: height, actual: 0 })
        }
    }

    #[async_trait]
    impl Attestor for FakeAttestor {
        async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
            Ok(self.answer(height)?.end_hash)
        }

        async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
            if let Some((held, release)) = self.hold {
                if height == held {
                    self.released.notified().await;
                }
                if Some(height) == release {
                    self.released.notify_one();
                }
            }
            self.answered.lock().unwrap().push(height);
            Ok(self.answer(height)?.roots)
        }

        async fn subtree_roots(
            &self,
            pool: Pool,
            start: NoteCommitmentSubtreeIndex,
            _count: u16,
        ) -> Result<Vec<SubtreeNode>, NodeError> {
            Err(NodeError::MissingSubtree { pool, index: start })
        }
    }

    /// The node blocks of the source index.
    struct FakeSampler;

    #[async_trait]
    impl Sampler for FakeSampler {
        async fn full_block(&self, height: u32) -> Result<Option<RawIndexBlock>, NodeError> {
            Ok(Some(raw_block(height)))
        }

        fn prune_height(&self) -> u32 {
            0
        }
    }

    /// A source index, signed snapshots of it over HTTP, and an empty target index.
    struct Import {
        source: SourceIndex,
        snapshots: Vec<Snapshot>,
        servers: Vec<FileServer>,
        target: Arc<Index>,
        workers: usize,
        _dir: TempDir,
    }

    impl Import {
        async fn new(ranges: u32, sources: usize) -> Self {
            let source = SourceIndex::new(ranges).await;
            let snapshots: Vec<_> = (0..sources).map(|_| Snapshot::export(&source)).collect();
            let mut servers = Vec::new();
            for snapshot in &snapshots {
                servers.push(FileServer::start(snapshot.dir.path()).await);
            }
            let dir = tempfile::tempdir().unwrap();
            let target = Arc::new(SourceIndex::open(&dir));
            Self { source, snapshots, servers, target, workers: 4, _dir: dir }
        }

        /// The blob at `start` with a wrong tree size at its record 500.
        fn mutated_blob(&self, start: u32) -> Vec<u8> {
            let blob = self.source.index.read_sealed_range_bytes(start).unwrap().unwrap();
            let decoder = RangeDecoder::new(&blob).unwrap();
            let mut records: Vec<_> =
                (0..RANGE_SIZE as usize).map(|at| decoder.record(at).unwrap()).collect();
            records[500].end_tree_sizes.sapling = 1;
            encode_range(&records).unwrap()
        }

        /// Changes one byte of the blob file of each entry, and keeps the manifest.
        fn corrupt_blobs(&self, source: usize, entries: &[ManifestEntry]) {
            for entry in entries {
                let path = self.snapshots[source].range_path(entry);
                let mut blob = std::fs::read(&path).unwrap();
                blob[100] ^= 1;
                std::fs::write(path, blob).unwrap();
            }
        }

        fn attestor(&self, hold: Option<Hold>) -> Arc<FakeAttestor> {
            let answers = self.snapshots[0].manifest.entries().iter().map(|entry| {
                let meta = self.source.index.range_meta(entry.start_height).unwrap().unwrap();
                (entry.end_height, meta.attested.unwrap())
            });
            let (released, answered) = (Notify::new(), Mutex::default());
            Arc::new(FakeAttestor { answers: answers.collect(), hold, released, answered })
        }

        fn importer(&self, attestor: Arc<FakeAttestor>, network: Network) -> Importer {
            let key = SigningKey::from_bytes(&SEED).verifying_key();
            let node = TrustedNode {
                attestor,
                sampler: Arc::new(FakeSampler),
                id: NodeId { url: "http://node.test".to_owned(), version: "test".to_owned() },
            };
            let config = ImportConfig {
                network,
                verify_sample: SamplePlan { per_range: 2, seed: 7 },
                import_workers: NonZeroUsize::new(self.workers).unwrap(),
            };
            let sources = self.servers.iter().map(|server| {
                Source::Http(Box::new(HttpSource::new(server.base.clone(), key).unwrap()))
            });
            Importer::new(sources.collect(), node, Arc::clone(&self.target), config)
        }

        async fn run(&self) -> Result<ImportReport, ImportError> {
            let (_stop, shutdown) = watch::channel(false);
            self.importer(self.attestor(None), Network::Mainnet).run(shutdown).await
        }

        /// The blob requests that all snapshot servers got.
        fn fetches(&self) -> usize {
            (0..self.servers.len()).map(|source| self.fetches_from(source)).sum()
        }

        /// The blob requests that the snapshot server of `source` got.
        fn fetches_from(&self, source: usize) -> usize {
            let requests = self.servers[source].requests();
            requests.iter().filter(|(path, _)| path.starts_with("ranges/")).count()
        }

        /// Checks that the target holds the blobs of the source, byte for byte, and is verified.
        fn assert_same_ranges(&self) {
            let sealed = self.snapshots[0].manifest.sealed_through();
            for start in (0..=sealed / RANGE_SIZE).map(|range| range * RANGE_SIZE) {
                let blob = self.target.read_sealed_range_bytes(start).unwrap();
                assert_eq!(blob, self.source.index.read_sealed_range_bytes(start).unwrap());
            }
            let state = self.target.state().unwrap();
            assert_eq!(
                (state.sealed_through(), state.verified_through()),
                (Some(sealed), Some(sealed))
            );
        }
    }

    #[tokio::test]
    async fn an_import_installs_the_same_ranges_and_verifies_them() {
        let import = Import::new(RANGES, 1).await;

        let report = import.run().await.unwrap();

        let sealed = import.snapshots[0].manifest.sealed_through();
        let expected = ImportReport {
            epochs: 3,
            ranges: 25,
            sealed_through: Some(sealed),
            verified_through: Some(sealed),
            stopped: false,
        };
        assert_eq!(report, expected);
        import.assert_same_ranges();
        let meta = import.target.range_meta(12_000).unwrap().unwrap();
        assert_eq!(meta.state, VerifyState::Verified);
        assert_eq!(meta.entry, import.snapshots[0].manifest.entries()[12]);
        assert_eq!(meta.verified_by.unwrap().url, "http://node.test");
        assert_eq!(meta.sampled_heights.len(), 2);
        assert!(meta.sampled_heights.iter().all(|height| (12_000..13_000).contains(height)));
    }

    #[tokio::test]
    async fn four_workers_install_twelve_epochs_byte_for_byte() {
        let import = Import::new(120, 1).await;

        let report = import.run().await.unwrap();

        assert_eq!((report.epochs, report.ranges), (12, 120));
        import.assert_same_ranges();
        assert_eq!(import.fetches(), 120);
    }

    #[tokio::test]
    async fn a_slow_epoch_verifies_last_and_installs_in_order() {
        let import = Import::new(RANGES, 1).await;
        // The first epoch gets its roots only after the last epoch.
        let attestor = import.attestor(Some((9_999, Some(24_999))));
        let (_stop, shutdown) = watch::channel(false);

        let importer = import.importer(Arc::clone(&attestor), Network::Mainnet);
        let report = importer.run(shutdown).await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_same_ranges();
        let answered = attestor.answered.lock().unwrap().clone();
        let at = |height| answered.iter().position(|&answered| answered == height).unwrap();
        assert!(at(24_999) < at(9_999), "{answered:?}");
    }

    #[tokio::test]
    async fn a_stopped_import_keeps_a_prefix_and_a_restart_fetches_only_the_rest() {
        let import = Import::new(RANGES, 1).await;
        // The second epoch never gets its roots.
        let attestor = import.attestor(Some((19_999, None)));
        let (stop, shutdown) = watch::channel(false);
        let importer = import.importer(attestor, Network::Mainnet);
        let task = tokio::spawn(importer.run(shutdown));
        while import.target.state().unwrap().sealed_through() != Some(9_999) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        stop.send(true).unwrap();

        let report = task.await.unwrap().unwrap();
        assert_eq!((report.epochs, report.ranges, report.stopped), (1, 10, true));
        assert_eq!(report.verified_through, Some(9_999));
        assert_eq!(import.target.range_meta(10_000).unwrap(), None);
        let first = import.fetches();
        assert!(first >= 10);
        let report = import.run().await.unwrap();
        assert_eq!((report.epochs, report.ranges), (2, 15));
        assert_eq!(import.fetches() - first, 15);
        import.assert_same_ranges();
    }

    #[tokio::test]
    async fn chain_ok_ranges_are_attested_again_from_the_index() {
        let import = Import::new(RANGES, 1).await;
        for entry in &import.snapshots[0].manifest.entries()[..10] {
            let start = entry.start_height;
            let blob = import.source.index.read_sealed_range_bytes(start).unwrap().unwrap();
            // An older manifest ended at 4,999, and marked its last range as an epoch end.
            let epoch_end = entry.epoch_end || start == 4_000;
            let meta = RangeMeta {
                entry: ManifestEntry { epoch_end, ..entry.clone() },
                state: VerifyState::ChainOk,
                verified_by: None,
                verified_at: None,
                attested: None,
                sampled_heights: Vec::new(),
            };
            import.target.install_sealed_range(start, &blob, &meta).unwrap();
        }

        let report = import.run().await.unwrap();

        assert_eq!((report.epochs, report.ranges), (2, 15));
        assert_eq!(import.fetches(), 15);
        import.assert_same_ranges();
        let meta = import.target.range_meta(3_000).unwrap().unwrap();
        assert_eq!(meta.state, VerifyState::Verified);
        assert!(meta.attested.is_some());
    }

    #[tokio::test]
    async fn a_mutated_blob_stops_the_import_at_its_epoch() {
        let mut import = Import::new(RANGES, 1).await;
        let blob = import.mutated_blob(15_000);
        import.snapshots[0].replace_blob(15_000, &blob);

        let error = import.run().await.unwrap_err();

        let expected = Reject::Rejected { check: Check::TreeSizes, height: 15_500 };
        assert!(
            matches!(error, ImportError::Rejected { epoch: 19_999, reject } if reject == expected),
            "{error:?}"
        );
        let state = import.target.state().unwrap();
        assert_eq!((state.sealed_through(), state.verified_through()), (Some(9_999), Some(9_999)));
        assert_eq!(import.target.range_meta(10_000).unwrap(), None);
    }

    #[tokio::test]
    async fn a_corrupted_source_is_dropped_after_three_strikes() {
        let mut import = Import::new(RANGES, 2).await;
        import.workers = 1;
        // Source 1 sends a wrong blob for each range.
        import.corrupt_blobs(1, import.snapshots[1].manifest.entries());

        let report = import.run().await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_same_ranges();
        // The first source of a fetch alternates, and each fetch from source 1 is a strike.
        // Two epochs fetch at the same time, thus one more fetch can start before the third
        // strike drops source 1.
        let strikes = usize::from(Strikes::LIMIT);
        assert!((strikes..=strikes + 1).contains(&import.fetches_from(1)));
        assert_eq!(import.fetches_from(0), 25);
    }

    #[tokio::test]
    async fn sources_that_disagree_on_a_digest_resolve_by_verification() {
        let mut import = Import::new(RANGES, 2).await;
        // The reference manifest gives the digest of a wrong blob, and a wrong end hash for the
        // entry before the last epoch. Thus the reference loses two disputes.
        let blob = import.mutated_blob(15_000);
        import.snapshots[0].replace_blob(15_000, &blob);
        let mut entries = import.snapshots[0].manifest.entries().to_vec();
        entries[19].end_hash = block::Hash([9; 32]);
        import.snapshots[0].replace_entries(entries);

        let report = import.run().await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_same_ranges();
        let meta = import.target.range_meta(15_000).unwrap().unwrap();
        assert_eq!(meta.entry, import.snapshots[1].manifest.entries()[15]);
    }

    #[tokio::test]
    async fn a_version_that_its_sources_cannot_send_is_skipped() {
        let mut import = Import::new(RANGES, 2).await;
        // The reference gives the digest of a wrong blob at 15,000, and has no file for it.
        let blob = import.mutated_blob(15_000);
        import.snapshots[0].replace_blob(15_000, &blob);
        let entry = &import.snapshots[0].manifest.entries()[15];
        std::fs::remove_file(import.snapshots[0].range_path(entry)).unwrap();

        let report = import.run().await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_same_ranges();
        let meta = import.target.range_meta(15_000).unwrap().unwrap();
        assert_eq!(meta.entry, import.snapshots[1].manifest.entries()[15]);
    }

    #[tokio::test]
    async fn a_source_without_a_blob_leaves_it_to_the_other_source() {
        let mut import = Import::new(RANGES, 2).await;
        import.workers = 1;
        for entry in &import.snapshots[1].manifest.entries()[..10] {
            std::fs::remove_file(import.snapshots[1].range_path(entry)).unwrap();
        }

        let report = import.run().await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_same_ranges();
    }

    #[tokio::test]
    async fn sources_that_all_send_a_wrong_blob_leave_no_source() {
        let import = Import::new(RANGES, 2).await;
        for source in 0..2 {
            import.corrupt_blobs(source, &import.snapshots[source].manifest.entries()[..1]);
        }

        let error = import.run().await.unwrap_err();

        assert!(matches!(error, ImportError::NoSources), "{error:?}");
        assert_eq!(import.target.state().unwrap().sealed_through(), None);
    }

    #[tokio::test]
    async fn an_index_at_the_manifest_tip_or_on_another_network_is_refused() {
        let import = Import::new(RANGES, 1).await;
        let (stop, shutdown) = watch::channel(true);
        let importer = import.importer(import.attestor(None), Network::Mainnet);
        let report = importer.run(shutdown).await.unwrap();
        assert_eq!((report.epochs, report.stopped), (0, true));
        drop(stop);
        import.run().await.unwrap();

        let error = import.run().await.unwrap_err();

        assert!(
            matches!(error, ImportError::Ahead { index: 24_999, manifest: 24_999 }),
            "{error:?}"
        );
        let (_stop, shutdown) = watch::channel(false);
        let testnet = Network::new_default_testnet();
        let error =
            import.importer(import.attestor(None), testnet).run(shutdown).await.unwrap_err();
        assert!(matches!(error, ImportError::Network { .. }), "{error:?}");
    }
}
