//! The importer: installs the sealed ranges of one source in an index, in verified epochs.
//!
//! [`Importer::run`] reads the manifest of the source. It refuses a manifest for another
//! network or genesis block, and an index that is sealed at or above the manifest. Then it starts the epochs after the sealed tip of the index in height order, at
//! most two for each of the [`ImportConfig::import_workers`] at a time. The work on one epoch
//! is:
//!
//! 1. Take its part of a byte budget of [`MAX_BLOB_BYTES_BEFORE_INSTALL`], in height order.
//!    The epoch keeps it until its install.
//! 2. Fetch its blobs with [`Fetcher::blob`]. A blob with a wrong digest, or a fetch that
//!    fails after its resumes, stops the import.
//! 3. Run [`Epoch::verify`] on the blocking thread pool. At most `import_workers` epochs do
//!    this at the same time.
//! 4. Run [`Epoch::attest`] and [`Epoch::sample`] against the [`TrustedNode`] at the same
//!    time. Thus the node calls of an epoch overlap the verification of the next epoch.
//!
//! The importer takes the verified epochs in height order, and installs each one before it
//! takes the next. It installs each range with [`Index::install_sealed_range`] and a
//! [`VerifyState::Verified`] row.
//!
//! The start frontiers of the first epoch come from the `range_meta` row of the last sealed
//! range. The importer does not trust them: wrong start frontiers give rebuilt roots that the
//! node does not attest. The start frontiers of each other epoch are the frontiers of the
//! manifest entry before it. [`Epoch::verify`] of the epoch before rebuilds them byte for byte,
//! and the importer installs an epoch only after all epochs below it.
//!
//! A failed epoch stops the import. The importer first installs the epochs below it, thus the
//! index keeps them. A restart continues after the last installed epoch, and fetches no range
//! that is installed.
//!
//! [`Epoch::verify`]: crate::epoch::Epoch::verify
//! [`Fetcher::blob`]: crate::fetch::Fetcher::blob
//! [`Epoch::attest`]: crate::epoch::Epoch::attest
//! [`Epoch::sample`]: crate::epoch::Epoch::sample
//! [`VerifyState::Verified`]: ztreamer_indexer::index::range_meta::VerifyState::Verified

mod epoch_import;

use std::{future, num::NonZeroUsize, panic, pin, sync::Arc, thread};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::{Semaphore, watch};
use tracing::{Instrument as _, info};
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::index::{
    Index, IndexError, IndexState, RANGE_SIZE,
    range_meta::{Frontiers, NodeId},
};

use self::epoch_import::EpochImport;
use crate::{
    attest::{Attestor, EpochError, NodeError},
    epoch::Epoch,
    fetch::{FetchError, Fetcher, RangeSource},
    manifest::Manifest,
    range::Reject,
    sample::{SamplePlan, Sampler},
};

/// The byte budget of the blobs from their fetch to their install. Each epoch takes the bytes
/// of its blobs before its fetch, in height order, and keeps them until its install. The
/// largest mainnet epoch holds 258 MB of blobs. An epoch above the budget takes the full budget.
pub const MAX_BLOB_BYTES_BEFORE_INSTALL: u64 = 2 << 30;

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
    /// The heights of each range that [`Epoch::sample`](crate::epoch::Epoch::sample) compares,
    /// and the secret seed.
    pub verify_sample: SamplePlan,
    /// The epochs that verify at the same time. Two epochs for each worker are in work at the
    /// same time: they fetch, verify, attest and sample.
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
    /// Adds the install of `epoch`.
    fn add_installed_epoch(&mut self, epoch: &Epoch<'_>, state: &IndexState) {
        self.epochs += 1;
        self.ranges += epoch.entries().len();
        self.sealed_through = state.sealed_through();
        self.verified_through = state.verified_through();
        info!(end_height = epoch.end_height(), ranges = self.ranges, "imported epoch");
    }
}

/// Why an import stopped.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// The source gave no manifest, or no blob with the manifest digest.
    #[error(transparent)]
    Fetch(#[from] FetchError),
    /// An index read or install failed.
    #[error(transparent)]
    Index(#[from] IndexError),
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
    /// The index is sealed at or above the manifest.
    #[error("index is sealed through {index}, at or above the manifest ({manifest})")]
    Ahead {
        /// The sealed tip of the index.
        index: u32,
        /// The sealed tip of the manifest.
        manifest: u32,
    },
    /// The sealed tip of the index is not the end of an epoch of the manifest.
    #[error("index tip {height} is inside an epoch of the manifest")]
    EpochBoundary {
        /// The sealed tip of the index.
        height: u32,
    },
    /// The last sealed range of the index has no `range_meta` row, thus no frontiers.
    #[error("sealed range {start} has no range_meta row")]
    MissingRow {
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

/// Installs the sealed ranges of one source in an index, in verified epochs.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use tokio::sync::watch;
/// use ztreamer_indexer::index::Index;
/// use ztreamer_verify::{
///     fetch::RangeSource,
///     importer::{ImportConfig, ImportError, ImportReport, Importer, TrustedNode},
/// };
///
/// # async fn run(
/// #     source: Box<dyn RangeSource>,
/// #     node: TrustedNode,
/// #     index: Arc<Index>,
/// #     config: ImportConfig,
/// # ) -> Result<ImportReport, ImportError> {
/// let (_stop, shutdown) = watch::channel(false);
/// let import = Importer::new(source, node, index, config).run(shutdown);
/// tokio::spawn(import).await.unwrap()
/// # }
/// ```
#[derive(Debug)]
pub struct Importer {
    fetcher: Fetcher,
    node: TrustedNode,
    index: Arc<Index>,
    config: ImportConfig,
}

impl Importer {
    /// An importer of the ranges of `source` into `index`, checked against `node`.
    pub fn new(
        source: Box<dyn RangeSource>,
        node: TrustedNode,
        index: Arc<Index>,
        config: ImportConfig,
    ) -> Self {
        Self { fetcher: Fetcher::new(source), node, index, config }
    }

    /// Imports the epochs of the manifest after the sealed tip of the index, as the module
    /// docs give.
    ///
    /// When `shutdown` becomes `true`, the importer completes the epoch that it installs, and
    /// the import stops. The import drops the other epochs. The future is `Send + 'static`. The
    /// caller spawns it.
    pub async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ImportReport, ImportError> {
        let manifest = self.fetcher.manifest().await?;
        self.check_identity(&manifest)?;
        let state = self.blocking(|index| index.state()).await?;
        let sealed = state.sealed_through();
        let tip = manifest.sealed_through();
        if let Some(index) = sealed.filter(|&sealed| sealed >= tip) {
            return Err(ImportError::Ahead { index, manifest: tip });
        }
        let first_epoch = manifest
            .epochs()
            .position(|epoch| epoch.previous().map(|entry| entry.end_height) == sealed)
            .ok_or(ImportError::EpochBoundary { height: sealed.unwrap_or_default() })?;
        let first_start_frontiers = self.frontiers_after_sealed_tip(sealed).await?;
        let mut report = ImportReport {
            sealed_through: sealed,
            verified_through: state.verified_through(),
            ..ImportReport::default()
        };
        let epoch_import = EpochImport::new(self, manifest, first_epoch, first_start_frontiers);
        let epoch_import = Arc::new(epoch_import);
        let blob_byte_budget = Arc::new(Semaphore::new(MAX_BLOB_BYTES_BEFORE_INSTALL as usize));
        let stop_signal = async move {
            // A dropped sender never stops the import.
            if shutdown.wait_for(|&stop| stop).await.is_err() {
                future::pending::<()>().await;
            }
        };
        let epochs = stream::iter(epoch_import.epochs_to_import())
            .then(|at| {
                let budget = Arc::clone(&blob_byte_budget);
                let bytes = epoch_import.blob_bytes_in_budget(at);
                async move {
                    let budget_permit = budget.acquire_many_owned(bytes).await;
                    (at, budget_permit.expect("the budget is never closed"))
                }
            })
            .map(|(at, budget_permit)| {
                let fetch_and_verify = Arc::clone(&epoch_import).fetch_and_verify(at);
                let rows = async move {
                    Ok::<_, ImportError>((at, fetch_and_verify.await?, budget_permit))
                };
                rows.instrument(epoch_import.epoch_span(at))
            })
            .buffered(epoch_import.epochs_in_work())
            .take_until(stop_signal);
        let mut epochs = pin::pin!(epochs);
        // `buffered` gives the epochs in height order. Thus the error of an epoch comes after
        // the install of each epoch below it.
        let mut next_epoch = epoch_import.epochs_to_import().start;
        while let Some((at, rows, _budget_permit)) = epochs.try_next().await? {
            let install = epoch_import.install_rows(rows);
            let state = install.instrument(epoch_import.epoch_span(at)).await?;
            let epoch = epoch_import.manifest_epoch(at);
            report.add_installed_epoch(&epoch, &state);
            next_epoch = at + 1;
        }
        // Only the shutdown signal ends the epochs early: an error returns above.
        report.stopped = next_epoch < epoch_import.epochs_to_import().end;
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

    /// The frontiers after the sealed tip: the frontiers of the row of the last sealed range,
    /// or empty trees for an empty index.
    async fn frontiers_after_sealed_tip(
        &self,
        sealed: Option<u32>,
    ) -> Result<Frontiers, ImportError> {
        let Some(sealed) = sealed else {
            return Ok(Frontiers::default());
        };
        let start = sealed + 1 - RANGE_SIZE;
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
        index::range_meta::{Attested, ManifestEntry, Pool, PoolRoots, SubtreeNode, VerifyState},
        test_utils::raw_block,
        types::RawIndexBlock,
    };

    use super::*;
    use crate::{
        fetch::{HttpSource, PeerSource},
        range::Check,
        test_utils::{FileServer, PeerServer, SEED, Snapshot, SourceIndex},
    };

    /// Ranges of empty blocks: the epochs end at 9,999, 19,999 and 24,999.
    const RANGES: u32 = 25;

    /// The `tree_roots` call at the first height waits until the call at the second height
    /// answers. With no second height, it waits forever.
    type HeldTreeRoots = (u32, Option<u32>);

    /// The answers in the `attested` records that the filler wrote in the source index.
    struct FakeAttestor {
        answers: BTreeMap<u32, Attested>,
        held_roots: Option<HeldTreeRoots>,
        release_signal: Notify,
        /// The heights of the answered `tree_roots` calls, in order.
        answered_heights: Mutex<Vec<u32>>,
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
            if let Some((held_height, release_height)) = self.held_roots {
                if height == held_height {
                    self.release_signal.notified().await;
                }
                if Some(height) == release_height {
                    self.release_signal.notify_one();
                }
            }
            self.answered_heights.lock().unwrap().push(height);
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
    }

    /// A source index, a signed snapshot of it over HTTP, and an empty target index.
    struct Import {
        source: SourceIndex,
        snapshot: Snapshot,
        server: FileServer,
        target: Arc<Index>,
        _dir: TempDir,
    }

    impl Import {
        async fn new(ranges: u32) -> Self {
            let source = SourceIndex::new(ranges).await;
            let snapshot = Snapshot::export(&source);
            let server = FileServer::start(snapshot.dir.path()).await;
            let dir = tempfile::tempdir().unwrap();
            let target = Arc::new(SourceIndex::open(&dir));
            Self { source, snapshot, server, target, _dir: dir }
        }

        /// The blob at `start` with a wrong tree size at its record 500.
        fn blob_with_wrong_tree_size(&self, start: u32) -> Vec<u8> {
            let blob = self.source.index.read_sealed_range_bytes(start).unwrap().unwrap();
            let decoder = RangeDecoder::new(&blob).unwrap();
            let mut records: Vec<_> =
                (0..RANGE_SIZE as usize).map(|at| decoder.record(at).unwrap()).collect();
            records[500].end_tree_sizes.sapling = 1;
            encode_range(&records).unwrap()
        }

        /// Changes one byte of the blob file of `entry`, and keeps the manifest.
        fn flip_byte_in_blob_file(&self, entry: &ManifestEntry) {
            let path = self.snapshot.range_path(entry);
            let mut blob = std::fs::read(&path).unwrap();
            blob[100] ^= 1;
            std::fs::write(path, blob).unwrap();
        }

        fn fake_attestor(&self, held_roots: Option<HeldTreeRoots>) -> Arc<FakeAttestor> {
            let answers = self.snapshot.manifest.entries().iter().map(|entry| {
                let meta = self.source.index.range_meta(entry.start_height).unwrap().unwrap();
                (entry.end_height, meta.attested.unwrap())
            });
            let (release_signal, answered_heights) = (Notify::new(), Mutex::default());
            Arc::new(FakeAttestor {
                answers: answers.collect(),
                held_roots,
                release_signal,
                answered_heights,
            })
        }

        fn importer(&self, attestor: Arc<FakeAttestor>, network: Network) -> Importer {
            let key = SigningKey::from_bytes(&SEED).verifying_key();
            let source = HttpSource::new(self.server.base.clone(), key).unwrap();
            self.importer_with_source(Box::new(source), attestor, network)
        }

        fn importer_with_source(
            &self,
            source: Box<dyn RangeSource>,
            attestor: Arc<FakeAttestor>,
            network: Network,
        ) -> Importer {
            let node = TrustedNode {
                attestor,
                sampler: Arc::new(FakeSampler),
                id: NodeId { url: "http://node.test".to_owned(), version: "test".to_owned() },
            };
            let config = ImportConfig {
                network,
                verify_sample: SamplePlan { per_range: 2, seed: 7 },
                import_workers: NonZeroUsize::new(4).unwrap(),
            };
            Importer::new(source, node, Arc::clone(&self.target), config)
        }

        async fn run(&self) -> Result<ImportReport, ImportError> {
            let (_stop, shutdown) = watch::channel(false);
            self.importer(self.fake_attestor(None), Network::Mainnet).run(shutdown).await
        }

        /// The blob requests that the snapshot server got.
        fn blob_requests(&self) -> usize {
            let requests = self.server.requests();
            requests.iter().filter(|(path, _)| path.starts_with("ranges/")).count()
        }

        /// Checks that the target holds the blobs of the source, byte for byte, and is verified.
        fn assert_target_has_source_blobs(&self) {
            let sealed = self.snapshot.manifest.sealed_through();
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
        let import = Import::new(RANGES).await;

        let report = import.run().await.unwrap();

        let sealed = import.snapshot.manifest.sealed_through();
        let expected = ImportReport {
            epochs: 3,
            ranges: 25,
            sealed_through: Some(sealed),
            verified_through: Some(sealed),
            stopped: false,
        };
        assert_eq!(report, expected);
        import.assert_target_has_source_blobs();
        let meta = import.target.range_meta(12_000).unwrap().unwrap();
        assert_eq!(meta.state, VerifyState::Verified);
        assert_eq!(meta.entry, import.snapshot.manifest.entries()[12]);
        assert_eq!(meta.verified_by.unwrap().url, "http://node.test");
        assert_eq!(meta.sampled_heights.len(), 2);
        assert!(meta.sampled_heights.iter().all(|height| (12_000..13_000).contains(height)));
    }

    #[tokio::test]
    async fn four_workers_install_twelve_epochs_byte_for_byte() {
        let import = Import::new(120).await;

        let report = import.run().await.unwrap();

        assert_eq!((report.epochs, report.ranges), (12, 120));
        import.assert_target_has_source_blobs();
        assert_eq!(import.blob_requests(), 120);
    }

    /// Four workers fetch up to eight epochs at a time, and the peer sends one stream at a time.
    #[tokio::test]
    async fn an_import_from_a_peer_with_one_stream_waits_for_the_peer_and_completes() {
        let import = Import::new(RANGES).await;
        let server = PeerServer::start(&import.source, 1).await;
        let peer = PeerSource::connect(server.endpoint.clone()).await.unwrap();
        let (_stop, shutdown) = watch::channel(false);
        let importer = import.importer_with_source(
            Box::new(peer),
            import.fake_attestor(None),
            Network::Mainnet,
        );

        let report = importer.run(shutdown).await.unwrap();

        // A peer lists the ranges up to the last closed epoch only: range 24 closes none.
        assert_eq!((report.epochs, report.ranges), (2, 20));
        assert_eq!((report.sealed_through, report.verified_through), (Some(19_999), Some(19_999)));
        for start in (0..20).map(|range| range * RANGE_SIZE) {
            let blob = import.target.read_sealed_range_bytes(start).unwrap();
            assert_eq!(blob, import.source.index.read_sealed_range_bytes(start).unwrap());
        }
        assert!(server.refusals() > 0);
        assert_eq!(server.requests().len(), 20);
    }

    #[tokio::test]
    async fn a_slow_epoch_verifies_last_and_installs_in_order() {
        let import = Import::new(RANGES).await;
        // The first epoch gets its roots only after the last epoch.
        let attestor = import.fake_attestor(Some((9_999, Some(24_999))));
        let (_stop, shutdown) = watch::channel(false);

        let importer = import.importer(Arc::clone(&attestor), Network::Mainnet);
        let report = importer.run(shutdown).await.unwrap();

        assert_eq!(report.ranges, 25);
        import.assert_target_has_source_blobs();
        let answered_heights = attestor.answered_heights.lock().unwrap().clone();
        let answer_order =
            |height| answered_heights.iter().position(|&answered| answered == height).unwrap();
        assert!(answer_order(24_999) < answer_order(9_999), "{answered_heights:?}");
    }

    #[tokio::test]
    async fn a_stopped_import_keeps_a_prefix_and_a_restart_fetches_only_the_rest() {
        let import = Import::new(RANGES).await;
        // The second epoch never gets its roots.
        let attestor = import.fake_attestor(Some((19_999, None)));
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
        let requests_before_restart = import.blob_requests();
        assert!(requests_before_restart >= 10);
        let report = import.run().await.unwrap();
        assert_eq!((report.epochs, report.ranges), (2, 15));
        assert_eq!(import.blob_requests() - requests_before_restart, 15);
        import.assert_target_has_source_blobs();
    }

    #[tokio::test]
    async fn a_mutated_blob_stops_the_import_at_its_epoch() {
        let mut import = Import::new(RANGES).await;
        let blob = import.blob_with_wrong_tree_size(15_000);
        import.snapshot.replace_blob(15_000, &blob);

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

    /// A blob with a wrong digest stops the import. The index keeps the verified prefix before
    /// the bad blob.
    #[tokio::test]
    async fn a_blob_with_a_wrong_digest_stops_the_import() {
        let import = Import::new(RANGES).await;
        import.flip_byte_in_blob_file(&import.snapshot.manifest.entries()[15]);

        let error = import.run().await.unwrap_err();

        assert!(
            matches!(error, ImportError::Fetch(FetchError::Digest { start_height: 15_000, .. })),
            "{error:?}"
        );
        let state = import.target.state().unwrap();
        assert_eq!((state.sealed_through(), state.verified_through()), (Some(9_999), Some(9_999)));
    }

    #[tokio::test]
    async fn an_index_at_the_manifest_tip_or_on_another_network_is_refused() {
        let import = Import::new(RANGES).await;
        let (stop, shutdown) = watch::channel(true);
        let importer = import.importer(import.fake_attestor(None), Network::Mainnet);
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
            import.importer(import.fake_attestor(None), testnet).run(shutdown).await.unwrap_err();
        assert!(matches!(error, ImportError::Network { .. }), "{error:?}");
    }
}
