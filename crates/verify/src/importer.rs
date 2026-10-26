//! The importer: installs the sealed ranges of a source in an index, one verified epoch at a
//! time.
//!
//! [`Importer::run`] reads the manifest of the source. It refuses a manifest for another
//! network or genesis block, and an index that is sealed at or above the manifest. Then, for
//! each epoch after the sealed tip of the index, in ascending order, it:
//!
//! 1. fetches the blob of each range of the epoch with the [`Fetcher`]. A blob with a wrong
//!    digest stops the import;
//! 2. runs [`Epoch::verify`] on the blocking thread pool, from the end frontiers of the epoch
//!    before;
//! 3. runs [`Epoch::attest`] and [`Epoch::sample`] against the [`TrustedNode`];
//! 4. installs each range with [`Index::install_sealed_range`] and a
//!    [`VerifyState::Verified`] row.
//!
//! The importer runs one epoch at a time and installs an epoch only after all of its checks
//! pass. Thus a rejected epoch leaves the index at the end of the epoch before it. The start
//! frontiers of the first epoch come from the `range_meta` row of the last sealed range. The
//! importer does not trust them: wrong start frontiers give rebuilt roots that the node does
//! not attest.

use std::{
    panic,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::sync::watch;
use tracing::info;
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::index::{
    Index, IndexError, RANGE_SIZE,
    range_meta::{Frontiers, NodeId, RangeMeta, VerifyState},
};

use crate::{
    attest::{Attestor, EpochError, NodeError},
    epoch::{Epoch, EpochProof},
    fetch::{FetchError, Fetcher, RangeSource, Source},
    manifest::Manifest,
    range::Reject,
    sample::{SamplePlan, Sampler},
};

/// The node that attests and samples the imported epochs.
#[derive(Clone)]
pub struct TrustedNode {
    /// Attests the end hashes, the tree roots and the subtree roots of each epoch.
    pub attestor: Arc<dyn Attestor>,
    /// Gives the full blocks that [`Epoch::sample`] compares.
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
    /// The heights of each range that [`Epoch::sample`] compares, and the secret seed.
    pub verify_sample: SamplePlan,
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

/// Why an import stopped.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// The source gave no manifest or no blob.
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
    #[error("index is sealed through {sealed_through}, inside an epoch of the manifest")]
    EpochBoundary {
        /// The sealed tip of the index.
        sealed_through: u32,
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

/// Installs the sealed ranges of one source in an index, one verified epoch at a time.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use tokio::sync::watch;
/// use ztreamer_indexer::index::Index;
/// use ztreamer_verify::{
///     fetch::Fetcher,
///     importer::{ImportConfig, ImportError, ImportReport, Importer, TrustedNode},
/// };
///
/// # async fn run(
/// #     fetcher: Fetcher,
/// #     node: TrustedNode,
/// #     index: Arc<Index>,
/// #     config: ImportConfig,
/// # ) -> Result<ImportReport, ImportError> {
/// let (_stop, shutdown) = watch::channel(false);
/// let import = Importer::new(fetcher, node, index, config).run(shutdown);
/// tokio::spawn(import).await.unwrap()
/// # }
/// ```
#[derive(Debug)]
pub struct Importer<S = Source> {
    source: Fetcher<S>,
    node: TrustedNode,
    index: Arc<Index>,
    config: ImportConfig,
}

impl<S: RangeSource> Importer<S> {
    /// An importer of the ranges of `source` into `index`, checked against `node`.
    pub const fn new(
        source: Fetcher<S>,
        node: TrustedNode,
        index: Arc<Index>,
        config: ImportConfig,
    ) -> Self {
        Self { source, node, index, config }
    }

    /// Imports the epochs of the manifest after the sealed tip of the index, in ascending
    /// order, as the module docs give.
    ///
    /// The import checks `shutdown` before each epoch, and stops when it is `true`. It does not
    /// stop inside an epoch. The future is `Send + 'static` when `S` is. The caller spawns it.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<ImportReport, ImportError> {
        let manifest = Arc::new(self.source.manifest().await?);
        self.check_identity(&manifest)?;
        let state = self.blocking(|index| index.state()).await?;
        let sealed = state.sealed_through();
        if let Some(index) = sealed.filter(|&sealed| sealed >= manifest.sealed_through()) {
            return Err(ImportError::Ahead { index, manifest: manifest.sealed_through() });
        }
        let first = manifest
            .epochs()
            .position(|epoch| epoch.previous().map(|entry| entry.end_height) == sealed)
            .ok_or(ImportError::EpochBoundary { sealed_through: sealed.unwrap_or_default() })?;
        let mut start = self.start_frontiers(sealed).await?;
        let mut report = ImportReport {
            sealed_through: sealed,
            verified_through: state.verified_through(),
            ..ImportReport::default()
        };
        for (at, epoch) in manifest.epochs().enumerate().skip(first) {
            if *shutdown.borrow() {
                report.stopped = true;
                break;
            }
            let installed = self.import_epoch(&manifest, at, &epoch, start).await?;
            start = installed.end_frontiers;
            report.epochs += 1;
            report.ranges += epoch.entries().len();
            report.sealed_through = Some(epoch.end_height());
            report.verified_through = installed.verified_through;
            info!(end_height = epoch.end_height(), ranges = report.ranges, "imported epoch");
        }
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
    async fn start_frontiers(&self, sealed: Option<u32>) -> Result<Frontiers, ImportError> {
        let Some(sealed) = sealed else {
            return Ok(Frontiers::default());
        };
        let start = sealed + 1 - RANGE_SIZE;
        self.blocking(move |index| index.range_meta(start))
            .await?
            .map(|meta| meta.entry.frontiers)
            .ok_or(ImportError::MissingRow { start })
    }

    /// Fetches, verifies, attests, samples and installs epoch `at` of `manifest`.
    #[tracing::instrument(level = "debug", skip_all, fields(end_height = epoch.end_height()))]
    async fn import_epoch(
        &self,
        manifest: &Arc<Manifest>,
        at: usize,
        epoch: &Epoch<'_>,
        start: Frontiers,
    ) -> Result<Installed, ImportError> {
        let end = epoch.end_height();
        let mut blobs = Vec::with_capacity(epoch.entries().len());
        for entry in epoch.entries() {
            blobs.push(self.source.blob(entry).await?);
        }
        let proof = Self::verify(Arc::clone(manifest), at, blobs.clone(), start)
            .await
            .map_err(|reject| ImportError::Rejected { epoch: end, reject })?;
        let attested = epoch
            .attest(&proof, &*self.node.attestor)
            .await
            .map_err(|error| ImportError::in_epoch(end, error))?;
        let sampled = epoch
            .sample(&blobs, &*self.node.sampler, self.config.verify_sample)
            .await
            .map_err(|error| ImportError::in_epoch(end, error))?;

        let verified_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let ranges: Vec<_> = epoch
            .entries()
            .iter()
            .zip(blobs)
            .zip(attested)
            .map(|((entry, blob), attested)| {
                let heights = entry.start_height..=entry.end_height;
                let meta = RangeMeta {
                    entry: entry.clone(),
                    state: VerifyState::Verified,
                    verified_by: Some(self.node.id.clone()),
                    verified_at: Some(verified_at.as_secs()),
                    attested: Some(attested),
                    sampled_heights: sampled
                        .iter()
                        .copied()
                        .filter(|h| heights.contains(h))
                        .collect(),
                };
                (blob, meta)
            })
            .collect();
        let state = self
            .blocking(move |index| {
                ranges.iter().try_fold(None, |_, (blob, meta)| {
                    index.install_sealed_range(meta.entry.start_height, blob, meta).map(Some)
                })
            })
            .await?;
        Ok(Installed {
            end_frontiers: proof.end_frontiers,
            verified_through: state.and_then(|state| state.verified_through()),
        })
    }

    /// Runs [`Epoch::verify`] for epoch `at` of `manifest` on the blocking thread pool.
    async fn verify(
        manifest: Arc<Manifest>,
        at: usize,
        blobs: Vec<Bytes>,
        start: Frontiers,
    ) -> Result<EpochProof, Reject> {
        tokio::task::spawn_blocking(move || {
            let epoch = manifest.epochs().nth(at).expect("the caller took `at` from the epochs");
            epoch.verify(&blobs, &start)
        })
        .await
        .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
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

/// What the install of one epoch left.
struct Installed {
    /// The start frontiers of the next epoch.
    end_frontiers: Frontiers,
    /// The verified tip of the index after the install.
    verified_through: Option<u32>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;
    use zakura_chain::subtree::NoteCommitmentSubtreeIndex;
    use ztreamer_indexer::{
        codec::{RangeDecoder, encode_range},
        index::range_meta::{Attested, PoolRoots, SubtreeNode},
        test_utils::raw_block,
        types::RawIndexBlock,
    };

    use super::*;
    use crate::{
        fetch::HttpSource,
        manifest::Pool,
        range::Check,
        test_utils::{FileServer, SEED, Snapshot, SourceIndex},
    };

    /// Ranges of empty blocks: the epochs end at 9,999, 19,999 and 24,999.
    const RANGES: u32 = 25;

    /// The answers in the `attested` records that the filler wrote in the source index.
    struct FakeAttestor(BTreeMap<u32, Attested>);

    impl FakeAttestor {
        fn answer(&self, height: u32) -> Result<&Attested, NodeError> {
            self.0.get(&height).ok_or(NodeError::Height { expected: height, actual: 0 })
        }
    }

    #[async_trait]
    impl Attestor for FakeAttestor {
        async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
            Ok(self.answer(height)?.end_hash)
        }

        async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
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

    /// A source index, its signed snapshot over HTTP, and an empty target index.
    struct Import {
        source: SourceIndex,
        snapshot: Snapshot,
        server: FileServer,
        target: Arc<Index>,
        _dir: TempDir,
    }

    impl Import {
        async fn new() -> Self {
            let source = SourceIndex::new(RANGES).await;
            let snapshot = Snapshot::export(&source);
            let server = FileServer::start(snapshot.dir.path()).await;
            let dir = tempfile::tempdir().unwrap();
            let target = Arc::new(SourceIndex::open(&dir));
            Self { source, snapshot, server, target, _dir: dir }
        }

        fn importer(&self, network: Network) -> Importer {
            let key = SigningKey::from_bytes(&SEED).verifying_key();
            let source = HttpSource::new(self.server.base.clone(), key).unwrap();
            let attested = (0..RANGES).map(|range| {
                let meta = self.source.index.range_meta(range * RANGE_SIZE).unwrap().unwrap();
                (meta.entry.end_height, meta.attested.unwrap())
            });
            let node = TrustedNode {
                attestor: Arc::new(FakeAttestor(attested.collect())),
                sampler: Arc::new(FakeSampler),
                id: NodeId { url: "http://node.test".to_owned(), version: "test".to_owned() },
            };
            let config =
                ImportConfig { network, verify_sample: SamplePlan { per_range: 2, seed: 7 } };
            let fetcher = Fetcher::new(Source::Http(Box::new(source)));
            Importer::new(fetcher, node, Arc::clone(&self.target), config)
        }

        async fn run(&self) -> Result<ImportReport, ImportError> {
            let (_stop, shutdown) = watch::channel(false);
            self.importer(Network::Mainnet).run(shutdown).await
        }
    }

    #[tokio::test]
    async fn an_import_installs_the_same_ranges_and_verifies_them() {
        let import = Import::new().await;

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
        for start in (0..RANGES).map(|range| range * RANGE_SIZE) {
            let blob = import.target.read_sealed_range_bytes(start).unwrap();
            assert_eq!(blob, import.source.index.read_sealed_range_bytes(start).unwrap());
        }
        assert_eq!(import.target.state().unwrap().verified_through(), Some(sealed));
        let meta = import.target.range_meta(12_000).unwrap().unwrap();
        assert_eq!(meta.state, VerifyState::Verified);
        assert_eq!(meta.entry, import.snapshot.manifest.entries()[12]);
        assert_eq!(meta.verified_by.unwrap().url, "http://node.test");
        assert_eq!(meta.sampled_heights.len(), 2);
        assert!(meta.sampled_heights.iter().all(|height| (12_000..13_000).contains(height)));
    }

    #[tokio::test]
    async fn a_mutated_blob_stops_the_import_at_its_epoch() {
        let mut import = Import::new().await;
        let blob = import.source.index.read_sealed_range_bytes(15_000).unwrap().unwrap();
        let decoder = RangeDecoder::new(&blob).unwrap();
        let mut records: Vec<_> =
            (0..RANGE_SIZE as usize).map(|at| decoder.record(at).unwrap()).collect();
        records[500].end_tree_sizes.sapling = 1;
        import.snapshot.replace_blob(15_000, &encode_range(&records).unwrap());

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
    async fn a_blob_with_a_wrong_digest_stops_the_import_at_its_epoch() {
        let import = Import::new().await;
        let entry = &import.snapshot.manifest.entries()[15];
        let path = import.snapshot.range_path(entry);
        let mut blob = std::fs::read(&path).unwrap();
        blob[100] ^= 1;
        std::fs::write(path, blob).unwrap();

        let error = import.run().await.unwrap_err();

        let digest =
            matches!(error, ImportError::Fetch(FetchError::Digest { start_height: 15_000 }));
        assert!(digest, "{error:?}");
        let state = import.target.state().unwrap();
        assert_eq!((state.sealed_through(), state.verified_through()), (Some(9_999), Some(9_999)));
    }

    #[tokio::test]
    async fn an_index_at_the_manifest_tip_or_on_another_network_is_refused() {
        let import = Import::new().await;
        let (stop, shutdown) = watch::channel(true);
        let report = import.importer(Network::Mainnet).run(shutdown).await.unwrap();
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
        let error = import.importer(testnet).run(shutdown).await.unwrap_err();
        assert!(matches!(error, ImportError::Network { .. }), "{error:?}");
    }
}
