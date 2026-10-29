//! The importer: installs the sealed ranges of one source in an index, in verified epochs.
//!
//! [`Importer::run`] reads the manifest of the source. It refuses a manifest for another
//! network or genesis block, and an index that is sealed at or above the manifest. If the index
//! has sealed ranges, the manifest entry that ends at the sealed tip must have the hash and the
//! tree sizes of the tip block ([`ImportError::SealedTip`], [`ImportError::ReorgedTip`]). The
//! importer then deletes the mutable rows above the sealed tip: an install needs an index with
//! none, and the tail indexes those blocks again.
//!
//! Then it starts the epochs after the sealed tip of the index in height order, at most two
//! for each of the [`ImportConfig::import_workers`] at a time. A sealed tip inside an epoch of
//! the manifest starts the import with the part of that epoch after the tip
//! ([`Epoch::part_after`]). The work on one epoch is:
//!
//! 1. Take its part of a byte budget of [`MAX_BLOB_BYTES_BEFORE_INSTALL`], in height order.
//!    The epoch keeps it until its install.
//! 2. Fetch its blobs with [`Fetcher::blob`], all at the same time, and keep them in height
//!    order. The request limit of the source sets the requests on the wire. A blob with a wrong
//!    digest, or a fetch that fails after its resumes, stops the import with the error of the
//!    first such blob in height order.
//! 3. Run [`EpochProof::rebuild`] on the blocking thread pool. At most `import_workers` epochs do
//!    this at the same time.
//! 4. Run [`EpochProof::attest`] and [`SamplePlan::sample`] against the [`TrustedNode`] at the same
//!    time. Thus the node calls of an epoch overlap the verification of the next epoch.
//!
//! The importer takes the verified epochs in height order, and installs each one before it
//! takes the next. It installs each range with [`Index::install_checked_range`] and a
//! [`VerifyState::Verified`] row. [`EpochProof::rebuild`] made the
//! [`CheckedRange`](ztreamer_index::index::CheckedRange) of each blob with its one decode.
//! Thus the install, on one writer thread, does only the checks that need the index: it does
//! not calculate the digest or decode the blob again.
//!
//! The row holds the manifest entry with the `epoch_end` flag of the epoch rule: a manifest
//! closes its last epoch at its last entry also where the rule does not
//! ([`Epoch::closed_by_rule`]). A part of an epoch has the flag of the full epoch.
//!
//! The start frontiers of each epoch are the frontiers of the manifest entry before it. For
//! the first epoch, only the manifest gives them. The importer does not trust them: wrong start
//! frontiers give rebuilt roots that the node does not attest. For each other epoch,
//! [`EpochProof::rebuild`] of the epoch before rebuilds them byte for byte, and the importer
//! installs an epoch only after all epochs below it.
//!
//! A failed epoch stops the import. The importer first installs the epochs below it, thus the
//! index keeps them. A restart continues after the last installed epoch, and fetches no range
//! that is installed.
//!
//! [`EpochProof::rebuild`]: crate::proof::EpochProof::rebuild
//! [`Fetcher::blob`]: crate::fetch::Fetcher::blob
//! [`Epoch::closed_by_rule`]: ztreamer_snapshot::epoch::Epoch::closed_by_rule
//! [`Epoch::part_after`]: ztreamer_snapshot::epoch::Epoch::part_after
//! [`EpochProof::attest`]: crate::proof::EpochProof::attest
//! [`SamplePlan::sample`]: crate::sample::SamplePlan::sample
//! [`VerifyState::Verified`]: ztreamer_index::index::range_meta::VerifyState::Verified

mod epoch_import;

use std::{future, num::NonZeroUsize, pin, sync::Arc, thread};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::{Semaphore, watch};
use tracing::{Instrument as _, info};
use zakura_chain::{block, parameters::Network};
use ztreamer_blocking::{BlockingTask, BlockingTaskCancelled};
use ztreamer_chain::codec::CompactBlockRecord;
use ztreamer_index::index::{BlockId, Index, IndexError, IndexState, range_meta::NodeId};
use ztreamer_snapshot::{epoch::Epoch, manifest::Manifest, manifest_entry::ManifestEntry};

use self::epoch_import::EpochImport;
use crate::{
    attest::{Attestor, EpochError, NodeError},
    fetch::{FetchError, Fetcher, RangeSource},
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
    /// Gives the full blocks that [`SamplePlan::sample`](crate::sample::SamplePlan::sample) compares.
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
    /// The heights of each range that [`SamplePlan::sample`](crate::sample::SamplePlan::sample) compares,
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
    /// The blocking pool stopped before a blocking task of the import ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
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
    /// The sealed tip of the index is not the end of a range of the manifest.
    #[error("index tip {height} is not the end of a range of the manifest")]
    RangeBoundary {
        /// The sealed tip of the index.
        height: u32,
    },
    /// The manifest entry that ends at the sealed tip does not have the hash or the tree sizes
    /// of the tip block, and the node has the tip block. The source is on another chain, or it
    /// is wrong.
    #[error(
        "the manifest entry through {height} does not agree with the index and the node: use \
         another --replicate-from source"
    )]
    SealedTip {
        /// The sealed tip of the index.
        height: u32,
    },
    /// The node does not have the block at the sealed tip of the index: a deep reorg replaced
    /// sealed ranges. The daemon does not recover such a reorg at the start.
    #[error(
        "the node does not have the index block at the sealed tip {height}: a deep reorg \
         replaced sealed ranges; remove the index and start again"
    )]
    ReorgedTip {
        /// The sealed tip of the index.
        height: u32,
    },
    /// The node gave no answer for the block hash at the sealed tip.
    #[error("node gave no answer for height {height}")]
    Node {
        /// The height that the failed call asked about.
        height: u32,
        /// Why the node gave no answer.
        #[source]
        source: NodeError,
    },
    /// A range of the epoch failed a check, or the node gave no answer for the epoch. The index
    /// keeps the epochs before it.
    #[error("epoch through {epoch} not accepted")]
    Epoch {
        /// The end height of the epoch.
        epoch: u32,
        /// The failed check, or the node call that failed.
        #[source]
        source: EpochError,
    },
}

impl ImportError {
    /// The error for `source` in the epoch through `epoch`. A cancel of a blocking task is
    /// [`ImportError::Cancelled`], not a failure of the epoch: the runtime stops.
    pub(crate) fn from_epoch_error(epoch: u32, source: EpochError) -> Self {
        match source {
            EpochError::Node { source: NodeError::Cancelled(cancelled), .. } => {
                Self::Cancelled(cancelled)
            }
            source => Self::Epoch { epoch, source },
        }
    }

    /// The error for `source` in a node call for `height` outside an epoch. A cancel of a
    /// blocking task is [`ImportError::Cancelled`].
    pub(crate) fn from_node_error(height: u32, source: NodeError) -> Self {
        match source {
            NodeError::Cancelled(cancelled) => Self::Cancelled(cancelled),
            source => Self::Node { height, source },
        }
    }
}

/// Installs the sealed ranges of one source in an index, in verified epochs.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use tokio::sync::watch;
/// use ztreamer_index::index::Index;
/// use ztreamer_import::{
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
        // The first epoch with entries after the sealed tip, and the entry that ends at the tip.
        let (first_epoch, tip_entry) = manifest
            .epochs()
            .enumerate()
            .find_map(|(at, epoch)| match sealed {
                None => Some((at, None)),
                Some(sealed) => epoch.part_after(sealed).map(|part| (at, part.previous().cloned())),
            })
            .ok_or(ImportError::RangeBoundary { height: sealed.unwrap_or_default() })?;
        let state = match tip_entry {
            Some(entry) => self.continue_at_sealed_tip(&entry, state).await?,
            None => state,
        };
        let mut report = ImportReport {
            sealed_through: sealed,
            verified_through: state.verified_through(),
            ..ImportReport::default()
        };
        let epoch_import = EpochImport::new(self, manifest, first_epoch, sealed);
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

    /// Checks that `entry`, the manifest entry that ends at the sealed tip, has the hash and the
    /// tree sizes of the tip block. Then deletes the mutable rows above the sealed tip, and
    /// gives the index state after the delete.
    ///
    /// If `entry` does not agree with the tip block, the node decides which one is wrong: a
    /// node with the tip block gives [`ImportError::SealedTip`], another node gives
    /// [`ImportError::ReorgedTip`].
    async fn continue_at_sealed_tip(
        &self,
        entry: &ManifestEntry,
        state: IndexState,
    ) -> Result<IndexState, ImportError> {
        let height = entry.end_height;
        let tip = self.blocking(move |index| Self::read_block(index, height)).await?;
        if tip.hash != entry.end_hash || tip.end_tree_sizes != entry.end_tree_sizes {
            let node_hash = self
                .node
                .attestor
                .block_hash(height)
                .await
                .map_err(|source| ImportError::from_node_error(height, source))?;
            return Err(if node_hash == tip.hash {
                ImportError::SealedTip { height }
            } else {
                ImportError::ReorgedTip { height }
            });
        }
        let Some(durable_tip) = state.durable_tip().filter(|durable| durable.height > height)
        else {
            return Ok(state);
        };
        let (generation, ancestor) = (state.generation(), BlockId::new(height, tip.hash));
        let state = self
            .blocking(move |index| {
                index.replace_mutable_suffix(generation, ancestor, Vec::new(), None)
            })
            .await?;
        info!(
            sealed_through = height,
            deleted_through = durable_tip.height,
            "deleted the mutable rows above the sealed tip; the tail indexes them again"
        );
        Ok(state)
    }

    /// Reads the block record at `height` from the index.
    fn read_block(index: &Index, height: u32) -> Result<CompactBlockRecord, IndexError> {
        let mut found = None;
        index.latest()?.read_blocks::<CompactBlockRecord>(height, height, |record| {
            found = Some(record);
            false
        })?;
        found.ok_or(IndexError::Coverage { height })
    }

    /// Runs an index call on the blocking thread pool.
    async fn blocking<T: Send + 'static>(
        &self,
        call: impl FnOnce(&Index) -> Result<T, IndexError> + Send + 'static,
    ) -> Result<T, ImportError> {
        let index = Arc::clone(&self.index);
        Ok(BlockingTask::spawn(move || call(&index)).join().await??)
    }
}

#[cfg(test)]
mod tests;
