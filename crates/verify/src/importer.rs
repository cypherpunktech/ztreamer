//! The importer: installs the sealed ranges of one source in an index, in verified epochs.
//!
//! [`Importer::run`] reads the manifest of the source. It refuses a manifest for another
//! network or genesis block, and an index that is sealed at or above the manifest. Then it starts the epochs after the sealed tip of the index in height order, at
//! most two for each of the [`ImportConfig::import_workers`] at a time. The work on one epoch
//! is:
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
//! [`CheckedRange`](ztreamer_indexer::index::CheckedRange) of each blob with its one decode.
//! Thus the install, on one writer thread, does only the checks that need the index: it does
//! not calculate the digest or decode the blob again.
//!
//! The row holds the manifest entry with the `epoch_end` flag of the epoch rule: a manifest
//! closes its last epoch at its last entry also where the rule does not
//! ([`Epoch::closed_by_rule`]).
//!
//! The start frontiers of the first epoch come from the `range_meta` row of the last sealed
//! range. The importer does not trust them: wrong start frontiers give rebuilt roots that the
//! node does not attest. The start frontiers of each other epoch are the frontiers of the
//! manifest entry before it. [`EpochProof::rebuild`] of the epoch before rebuilds them byte for byte,
//! and the importer installs an epoch only after all epochs below it.
//!
//! A failed epoch stops the import. The importer first installs the epochs below it, thus the
//! index keeps them. A restart continues after the last installed epoch, and fetches no range
//! that is installed.
//!
//! [`EpochProof::rebuild`]: crate::proof::EpochProof::rebuild
//! [`Fetcher::blob`]: crate::fetch::Fetcher::blob
//! [`Epoch::closed_by_rule`]: crate::epoch::Epoch::closed_by_rule
//! [`EpochProof::attest`]: crate::proof::EpochProof::attest
//! [`SamplePlan::sample`]: crate::sample::SamplePlan::sample
//! [`VerifyState::Verified`]: ztreamer_indexer::index::range_meta::VerifyState::Verified

mod epoch_import;

use std::{future, num::NonZeroUsize, pin, sync::Arc, thread};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::{Semaphore, watch};
use tracing::{Instrument as _, info};
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::{
    blocking::{BlockingTask, BlockingTaskCancelled},
    index::{Index, IndexError, IndexState, RANGE_SIZE, range_meta::NodeId},
    manifest_entry::Frontiers,
};

use self::epoch_import::EpochImport;
use crate::{
    attest::{Attestor, EpochError, NodeError},
    epoch::Epoch,
    fetch::{FetchError, Fetcher, RangeSource},
    manifest::Manifest,
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
        self.blocking(move |index| index.latest()?.range_meta(start))
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
        Ok(BlockingTask::spawn(move || call(&index)).join().await??)
    }
}

#[cfg(test)]
mod tests;
