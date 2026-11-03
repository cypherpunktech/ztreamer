//! The verifier: checks the sealed ranges of one source, in epochs, and installs nothing.
//!
//! [`SnapshotVerifier::run`] reads the manifest of the source. It refuses a manifest for a
//! network that it does not know, or with another genesis block than that network. Then it
//! checks each epoch of the manifest, at most two for each core at a time. The work on one
//! epoch is:
//!
//! 1. Take its part of a byte budget of [`MAX_BLOB_BYTES_BEFORE_INSTALL`], in height order.
//!    The epoch keeps it until its checks end.
//! 2. Fetch its blobs with [`Fetcher::epoch_blobs`].
//! 3. Run [`EpochProof::rebuild`] on the blocking thread pool. At most one epoch for each core
//!    does this at the same time.
//! 4. With a [`NodeCheck`], run [`EpochProof::attest`] and [`SamplePlan::sample`] against its
//!    node at the same time. Thus the node calls of an epoch overlap the rebuild of the next
//!    epochs.
//!
//! These are the checks of [`Importer::run`](crate::importer::Importer::run) on each epoch.
//! Thus a verification with a node passes only if an empty replica on that node accepts the
//! source. A verification without a node proves that the manifest is signed, that each blob is
//! there with its digest, and that the ranges agree with the manifest: the hash links, the tree
//! sizes, and each frontier and subtree root of the rebuilt trees. It does not prove that the
//! ranges are the chain.
//!
//! The first epoch starts from empty trees. Each other epoch starts from the frontiers of the
//! manifest entry before it, and the rebuild of the epoch before checks them byte for byte.
//! Thus the epochs can verify at the same time.
//!
//! The epochs end in any order: a slow epoch does not stop the start of the next epochs. The
//! first epoch that fails stops the verification with its error, and the drop stops the other
//! epochs.
//!
//! [`EpochProof::rebuild`]: crate::proof::EpochProof::rebuild
//! [`EpochProof::attest`]: crate::proof::EpochProof::attest
//! [`SamplePlan::sample`]: crate::sample::SamplePlan::sample

use std::{future, num::NonZeroUsize, sync::Arc, thread};

use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::Semaphore;
use tracing::info;
use zakura_chain::{block, parameters::Network};
use ztreamer_blocking::{BlockingTask, BlockingTaskCancelled};
use ztreamer_snapshot::{manifest::Manifest, manifest_entry::Frontiers};

use crate::{
    attest::EpochError,
    fetch::{FetchError, Fetcher, RangeSource},
    importer::{MAX_BLOB_BYTES_BEFORE_INSTALL, TrustedNode},
    proof::EpochProof,
    sample::SamplePlan,
};

/// The node checks of a verification.
#[derive(Clone, Debug)]
pub struct NodeCheck {
    /// The node that attests and samples each epoch.
    pub node: TrustedNode,
    /// The heights of each range that the node samples, and the secret seed.
    pub sample: SamplePlan,
}

/// What a verification checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifyReport {
    /// The last height of the last range of the manifest.
    pub sealed_through: u32,
    /// The epochs checked.
    pub epochs: usize,
    /// The ranges checked.
    pub ranges: usize,
    /// The bytes of the blobs checked.
    pub blob_bytes: u64,
    /// The heights that the node sampled. Zero without a [`NodeCheck`].
    pub sampled_heights: usize,
}

/// Why a verification stopped.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The source gave no manifest, or no blob with the manifest digest.
    #[error(transparent)]
    Fetch(#[from] FetchError),
    /// The blocking pool stopped before a rebuild ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
    /// The network of the manifest is not a known network.
    #[error("manifest is for network {0:?}, which is not a known network")]
    Network(String),
    /// The manifest has another genesis block than its network.
    #[error("manifest has genesis {manifest}, its network {network}")]
    Genesis {
        /// The genesis hash of the manifest.
        manifest: block::Hash,
        /// The genesis hash of the network of the manifest.
        network: block::Hash,
    },
    /// A range of the epoch failed a check, or the node gave no answer for the epoch.
    #[error("epoch through {epoch} does not verify")]
    Epoch {
        /// The end height of the epoch.
        epoch: u32,
        /// The failed check, or the node call that failed.
        #[source]
        source: EpochError,
    },
}

/// Checks the sealed ranges of one source, in epochs, and installs nothing.
///
/// ```no_run
/// use ztreamer_import::{
///     fetch::RangeSource,
///     verifier::{NodeCheck, SnapshotVerifier, VerifyError, VerifyReport},
/// };
///
/// # async fn run(
/// #     source: Box<dyn RangeSource>,
/// #     node: Option<NodeCheck>,
/// # ) -> Result<VerifyReport, VerifyError> {
/// SnapshotVerifier::new(source, node).run().await
/// # }
/// ```
#[derive(Debug)]
pub struct SnapshotVerifier {
    fetcher: Fetcher,
    node: Option<NodeCheck>,
}

impl SnapshotVerifier {
    /// A verifier of the ranges of `source`. With `node`, the node attests and samples each
    /// epoch.
    pub fn new(source: Box<dyn RangeSource>, node: Option<NodeCheck>) -> Self {
        Self { fetcher: Fetcher::new(source), node }
    }

    /// Checks each epoch of the manifest of the source, as the module docs give.
    pub async fn run(&self) -> Result<VerifyReport, VerifyError> {
        let manifest = Arc::new(self.fetcher.manifest().await?);
        let network: Network = manifest
            .network()
            .parse()
            .map_err(|_| VerifyError::Network(manifest.network().to_owned()))?;
        if manifest.genesis_hash() != network.genesis_hash() {
            let (manifest, network) = (manifest.genesis_hash(), network.genesis_hash());
            return Err(VerifyError::Genesis { manifest, network });
        }
        let workers = thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let blob_byte_budget = Semaphore::new(MAX_BLOB_BYTES_BEFORE_INSTALL as usize);
        let rebuild_permits = Semaphore::new(workers);
        let epochs = manifest.epochs().count();
        // The futures in the `Vec` do no work until `buffer_unordered` polls them, and it starts
        // them in height order. Thus the epochs take their parts of the budget in height order.
        let checks: Vec<_> = (0..epochs)
            .map(|at| self.verify_epoch(&manifest, at, &blob_byte_budget, &rebuild_permits))
            .collect();
        let sampled_heights = stream::iter(checks)
            .buffer_unordered(2 * workers)
            .try_fold(0, |sum, sampled| future::ready(Ok(sum + sampled)))
            .await?;
        let entries = manifest.entries();
        Ok(VerifyReport {
            sealed_through: manifest.sealed_through(),
            epochs,
            ranges: entries.len(),
            blob_bytes: entries.iter().map(|entry| entry.blob_len).sum(),
            sampled_heights,
        })
    }

    /// Checks the epoch at position `at` in `manifest`. The rebuild holds one of
    /// `rebuild_permits`. Gives the number of heights that the node sampled.
    async fn verify_epoch(
        &self,
        manifest: &Arc<Manifest>,
        at: usize,
        blob_byte_budget: &Semaphore,
        rebuild_permits: &Semaphore,
    ) -> Result<usize, VerifyError> {
        let epoch = manifest.epochs().nth(at).expect("`run` takes `at` from the epochs");
        let bytes: u64 = epoch.entries().iter().map(|entry| entry.blob_len).sum();
        let bytes = u32::try_from(bytes.min(MAX_BLOB_BYTES_BEFORE_INSTALL))
            .expect("`MAX_BLOB_BYTES_BEFORE_INSTALL` fits in a `u32`");
        let _budget_permit =
            blob_byte_budget.acquire_many(bytes).await.expect("the budget is never closed");
        let blobs = self.fetcher.epoch_blobs(&epoch).await?;
        let in_epoch = |source| VerifyError::Epoch { epoch: epoch.end_height(), source };
        let rebuild_permit =
            rebuild_permits.acquire().await.expect("the rebuild permits are never closed");
        let rebuild = {
            let manifest = Arc::clone(manifest);
            BlockingTask::spawn(move || {
                let epoch = manifest.epochs().nth(at).expect("`run` takes `at` from the epochs");
                let empty = Frontiers::default();
                let start = epoch.previous().map_or(&empty, |previous| &previous.frontiers);
                (EpochProof::rebuild(&epoch, &blobs, start), blobs)
            })
        };
        let (proof, blobs) = rebuild.join().await?;
        drop(rebuild_permit);
        let proof = proof.map_err(|reject| in_epoch(reject.into()))?;
        let Some(check) = &self.node else {
            info!(end_height = epoch.end_height(), "verified epoch");
            return Ok(0);
        };
        let attest = proof.attest(&epoch, &*check.node.attestor);
        let sample = check.sample.sample(&epoch, &blobs, &*check.node.sampler);
        let (attested, sampled) = tokio::join!(attest, sample);
        attested.map_err(in_epoch)?;
        let sampled = sampled.map_err(in_epoch)?.len();
        info!(end_height = epoch.end_height(), sampled, "verified and attested epoch");
        Ok(sampled)
    }
}

#[cfg(test)]
mod tests {
    use ed25519_zebra::SigningKey;

    use super::*;
    use crate::{
        fetch::HttpSource,
        test_utils::{FileServer, SEED, Snapshot, SourceIndex},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn a_snapshot_verifies_without_a_node_and_a_changed_range_does_not() {
        let source = SourceIndex::new(25).await;
        let mut snapshot = Snapshot::export(&source);
        let server = FileServer::start(snapshot.dir.path()).await;
        let key = SigningKey::from_bytes(&SEED).verification_key();
        let verifier = || {
            let source = HttpSource::new(server.base.clone(), key).unwrap();
            SnapshotVerifier::new(Box::new(source), None)
        };

        let report = verifier().run().await.unwrap();
        let blob_bytes = source.entries().iter().map(|entry| entry.blob_len).sum();
        let expected = VerifyReport {
            sealed_through: 24_999,
            epochs: 3,
            ranges: 25,
            blob_bytes,
            sampled_heights: 0,
        };
        assert_eq!(report, expected);

        // A blob with the digest of its entry, but with the records of another range.
        let other = std::fs::read(snapshot.range_path(&source.entries()[3])).unwrap();
        snapshot.replace_blob(12_000, &other);
        let error = verifier().run().await.unwrap_err();
        assert!(matches!(error, VerifyError::Epoch { epoch: 19_999, .. }), "{error:?}");
    }
}
