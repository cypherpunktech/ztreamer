//! The work of [`Importer::run`] on one epoch: fetch, verify, attest and sample, then install.

use std::{
    ops::Range,
    panic,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use tokio::sync::Semaphore;
use tracing::Span;
use ztreamer_indexer::index::{
    CheckedRange, IndexState,
    range_meta::{Frontiers, ManifestEntry, RangeMeta, VerifyState},
};

use super::{ImportError, Importer, MAX_BLOB_BYTES_BEFORE_INSTALL};
use crate::{
    epoch::{Epoch, EpochProof, Epochs},
    manifest::Manifest,
};

/// The checked ranges of an epoch and their `range_meta` rows, in height order.
pub(crate) type VerifiedRows = Vec<(CheckedRange, RangeMeta)>;

/// The import of the epochs of one manifest: the importer, the manifest, and the first epoch to
/// import. The futures of all epochs use one `EpochImport`.
pub(crate) struct EpochImport {
    importer: Importer,
    manifest: Manifest,
    /// The position of the first entry of each epoch of the manifest.
    epoch_offsets: Vec<usize>,
    /// The position of the first epoch to import.
    first_epoch: usize,
    /// The start frontiers of the first epoch.
    first_start_frontiers: Frontiers,
    /// One permit for each run of [`Epoch::verify`]: `import_workers` permits.
    verify_permits: Semaphore,
}

impl EpochImport {
    pub(crate) fn new(
        importer: Importer,
        manifest: Manifest,
        first_epoch: usize,
        first_start_frontiers: Frontiers,
    ) -> Self {
        let epoch_offsets = manifest
            .epochs()
            .scan(0, |next_offset, epoch| {
                let epoch_offset = *next_offset;
                *next_offset += epoch.entries().len();
                Some(epoch_offset)
            })
            .collect();
        let verify_permits = Semaphore::new(importer.config.import_workers.get());
        Self {
            importer,
            manifest,
            epoch_offsets,
            first_epoch,
            first_start_frontiers,
            verify_permits,
        }
    }

    /// The positions of the epochs to import: from the first epoch to the end of the manifest.
    pub(crate) const fn epochs_to_import(&self) -> Range<usize> {
        self.first_epoch..self.epoch_offsets.len()
    }

    /// The epochs in work at the same time: two for each verify worker. Thus the node calls of
    /// an epoch overlap the verification of the next epoch.
    pub(crate) const fn epochs_in_work(&self) -> usize {
        2 * self.importer.config.import_workers.get()
    }

    pub(crate) fn manifest_epoch(&self, at: usize) -> Epoch<'_> {
        Epochs::starting_at(self.manifest.entries(), self.epoch_offsets[at])
            .next()
            .expect("the import takes `at` from the epochs")
    }

    /// The span of the work on the epoch at `at`.
    pub(crate) fn epoch_span(&self, at: usize) -> Span {
        tracing::debug_span!("import_epoch", end_height = self.manifest_epoch(at).end_height())
    }

    /// The bytes of the blobs of the epoch at `at`, at most [`MAX_BLOB_BYTES_BEFORE_INSTALL`].
    /// The epoch holds this part of the byte budget from its fetch to its install.
    pub(crate) fn blob_bytes_in_budget(&self, at: usize) -> u32 {
        let bytes: u64 = self.manifest_epoch(at).entries().iter().map(|entry| entry.blob_len).sum();
        u32::try_from(bytes.min(MAX_BLOB_BYTES_BEFORE_INSTALL))
            .expect("`MAX_BLOB_BYTES_BEFORE_INSTALL` fits in a `u32`")
    }

    /// Fetches, verifies, attests and samples the epoch at `at`, and gives its
    /// [`VerifyState::Verified`] rows.
    pub(crate) async fn fetch_and_verify(
        self: Arc<Self>,
        at: usize,
    ) -> Result<VerifiedRows, ImportError> {
        let blobs = self.fetch_blobs(at).await?;
        let (ranges, proof) = self.verify_on_blocking_pool(at, blobs).await?;
        self.attest_and_sample(at, ranges, proof).await
    }

    /// The blobs of the epoch at `at`, from the source.
    async fn fetch_blobs(&self, at: usize) -> Result<Vec<Bytes>, ImportError> {
        let entries = self.manifest_epoch(at).entries();
        let blobs = stream::iter(entries).then(|entry| self.importer.fetcher.blob(entry));
        Ok(blobs.try_collect().await?)
    }

    /// Runs [`Epoch::verify`] for the epoch at `at` on the blocking thread pool, with one of
    /// the `import_workers` permits.
    async fn verify_on_blocking_pool(
        self: &Arc<Self>,
        at: usize,
        blobs: Vec<Bytes>,
    ) -> Result<(Vec<CheckedRange>, EpochProof), ImportError> {
        let _permit =
            self.verify_permits.acquire().await.expect("the verify permits are never closed");
        let (run, span) = (Arc::clone(self), Span::current());
        tokio::task::spawn_blocking(move || span.in_scope(|| run.verify_blobs(at, blobs)))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
    }

    /// Runs [`Epoch::verify`] for the epoch at `at`, and gives the checked range of each blob.
    /// Call it on the blocking thread pool.
    fn verify_blobs(
        &self,
        at: usize,
        blobs: Vec<Bytes>,
    ) -> Result<(Vec<CheckedRange>, EpochProof), ImportError> {
        let epoch = self.manifest_epoch(at);
        // `Epoch::verify` of the epoch before rebuilt the frontiers of the entry before.
        let start = epoch
            .previous()
            .filter(|_| at != self.first_epoch)
            .map_or(&self.first_start_frontiers, |previous| &previous.frontiers);
        let (proof, ranges) = epoch
            .verify_ranges(&blobs, start)
            .map_err(|reject| ImportError::Rejected { epoch: epoch.end_height(), reject })?;
        Ok((ranges, proof))
    }

    /// Runs [`Epoch::attest`] and [`Epoch::sample`] for the epoch at `at` at the same time,
    /// and makes the [`VerifyState::Verified`] rows.
    async fn attest_and_sample(
        &self,
        at: usize,
        ranges: Vec<CheckedRange>,
        proof: EpochProof,
    ) -> Result<VerifiedRows, ImportError> {
        let epoch = self.manifest_epoch(at);
        let node = &self.importer.node;
        let attest = epoch.attest(&proof, &*node.attestor);
        let sample = epoch.sample(&ranges, &*node.sampler, self.importer.config.verify_sample);
        let (attested, sampled) = tokio::join!(attest, sample);
        let in_epoch = |error| ImportError::in_epoch(epoch.end_height(), error);
        let (attested, sampled) = (attested.map_err(in_epoch)?, sampled.map_err(in_epoch)?);

        let verified_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        // The manifest closes its last epoch at its last entry, also where the epoch rule does
        // not. The row keeps the flag of the rule, as the row of the source does. Then the ranges
        // that the index seals later continue the epoch of the rule.
        let closed_by_rule = epoch.closed_by_rule();
        let rows =
            epoch.entries().iter().zip(ranges).zip(attested).map(|((entry, range), attested)| {
                let heights = entry.start_height..=entry.end_height;
                let meta = RangeMeta {
                    entry: ManifestEntry {
                        epoch_end: entry.epoch_end && closed_by_rule,
                        ..entry.clone()
                    },
                    state: VerifyState::Verified,
                    verified_by: Some(node.id.clone()),
                    verified_at: Some(verified_at.as_secs()),
                    attested: Some(attested),
                    sampled_heights: sampled
                        .iter()
                        .copied()
                        .filter(|h| heights.contains(h))
                        .collect(),
                };
                (range, meta)
            });
        Ok(rows.collect())
    }

    /// Installs each checked range of an epoch with its row. The install does only the checks
    /// that need the index: [`Epoch::verify`] checked the blobs. Returns the index state after
    /// the last range.
    pub(crate) async fn install_rows(&self, rows: VerifiedRows) -> Result<IndexState, ImportError> {
        let state = self
            .importer
            .blocking(move |index| {
                rows.iter().try_fold(None, |_, (range, meta)| {
                    index.install_checked_range(range, meta).map(Some)
                })
            })
            .await?;
        Ok(state.expect("an epoch is never empty"))
    }
}
