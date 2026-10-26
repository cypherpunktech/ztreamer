//! The work of [`Importer::run`] on one epoch: fetch, rebuild, attest and sample, then install.

use std::{
    ops::Range,
    panic,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::sync::Semaphore;
use tracing::Span;
use ztreamer_indexer::index::{
    IndexState,
    range_meta::{Frontiers, RangeMeta, VerifyState},
};

use super::{
    ImportError, Importer, MAX_PENDING_BYTES,
    telemetry::{ImportMetrics, Stage},
};
use crate::{
    epoch::{Epoch, EpochProof, Epochs},
    fetch::RangeSource,
    manifest::Manifest,
};

/// The blobs of an epoch and their `range_meta` rows, in height order.
pub(crate) type Rows = Vec<(Bytes, RangeMeta)>;

/// One import: the importer, the manifest, and where the import starts. The futures of the
/// epochs share it.
pub(crate) struct Run<S> {
    importer: Importer<S>,
    manifest: Manifest,
    /// The position of the first entry of each epoch of the manifest.
    offsets: Vec<usize>,
    /// The position of the first epoch to import.
    first: usize,
    /// The start frontiers of the first epoch.
    start: Frontiers,
    /// The sealed tip of the index before the import.
    sealed: Option<u32>,
    /// One permit for each run of [`Epoch::verify`]: `import_workers` permits.
    workers: Semaphore,
    metrics: ImportMetrics,
}

impl<S: RangeSource + 'static> Run<S> {
    pub(crate) fn new(
        importer: Importer<S>,
        manifest: Manifest,
        first: usize,
        start: Frontiers,
        sealed: Option<u32>,
    ) -> Self {
        let offsets = manifest
            .epochs()
            .scan(0, |offset, epoch| {
                let first = *offset;
                *offset += epoch.entries().len();
                Some(first)
            })
            .collect();
        let workers = Semaphore::new(importer.config.import_workers.get());
        let metrics = ImportMetrics::new();
        Self { importer, manifest, offsets, first, start, sealed, workers, metrics }
    }

    /// The positions of the epochs to import: from the first epoch to the end of the manifest.
    pub(crate) const fn epochs_to_import(&self) -> Range<usize> {
        self.first..self.offsets.len()
    }

    /// The epochs in work at the same time: two for each verify worker. Thus the node calls of
    /// an epoch overlap the rebuild of the next epoch.
    pub(crate) const fn window(&self) -> usize {
        2 * self.importer.config.import_workers.get()
    }

    pub(crate) fn epoch(&self, at: usize) -> Epoch<'_> {
        Epochs::starting_at(self.manifest.entries(), self.offsets[at])
            .next()
            .expect("the import takes `at` from the epochs")
    }

    /// The span of the work on the epoch at `at`.
    pub(crate) fn span(&self, at: usize) -> Span {
        tracing::debug_span!("import_epoch", end_height = self.epoch(at).end_height())
    }

    /// The bytes of the blobs of the epoch at `at`, at most [`MAX_PENDING_BYTES`]. The epoch
    /// holds this part of the byte budget from its fetch to its install.
    pub(crate) fn budget_bytes(&self, at: usize) -> u32 {
        let bytes: u64 = self.epoch(at).entries().iter().map(|entry| entry.blob_len).sum();
        u32::try_from(bytes.min(MAX_PENDING_BYTES)).expect("`MAX_PENDING_BYTES` fits in a `u32`")
    }

    /// Returns `true` if the epoch at `at` was installed before the import, with
    /// [`VerifyState::ChainOk`] rows.
    pub(crate) fn is_installed(&self, at: usize) -> bool {
        self.sealed.is_some_and(|sealed| self.epoch(at).end_height() <= sealed)
    }

    /// Fetches, rebuilds, attests and samples the epoch at `at`, and gives its
    /// [`VerifyState::Verified`] rows.
    pub(crate) async fn check(self: Arc<Self>, at: usize) -> Result<Rows, ImportError> {
        let blobs = self.fetch(at).await?;
        let (blobs, proof) = self.rebuild(at, blobs).await?;
        self.attest(at, blobs, proof).await
    }

    /// The blobs of the epoch at `at`: from the index for an installed epoch, else from the
    /// source.
    async fn fetch(&self, at: usize) -> Result<Vec<Bytes>, ImportError> {
        let started = Instant::now();
        let entries = self.epoch(at).entries();
        let blobs = if self.is_installed(at) {
            let starts: Vec<_> = entries.iter().map(|entry| entry.start_height).collect();
            self.importer
                .blocking(move |index| {
                    let read =
                        |start| index.read_sealed_range_bytes(start).map(|blob| (start, blob));
                    starts.into_iter().map(read).collect::<Result<Vec<_>, _>>()
                })
                .await?
                .into_iter()
                .map(|(start, blob)| {
                    blob.map(Bytes::from).ok_or(ImportError::MissingBlob { start })
                })
                .collect::<Result<_, _>>()?
        } else {
            let mut blobs = Vec::with_capacity(entries.len());
            for entry in entries {
                blobs.push(self.importer.blob(entry).await?);
                self.metrics.add_fetched_bytes(entry.blob_len);
            }
            blobs
        };
        self.metrics.record(Stage::Fetch, started);
        Ok(blobs)
    }

    /// Runs [`Epoch::verify`] for the epoch at `at` on the blocking thread pool, with one of
    /// the `import_workers` permits.
    async fn rebuild(
        self: &Arc<Self>,
        at: usize,
        blobs: Vec<Bytes>,
    ) -> Result<(Vec<Bytes>, EpochProof), ImportError> {
        let _permit = {
            let _pending = self.metrics.pending_epoch();
            self.workers.acquire().await.expect("the workers are never closed")
        };
        let _busy = self.metrics.busy_worker();
        let (run, span) = (Arc::clone(self), Span::current());
        tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let started = Instant::now();
                let rebuilt = run.verify(at, blobs);
                run.metrics.record(Stage::Verify, started);
                rebuilt
            })
        })
        .await
        .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
    }

    /// Runs [`Epoch::verify`] for the epoch at `at`. Call it on the blocking thread pool.
    fn verify(
        &self,
        at: usize,
        blobs: Vec<Bytes>,
    ) -> Result<(Vec<Bytes>, EpochProof), ImportError> {
        let epoch = self.epoch(at);
        // `Epoch::verify` of the epoch before rebuilt the frontiers of the entry before.
        let start = epoch
            .previous()
            .filter(|_| at != self.first)
            .map_or(&self.start, |previous| &previous.frontiers);
        let proof = epoch
            .verify(&blobs, start)
            .map_err(|reject| ImportError::Rejected { epoch: epoch.end_height(), reject })?;
        Ok((blobs, proof))
    }

    /// Runs [`Epoch::attest`] and [`Epoch::sample`] for the epoch at `at` at the same time,
    /// and makes the [`VerifyState::Verified`] rows.
    async fn attest(
        &self,
        at: usize,
        blobs: Vec<Bytes>,
        proof: EpochProof,
    ) -> Result<Rows, ImportError> {
        let epoch = self.epoch(at);
        let node = &self.importer.node;
        let attest = async {
            let started = Instant::now();
            let attested = epoch.attest(&proof, &*node.attestor).await;
            self.metrics.record(Stage::Attest, started);
            attested
        };
        let sample = async {
            let started = Instant::now();
            let plan = self.importer.config.verify_sample;
            let sampled = epoch.sample(&blobs, &*node.sampler, plan).await;
            self.metrics.record(Stage::Sample, started);
            sampled
        };
        let (attested, sampled) = tokio::join!(attest, sample);
        let in_epoch = |error| ImportError::in_epoch(epoch.end_height(), error);
        let (attested, sampled) = (attested.map_err(in_epoch)?, sampled.map_err(in_epoch)?);

        let verified_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let rows =
            epoch.entries().iter().zip(blobs).zip(attested).map(|((entry, blob), attested)| {
                let heights = entry.start_height..=entry.end_height;
                let meta = RangeMeta {
                    entry: entry.clone(),
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
                (blob, meta)
            });
        Ok(rows.collect())
    }

    /// Installs the rows of the epoch at `at`, or marks the rows of an installed epoch
    /// verified. Returns the index state after the last range.
    pub(crate) async fn install(&self, at: usize, rows: Rows) -> Result<IndexState, ImportError> {
        let installed = self.is_installed(at);
        let state = self
            .importer
            .blocking(move |index| {
                rows.into_iter().try_fold(None, |_, (blob, mut meta)| {
                    let start = meta.entry.start_height;
                    let state = if installed {
                        // The row keeps `epoch_end` from the manifest that installed it. A
                        // manifest that ended inside this epoch marks its last range.
                        if let Some(row) = index.range_meta(start)? {
                            meta.entry.epoch_end = row.entry.epoch_end;
                        }
                        index.advance_verified_through(start, &meta)
                    } else {
                        index.install_sealed_range(start, &blob, &meta)
                    };
                    state.map(Some)
                })
            })
            .await?;
        Ok(state.expect("an epoch is never empty"))
    }
}
