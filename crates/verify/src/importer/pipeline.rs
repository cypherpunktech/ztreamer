//! The stages of [`Importer::run`] and the scheduler that connects them.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    panic,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use metrics::{Counter, Gauge, Histogram};
use tokio::{
    sync::watch,
    task::{JoinError, JoinSet},
};
use tracing::{Instrument as _, Span, info};
use ztreamer_indexer::index::{
    IndexState,
    range_meta::{Frontiers, RangeMeta, VerifyState},
};

use super::{ImportError, ImportReport, Importer, MAX_BUFFERED_RANGES, MAX_PENDING_BYTES};
use crate::{
    epoch::{Epoch, EpochProof, Epochs},
    fetch::RangeSource,
    manifest::Manifest,
};

/// The blobs of an epoch and their `range_meta` rows, in height order.
type Rows = Vec<(Bytes, RangeMeta)>;

/// What a task of a stage returns: the position of its epoch, and its result.
type Done<T> = (usize, Result<T, ImportError>);

/// The metrics of an import, below `ztreamer.replicate`.
struct ImportMetrics {
    fetch: Histogram,
    verify: Histogram,
    attest: Histogram,
    sample: Histogram,
    bytes_fetched: Counter,
    epochs_pending: Gauge,
    workers_busy: Gauge,
}

impl ImportMetrics {
    fn new() -> Self {
        Self {
            fetch: metrics::histogram!("ztreamer.replicate.fetch_seconds"),
            verify: metrics::histogram!("ztreamer.replicate.verify_seconds"),
            attest: metrics::histogram!("ztreamer.replicate.attest_seconds"),
            sample: metrics::histogram!("ztreamer.replicate.sample_seconds"),
            bytes_fetched: metrics::counter!("ztreamer.replicate.bytes_fetched_total", "source" => "0"),
            epochs_pending: metrics::gauge!("ztreamer.replicate.epochs_pending"),
            workers_busy: metrics::gauge!("ztreamer.replicate.workers_busy"),
        }
    }
}

/// One import: the importer, the manifest, and where the import starts. The tasks of the
/// stages share it.
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
        let metrics = ImportMetrics::new();
        Self { importer, manifest, offsets, first, start, sealed, metrics }
    }

    fn epoch(&self, at: usize) -> Epoch<'_> {
        Epochs::starting_at(self.manifest.entries(), self.offsets[at])
            .next()
            .expect("the scheduler takes `at` from the epochs")
    }

    /// The span of the work on the epoch at `at`.
    fn span(&self, at: usize) -> Span {
        tracing::debug_span!("import_epoch", end_height = self.epoch(at).end_height())
    }

    /// Returns `true` if the epoch at `at` was installed before the import, with
    /// [`VerifyState::ChainOk`] rows.
    fn is_installed(&self, at: usize) -> bool {
        self.sealed.is_some_and(|sealed| self.epoch(at).end_height() <= sealed)
    }

    /// The blobs of the epoch at `at`: from the index for an installed epoch, else from the
    /// source.
    async fn fetch(self: Arc<Self>, at: usize) -> Result<Vec<Bytes>, ImportError> {
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
                self.metrics.bytes_fetched.increment(entry.blob_len);
            }
            blobs
        };
        self.metrics.fetch.record(started.elapsed());
        Ok(blobs)
    }

    /// Runs [`Epoch::verify`] for the epoch at `at`. Call it on the blocking thread pool.
    fn rebuild(
        &self,
        at: usize,
        blobs: Vec<Bytes>,
    ) -> Result<(Vec<Bytes>, EpochProof), ImportError> {
        let started = Instant::now();
        let epoch = self.epoch(at);
        // `Epoch::verify` of the epoch before rebuilt the frontiers of the entry before.
        let start = epoch
            .previous()
            .filter(|_| at != self.first)
            .map_or(&self.start, |previous| &previous.frontiers);
        let proof = epoch
            .verify(&blobs, start)
            .map_err(|reject| ImportError::Rejected { epoch: epoch.end_height(), reject })?;
        self.metrics.verify.record(started.elapsed());
        Ok((blobs, proof))
    }

    /// Runs [`Epoch::attest`] and [`Epoch::sample`] for the epoch at `at` at the same time,
    /// and makes the [`VerifyState::Verified`] rows.
    async fn attest(
        self: Arc<Self>,
        at: usize,
        blobs: Vec<Bytes>,
        proof: EpochProof,
    ) -> Result<Rows, ImportError> {
        let epoch = self.epoch(at);
        let node = &self.importer.node;
        let attest = async {
            let started = Instant::now();
            let attested = epoch.attest(&proof, &*node.attestor).await;
            self.metrics.attest.record(started.elapsed());
            attested
        };
        let sample = async {
            let started = Instant::now();
            let plan = self.importer.config.verify_sample;
            let sampled = epoch.sample(&blobs, &*node.sampler, plan).await;
            self.metrics.sample.record(started.elapsed());
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
    async fn install(self: Arc<Self>, at: usize, rows: Rows) -> Result<IndexState, ImportError> {
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

/// The size of one epoch, for the limits of the scheduler.
struct EpochSize {
    ranges: usize,
    commitments: u64,
    bytes: u64,
}

/// The scheduler: it moves each epoch from the fetch stage to the verify stage to the writer.
pub(crate) struct Pipeline<S> {
    run: Arc<Run<S>>,
    sizes: Vec<EpochSize>,
    workers: usize,
    /// The position of the next epoch to fetch.
    next_fetch: usize,
    /// The position of the next epoch to install.
    next_install: usize,
    /// The position of the first epoch not to install: the end of the manifest, the failed
    /// epoch, or the epoch after the shutdown signal.
    end: usize,
    failure: Option<ImportError>,
    fetches: JoinSet<Done<Vec<Bytes>>>,
    /// The fetched epochs that wait for a verify worker.
    fetched: BTreeMap<usize, Vec<Bytes>>,
    /// The bytes of the epochs in `fetches` and `fetched`.
    pending_bytes: u64,
    rebuilds: JoinSet<Done<(Vec<Bytes>, EpochProof)>>,
    attests: JoinSet<Done<Rows>>,
    /// The verified epochs that wait for the writer.
    verified: BTreeMap<usize, Rows>,
    /// The epochs in `rebuilds`, `attests` and `verified`.
    ahead: BTreeSet<usize>,
    installs: JoinSet<Done<IndexState>>,
    report: ImportReport,
}

impl<S: RangeSource + 'static> Pipeline<S> {
    pub(crate) fn new(run: Run<S>, report: ImportReport) -> Self {
        let sizes: Vec<_> = run
            .manifest
            .epochs()
            .map(|epoch| EpochSize {
                ranges: epoch.entries().len(),
                commitments: epoch.commitments(),
                bytes: epoch.entries().iter().map(|entry| entry.blob_len).sum(),
            })
            .collect();
        let first = run.first;
        Self {
            workers: run.importer.config.import_workers.get(),
            run: Arc::new(run),
            end: sizes.len(),
            sizes,
            next_fetch: first,
            next_install: first,
            failure: None,
            fetches: JoinSet::new(),
            fetched: BTreeMap::new(),
            pending_bytes: 0,
            rebuilds: JoinSet::new(),
            attests: JoinSet::new(),
            verified: BTreeMap::new(),
            ahead: BTreeSet::new(),
            installs: JoinSet::new(),
            report,
        }
    }

    /// Runs the stages until the writer installs the epoch before [`Pipeline::end`].
    pub(crate) async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ImportReport, ImportError> {
        let stop = shutdown.wait_for(|&stop| stop);
        tokio::pin!(stop);
        let mut listening = true;
        loop {
            self.start_work();
            if self.next_install >= self.end && self.installs.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                stopped = &mut stop, if listening => {
                    listening = false;
                    if stopped.is_ok() {
                        self.stop();
                    }
                }
                Some(done) = self.installs.join_next() => {
                    if let Some((at, state)) = Self::finished(done) {
                        self.installed(at, state);
                    }
                }
                Some(done) = self.attests.join_next() => {
                    if let Some((at, rows)) = Self::finished(done) {
                        self.attested(at, rows);
                    }
                }
                Some(done) = self.rebuilds.join_next() => {
                    if let Some((at, rebuilt)) = Self::finished(done) {
                        self.rebuilt(at, rebuilt);
                    }
                }
                Some(done) = self.fetches.join_next() => {
                    if let Some((at, blobs)) = Self::finished(done) {
                        self.fetched(at, blobs);
                    }
                }
            }
        }
        self.failure.map_or(Ok(self.report), Err)
    }

    /// Starts the writer, the verify workers and the fetches that the limits allow.
    fn start_work(&mut self) {
        if self.installs.is_empty()
            && self.next_install < self.end
            && let Some(rows) = self.verified.remove(&self.next_install)
        {
            let (at, run) = (self.next_install, Arc::clone(&self.run));
            self.ahead.remove(&at);
            self.next_install += 1;
            let span = run.span(at);
            self.installs.spawn(async move { (at, run.install(at, rows).await) }.instrument(span));
        }
        while self.rebuilds.len() < self.workers {
            let Some(at) = self.next_rebuild() else {
                break;
            };
            let blobs = self.fetched.remove(&at).expect("`next_rebuild` takes `at` from `fetched`");
            self.pending_bytes -= self.sizes[at].bytes;
            self.ahead.insert(at);
            let (run, span) = (Arc::clone(&self.run), self.run.span(at));
            self.rebuilds.spawn_blocking(move || span.in_scope(|| (at, run.rebuild(at, blobs))));
        }
        while self.next_fetch < self.end
            && self.fetches.len() < self.workers
            && self.fetches.len() + self.fetched.len() < 2 * self.workers
        {
            let (at, bytes) = (self.next_fetch, self.sizes[self.next_fetch].bytes);
            if self.pending_bytes > 0 && self.pending_bytes + bytes > MAX_PENDING_BYTES {
                break;
            }
            self.next_fetch += 1;
            self.pending_bytes += bytes;
            let (run, span) = (Arc::clone(&self.run), self.run.span(at));
            self.fetches.spawn(async move { (at, run.fetch(at).await) }.instrument(span));
        }
        let metrics = &self.run.metrics;
        metrics.epochs_pending.set(self.fetched.len() as f64);
        metrics.workers_busy.set(self.rebuilds.len() as f64);
    }

    /// The fetched epoch to verify next: the epoch with the most commitments, if its ranges and
    /// the ranges of the verified epochs that wait for the writer fit in
    /// [`MAX_BUFFERED_RANGES`]. Else the lowest fetched epoch, if it is below each epoch in
    /// verification or in the buffer: the writer needs it first.
    fn next_rebuild(&self) -> Option<usize> {
        let buffered: usize = self.verified.keys().map(|&at| self.sizes[at].ranges).sum();
        self.fetched
            .keys()
            .copied()
            .filter(|&at| buffered + self.sizes[at].ranges <= MAX_BUFFERED_RANGES)
            .max_by_key(|&at| (self.sizes[at].commitments, Reverse(at)))
            .or_else(|| {
                let lowest = *self.fetched.keys().next()?;
                self.ahead.first().is_none_or(|&ahead| lowest < ahead).then_some(lowest)
            })
    }

    fn fetched(&mut self, at: usize, blobs: Result<Vec<Bytes>, ImportError>) {
        match blobs {
            Ok(blobs) if at < self.end => {
                self.fetched.insert(at, blobs);
            }
            Ok(_) => self.pending_bytes -= self.sizes[at].bytes,
            Err(error) => {
                self.pending_bytes -= self.sizes[at].bytes;
                self.fail(at, error);
            }
        }
    }

    fn rebuilt(&mut self, at: usize, rebuilt: Result<(Vec<Bytes>, EpochProof), ImportError>) {
        match rebuilt {
            Ok((blobs, proof)) if at < self.end => {
                let (run, span) = (Arc::clone(&self.run), self.run.span(at));
                let attest = async move { (at, run.attest(at, blobs, proof).await) };
                self.attests.spawn(attest.instrument(span));
            }
            Ok(_) => {}
            Err(error) => self.fail(at, error),
        }
    }

    fn attested(&mut self, at: usize, rows: Result<Rows, ImportError>) {
        match rows {
            Ok(rows) if at < self.end => {
                self.verified.insert(at, rows);
            }
            Ok(_) => {}
            Err(error) => self.fail(at, error),
        }
    }

    fn installed(&mut self, at: usize, state: Result<IndexState, ImportError>) {
        let state = match state {
            Ok(state) => state,
            Err(error) => return self.fail(at, error),
        };
        let epoch = self.run.epoch(at);
        if !self.run.is_installed(at) {
            self.report.epochs += 1;
            self.report.ranges += epoch.entries().len();
        }
        self.report.sealed_through = state.sealed_through();
        self.report.verified_through = state.verified_through();
        info!(end_height = epoch.end_height(), ranges = self.report.ranges, "imported epoch");
    }

    /// Stops the import at the failed epoch at `at`, if no epoch below it failed. The writer
    /// still installs the epochs below it.
    fn fail(&mut self, at: usize, error: ImportError) {
        self.ahead.remove(&at);
        if at >= self.end {
            return;
        }
        self.end = at;
        self.failure = Some(error);
        let dropped = self.fetched.split_off(&at);
        self.pending_bytes -= dropped.keys().map(|&at| self.sizes[at].bytes).sum::<u64>();
        self.verified.split_off(&at);
        self.ahead.split_off(&at);
    }

    /// Stops the import after the epoch that the writer installs, and drops the other epochs.
    fn stop(&mut self) {
        if self.next_install < self.end {
            self.report.stopped = true;
            self.end = self.next_install;
        }
        self.fetches.abort_all();
        self.rebuilds.abort_all();
        self.attests.abort_all();
        self.fetched.clear();
        self.verified.clear();
        self.ahead.clear();
    }

    /// The output of a task, or `None` for a task that [`Pipeline::stop`] cancelled. Gives the
    /// panic of a task to the caller.
    fn finished<T>(done: Result<T, JoinError>) -> Option<T> {
        done.map_err(|error| error.try_into_panic().map(panic::resume_unwind)).ok()
    }
}
