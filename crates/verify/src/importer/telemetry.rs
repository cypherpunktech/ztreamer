//! The metrics of an import, below `ztreamer.replicate`: one histogram per stage of an epoch,
//! and the gauges of the verify workers.

use std::time::Instant;

use metrics::{Gauge, Histogram};

/// A stage of the work on one epoch, with a histogram of its duration.
#[derive(Clone, Copy)]
pub(crate) enum EpochStage {
    Fetch,
    Verify,
    Attest,
    Sample,
}

pub(crate) struct ImportMetrics {
    fetch: Histogram,
    verify: Histogram,
    attest: Histogram,
    sample: Histogram,
    epochs_pending: Gauge,
    workers_busy: Gauge,
}

impl ImportMetrics {
    pub(crate) fn new() -> Self {
        Self {
            fetch: metrics::histogram!("ztreamer.replicate.fetch_seconds"),
            verify: metrics::histogram!("ztreamer.replicate.verify_seconds"),
            attest: metrics::histogram!("ztreamer.replicate.attest_seconds"),
            sample: metrics::histogram!("ztreamer.replicate.sample_seconds"),
            epochs_pending: metrics::gauge!("ztreamer.replicate.epochs_pending"),
            workers_busy: metrics::gauge!("ztreamer.replicate.workers_busy"),
        }
    }

    /// Records the time since `started` in the histogram of `stage`.
    pub(crate) fn record_stage_seconds(&self, stage: EpochStage, started: Instant) {
        let histogram = match stage {
            EpochStage::Fetch => &self.fetch,
            EpochStage::Verify => &self.verify,
            EpochStage::Attest => &self.attest,
            EpochStage::Sample => &self.sample,
        };
        histogram.record(started.elapsed());
    }

    /// Counts an epoch in `epochs_pending` until the guard drops: the epoch waits for a verify
    /// worker.
    pub(crate) fn count_pending_epoch(&self) -> EpochsPendingGuard<'_> {
        self.epochs_pending.increment(1.0);
        EpochsPendingGuard(self)
    }

    /// Counts a worker in `workers_busy` until the guard drops: the worker runs
    /// `Epoch::verify`.
    pub(crate) fn count_busy_worker(&self) -> WorkersBusyGuard<'_> {
        self.workers_busy.increment(1.0);
        WorkersBusyGuard(self)
    }
}

pub(crate) struct EpochsPendingGuard<'a>(&'a ImportMetrics);

impl Drop for EpochsPendingGuard<'_> {
    fn drop(&mut self) {
        self.0.epochs_pending.decrement(1.0);
    }
}

pub(crate) struct WorkersBusyGuard<'a>(&'a ImportMetrics);

impl Drop for WorkersBusyGuard<'_> {
    fn drop(&mut self) {
        self.0.workers_busy.decrement(1.0);
    }
}
