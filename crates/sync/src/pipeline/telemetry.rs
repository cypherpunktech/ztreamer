//! Cached handles and scope guards. Worker timings are aggregated per segment.

use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram};

use super::{OrderedBuilder, WorkerStats};

pub(crate) struct PipelineMetrics {
    pub target: Gauge,
    active_workers: Gauge,
    pending_bytes: Gauge,
    ready_bytes: Gauge,
    first_missing: Gauge,
    blocks: Counter,
    bytes: Counter,
    transactions: Counter,
    committed_blocks: Counter,
    stages: [Histogram; 5],
    segment: [Histogram; 2],
    commit: [Histogram; 2],
    pass: [Histogram; 4],
    pub receive_wait: Histogram,
    pub batch_wait: Histogram,
    pub encode: Histogram,
}

impl PipelineMetrics {
    pub(crate) fn new() -> Self {
        Self {
            target: metrics::gauge!("ztreamer.pipeline.target.height"),
            active_workers: metrics::gauge!("ztreamer.pipeline.workers.active"),
            pending_bytes: metrics::gauge!("ztreamer.pipeline.pending.bytes"),
            ready_bytes: metrics::gauge!("ztreamer.pipeline.ready.bytes"),
            first_missing: metrics::gauge!("ztreamer.pipeline.first_missing.height"),
            blocks: metrics::counter!("ztreamer.pipeline.source.blocks"),
            bytes: metrics::counter!("ztreamer.pipeline.source.bytes"),
            transactions: metrics::counter!("ztreamer.pipeline.source.transactions"),
            committed_blocks: metrics::counter!("ztreamer.pipeline.committed.blocks"),
            stages: ["header_read", "transaction_read", "txid_read", "parse", "worker_send_wait"]
                .map(|stage| metrics::histogram!("ztreamer.pipeline.stage.seconds", "stage" => stage)),
            segment: ["success", "error"].map(|outcome| {
                metrics::histogram!("ztreamer.pipeline.segment.seconds", "outcome" => outcome)
            }),
            commit: ["success", "error"].map(|outcome| {
                metrics::histogram!("ztreamer.pipeline.commit.seconds", "outcome" => outcome)
            }),
            pass: ["success", "error", "panic", "stopped"].map(|outcome| {
                metrics::histogram!("ztreamer.pipeline.pass.seconds", "outcome" => outcome)
            }),
            receive_wait: metrics::histogram!("ztreamer.pipeline.receive_wait.seconds"),
            batch_wait: metrics::histogram!("ztreamer.pipeline.batch_send_wait.seconds"),
            encode: metrics::histogram!("ztreamer.pipeline.encode.seconds"),
        }
    }

    pub(crate) fn pass(&self) -> PassMetrics<'_> {
        self.first_missing.set(f64::NAN);
        self.target.set(f64::NAN);
        PassMetrics { metrics: self, start: Instant::now(), outcome: 2 }
    }

    pub(crate) fn worker(&self) -> ActiveWorker<'_> {
        self.active_workers.increment(1.0);
        ActiveWorker(self)
    }

    pub(crate) fn buffer(&self, builder: &OrderedBuilder) {
        self.pending_bytes.set(builder.pending_bytes() as f64);
        self.ready_bytes.set(builder.ready_bytes() as f64);
        self.first_missing.set(builder.first_missing_height() as f64);
    }

    pub(crate) fn commit(&self, elapsed: Duration, success: bool, blocks: u64) {
        self.commit[usize::from(!success)].record(elapsed.as_secs_f64());
        if success {
            self.committed_blocks.increment(blocks);
        }
    }
}

pub(crate) struct PassMetrics<'a> {
    metrics: &'a PipelineMetrics,
    start: Instant,
    outcome: usize,
}

impl PassMetrics<'_> {
    pub(crate) fn finish(&mut self, success: bool) {
        self.outcome = usize::from(!success);
    }

    /// Records the pass as stopped early by its stop flag, not as complete.
    pub(crate) const fn stopped(&mut self) {
        self.outcome = 3;
    }
}

impl Drop for PassMetrics<'_> {
    fn drop(&mut self) {
        self.metrics.pass[self.outcome].record(self.start.elapsed().as_secs_f64());
        self.metrics.pending_bytes.set(0.0);
        self.metrics.ready_bytes.set(0.0);
        self.metrics.first_missing.set(f64::NAN);
    }
}

pub(crate) struct ActiveWorker<'a>(&'a PipelineMetrics);

impl Drop for ActiveWorker<'_> {
    fn drop(&mut self) {
        self.0.active_workers.decrement(1.0);
    }
}

pub(crate) struct SegmentMetrics<'a> {
    metrics: &'a PipelineMetrics,
    start: Instant,
    pub stats: WorkerStats,
    pub success: bool,
    /// The segment stopped because another thread failed or the pass stopped. It records no
    /// outcome.
    pub cancelled: bool,
}

impl<'a> SegmentMetrics<'a> {
    pub(crate) fn new(metrics: &'a PipelineMetrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            stats: WorkerStats::default(),
            success: false,
            cancelled: false,
        }
    }
}

impl Drop for SegmentMetrics<'_> {
    fn drop(&mut self) {
        let stats = &self.stats;
        self.metrics.blocks.increment(stats.blocks);
        self.metrics.bytes.increment(stats.bytes);
        self.metrics.transactions.increment(stats.transactions);
        for (handle, elapsed) in self.metrics.stages.iter().zip([
            stats.header_read,
            stats.transaction_read,
            stats.txid_read,
            stats.parse,
            stats.send_wait,
        ]) {
            handle.record(elapsed.as_secs_f64());
        }
        if !self.cancelled {
            self.metrics.segment[usize::from(!self.success)]
                .record(self.start.elapsed().as_secs_f64());
        }
    }
}
