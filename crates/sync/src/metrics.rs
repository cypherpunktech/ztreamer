//! The metrics of the historical pass and the head follower.
//!
//! Each metric name and its help text is a constant in this file, and [`DESCRIBE`] gives the help
//! text to the recorder once. Other modules record only through the types and functions of this
//! file. A type of handles takes its handles once, on the thread that makes it, so that a test can
//! catch the samples of the pipeline workers with a local recorder.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use metrics::{
    Counter, Gauge, Histogram, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
};
use zakura_chain::block;
use ztreamer_chain::types::RawIndexBlock;
use ztreamer_index::index::BlockId;

use crate::{
    head::{CanonicalBlockSource, HeadError, ServingSnapshot},
    pipeline::WorkerStats,
};

const PIPELINE_TARGET_HEIGHT: &str = "ztreamer.pipeline.target.height";
const PIPELINE_TARGET_HEIGHT_HELP: &str =
    "The source tip that the current historical pass indexes to. NaN between passes.";

const PIPELINE_SOURCE_BLOCKS: &str = "ztreamer.pipeline.source.blocks";
const PIPELINE_SOURCE_BLOCKS_HELP: &str =
    "Blocks that the historical workers read from the source.";

const PIPELINE_SOURCE_BYTES: &str = "ztreamer.pipeline.source.bytes";
const PIPELINE_SOURCE_BYTES_HELP: &str = "Bytes that the historical workers read from the source.";

const PIPELINE_COMMITTED_BLOCKS: &str = "ztreamer.pipeline.committed.blocks";
const PIPELINE_COMMITTED_BLOCKS_HELP: &str = "Blocks that the historical pass wrote to the index.";

const PIPELINE_STAGE_SECONDS: &str = "ztreamer.pipeline.stage.seconds";
const PIPELINE_STAGE_SECONDS_HELP: &str = "Worker time per segment in each stage: header, transaction and txid reads, parse, and the wait for read-ahead budget.";

const PIPELINE_SEGMENT_SECONDS: &str = "ztreamer.pipeline.segment.seconds";
const PIPELINE_SEGMENT_SECONDS_HELP: &str = "Time to read and parse one segment, by outcome. A segment that another failure stopped has no sample.";

const PIPELINE_COMMIT_SECONDS: &str = "ztreamer.pipeline.commit.seconds";
const PIPELINE_COMMIT_SECONDS_HELP: &str = "Time to write one batch to the index, by outcome.";

const PIPELINE_ENCODE_SECONDS: &str = "ztreamer.pipeline.encode.seconds";
const PIPELINE_ENCODE_SECONDS_HELP: &str = "Time to encode the records of one batch.";

const PIPELINE_BATCH_SEND_WAIT_SECONDS: &str = "ztreamer.pipeline.batch_send_wait.seconds";
const PIPELINE_BATCH_SEND_WAIT_SECONDS_HELP: &str =
    "Time that the coordinator waits to give one batch to the writer.";

const PIPELINE_PASS_SECONDS: &str = "ztreamer.pipeline.pass.seconds";
const PIPELINE_PASS_SECONDS_HELP: &str =
    "Time of one historical pass, by outcome: success, error, panic or stopped.";

const HEAD_VISIBLE_HEIGHT: &str = "ztreamer.head.visible.height";
const HEAD_VISIBLE_HEIGHT_HELP: &str =
    "The highest block that the service serves. NaN on an empty index.";

const HEAD_READY: &str = "ztreamer.head.ready";
const HEAD_READY_HELP: &str =
    "1 when the service serves chain data, 0 while deep reorg recovery runs.";

const HEAD_FRESH: &str = "ztreamer.head.fresh";
const HEAD_FRESH_HELP: &str =
    "1 when the head source answered within the freshness timeout, else 0.";

const HEAD_SOURCE_HEIGHT: &str = "ztreamer.head.source.height";
const HEAD_SOURCE_HEIGHT_HELP: &str =
    "The tip that the head source gave last. NaN when it has no tip.";

const HEAD_PUBLICATION_SECONDS: &str = "ztreamer.head.publication.seconds";
const HEAD_PUBLICATION_SECONDS_HELP: &str =
    "Time from the first read of a new source tip to its publication to the service.";

const HEAD_RECONCILE_SECONDS: &str = "ztreamer.head.reconcile.seconds";
const HEAD_RECONCILE_SECONDS_HELP: &str = "Time of one head reconciliation, by outcome.";

const STAGES: [&str; 5] =
    ["header_read", "transaction_read", "txid_read", "parse", "worker_send_wait"];
const OUTCOMES: [&str; 2] = ["success", "error"];
const PASS_OUTCOMES: [&str; 4] = ["success", "error", "panic", "stopped"];

/// Gives the help text of each metric to the recorder. Each constructor of handles forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_gauge!(PIPELINE_TARGET_HEIGHT, PIPELINE_TARGET_HEIGHT_HELP);
    describe_counter!(PIPELINE_SOURCE_BLOCKS, PIPELINE_SOURCE_BLOCKS_HELP);
    describe_counter!(PIPELINE_SOURCE_BYTES, PIPELINE_SOURCE_BYTES_HELP);
    describe_counter!(PIPELINE_COMMITTED_BLOCKS, PIPELINE_COMMITTED_BLOCKS_HELP);
    describe_histogram!(PIPELINE_STAGE_SECONDS, PIPELINE_STAGE_SECONDS_HELP);
    describe_histogram!(PIPELINE_SEGMENT_SECONDS, PIPELINE_SEGMENT_SECONDS_HELP);
    describe_histogram!(PIPELINE_COMMIT_SECONDS, PIPELINE_COMMIT_SECONDS_HELP);
    describe_histogram!(PIPELINE_ENCODE_SECONDS, PIPELINE_ENCODE_SECONDS_HELP);
    describe_histogram!(PIPELINE_BATCH_SEND_WAIT_SECONDS, PIPELINE_BATCH_SEND_WAIT_SECONDS_HELP);
    describe_histogram!(PIPELINE_PASS_SECONDS, PIPELINE_PASS_SECONDS_HELP);
    describe_gauge!(HEAD_VISIBLE_HEIGHT, HEAD_VISIBLE_HEIGHT_HELP);
    describe_gauge!(HEAD_READY, HEAD_READY_HELP);
    describe_gauge!(HEAD_FRESH, HEAD_FRESH_HELP);
    describe_gauge!(HEAD_SOURCE_HEIGHT, HEAD_SOURCE_HEIGHT_HELP);
    describe_histogram!(HEAD_PUBLICATION_SECONDS, HEAD_PUBLICATION_SECONDS_HELP);
    describe_histogram!(HEAD_RECONCILE_SECONDS, HEAD_RECONCILE_SECONDS_HELP);
});

/// The handles of the historical pass.
pub(crate) struct PipelineMetrics {
    target: Gauge,
    blocks: Counter,
    bytes: Counter,
    committed_blocks: Counter,
    stages: [Histogram; 5],
    segment: [Histogram; 2],
    commit: [Histogram; 2],
    pass: [Histogram; 4],
    encode: Histogram,
    batch_send_wait: Histogram,
}

impl PipelineMetrics {
    pub(crate) fn new() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            target: gauge!(PIPELINE_TARGET_HEIGHT),
            blocks: counter!(PIPELINE_SOURCE_BLOCKS),
            bytes: counter!(PIPELINE_SOURCE_BYTES),
            committed_blocks: counter!(PIPELINE_COMMITTED_BLOCKS),
            stages: STAGES.map(|stage| histogram!(PIPELINE_STAGE_SECONDS, "stage" => stage)),
            segment: OUTCOMES
                .map(|outcome| histogram!(PIPELINE_SEGMENT_SECONDS, "outcome" => outcome)),
            commit: OUTCOMES
                .map(|outcome| histogram!(PIPELINE_COMMIT_SECONDS, "outcome" => outcome)),
            pass: PASS_OUTCOMES
                .map(|outcome| histogram!(PIPELINE_PASS_SECONDS, "outcome" => outcome)),
            encode: histogram!(PIPELINE_ENCODE_SECONDS),
            batch_send_wait: histogram!(PIPELINE_BATCH_SEND_WAIT_SECONDS),
        }
    }

    /// Starts the measurement of one pass. The target is unknown until the pass reads the tip.
    pub(crate) fn pass(&self) -> PassMetrics<'_> {
        self.target.set(f64::NAN);
        PassMetrics { metrics: self, start: Instant::now(), outcome: 2 }
    }

    pub(crate) fn target(&self, height: u32) {
        self.target.set(f64::from(height));
    }

    pub(crate) fn commit(&self, elapsed: Duration, success: bool, blocks: u64) {
        self.commit[usize::from(!success)].record(elapsed.as_secs_f64());
        if success {
            self.committed_blocks.increment(blocks);
        }
    }

    pub(crate) fn encode(&self, elapsed: Duration) {
        self.encode.record(elapsed.as_secs_f64());
    }

    pub(crate) fn batch_send_wait(&self, elapsed: Duration) {
        self.batch_send_wait.record(elapsed.as_secs_f64());
    }
}

/// Records the time of one pass when it drops, with the outcome that the pass set.
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
    }
}

/// Records the reads, the stage times and the outcome of one segment when it drops.
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

/// The handles of the head follower.
pub(crate) struct HeadMetrics {
    visible: Gauge,
    ready: Gauge,
    fresh: Gauge,
    source_tip: Gauge,
    publication: Histogram,
    reconcile: [Histogram; 2],
}

impl HeadMetrics {
    pub(crate) fn new() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            visible: gauge!(HEAD_VISIBLE_HEIGHT),
            ready: gauge!(HEAD_READY),
            fresh: gauge!(HEAD_FRESH),
            source_tip: gauge!(HEAD_SOURCE_HEIGHT),
            publication: histogram!(HEAD_PUBLICATION_SECONDS),
            reconcile: OUTCOMES
                .map(|outcome| histogram!(HEAD_RECONCILE_SECONDS, "outcome" => outcome)),
        }
    }

    pub(crate) fn snapshot(&self, snapshot: &ServingSnapshot) {
        self.visible.set(snapshot.visible_tip.map_or(f64::NAN, |tip| f64::from(tip.height)));
        self.ready.set(f64::from(snapshot.ready));
        self.fresh.set(f64::from(snapshot.tip_fresh));
    }

    pub(crate) fn published(&self, observed: Option<(BlockId, Instant)>, visible: Option<BlockId>) {
        if let Some((tip, at)) = observed
            && visible == Some(tip)
        {
            self.publication.record(at.elapsed().as_secs_f64());
        }
    }

    pub(crate) fn reconcile(&self, elapsed: Duration, success: bool) {
        self.reconcile[usize::from(!success)].record(elapsed.as_secs_f64());
    }
}

/// Observe existing tip calls without adding source reads to reconciliation.
pub(crate) struct ObservedSource<'a, S> {
    pub source: &'a mut S,
    pub previous: Option<BlockId>,
    pub observed: Option<(BlockId, Instant)>,
    pub metrics: &'a HeadMetrics,
}

#[tonic::async_trait]
impl<S: CanonicalBlockSource> CanonicalBlockSource for ObservedSource<'_, S> {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        let tip = self.source.tip().await?;
        self.metrics.source_tip.set(tip.map_or(f64::NAN, |tip| f64::from(tip.height)));
        if tip != self.previous {
            self.observed = tip.map(|tip| (tip, Instant::now()));
        }
        Ok(tip)
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        self.source.block(height).await
    }

    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        self.source.block_hash(height).await
    }
}
