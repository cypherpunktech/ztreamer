//! The metrics of the historical pass. `scripts/benchmark-stages.py` reads each of them.
//!
//! Each metric name and its help text is a constant in this file, and [`DESCRIBE`] gives the help
//! text to the recorder once. Other modules record only through the types of this file.
//!
//! A type of handles takes its handles once, on the thread that makes it. Thus a test with a
//! local recorder catches the samples of the pipeline workers.

use std::{sync::LazyLock, time::Duration};

use metrics::{Counter, Histogram, counter, describe_counter, describe_histogram, histogram};

use crate::pipeline::WorkerStats;

const PIPELINE_SOURCE_BYTES: &str = "ztreamer.pipeline.source.bytes";
const PIPELINE_SOURCE_BYTES_HELP: &str = "Bytes that the historical workers read from the source.";

const PIPELINE_STAGE_SECONDS: &str = "ztreamer.pipeline.stage.seconds";
const PIPELINE_STAGE_SECONDS_HELP: &str = "Worker time per segment in each stage: header, transaction and txid reads, parse, and the wait for read-ahead budget.";

const PIPELINE_COMMIT_SECONDS: &str = "ztreamer.pipeline.commit.seconds";
const PIPELINE_COMMIT_SECONDS_HELP: &str = "Time to write one batch to the index, by outcome.";

const PIPELINE_ENCODE_SECONDS: &str = "ztreamer.pipeline.encode.seconds";
const PIPELINE_ENCODE_SECONDS_HELP: &str = "Time to encode the records of one batch.";

const PIPELINE_BATCH_SEND_WAIT_SECONDS: &str = "ztreamer.pipeline.batch_send_wait.seconds";
const PIPELINE_BATCH_SEND_WAIT_SECONDS_HELP: &str =
    "Time that the coordinator waits to give one batch to the writer.";

const STAGES: [&str; 5] =
    ["header_read", "transaction_read", "txid_read", "parse", "worker_send_wait"];
const OUTCOMES: [&str; 2] = ["success", "error"];

/// Gives the help text of each metric to the recorder. Each constructor of handles forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_counter!(PIPELINE_SOURCE_BYTES, PIPELINE_SOURCE_BYTES_HELP);
    describe_histogram!(PIPELINE_STAGE_SECONDS, PIPELINE_STAGE_SECONDS_HELP);
    describe_histogram!(PIPELINE_COMMIT_SECONDS, PIPELINE_COMMIT_SECONDS_HELP);
    describe_histogram!(PIPELINE_ENCODE_SECONDS, PIPELINE_ENCODE_SECONDS_HELP);
    describe_histogram!(PIPELINE_BATCH_SEND_WAIT_SECONDS, PIPELINE_BATCH_SEND_WAIT_SECONDS_HELP);
});

/// The handles of the historical pass.
pub(crate) struct PipelineMetrics {
    bytes: Counter,
    stages: [Histogram; 5],
    commit: [Histogram; 2],
    encode: Histogram,
    batch_send_wait: Histogram,
}

impl PipelineMetrics {
    pub(crate) fn new() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            bytes: counter!(PIPELINE_SOURCE_BYTES),
            stages: STAGES.map(|stage| histogram!(PIPELINE_STAGE_SECONDS, "stage" => stage)),
            commit: OUTCOMES
                .map(|outcome| histogram!(PIPELINE_COMMIT_SECONDS, "outcome" => outcome)),
            encode: histogram!(PIPELINE_ENCODE_SECONDS),
            batch_send_wait: histogram!(PIPELINE_BATCH_SEND_WAIT_SECONDS),
        }
    }

    pub(crate) fn commit(&self, elapsed: Duration, success: bool) {
        self.commit[usize::from(!success)].record(elapsed.as_secs_f64());
    }

    pub(crate) fn encode(&self, elapsed: Duration) {
        self.encode.record(elapsed.as_secs_f64());
    }

    pub(crate) fn batch_send_wait(&self, elapsed: Duration) {
        self.batch_send_wait.record(elapsed.as_secs_f64());
    }
}

/// Records the bytes and the stage times of one segment when it drops. It records them also
/// when the segment fails.
pub(crate) struct SegmentMetrics<'a> {
    metrics: &'a PipelineMetrics,
    pub stats: WorkerStats,
}

impl<'a> SegmentMetrics<'a> {
    pub(crate) fn new(metrics: &'a PipelineMetrics) -> Self {
        Self { metrics, stats: WorkerStats::default() }
    }
}

impl Drop for SegmentMetrics<'_> {
    fn drop(&mut self) {
        let stats = &self.stats;
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
    }
}
