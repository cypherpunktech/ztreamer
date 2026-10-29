//! The metrics of the range streams of the service.
//!
//! Each metric name and its help text is a constant in this file, and [`DESCRIBE`] gives the help
//! text to the recorder once. Other modules record only through the types of this file. A range
//! timing covers the validation and the admission of a request, through its last block or the
//! drop of its stream.

use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use metrics::{
    Counter, Gauge, Histogram, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
};
use tokio_stream::Stream;
use tonic::Status;
use ztreamer_protocol::RpcStream;

const RANGE_ACTIVE: &str = "ztreamer.range.active";
const RANGE_ACTIVE_HELP: &str = "Range streams that run now.";

const RANGE_EMITTED_BLOCKS: &str = "ztreamer.range.emitted.blocks";
const RANGE_EMITTED_BLOCKS_HELP: &str = "Blocks that the range streams sent.";

const RANGE_FIRST_SECONDS: &str = "ztreamer.range.first.seconds";
const RANGE_FIRST_SECONDS_HELP: &str = "Time from a range request to its first block.";

const RANGE_DURATION_SECONDS: &str = "ztreamer.range.duration.seconds";
const RANGE_DURATION_SECONDS_HELP: &str =
    "Time of one range stream, by outcome: success, error or cancelled.";

const RANGE_ADMISSION_SECONDS: &str = "ztreamer.range.admission.seconds";
const RANGE_ADMISSION_SECONDS_HELP: &str =
    "Time that a range request waits for a free range stream.";

const OUTCOMES: [&str; 3] = ["success", "error", "cancelled"];

/// Gives the help text of each metric to the recorder. [`ServiceMetrics::new`] forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_gauge!(RANGE_ACTIVE, RANGE_ACTIVE_HELP);
    describe_counter!(RANGE_EMITTED_BLOCKS, RANGE_EMITTED_BLOCKS_HELP);
    describe_histogram!(RANGE_FIRST_SECONDS, RANGE_FIRST_SECONDS_HELP);
    describe_histogram!(RANGE_DURATION_SECONDS, RANGE_DURATION_SECONDS_HELP);
    describe_histogram!(RANGE_ADMISSION_SECONDS, RANGE_ADMISSION_SECONDS_HELP);
});

pub(crate) struct ServiceMetrics {
    active: Gauge,
    blocks: Counter,
    first: Histogram,
    duration: [Histogram; 3],
    admission: Histogram,
}

impl ServiceMetrics {
    pub(crate) fn new() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            active: gauge!(RANGE_ACTIVE),
            blocks: counter!(RANGE_EMITTED_BLOCKS),
            first: histogram!(RANGE_FIRST_SECONDS),
            duration: OUTCOMES
                .map(|outcome| histogram!(RANGE_DURATION_SECONDS, "outcome" => outcome)),
            admission: histogram!(RANGE_ADMISSION_SECONDS),
        }
    }

    pub(crate) fn admission(&self, elapsed: Duration) {
        self.admission.record(elapsed.as_secs_f64());
    }

    pub(crate) fn range(self: &Arc<Self>) -> RangeGuard {
        self.active.increment(1.0);
        RangeGuard { metrics: Arc::clone(self), start: Instant::now(), emitted: 0, outcome: 2 }
    }
}

pub(crate) struct RangeGuard {
    metrics: Arc<ServiceMetrics>,
    start: Instant,
    emitted: u64,
    outcome: usize,
}

impl RangeGuard {
    pub(crate) fn response<T: Send + 'static>(
        mut self,
        result: Result<RpcStream<T>, Status>,
    ) -> Result<RpcStream<T>, Status> {
        match result {
            Ok(inner) => Ok(Box::pin(MeasuredStream { inner, guard: Some(self) })),
            Err(error) => {
                self.outcome = 1;
                Err(error)
            }
        }
    }
}

impl Drop for RangeGuard {
    fn drop(&mut self) {
        self.metrics.blocks.increment(self.emitted);
        self.metrics.duration[self.outcome].record(self.start.elapsed().as_secs_f64());
        self.metrics.active.decrement(1.0);
    }
}

struct MeasuredStream<T> {
    inner: RpcStream<T>,
    guard: Option<RangeGuard>,
}

impl<T> Stream for MeasuredStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Terminate after an error, even if an underlying stream could continue.
        if this.guard.is_none() {
            return Poll::Ready(None);
        }
        let result = this.inner.as_mut().poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(_))) => {
                let guard = this.guard.as_mut().unwrap();
                if guard.emitted == 0 {
                    guard.metrics.first.record(guard.start.elapsed().as_secs_f64());
                }
                guard.emitted += 1;
            }
            Poll::Ready(end) => {
                let mut guard = this.guard.take().unwrap();
                guard.outcome = usize::from(end.is_some());
            }
            Poll::Pending => {}
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn streams_record_success_error_and_drop_once() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = metrics::with_local_recorder(&recorder, || Arc::new(ServiceMetrics::new()));

        let mut complete =
            metrics.range().response(Ok(Box::pin(tokio_stream::iter([Ok(1), Ok(2)])))).unwrap();
        assert_eq!(complete.next().await.unwrap().unwrap(), 1);
        assert_eq!(complete.next().await.unwrap().unwrap(), 2);
        assert!(complete.next().await.is_none());
        assert!(complete.next().await.is_none());
        drop(complete);

        let mut failed = metrics
            .range()
            .response(Ok(Box::pin(tokio_stream::iter([Ok(3), Err(Status::internal("injected"))]))))
            .unwrap();
        assert!(failed.next().await.unwrap().is_ok());
        assert!(failed.next().await.unwrap().is_err());
        drop(failed);

        let mut cancelled =
            metrics.range().response(Ok(Box::pin(tokio_stream::iter([Ok(4), Ok(5)])))).unwrap();
        assert!(cancelled.next().await.unwrap().is_ok());
        drop(cancelled);
        // Cancellation before first poll, validation failure, and empty success.
        drop(metrics.range().response::<u32>(Ok(Box::pin(tokio_stream::pending()))).unwrap());
        assert!(
            metrics.range().response::<u32>(Err(Status::invalid_argument("injected"))).is_err()
        );
        let mut empty =
            metrics.range().response::<u32>(Ok(Box::pin(tokio_stream::empty()))).unwrap();
        assert!(empty.next().await.is_none());

        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            match key.key().name() {
                "ztreamer.range.active" => assert_eq!(value, DebugValue::Gauge(0.0.into())),
                "ztreamer.range.emitted.blocks" => assert_eq!(value, DebugValue::Counter(4)),
                "ztreamer.range.first.seconds" => {
                    assert!(matches!(value, DebugValue::Histogram(samples) if samples.len() == 3));
                }
                "ztreamer.range.duration.seconds" => {
                    assert!(matches!(value, DebugValue::Histogram(samples) if samples.len() == 2));
                }
                _ => {}
            }
        }
    }
}
