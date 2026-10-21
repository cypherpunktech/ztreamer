//! Range timings cover validation/admission through final emission or stream drop.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use metrics::{Counter, Gauge, Histogram};
use tokio_stream::Stream;
use tonic::Status;
use ztreamer_indexer::{
    head::{CanonicalBlockSource, HeadError},
    index::BlockId,
    parser::RawIndexBlock,
};

use crate::service::{RpcStream, ServingSnapshot};

pub(crate) struct ServiceMetrics {
    active: Gauge,
    blocks: Counter,
    first: Histogram,
    duration: [Histogram; 3],
    pub admission: Histogram,
    visible: Gauge,
    ready: Gauge,
    fresh: Gauge,
    source_tip: Gauge,
    publication: Histogram,
    pub head_success: Histogram,
    pub head_error: Histogram,
}

impl ServiceMetrics {
    pub fn new() -> Self {
        Self {
            active: metrics::gauge!("ztreamer.range.active"),
            blocks: metrics::counter!("ztreamer.range.emitted.blocks"),
            first: metrics::histogram!("ztreamer.range.first.seconds"),
            duration: ["success", "error", "cancelled"].map(|outcome| {
                metrics::histogram!("ztreamer.range.duration.seconds", "outcome" => outcome)
            }),
            admission: metrics::histogram!("ztreamer.range.admission.seconds"),
            visible: metrics::gauge!("ztreamer.head.visible.height"),
            ready: metrics::gauge!("ztreamer.head.ready"),
            fresh: metrics::gauge!("ztreamer.head.fresh"),
            source_tip: metrics::gauge!("ztreamer.head.source.height"),
            publication: metrics::histogram!("ztreamer.head.publication.seconds"),
            head_success: metrics::histogram!("ztreamer.head.reconcile.seconds", "outcome" => "success"),
            head_error: metrics::histogram!("ztreamer.head.reconcile.seconds", "outcome" => "error"),
        }
    }

    pub fn snapshot(&self, snapshot: &ServingSnapshot) {
        self.visible.set(snapshot.visible_tip.map_or(f64::NAN, |tip| f64::from(tip.height)));
        self.ready.set(f64::from(snapshot.ready));
        self.fresh.set(f64::from(snapshot.tip_fresh));
    }

    pub fn range(self: &Arc<Self>) -> RangeGuard {
        self.active.increment(1.0);
        RangeGuard { metrics: Arc::clone(self), start: Instant::now(), emitted: 0, outcome: 2 }
    }

    pub fn published(&self, observed: Option<(BlockId, Instant)>, visible: Option<BlockId>) {
        if let Some((tip, at)) = observed
            && visible == Some(tip)
        {
            self.publication.record(at.elapsed().as_secs_f64());
        }
    }
}

/// Observe existing tip calls without adding source reads to reconciliation.
pub(crate) struct ObservedSource<'a, S> {
    pub source: &'a mut S,
    pub previous: Option<BlockId>,
    pub observed: Option<(BlockId, Instant)>,
    pub metrics: &'a ServiceMetrics,
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
}

pub(crate) struct RangeGuard {
    metrics: Arc<ServiceMetrics>,
    start: Instant,
    emitted: u64,
    outcome: usize,
}

impl RangeGuard {
    pub fn response<T: Send + 'static>(
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
