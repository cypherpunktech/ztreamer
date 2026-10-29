//! The tasks of the daemon after the node is ready: the historical index pass and the head
//! reconcile run one after the other. Then the head follower, the gRPC server and the
//! `range_meta` filler run together until [`ServerShutdown`].

use std::{
    net::SocketAddr,
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use eyre::{Result, WrapErr, eyre};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;
use tracing::info;
use ztreamer_index::index::{Index, IndexState};
use ztreamer_replicator::RangeMetaFiller;
use ztreamer_sync::{
    head::{
        CanonicalBlockSource, HeadFollower, HeadFollowerConfig, HeadSyncError, ServingSnapshot,
    },
    pipeline::{HistoricalPipeline, HistoricalSource, PipelineConfig},
};

/// The historical index pass on a blocking thread.
pub(crate) struct HistoricalPass<S> {
    /// The finalized blocks.
    pub(crate) source: S,
    pub(crate) index: Arc<Index>,
    pub(crate) pipeline: PipelineConfig,
    /// The pass is the whole job: a stop before the end of the pass is an error.
    pub(crate) index_only: bool,
}

impl<S: HistoricalSource + Send + 'static> HistoricalPass<S> {
    /// Runs the pass. Breaks if shutdown stopped the pass, which is an error with `index_only`
    /// because the index was the whole job.
    pub(crate) async fn run(
        self,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), IndexState>> {
        let Self { source, index: sync_index, pipeline, index_only } = self;
        info!("syncing historical compact index");
        let historical_started = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let mut historical = tokio::task::spawn_blocking({
            let stop = Arc::clone(&stop);
            move || {
                HistoricalPipeline::new(&sync_index, &source, pipeline)?
                    .with_stop(&stop)
                    .sync_until_stable()
            }
        });
        let state = tokio::select! {
            result = &mut historical => result.wrap_err("historical index task panicked")??,
            _ = shutdown.cancelled() => {
                info!("stopping the historical index pass before shutdown");
                stop.store(true, Ordering::Relaxed);
                let state = historical.await.wrap_err("historical index task panicked")??;
                info!(
                    height = state.durable_tip().map(|tip| tip.height),
                    elapsed_seconds = historical_started.elapsed().as_secs_f64(),
                    "historical compact index stopped before shutdown"
                );
                if index_only {
                    return Err(eyre!(
                        "historical compact index stopped by shutdown before it completed"
                    ));
                }
                return Ok(ControlFlow::Break(()));
            }
        };
        info!(
            height = state.durable_tip().map(|tip| tip.height),
            elapsed_seconds = historical_started.elapsed().as_secs_f64(),
            "historical compact index complete"
        );
        Ok(ControlFlow::Continue(state))
    }
}

/// The reconcile of the compact index with the canonical head of the node.
pub(crate) struct HeadReconcile<'a, S> {
    pub(crate) follower: &'a HeadFollower,
    /// The canonical head.
    pub(crate) source: S,
    pub(crate) pipeline: PipelineConfig,
}

impl<S: CanonicalBlockSource> HeadReconcile<'_, S> {
    /// Brings the compact index onto Zakura's canonical head, recovering a deep reorg if needed.
    /// Breaks on shutdown.
    pub(crate) async fn run(self, shutdown: &CancellationToken) -> Result<ControlFlow<()>> {
        let Self { follower, mut source, pipeline } = self;
        let reconcile = async {
            match follower.sync_head(&mut source, pipeline).await {
                Err(HeadSyncError::DeepReorg { .. }) => {
                    follower.recover_deep_reorg(&mut source, pipeline).await
                }
                result => result,
            }
            .wrap_err("reconcile compact index with Zakura's canonical head")
        };
        tokio::select! {
            _ = shutdown.cancelled() => Ok(ControlFlow::Break(())),
            result = reconcile => result.map(|_| ControlFlow::Continue(())),
        }
    }
}

/// The stop of the server tasks: the head follower, the gRPC server and the filler.
pub(crate) struct ServerShutdown(watch::Sender<bool>);

impl ServerShutdown {
    pub(crate) fn new() -> Self {
        Self(watch::channel(false).0)
    }

    /// Tells each server task to stop.
    pub(crate) fn send(&self) {
        let _ = self.0.send(true);
    }

    /// Completes when [`Self::send`] tells the tasks to stop.
    fn requested(&self) -> impl Future<Output = ()> + use<> {
        let mut receiver = self.0.subscribe();
        async move { while !*receiver.borrow() && receiver.changed().await.is_ok() {} }
    }
}

/// The head follower, from the canonical head of the node.
pub(crate) struct HeadFollowerTask<S> {
    pub(crate) follower: HeadFollower,
    /// The canonical head.
    pub(crate) source: S,
    pub(crate) pipeline: PipelineConfig,
}

impl<S: CanonicalBlockSource + 'static> HeadFollowerTask<S> {
    /// Spawns the follower until `shutdown`. The returned future gives the result of the task.
    pub(crate) fn spawn(
        self,
        shutdown: &ServerShutdown,
    ) -> impl Future<Output = Result<()>> + use<S> {
        let Self { follower, source, pipeline } = self;
        let receiver = shutdown.0.subscribe();
        let task = tokio::spawn(async move {
            follower
                .follow_head(source, pipeline, HeadFollowerConfig::default(), receiver)
                .await
                .wrap_err("head follower failed")
        });
        async move { task.await.wrap_err("head follower task panicked")? }
    }
}

/// The gRPC server of CompactTxStreamer and Replicator on one listener.
pub(crate) struct GrpcTask {
    pub(crate) router: Router,
    pub(crate) address: SocketAddr,
    /// The router has a TLS identity. Only the log uses it.
    pub(crate) tls: bool,
}

impl GrpcTask {
    /// Spawns the server until `shutdown`. The returned future gives the result of the task.
    pub(crate) fn spawn(
        self,
        shutdown: &ServerShutdown,
    ) -> impl Future<Output = Result<()>> + use<> {
        let Self { router, address, tls } = self;
        let requested = shutdown.requested();
        let task = tokio::spawn(async move {
            info!(%address, tls, "serving CompactTxStreamer and Replicator gRPC");
            router.serve_with_shutdown(address, requested).await.wrap_err("gRPC server failed")
        });
        async move { task.await.wrap_err("gRPC task panicked")? }
    }
}

/// The `range_meta` filler of each range that the head follower seals.
pub(crate) struct FillerTask {
    pub(crate) filler: RangeMetaFiller,
    /// The snapshots of the head follower.
    pub(crate) snapshots: watch::Receiver<ServingSnapshot>,
}

impl FillerTask {
    /// Spawns the filler until `shutdown`. The returned future gives the result of the task. The
    /// task stops after its last index read on a blocking thread.
    pub(crate) fn spawn(
        self,
        shutdown: &ServerShutdown,
    ) -> impl Future<Output = Result<()>> + use<> {
        let Self { filler, snapshots } = self;
        let task = tokio::spawn(filler.run(snapshots, shutdown.requested()));
        async move { task.await.wrap_err("range_meta filler task panicked") }
    }
}
