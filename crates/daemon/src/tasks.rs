//! The server tasks of the daemon: the head follower, the gRPC server and the `range_meta`
//! filler. Each one runs in the [`TaskSet`](crate::lifecycle::TaskSet) until shutdown.

use std::{net::SocketAddr, pin::pin};

use eyre::{Result, WrapErr};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;
use tracing::info;
use ztreamer_replicator::RangeMetaFiller;
use ztreamer_sync::{
    head::{CanonicalBlockSource, HeadFollower, HeadFollowerConfig, ServingSnapshot},
    pipeline::PipelineConfig,
};

/// The error when a server task stops before shutdown.
pub(crate) const TASK_STOPPED: &str = "daemon task stopped unexpectedly";

/// The head follower, from the canonical head of the node.
pub(crate) struct HeadFollowerTask<S> {
    pub(crate) follower: HeadFollower,
    /// The canonical head.
    pub(crate) source: S,
    pub(crate) pipeline: PipelineConfig,
}

impl<S: CanonicalBlockSource> HeadFollowerTask<S> {
    /// Follows the head until `shutdown`.
    pub(crate) async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let Self { follower, source, pipeline } = self;
        let (stop, stopped) = watch::channel(false);
        let mut follow =
            pin!(follower.follow_head(source, pipeline, HeadFollowerConfig::default(), stopped));
        tokio::select! {
            result = &mut follow => return result.wrap_err("head follower failed"),
            () = shutdown.cancelled() => {}
        }
        let _ = stop.send(true);
        follow.await.wrap_err("head follower failed")
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
    /// Serves until `shutdown`.
    pub(crate) async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let Self { router, address, tls } = self;
        info!(%address, tls, "serving CompactTxStreamer and Replicator gRPC");
        router
            .serve_with_shutdown(address, shutdown.cancelled_owned())
            .await
            .wrap_err("gRPC server failed")
    }
}

/// The `range_meta` filler of each range that the head follower seals.
pub(crate) struct FillerTask {
    pub(crate) filler: RangeMetaFiller,
    /// The snapshots of the head follower.
    pub(crate) snapshots: watch::Receiver<ServingSnapshot>,
}

impl FillerTask {
    /// Fills until `shutdown`. Returns after its last index read on a blocking thread.
    pub(crate) async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let Self { filler, snapshots } = self;
        filler.run(snapshots, shutdown.cancelled_owned()).await;
        Ok(())
    }
}
