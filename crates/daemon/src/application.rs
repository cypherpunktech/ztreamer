//! The part of the daemon that both node modes share: the historical index pass, the head
//! reconcile, then the head follower, the gRPC server and the `range_meta` filler until
//! shutdown. The tasks are in [`crate::tasks`].

use std::{ops::ControlFlow, sync::Arc};

use eyre::{Result, eyre};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;
use zakura_chain::parameters::Network;
use ztreamer_backend::StateBackend;
use ztreamer_embedded as node;
use ztreamer_index::index::{Index, range_meta::NodeId};
use ztreamer_protocol::{
    replicate::replicator_server::ReplicatorServer,
    wire::compact_tx_streamer_server::CompactTxStreamerServer,
};
use ztreamer_replicator::{RangeMetaFiller, ReplicatorService};
use ztreamer_service::CompactService;
use ztreamer_sync::{
    head::{CanonicalBlockSource, HeadFollower},
    pipeline::HistoricalSource,
};

use crate::{
    DaemonConfig, SyncConfig,
    embedded::Embedded,
    lifecycle::Supervisor,
    tasks::{
        FillerTask, GrpcTask, HeadFollowerTask, HeadReconcile, HistoricalPass, ServerShutdown,
    },
};

/// The error when a server task stops before shutdown.
const TASK_STOPPED: &str = "daemon task stopped unexpectedly";

/// The chain access that one node mode gives the daemon.
pub(crate) struct NodeSources<T, H, B> {
    /// Finalized blocks for the historical index pass.
    pub(crate) tail: T,
    /// The canonical head, for head reconciliation and the head follower.
    pub(crate) head: H,
    /// Chain state and node requests for the gRPC service.
    pub(crate) backend: B,
    /// The node that answers the `range_meta` filler through `backend`. Each row that the filler
    /// writes names it in `verified_by`.
    pub(crate) verified_by: NodeId,
}

/// Indexes, then serves until shutdown. The node of `sources` must be ready for the historical
/// index pass. The `range_meta` filler runs in both modes. `embedded` is set when a node runs in
/// this process. Then the p2p service gets the compact service. Else the remote mode logs the
/// import state `following` after the head reconcile.
pub(crate) async fn run_application<T, H, B>(
    config: DaemonConfig,
    mut grpc_server: Server,
    index: Arc<Index>,
    network: &Network,
    embedded: Option<Embedded>,
    sources: NodeSources<T, H, B>,
    shutdown: CancellationToken,
) -> Result<()>
where
    T: HistoricalSource + Send + 'static,
    H: CanonicalBlockSource + Clone + 'static,
    B: StateBackend,
{
    let NodeSources { tail, head, backend, verified_by } = sources;

    let SyncConfig { pipeline, index_only } = config.sync;
    let pass = HistoricalPass { source: tail, index: Arc::clone(&index), pipeline, index_only };
    let ControlFlow::Continue(state) = pass.run(&shutdown).await? else {
        return Ok(());
    };
    if index_only || shutdown.is_cancelled() {
        return Ok(());
    }

    let filler_index = Arc::clone(&index);
    let replicate_index = Arc::clone(&index);
    let follower = HeadFollower::new(Arc::clone(&index), state);
    let compact = CompactService::with_backend(
        index,
        follower.subscribe(),
        network.bip70_network_name(),
        backend,
    )
    .with_ping_enabled(config.servers.ping_very_insecure)
    .with_node_version(node::NodeClient::version());
    let reconcile = HeadReconcile { follower: &follower, source: head.clone(), pipeline };
    if reconcile.run(&shutdown).await?.is_break() {
        return Ok(());
    }
    if let Some(Embedded { p2p }) = embedded {
        p2p.install(compact.clone()).map_err(|error| eyre!(error))?;
    } else {
        info!(import_state = "following");
    }
    let filler = RangeMetaFiller::new(filler_index, compact.backend(), verified_by);
    let replicator = ReplicatorService::new(replicate_index, follower.subscribe(), network);
    let grpc = grpc_server
        .add_service(CompactTxStreamerServer::new(compact))
        .add_service(ReplicatorServer::new(replicator));

    // Run the head follower, the gRPC server and the filler until shutdown or until one of the
    // three tasks stops. The supervisor drains all three tasks before it gives a result, also on
    // failure. Thus the caller can close the index: the filler holds it no more.
    let stop = ServerShutdown::new();
    let filler = FillerTask { filler, snapshots: follower.subscribe() }.spawn(&stop);
    let follower = HeadFollowerTask { follower, source: head, pipeline }.spawn(&stop);
    let address = config.servers.grpc_listen;
    let grpc = GrpcTask { router: grpc, address, tls: config.servers.tls.is_some() }.spawn(&stop);
    let shutdown_requested = async {
        shutdown.cancelled().await;
        Ok(())
    };
    Supervisor::new()
        .until_shutdown(grpc, TASK_STOPPED)
        .until_shutdown(follower, TASK_STOPPED)
        .until_shutdown(filler, TASK_STOPPED)
        .run(shutdown_requested, || stop.send())
        .await
}
