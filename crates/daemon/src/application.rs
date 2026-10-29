//! The part of the daemon that both node modes share: the historical index pass, the head
//! reconcile, then the head follower, the gRPC server and the `range_meta` filler until
//! shutdown.

use std::{
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
use tonic::transport::{Server, server::Router};
use tracing::info;
use zakura_chain::parameters::Network;
use ztreamer_backend::StateBackend;
use ztreamer_embedded as node;
use ztreamer_index::index::{Index, IndexState, range_meta::NodeId};
use ztreamer_protocol::{
    replicate::replicator_server::ReplicatorServer,
    wire::compact_tx_streamer_server::CompactTxStreamerServer,
};
use ztreamer_replicator::{RangeMetaFiller, ReplicatorService};
use ztreamer_service::CompactService;
use ztreamer_sync::{
    head::{CanonicalBlockSource, HeadFollower, HeadFollowerConfig, HeadSyncError},
    pipeline::{HistoricalPipeline, HistoricalSource, PipelineConfig},
};

use crate::{DaemonConfig, ServerConfig, SyncConfig, embedded::Embedded, lifecycle};

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
    let ControlFlow::Continue(state) =
        sync_historical(tail, &index, pipeline, index_only, &shutdown).await?
    else {
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
    if reconcile_head(&follower, head.clone(), pipeline, &shutdown).await?.is_break() {
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
    serve(follower, head, pipeline, grpc, &config.servers, filler, &shutdown).await
}

/// Runs the historical index pass. Breaks if shutdown stopped the pass, which is an error
/// with `index_only` because the index was the whole job.
async fn sync_historical(
    source: impl HistoricalSource + Send + 'static,
    index: &Arc<Index>,
    pipeline: PipelineConfig,
    index_only: bool,
    shutdown: &CancellationToken,
) -> Result<ControlFlow<(), IndexState>> {
    let sync_index = Arc::clone(index);
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

/// Brings the compact index onto Zakura's canonical head, recovering a deep reorg if needed.
/// Breaks on shutdown.
async fn reconcile_head(
    follower: &HeadFollower,
    mut source: impl CanonicalBlockSource,
    pipeline: PipelineConfig,
    shutdown: &CancellationToken,
) -> Result<ControlFlow<()>> {
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

/// Runs the head follower, the gRPC server and `filler` until shutdown or until one of the three
/// tasks stops. The server serves CompactTxStreamer and Replicator on one listener. Returns when
/// the filler holds the index no more, so the caller can close the index.
async fn serve(
    head: HeadFollower,
    source: impl CanonicalBlockSource + 'static,
    pipeline: PipelineConfig,
    grpc: Router,
    servers: &ServerConfig,
    filler: RangeMetaFiller,
    shutdown: &CancellationToken,
) -> Result<()> {
    let (server_shutdown, _) = watch::channel(false);
    let mut filler =
        tokio::spawn(filler.run(head.subscribe(), shutdown_requested(server_shutdown.subscribe())));
    let mut follower = tokio::spawn({
        let receiver = server_shutdown.subscribe();
        async move {
            head.follow_head(source, pipeline, HeadFollowerConfig::default(), receiver)
                .await
                .wrap_err("head follower failed")
        }
    });
    let mut grpc = tokio::spawn({
        let receiver = server_shutdown.subscribe();
        let address = servers.grpc_listen;
        let tls = servers.tls.is_some();
        async move {
            info!(%address, tls, "serving CompactTxStreamer and Replicator gRPC");
            grpc.serve_with_shutdown(address, shutdown_requested(receiver))
                .await
                .wrap_err("gRPC server failed")
        }
    });

    let (mut grpc_result, mut follower_result, mut filler_result) = (None, None, None);
    tokio::select! {
        _ = shutdown.cancelled() => {},
        result = &mut grpc => grpc_result = Some(result),
        result = &mut follower => follower_result = Some(result),
        result = &mut filler => filler_result = Some(result),
    }
    let task_exited = grpc_result.is_some() || follower_result.is_some() || filler_result.is_some();
    let _ = server_shutdown.send(true);

    // Drain all three tasks before inspecting a result, including on failure. The filler
    // returns after its last index read on a blocking thread.
    let ((grpc_result, follower_result), filler_result) = lifecycle::drain_both(
        (lifecycle::drain_both((grpc, grpc_result), (follower, follower_result)), None),
        (filler, filler_result),
    )
    .await;
    task_result(grpc_result, "gRPC")?;
    task_result(follower_result, "head follower")?;
    filler_result.wrap_err("range_meta filler task panicked")?;
    if task_exited {
        return Err(eyre!("daemon task stopped unexpectedly"));
    }
    Ok(())
}

async fn shutdown_requested(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
}

fn task_result(result: Result<Result<()>, tokio::task::JoinError>, name: &str) -> Result<()> {
    result.wrap_err_with(|| format!("{name} task panicked"))?
}
