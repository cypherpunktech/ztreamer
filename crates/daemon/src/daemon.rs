//! The entry of the daemon, and [`Daemon`]: it starts the node of a [`NodeMode`], waits until the
//! node is ready, then indexes and serves until shutdown.

use std::{ops::ControlFlow, sync::Arc};

use eyre::{Result, eyre};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;
use ztreamer_backend::StateBackend;
use ztreamer_embedded as node;
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
    DaemonConfig, Mode, SyncConfig,
    embedded::{Embedded, EmbeddedMode},
    lifecycle::{self, Supervisor},
    metrics_endpoint::MetricsEndpoint,
    node_mode::{NodeMode, NodeSources, ReadyNode, Started},
    tasks::{
        FillerTask, GrpcTask, HeadFollowerTask, HeadReconcile, HistoricalPass, ServerShutdown,
    },
};

/// The error when a server task stops before shutdown.
const TASK_STOPPED: &str = "daemon task stopped unexpectedly";

/// The daemon of one configuration and one gRPC server.
pub(crate) struct Daemon {
    config: DaemonConfig,
    grpc_server: Server,
}

impl Daemon {
    pub(crate) const fn new(config: DaemonConfig, grpc_server: Server) -> Self {
        Self { config, grpc_server }
    }

    /// Runs the daemon on the node of `mode` until `signal`, the end of the work
    /// ([`SyncConfig::index_only`]), or a failure.
    ///
    /// It starts the node. Then it supervises the node tasks of the mode, and an application that
    /// waits until the node is ready, indexes, and serves.
    pub(crate) async fn run<M: NodeMode>(
        self,
        mode: M,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let Self { config, grpc_server } = self;
        let shutdown = CancellationToken::new();
        let index = config.index.clone();
        let Started { node, startup } = mode.start(&index, &shutdown).await?;
        let application = async {
            let ControlFlow::Continue(ready) = startup.await? else {
                return Ok(());
            };
            Self { config, grpc_server }.run_application(ready, shutdown.clone()).await
        };
        node.finite(application).run_until_signal(signal, &shutdown).await
    }

    /// Indexes, then serves until shutdown. The node of `ready` must be ready for the historical
    /// index pass. The `range_meta` filler runs in both modes. `ready.embedded` is set when a
    /// node runs in this process. Then the p2p service gets the compact service. Else the remote
    /// mode logs the import state `following` after the head reconcile.
    async fn run_application<T, H, B>(
        self,
        ready: ReadyNode<T, H, B>,
        shutdown: CancellationToken,
    ) -> Result<()>
    where
        T: HistoricalSource + Send + 'static,
        H: CanonicalBlockSource + Clone + 'static,
        B: StateBackend,
    {
        let Self { config, mut grpc_server } = self;
        let ReadyNode { network, index, sources, embedded } = ready;
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
        let replicator = ReplicatorService::new(replicate_index, follower.subscribe(), &network);
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
        let grpc =
            GrpcTask { router: grpc, address, tls: config.servers.tls.is_some() }.spawn(&stop);
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
}

/// Runs the daemon of `config` until a shutdown signal, the end of the work
/// ([`SyncConfig::index_only`]), or a failure.
#[tokio::main]
pub async fn run_daemon(config: DaemonConfig) -> Result<()> {
    // Read and validate the identity before starting the node or the potentially lengthy index.
    let grpc_server = config.servers.grpc_server()?;
    let metrics = MetricsEndpoint::new(config.servers.metrics_listen);
    match &config.mode {
        Mode::Remote(remote) => {
            let mode = remote.mode()?;
            metrics.start()?;
            Daemon::new(config, grpc_server).run(mode, lifecycle::shutdown_signal()).await
        }
        Mode::Embedded { zakura_config } => {
            let mode = EmbeddedMode::load(zakura_config.clone())?;
            metrics.start()?;
            Daemon::new(config, grpc_server).run(mode, lifecycle::shutdown_signal()).await
        }
    }
}
