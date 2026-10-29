//! The [`Launcher`]: it runs a [`Node`] through the fixed stages of the daemon. Each mode runs
//! the same stages, in the same order:
//!
//! 1. Boot: the launcher reads the network of the node and opens the index of that network.
//!    Then the node starts, and is ready for the historical stage.
//! 2. Historical: the historical source fills the index, or skips.
//! 3. Catch up: the block pipeline indexes the finalized blocks of the tail until it is stable.
//!    Then the head reconcile brings the index onto the canonical head of the node.
//! 4. Serve: the head follower, the gRPC server and the `range_meta` filler run until shutdown.

use std::{ops::ControlFlow, sync::Arc};

use eyre::{Result, WrapErr};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use ztreamer_embedded as node;
use ztreamer_protocol::{
    replicate::replicator_server::ReplicatorServer,
    wire::compact_tx_streamer_server::CompactTxStreamerServer,
};
use ztreamer_replicator::{RangeMetaFiller, ReplicatorService};
use ztreamer_service::CompactService;
use ztreamer_sync::head::{CanonicalBlockSource, HeadFollower, HeadSyncError};

use crate::{
    DaemonConfig,
    index_pass::{IndexPass, PassKind},
    lifecycle::{Supervisor, TaskSet},
    node::{HistoricalSource, IndexTip, Node, NodeComponents, StageContext, Tail},
    tasks::{FillerTask, GrpcTask, HeadFollowerTask, TASK_STOPPED},
};

/// Runs a node through the stages of the daemon, with one configuration and one gRPC server.
pub(crate) struct Launcher {
    config: DaemonConfig,
    grpc_server: Server,
}

impl Launcher {
    pub(crate) const fn new(config: DaemonConfig, grpc_server: Server) -> Self {
        Self { config, grpc_server }
    }

    /// Runs the stages on `node` until `signal`, the end of the work
    /// ([`SyncConfig::index_only`](crate::SyncConfig::index_only)), or a failure.
    pub(crate) async fn run<N: Node>(
        self,
        node: N,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let supervisor = Supervisor::new(CancellationToken::new());
        let tasks = supervisor.tasks().clone();
        supervisor.run(signal, self.stages(node, tasks)).await
    }

    /// The stages. Each one ends the work when it breaks: on shutdown, or at the end of the work.
    async fn stages<N: Node>(self, mut node: N, tasks: TaskSet) -> Result<()> {
        let shutdown = tasks.shutdown().clone();

        // Boot.
        let ControlFlow::Continue(network) = node.network(&shutdown).await? else {
            return Ok(());
        };
        let index = self.config.index.open(&network).await?;
        let ControlFlow::Continue(components) = node.boot(&tasks).await? else {
            return Ok(());
        };
        let stage = StageContext {
            index: &index,
            network: &network,
            sync: self.config.sync,
            shutdown: &shutdown,
        };

        // Historical.
        if components.historical().fill(&stage).await?.is_break() {
            return Ok(());
        }

        // Catch up.
        let ControlFlow::Continue(caught_up) = self.catch_up(&components, &stage).await? else {
            return Ok(());
        };

        // Serve.
        self.serve(&components, caught_up, &stage, &tasks).await
    }

    /// Indexes the finalized blocks of the tail of the node, then reconciles the index with the
    /// canonical head. Breaks on shutdown, and after the pass with `--index-only`.
    async fn catch_up<C: NodeComponents>(
        &self,
        components: &C,
        stage: &StageContext<'_>,
    ) -> Result<ControlFlow<(), CaughtUp<C::Head>>> {
        let wallet = components.wallet().await?;
        let tip = IndexTip::read(stage.index).await?;
        let ControlFlow::Continue(Tail { finalized, head }) =
            components.tail(tip, stage.shutdown).await?
        else {
            return Ok(ControlFlow::Break(()));
        };
        let pass = IndexPass { kind: PassKind::CatchUp, source: finalized };
        let ControlFlow::Continue(state) = pass.run(stage).await? else {
            return Ok(ControlFlow::Break(()));
        };
        if stage.sync.index_only || stage.shutdown.is_cancelled() {
            return Ok(ControlFlow::Break(()));
        }

        let follower = HeadFollower::new(Arc::clone(stage.index), state);
        let compact = CompactService::with_backend(
            Arc::clone(stage.index),
            follower.subscribe(),
            stage.network.bip70_network_name(),
            wallet,
        )
        .with_ping_enabled(self.config.servers.ping_very_insecure)
        .with_node_version(node::NodeClient::version());
        let caught_up = CaughtUp { follower, compact, head };
        Ok(if caught_up.reconcile(stage).await?.is_break() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(caught_up)
        })
    }

    /// Serves until shutdown: the head follower, the gRPC server and the `range_meta` filler join
    /// `tasks`.
    async fn serve<C: NodeComponents>(
        self,
        components: &C,
        caught_up: CaughtUp<C::Head>,
        stage: &StageContext<'_>,
        tasks: &TaskSet,
    ) -> Result<()> {
        let Self { config, mut grpc_server } = self;
        let CaughtUp { follower, compact, head } = caught_up;
        components.start_following(&compact)?;
        let filler =
            RangeMetaFiller::new(Arc::clone(stage.index), compact.backend(), components.id());
        let replicator =
            ReplicatorService::new(Arc::clone(stage.index), follower.subscribe(), stage.network);
        let router = grpc_server
            .add_service(CompactTxStreamerServer::new(compact))
            .add_service(ReplicatorServer::new(replicator));

        // The supervisor drains the three tasks before it gives a result, also on failure. Thus
        // the caller can close the index: the filler holds it no more.
        let address = config.servers.grpc_listen;
        let grpc = GrpcTask { router, address, tls: config.servers.tls.is_some() };
        let filler = FillerTask { filler, snapshots: follower.subscribe() };
        let follower = HeadFollowerTask { follower, source: head, pipeline: stage.sync.pipeline };
        let shutdown = stage.shutdown;
        tasks.spawn("gRPC", TASK_STOPPED, grpc.run(shutdown.clone()));
        tasks.spawn("head follower", TASK_STOPPED, follower.run(shutdown.clone()));
        tasks.spawn("range_meta filler", TASK_STOPPED, filler.run(shutdown.clone()));
        shutdown.cancelled().await;
        Ok(())
    }
}

/// The index after the catch-up: on the canonical head of the node.
struct CaughtUp<H> {
    follower: HeadFollower,
    /// The wallet service.
    compact: CompactService,
    /// The canonical head.
    head: H,
}

impl<H: CanonicalBlockSource + Clone> CaughtUp<H> {
    /// Brings the compact index onto the canonical head of the node, and recovers a deep reorg if
    /// needed. Breaks on shutdown.
    async fn reconcile(&self, stage: &StageContext<'_>) -> Result<ControlFlow<()>> {
        let (follower, mut source, pipeline) =
            (&self.follower, self.head.clone(), stage.sync.pipeline);
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
            _ = stage.shutdown.cancelled() => Ok(ControlFlow::Break(())),
            result = reconcile => result.map(|_| ControlFlow::Continue(())),
        }
    }
}
