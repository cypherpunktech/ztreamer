//! `ztreamerd` process lifecycle: embedded or remote Zakura, replica import, historical indexing,
//! head following, and servers. The `export-snapshot` subcommand is in [`export`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use clap::{Args, Parser, Subcommand};
use eyre::{Result, WrapErr, ensure, eyre};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity, Server, ServerTlsConfig, server::Router};
use tracing::info;
use tracing_subscriber::EnvFilter;
use zakura_chain::parameters::Network;
use zakurad::{components::metrics::MetricsEndpoint, config::ZakuradConfig};
use ztreamer_indexer::{
    head::{CanonicalBlockSource, HeadSyncError},
    index::{Index, IndexState},
    pipeline::{HistoricalPipeline, HistoricalSource, PipelineConfig},
};
use ztreamer_node as node;
use ztreamer_protocol::{
    replicate::replicator_server::ReplicatorServer,
    wire::compact_tx_streamer_server::CompactTxStreamerServer,
};
use ztreamer_service::{
    CompactService, HeadFollowerConfig, InProcessBackend, RangeMetaFiller, ReplicatorService,
    StateBackend, p2p::P2pCompactService,
};

mod export;
mod lifecycle;
mod remote;
mod replicate;

/// Must be large enough to hold full mainnet index, which is 21 GiB at the time of writing
const DEFAULT_MAP_SIZE: usize = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "CompactTxStreamer daemon backed by an embedded or remote Zakura node",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    daemon: DaemonArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the sealed ranges of an index as a snapshot that a replica can import.
    ExportSnapshot(export::ExportArgs),
}

/// The daemon flags. They apply when no subcommand is given.
#[derive(Debug, Args)]
struct DaemonArgs {
    /// Zakura TOML configuration. Zakura defaults and ZAKURA_* variables apply when omitted.
    #[arg(long, value_name = "FILE")]
    zakura_config: Option<PathBuf>,

    /// Directory containing the LMDB compact index. A subcommand reads it too: give it after
    /// the subcommand name.
    #[arg(long, global = true, default_value = "ztreamer-index", value_name = "DIR")]
    index_dir: PathBuf,

    /// Maximum LMDB map size in bytes. A subcommand reads it too: give it after the subcommand
    /// name.
    #[arg(long, global = true, default_value_t = NonZeroUsize::new(DEFAULT_MAP_SIZE).unwrap())]
    index_map_size: NonZeroUsize,

    /// Historical fetch/parse workers. Defaults to the pipeline setting.
    #[arg(long = "fetch-workers")]
    workers: Option<NonZeroUsize>,

    /// Consecutive blocks scanned by each historical fetch worker.
    #[arg(long)]
    source_segment_blocks: Option<NonZeroU32>,

    /// Maximum bytes awaiting ordered historical ingestion.
    #[arg(long)]
    max_pending_bytes: Option<NonZeroUsize>,

    /// Maximum compact-index write batch size in bytes. It also limits how far the historical
    /// fetch workers read ahead: they can have at most `--max-pending-bytes` minus the larger of
    /// this value and 2 MB (the largest block record) in flight, plus the one block that the
    /// index needs next.
    #[arg(long)]
    max_batch_bytes: Option<NonZeroUsize>,

    /// CompactTxStreamer gRPC listener.
    #[arg(long, default_value = "127.0.0.1:9067")]
    grpc_listen: SocketAddr,

    /// Prometheus metrics listener.
    #[arg(long, default_value = "127.0.0.1:9999")]
    metrics_listen: SocketAddr,

    /// PEM-encoded TLS certificate chain for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM-encoded TLS private key for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Answer Ping. Testing only.
    #[arg(long)]
    ping_very_insecure: bool,

    /// Exit after historical indexing instead of starting servers and the head follower.
    #[arg(long)]
    index_only: bool,

    #[command(flatten)]
    remote: remote::RemoteArgs,

    #[command(flatten)]
    replicate: replicate::ReplicateArgs,
}

impl DaemonArgs {
    /// Opens the index of `--index-dir` on a blocking thread and refuses an index of another
    /// network.
    async fn open_index(&self, network: &Network) -> Result<Arc<Index>> {
        let (path, map_size) = (self.index_dir.clone(), self.index_map_size.get());
        let (name, genesis_hash) = (network.to_string(), network.genesis_hash());
        let index =
            tokio::task::spawn_blocking(move || Index::open(path, map_size, &name, genesis_hash))
                .await
                .wrap_err("index open task panicked")??;
        Ok(Arc::new(index))
    }

    /// Opens the index of `--index-dir` for the chain in its identity row. Refuses a directory
    /// with no index.
    fn open_existing_index(&self) -> Result<Index> {
        Ok(Index::open_existing(&self.index_dir, self.index_map_size.get())?)
    }
}

impl From<&DaemonArgs> for PipelineConfig {
    /// Starts from the pipeline defaults and applies the limits given on the command line.
    fn from(cli: &DaemonArgs) -> Self {
        let defaults = Self::default();
        Self {
            workers: cli.workers.map_or(defaults.workers, NonZeroUsize::get),
            source_segment_blocks: cli
                .source_segment_blocks
                .map_or(defaults.source_segment_blocks, NonZeroU32::get),
            max_pending_bytes: cli
                .max_pending_bytes
                .map_or(defaults.max_pending_bytes, NonZeroUsize::get),
            max_batch_bytes: cli
                .max_batch_bytes
                .map_or(defaults.max_batch_bytes, NonZeroUsize::get),
            ..defaults
        }
    }
}

fn main() -> Result<()> {
    let Cli { command, daemon } = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match command {
        Some(Command::ExportSnapshot(args)) => args.run(&daemon.open_existing_index()?),
        None => run_daemon(daemon),
    }
}

#[tokio::main]
async fn run_daemon(cli: DaemonArgs) -> Result<()> {
    // Read and validate the identity before starting the node or the potentially lengthy index.
    let grpc_server = grpc_server(cli.tls_cert.as_deref(), cli.tls_key.as_deref())?;
    let replica = cli.replicate.replica();
    if let Some(node) = cli.remote.node()? {
        let node = node.with_replica(replica);
        return node.run(cli, grpc_server, lifecycle::shutdown_signal()).await;
    }
    ensure!(
        replica.is_none(),
        "--replicate-from needs --remote-rpc: the node that attests the import"
    );

    let mut config =
        ZakuradConfig::load(cli.zakura_config.clone()).wrap_err("load Zakura configuration")?;
    config.metrics.endpoint_addr = Some(cli.metrics_listen);
    let _metrics = MetricsEndpoint::new(&config.metrics).wrap_err("start Prometheus endpoint")?;
    let network = config.network.network.clone();
    let index = cli.open_index(&network).await?;

    let p2p = Arc::new(P2pCompactService::pending());
    info!(network = %network, "starting embedded Zakura");
    let shutdown = CancellationToken::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let node = async {
        // Keep Zakura's large startup future on the heap while polling it here.
        Box::pin(zakurad::node::run_with_services_ready(
            config,
            vec![p2p.registration()],
            shutdown.clone(),
            ready_tx,
        ))
        .await
        .map_err(|error| eyre!("Zakura failed: {error}"))
    };
    let application = async {
        let services = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            result = ready_rx => result.wrap_err("Zakura stopped before readiness")?,
        };
        let client = node::NodeClient::from_services(services);
        let sources = NodeSources {
            tail: client.database(),
            head: client.clone(),
            backend: InProcessBackend::with_node(client),
        };
        if sources.wait_ready(&shutdown).await?.is_break() {
            return Ok(());
        }
        let embedded = Some(Embedded { p2p: p2p.clone() });
        run_application(cli, grpc_server, index, &network, embedded, sources, shutdown.clone())
            .await
    };

    lifecycle::supervise(node, application, shutdown.clone(), lifecycle::shutdown_signal()).await
}

/// The chain access that one node mode gives the daemon.
struct NodeSources<T, H, B> {
    /// Finalized blocks for the historical index pass.
    tail: T,
    /// The canonical head, for head reconciliation and the head follower.
    head: H,
    /// Chain state and node requests for the gRPC service.
    backend: B,
}

impl<T, B> NodeSources<T, node::NodeClient, B> {
    /// Waits until the embedded Zakura is near the chain tip and has a finalized block. Then the
    /// node is ready for the historical index pass. Breaks on shutdown.
    ///
    /// The remote mode waits in `RemoteNode::sources`, before it makes its sources.
    async fn wait_ready(&self, shutdown: &CancellationToken) -> Result<ControlFlow<()>> {
        let client = &self.head;
        info!("waiting for Zakura to sync near the chain tip");
        tokio::select! {
            result = client.wait_until_close_to_tip() => {
                result.map_err(|error| eyre!("wait for Zakura sync: {error}"))?;
            }
            _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
        }
        info!(tip = ?client.tip(), "Zakura is near the chain tip");

        while client.database().tip().is_none() {
            info!("waiting for Zakura's first finalized block");
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}

/// What only the embedded mode gives the daemon.
struct Embedded {
    /// The Zakura p2p compact service. It gets the compact service after the head reconcile.
    p2p: Arc<P2pCompactService>,
}

/// Indexes, then serves until shutdown. The node of `sources` must be ready for the historical
/// index pass. `embedded` is set when a node runs in this process. Then the p2p service gets the
/// compact service, and the `range_meta` filler runs.
async fn run_application<T, H, B>(
    cli: DaemonArgs,
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
    let NodeSources { tail, head, backend } = sources;

    let pipeline = PipelineConfig::from(&cli);
    cli.replicate.log_import_state("tail");
    let ControlFlow::Continue(state) =
        sync_historical(tail, &index, pipeline, cli.index_only, &shutdown).await?
    else {
        return Ok(());
    };
    if cli.index_only || shutdown.is_cancelled() {
        return Ok(());
    }

    let filler_index = Arc::clone(&index);
    let replicate_index = Arc::clone(&index);
    let compact = CompactService::with_backend(index, state, network.bip70_network_name(), backend)
        .with_ping_enabled(cli.ping_very_insecure);
    if reconcile_head(&compact, head.clone(), pipeline, &shutdown).await?.is_break() {
        return Ok(());
    }
    let filler = embedded
        .map(|Embedded { p2p }| -> Result<_> {
            p2p.install(compact.clone()).map_err(|error| eyre!(error))?;
            let filler = RangeMetaFiller::embedded(filler_index, compact.backend());
            Ok(filler.run(compact.subscribe()))
        })
        .transpose()?;
    cli.replicate.log_import_state("following");
    let replicator = ReplicatorService::new(replicate_index, compact.subscribe(), network);
    let grpc = grpc_server
        .add_service(CompactTxStreamerServer::new(compact.clone()))
        .add_service(ReplicatorServer::new(replicator));
    serve(compact, head, pipeline, grpc, &cli, filler, &shutdown).await
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
    compact: &CompactService,
    mut source: impl CanonicalBlockSource,
    pipeline: PipelineConfig,
    shutdown: &CancellationToken,
) -> Result<ControlFlow<()>> {
    let reconcile = async {
        match compact.sync_head(&mut source, pipeline).await {
            Err(HeadSyncError::DeepReorg { .. }) => {
                compact.recover_deep_reorg(&mut source, pipeline).await
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

/// Runs the head follower and the gRPC server until shutdown or until either task stops. The
/// server serves CompactTxStreamer and Replicator on one listener. If `filler` is set, runs it
/// beside them and stops it with them.
async fn serve(
    compact: CompactService,
    source: impl CanonicalBlockSource + 'static,
    pipeline: PipelineConfig,
    grpc: Router,
    cli: &DaemonArgs,
    filler: Option<impl Future<Output = ()> + Send + 'static>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let filler = filler.map(tokio::spawn);
    let (server_shutdown, _) = watch::channel(false);
    let mut follower = tokio::spawn({
        let compact = compact.clone();
        let receiver = server_shutdown.subscribe();
        async move {
            compact
                .follow_head(source, pipeline, HeadFollowerConfig::default(), receiver)
                .await
                .wrap_err("head follower failed")
        }
    });
    let mut grpc = tokio::spawn({
        let receiver = server_shutdown.subscribe();
        let address = cli.grpc_listen;
        let tls = cli.tls_cert.is_some();
        async move {
            info!(%address, tls, "serving CompactTxStreamer and Replicator gRPC");
            grpc.serve_with_shutdown(address, shutdown_requested(receiver))
                .await
                .wrap_err("gRPC server failed")
        }
    });

    let mut grpc_result = None;
    let mut follower_result = None;
    tokio::select! {
        _ = shutdown.cancelled() => {},
        result = &mut grpc => grpc_result = Some(result),
        result = &mut follower => follower_result = Some(result),
    }
    let task_exited = grpc_result.is_some() || follower_result.is_some();
    let _ = server_shutdown.send(true);
    if let Some(filler) = &filler {
        filler.abort();
    }

    // Drain both tasks before inspecting either result, including on failure.
    let (grpc_result, follower_result) =
        lifecycle::drain_both((grpc, grpc_result), (follower, follower_result)).await;
    task_result(grpc_result, "gRPC")?;
    task_result(follower_result, "head follower")?;
    if task_exited {
        return Err(eyre!("daemon task stopped unexpectedly"));
    }
    Ok(())
}

fn grpc_server(tls_cert: Option<&Path>, tls_key: Option<&Path>) -> Result<Server> {
    let server = Server::builder();
    let (Some(cert_path), Some(key_path)) = (tls_cert, tls_key) else {
        if tls_cert.is_some() || tls_key.is_some() {
            return Err(eyre!("--tls-cert and --tls-key must be provided together"));
        }
        return Ok(server);
    };

    let cert = std::fs::read(cert_path)
        .wrap_err_with(|| format!("read TLS certificate {}", cert_path.display()))?;
    let key = std::fs::read(key_path)
        .wrap_err_with(|| format!("read TLS private key {}", key_path.display()))?;
    server
        .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
        .wrap_err("configure gRPC TLS")
}

async fn shutdown_requested(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
}

fn task_result(result: Result<Result<()>, tokio::task::JoinError>, name: &str) -> Result<()> {
    result.wrap_err_with(|| format!("{name} task panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_config_keeps_defaults_for_unset_flags() {
        let cli =
            Cli::parse_from(["ztreamerd", "--fetch-workers", "3", "--max-batch-bytes", "4096"]);
        assert!(cli.command.is_none());
        let defaults = PipelineConfig::default();
        let pipeline = PipelineConfig::from(&cli.daemon);
        assert_eq!(pipeline.workers, 3);
        assert_eq!(pipeline.max_batch_bytes, 4096);
        assert_eq!(pipeline.source_segment_blocks, defaults.source_segment_blocks);
        assert_eq!(pipeline.max_pending_bytes, defaults.max_pending_bytes);
        assert_eq!(pipeline.max_source_bytes, defaults.max_source_bytes);
    }

    /// The Replicator shares the CompactTxStreamer listener, so it has no listener flag.
    #[test]
    fn replicate_listen_is_not_a_flag() {
        let error =
            Cli::try_parse_from(["ztreamerd", "--replicate-listen", "127.0.0.1:9068"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn export_snapshot_is_a_subcommand_beside_the_daemon_flags() {
        let export = ["export-snapshot", "--out", "snapshot", "--signing-key", "key"];
        let cli = Cli::parse_from(["ztreamerd"].into_iter().chain(export));
        assert!(matches!(cli.command, Some(Command::ExportSnapshot(_))));
        assert_eq!(cli.daemon.index_dir, PathBuf::from("ztreamer-index"));
        assert!(
            Cli::try_parse_from(["ztreamerd", "--index-only"].into_iter().chain(export)).is_err()
        );
        assert!(Cli::try_parse_from(["ztreamerd", "export-snapshot", "--out", "x"]).is_err());
    }

    #[test]
    fn export_snapshot_reads_the_global_index_flags() {
        let export = ["export-snapshot", "--out", "snapshot", "--signing-key", "key"];
        let index = ["--index-dir", "index", "--index-map-size", "4096"];
        let cli = Cli::parse_from(["ztreamerd"].into_iter().chain(export).chain(index));
        assert!(matches!(cli.command, Some(Command::ExportSnapshot(_))));
        assert_eq!(cli.daemon.index_dir, PathBuf::from("index"));
        assert_eq!(cli.daemon.index_map_size.get(), 4096);
    }

    #[test]
    fn export_snapshot_refuses_a_directory_with_no_index() {
        let dir = tempfile::tempdir().unwrap();
        let cli =
            Cli::parse_from(["ztreamerd".as_ref(), "--index-dir".as_ref(), dir.path().as_os_str()]);
        let error = cli.daemon.open_existing_index().unwrap_err();
        assert!(format!("{error:?}").contains("holds no index"), "{error:?}");
    }
}
