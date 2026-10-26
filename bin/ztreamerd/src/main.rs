//! `ztreamerd` process lifecycle: embedded Zakura, historical indexing, head following, and servers.

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

use clap::Parser;
use eyre::{Result, WrapErr, eyre};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity, Server, ServerTlsConfig};
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

mod lifecycle;

/// Must be large enough to hold full mainnet index, which is 21 GiB at the time of writing
const DEFAULT_MAP_SIZE: usize = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(version, about = "CompactTxStreamer daemon backed by embedded Zakura")]
struct Cli {
    /// Zakura TOML configuration. Zakura defaults and ZAKURA_* variables apply when omitted.
    #[arg(long, value_name = "FILE")]
    zakura_config: Option<PathBuf>,

    /// Directory containing the LMDB compact index.
    #[arg(long, default_value = "ztreamer-index", value_name = "DIR")]
    index_dir: PathBuf,

    /// Maximum LMDB map size in bytes.
    #[arg(long, default_value_t = NonZeroUsize::new(DEFAULT_MAP_SIZE).unwrap())]
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

    /// Write a `range_meta` row for each sealed range in the background. Then the index can be a
    /// snapshot source.
    #[arg(long)]
    fill_range_meta: bool,

    /// Replicator gRPC listener. It serves the sealed ranges that have a `range_meta` row to a
    /// replica, with the TLS settings of the CompactTxStreamer server. Off when not set.
    #[arg(long, value_name = "ADDR")]
    replicate_listen: Option<SocketAddr>,
}

impl From<&Cli> for PipelineConfig {
    /// Starts from the pipeline defaults and applies the limits given on the command line.
    fn from(cli: &Cli) -> Self {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Read and validate the identity before starting the node or the potentially lengthy index.
    let grpc_server = grpc_server(cli.tls_cert.as_deref(), cli.tls_key.as_deref())?;

    let mut config =
        ZakuradConfig::load(cli.zakura_config.clone()).wrap_err("load Zakura configuration")?;
    config.metrics.endpoint_addr = Some(cli.metrics_listen);
    let _metrics = MetricsEndpoint::new(&config.metrics).wrap_err("start Prometheus endpoint")?;
    let network = config.network.network.clone();
    let index = Arc::new(Index::open(
        &cli.index_dir,
        cli.index_map_size.get(),
        &network.to_string(),
        network.genesis_hash(),
    )?);

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
        run_application(cli, grpc_server, index, &network, p2p.clone(), sources, shutdown.clone())
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

/// The wait until a node mode can serve the daemon.
trait Readiness {
    /// Waits until the node is ready for the historical index pass. Breaks on shutdown.
    async fn wait_ready(&self, shutdown: &CancellationToken) -> Result<ControlFlow<()>>;
}

impl<T, B> Readiness for NodeSources<T, node::NodeClient, B> {
    /// Waits until Zakura is near the chain tip and has a finalized block. Breaks on shutdown.
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

async fn run_application<T, H, B>(
    cli: Cli,
    grpc_server: Server,
    index: Arc<Index>,
    network: &Network,
    p2p: Arc<P2pCompactService>,
    sources: NodeSources<T, H, B>,
    shutdown: CancellationToken,
) -> Result<()>
where
    NodeSources<T, H, B>: Readiness,
    T: HistoricalSource + Send + 'static,
    H: CanonicalBlockSource + Clone + 'static,
    B: StateBackend,
{
    if sources.wait_ready(&shutdown).await?.is_break() {
        return Ok(());
    }
    let NodeSources { tail, head, backend } = sources;

    let pipeline = PipelineConfig::from(&cli);
    let ControlFlow::Continue(state) =
        sync_historical(tail, &index, pipeline, cli.index_only, &shutdown).await?
    else {
        return Ok(());
    };
    if cli.index_only || shutdown.is_cancelled() {
        return Ok(());
    }

    let filler_index = cli.fill_range_meta.then(|| Arc::clone(&index));
    let replicate_index = cli.replicate_listen.map(|_| Arc::clone(&index));
    let compact = CompactService::with_backend(index, state, network.bip70_network_name(), backend)
        .with_ping_enabled(cli.ping_very_insecure);
    if reconcile_head(&compact, head.clone(), pipeline, &shutdown).await?.is_break() {
        return Ok(());
    }
    p2p.install(compact.clone()).map_err(|error| eyre!(error))?;
    drop(p2p);

    let extras = Extras {
        filler: filler_index.map(|index| {
            RangeMetaFiller::embedded(index, compact.backend()).run(compact.subscribe())
        }),
        replicator: replicate_index
            .map(|index| ReplicatorService::new(index, compact.subscribe(), network)),
    };
    serve(compact, head, pipeline, grpc_server, &cli, extras, &shutdown).await
}

/// The optional tasks that run beside the head follower and the CompactTxStreamer server.
struct Extras<F> {
    /// The `range_meta` filler, with `--fill-range-meta`.
    filler: Option<F>,
    /// The Replicator service, with `--replicate-listen`.
    replicator: Option<ReplicatorService>,
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

/// Runs the head follower and gRPC server until shutdown or until either task stops. If the
/// filler is set, runs it beside them and stops it with them. If the Replicator is set, its
/// server runs in the gRPC task, and the task stops when either server stops.
async fn serve(
    compact: CompactService,
    source: impl CanonicalBlockSource + 'static,
    pipeline: PipelineConfig,
    mut grpc_server: Server,
    cli: &Cli,
    extras: Extras<impl Future<Output = ()> + Send + 'static>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let filler = extras.filler.map(tokio::spawn);
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
        let replicate = extras.replicator.zip(cli.replicate_listen).map(|(replicator, address)| {
            info!(%address, tls, "serving Replicator gRPC");
            grpc_server
                .clone()
                .add_service(ReplicatorServer::new(replicator))
                .serve_with_shutdown(address, shutdown_requested(server_shutdown.subscribe()))
        });
        async move {
            info!(%address, tls, "serving CompactTxStreamer gRPC");
            let streamer = grpc_server
                .add_service(CompactTxStreamerServer::new(compact))
                .serve_with_shutdown(address, shutdown_requested(receiver));
            match replicate {
                Some(replicate) => tokio::try_join!(streamer, replicate).map(drop),
                None => streamer.await,
            }
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
        let defaults = PipelineConfig::default();
        let pipeline = PipelineConfig::from(&cli);
        assert_eq!(pipeline.workers, 3);
        assert_eq!(pipeline.max_batch_bytes, 4096);
        assert_eq!(pipeline.source_segment_blocks, defaults.source_segment_blocks);
        assert_eq!(pipeline.max_pending_bytes, defaults.max_pending_bytes);
        assert_eq!(pipeline.max_source_bytes, defaults.max_source_bytes);
    }

    #[test]
    fn replicate_listen_is_off_by_default() {
        assert_eq!(Cli::parse_from(["ztreamerd"]).replicate_listen, None);
        let cli = Cli::parse_from(["ztreamerd", "--replicate-listen", "127.0.0.1:9068"]);
        assert_eq!(cli.replicate_listen, Some(SocketAddr::from(([127, 0, 0, 1], 9068))));
    }
}
