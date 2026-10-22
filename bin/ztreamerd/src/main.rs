//! `ztreamerd` process lifecycle: embedded Zakura, historical indexing, head following, and servers.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
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
use zakurad::{components::metrics::MetricsEndpoint, config::ZakuradConfig};
use ztreamer_indexer::{
    head::HeadSyncError, index::Index, pipeline::PipelineConfig, source::ZakuraSource,
};
use ztreamer_node as node;
use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamerServer;
use ztreamer_service::{CompactService, HeadFollowerConfig, p2p::P2pCompactService};

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
        network.genesis_hash().0,
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
        run_application(
            cli,
            grpc_server,
            index,
            network.bip70_network_name(),
            p2p.clone(),
            node::NodeClient::from_services(services),
            shutdown.clone(),
        )
        .await
    };

    lifecycle::supervise(node, application, shutdown.clone(), lifecycle::shutdown_signal()).await
}

async fn run_application(
    cli: Cli,
    mut grpc_server: Server,
    index: Arc<Index>,
    chain_name: String,
    p2p: Arc<P2pCompactService>,
    client: node::NodeClient,
    shutdown: CancellationToken,
) -> Result<()> {
    info!("waiting for Zakura to sync near the chain tip");
    tokio::select! {
        result = client.wait_until_close_to_tip() => {
            result.map_err(|error| eyre!("wait for Zakura sync: {error}"))?;
        }
        _ = shutdown.cancelled() => return Ok(()),
    }
    info!(tip = ?client.tip(), "Zakura is near the chain tip");

    while client.database().tip().is_none() {
        info!("waiting for Zakura's first finalized block");
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }

    let mut pipeline = PipelineConfig::default();
    if let Some(workers) = cli.workers {
        pipeline.workers = workers.get();
    }
    if let Some(source_segment_blocks) = cli.source_segment_blocks {
        pipeline.source_segment_blocks = source_segment_blocks.get();
    }
    if let Some(max_pending_bytes) = cli.max_pending_bytes {
        pipeline.max_pending_bytes = max_pending_bytes.get();
    }
    if let Some(max_batch_bytes) = cli.max_batch_bytes {
        pipeline.max_batch_bytes = max_batch_bytes.get();
    }
    let source = ZakuraSource::new(client.database());
    let sync_index = Arc::clone(&index);
    info!("syncing historical compact index");
    let historical_started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let mut historical = tokio::task::spawn_blocking({
        let stop = Arc::clone(&stop);
        move || source.sync(&sync_index, pipeline, &stop)
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
            if cli.index_only {
                return Err(eyre!(
                    "historical compact index stopped by shutdown before it completed"
                ));
            }
            return Ok(());
        }
    };
    info!(
        height = state.durable_tip().map(|tip| tip.height),
        elapsed_seconds = historical_started.elapsed().as_secs_f64(),
        "historical compact index complete"
    );

    if cli.index_only || shutdown.is_cancelled() {
        return Ok(());
    }

    let compact = CompactService::with_node(index, state, chain_name, client.clone())
        .with_ping_enabled(cli.ping_very_insecure);
    let mut startup_client = client.clone();
    let reconcile = async {
        match compact.sync_head(&mut startup_client, pipeline).await {
            Err(HeadSyncError::DeepReorg { .. }) => {
                compact.recover_deep_reorg(&mut startup_client, pipeline).await
            }
            result => result,
        }
        .wrap_err("reconcile compact index with Zakura's canonical head")
    };
    tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        result = reconcile => { result?; }
    }
    p2p.install(compact.clone()).map_err(|error| eyre!(error))?;
    drop(p2p);

    let (server_shutdown, _) = watch::channel(false);
    let mut follower = tokio::spawn({
        let compact = compact.clone();
        let receiver = server_shutdown.subscribe();
        async move {
            compact
                .follow_head(client, pipeline, HeadFollowerConfig::default(), receiver)
                .await
                .wrap_err("head follower failed")
        }
    });
    let mut grpc = tokio::spawn({
        let receiver = server_shutdown.subscribe();
        async move {
            info!(
                address = %cli.grpc_listen,
                tls = cli.tls_cert.is_some(),
                "serving CompactTxStreamer gRPC"
            );
            grpc_server
                .add_service(CompactTxStreamerServer::new(compact))
                .serve_with_shutdown(cli.grpc_listen, shutdown_requested(receiver))
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

    // Drain both tasks before inspecting either result, including on failure.
    let (grpc_result, follower_result) = tokio::join!(
        async {
            match grpc_result {
                Some(result) => result,
                None => grpc.await,
            }
        },
        async {
            match follower_result {
                Some(result) => result,
                None => follower.await,
            }
        },
    );
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
