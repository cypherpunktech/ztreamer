//! The entry of the daemon: it selects the node mode, opens the metrics endpoint and runs the
//! daemon until shutdown.

use eyre::Result;

use crate::{
    DaemonConfig, Mode, embedded::EmbeddedNode, lifecycle, metrics_endpoint::MetricsEndpoint,
};

/// Runs the daemon of `config` until a shutdown signal, the end of the work
/// ([`SyncConfig::index_only`](crate::SyncConfig::index_only)), or a failure.
#[tokio::main]
pub async fn run_daemon(config: DaemonConfig) -> Result<()> {
    // Read and validate the identity before starting the node or the potentially lengthy index.
    let grpc_server = config.servers.grpc_server()?;
    let metrics = MetricsEndpoint::new(config.servers.metrics_listen);
    match &config.mode {
        Mode::Remote(remote) => {
            let node = remote.node()?;
            metrics.start()?;
            node.run_until(config, grpc_server, lifecycle::shutdown_signal()).await
        }
        Mode::Embedded { zakura_config } => {
            let node = EmbeddedNode::load(zakura_config.clone())?;
            metrics.start()?;
            node.run(config, grpc_server, lifecycle::shutdown_signal()).await
        }
    }
}
