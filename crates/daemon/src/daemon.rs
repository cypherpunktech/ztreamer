//! The entry of the daemon: it selects the node mode, opens the metrics endpoint and runs the
//! daemon until shutdown.

use std::path::Path;

use eyre::{Result, WrapErr, eyre};
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::{DaemonArgs, embedded::EmbeddedNode, lifecycle, metrics_endpoint::MetricsEndpoint};

/// Runs the daemon of `cli` until a shutdown signal, the end of the work (`--index-only`), or a
/// failure.
#[tokio::main]
pub async fn run_daemon(cli: DaemonArgs) -> Result<()> {
    // Read and validate the identity before starting the node or the potentially lengthy index.
    let grpc_server = grpc_server(cli.tls_cert.as_deref(), cli.tls_key.as_deref())?;
    if let Some(node) = cli.remote.node(&cli.replicate)? {
        MetricsEndpoint::new(cli.metrics_listen).start()?;
        return node.run_until(cli, grpc_server, lifecycle::shutdown_signal()).await;
    }
    let node = EmbeddedNode::load(&cli)?;
    MetricsEndpoint::new(cli.metrics_listen).start()?;
    node.run(cli, grpc_server, lifecycle::shutdown_signal()).await
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
