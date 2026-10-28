//! The embedded mode: `ztreamerd` runs a Zakura node in this process. It reads the tail from the
//! finalized database of the node, and the head and the chain state from the node. The node also
//! serves the compact service to its p2p peers.

use std::{ops::ControlFlow, sync::Arc};

use eyre::{Result, WrapErr, eyre};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;
use zakurad::{components::metrics, config::ZakuradConfig};
use ztreamer_node::{self as node, InProcessBackend, p2p::P2pCompactService};

use crate::{DaemonArgs, NodeSources, lifecycle, run_application};

/// An embedded Zakura node, from its configuration.
#[derive(Debug)]
pub(crate) struct EmbeddedNode {
    config: ZakuradConfig,
}

impl EmbeddedNode {
    /// Loads the Zakura configuration of `--zakura-config`. Zakura defaults and `ZAKURA_*`
    /// variables apply.
    pub(crate) fn load(cli: &DaemonArgs) -> Result<Self> {
        let config = ZakuradConfig::load(cli.zakura_config.clone())
            .map_err(|error| eyre!(error))
            .wrap_err("load Zakura configuration")?;
        Ok(Self { config })
    }

    /// Runs the daemon on this node until `signal`, the end of the work, or a failure. It starts
    /// the Prometheus endpoint, opens the index, and starts Zakura.
    pub(crate) async fn run(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let Self { mut config } = self;
        config.metrics.endpoint_addr = Some(cli.metrics_listen);
        metrics::start_endpoint(&config.metrics);
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
                tail: client.finalized_db(),
                head: client.clone(),
                backend: InProcessBackend::with_node(client),
                verified_by: node::NodeClient::embedded_id(),
            };
            if sources.wait_ready(&shutdown).await?.is_break() {
                return Ok(());
            }
            let embedded = Some(Embedded { p2p: p2p.clone() });
            run_application(cli, grpc_server, index, &network, embedded, sources, shutdown.clone())
                .await
        };

        lifecycle::supervise(node, application, shutdown.clone(), signal).await
    }
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
pub(crate) struct Embedded {
    /// The Zakura p2p compact service. It gets the compact service after the head reconcile.
    pub(crate) p2p: Arc<P2pCompactService>,
}
