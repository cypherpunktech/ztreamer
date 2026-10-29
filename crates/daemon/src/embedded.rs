//! The embedded mode: `ztreamerd` runs a Zakura node in this process. It reads the tail from the
//! finalized database of the node, and the head and the chain state from the node. The node also
//! serves the compact service to its p2p peers.

use std::{ops::ControlFlow, path::PathBuf, sync::Arc};

use eyre::{Result, WrapErr, eyre};
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakurad::config::ZakuradConfig;
use ztreamer_embedded::{self as node, FinalizedDb, InProcessBackend};
use ztreamer_p2p::P2pCompactService;

use crate::{
    IndexConfig,
    lifecycle::{Supervisor, Tasks},
    node_mode::{NodeMode, NodeSources, ReadyNode, Started},
};

/// The embedded mode: a Zakura node in this process, from its configuration.
#[derive(Debug)]
pub(crate) struct EmbeddedMode {
    config: ZakuradConfig,
}

impl EmbeddedMode {
    /// Loads the Zakura configuration of `zakura_config`. Zakura defaults and `ZAKURA_*`
    /// variables apply.
    pub(crate) fn load(zakura_config: Option<PathBuf>) -> Result<Self> {
        let config = ZakuradConfig::load(zakura_config)
            .map_err(|error| eyre!(error))
            .wrap_err("load Zakura configuration")?;
        Ok(Self { config })
    }

    /// Waits until the Zakura of `client` is near the chain tip and has a finalized block. Then
    /// the node is ready for the historical index pass. Breaks on shutdown.
    ///
    /// The remote mode waits in `RemoteMode::sources`, before it makes its sources.
    async fn wait_ready(
        client: &node::NodeClient,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<()>> {
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

impl NodeMode for EmbeddedMode {
    type Tail = FinalizedDb;
    type Head = node::NodeClient;
    type Backend = InProcessBackend;

    /// Opens the index of the network of the Zakura configuration. Then gives Zakura as the node
    /// task, and a startup that waits for the services of Zakura and for [`Self::wait_ready`].
    async fn start(
        self,
        index: &IndexConfig,
        shutdown: &CancellationToken,
    ) -> Result<
        Started<
            impl Tasks,
            impl Future<
                Output = Result<
                    ControlFlow<(), ReadyNode<FinalizedDb, node::NodeClient, InProcessBackend>>,
                >,
            >,
        >,
    > {
        let Self { config } = self;
        let network = config.network.network.clone();
        let index = index.open(&network).await?;

        let p2p = Arc::new(P2pCompactService::pending());
        info!(network = %network, "starting embedded Zakura");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let node = {
            let (p2p, shutdown) = (Arc::clone(&p2p), shutdown.clone());
            async move {
                // Keep Zakura's large startup future on the heap while polling it here.
                Box::pin(zakurad::node::run_with_services_ready(
                    config,
                    vec![p2p.registration()],
                    shutdown,
                    ready_tx,
                ))
                .await
                .map_err(|error| eyre!("Zakura failed: {error}"))
            }
        };
        let shutdown = shutdown.clone();
        let startup = async move {
            let services = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
                result = ready_rx => result.wrap_err("Zakura stopped before readiness")?,
            };
            let client = node::NodeClient::from_services(services);
            let sources = NodeSources {
                tail: client.finalized_db(),
                head: client.clone(),
                backend: InProcessBackend::with_node(client),
                verified_by: node::NodeClient::embedded_id(),
            };
            if Self::wait_ready(&sources.head, &shutdown).await?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
            let embedded = Some(Embedded { p2p });
            Ok(ControlFlow::Continue(ReadyNode { network, index, sources, embedded }))
        };
        let node = Supervisor::new().until_shutdown(node, "Zakura stopped unexpectedly");
        Ok(Started { node, startup })
    }
}

/// What only the embedded mode gives the daemon.
pub(crate) struct Embedded {
    /// The Zakura p2p compact service. It gets the compact service after the head reconcile.
    pub(crate) p2p: Arc<P2pCompactService>,
}
