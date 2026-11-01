//! The embedded node: `ztreamerd` runs a Zakura node in this process. The historical stage and
//! the catch-up read the finalized database of the node. The head and the chain state come from
//! the node. The node also serves the wallet service to its p2p peers.

use std::{ops::ControlFlow, path::PathBuf, sync::Arc};

use eyre::{Result, WrapErr, ensure, eyre};
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakura_chain::parameters::Network;
use zakurad::config::ZakuradConfig;
use ztreamer_embedded::{FinalizedDb, InProcessBackend, NodeClient};
use ztreamer_index::index::range_meta::NodeId;
use ztreamer_p2p::P2pCompactService;
use ztreamer_service::{CompactService, NodeVersion};

use crate::{
    index_pass::{IndexPass, PassKind},
    lifecycle::TaskSet,
    node::{HistoricalSource, IndexTip, Node, NodeComponents, StageContext, Tail},
};

/// An embedded Zakura node, from its configuration.
#[derive(Debug)]
pub(crate) struct EmbeddedNode {
    config: ZakuradConfig,
}

impl EmbeddedNode {
    /// Loads the Zakura configuration of `zakura_config`. Zakura defaults and `ZAKURA_*`
    /// variables apply. It fails when the configuration sets the tracing or the metrics of
    /// Zakura: see [`reject_zakura_telemetry`].
    pub(crate) fn load(zakura_config: Option<PathBuf>) -> Result<Self> {
        let config = ZakuradConfig::load(zakura_config)
            .map_err(|error| eyre!(error))
            .wrap_err("load Zakura configuration")?;
        reject_zakura_telemetry(&config)?;
        Ok(Self { config })
    }

    /// Waits until the Zakura of `client` is near the chain tip and has a finalized block. Then
    /// the node is ready for the historical stage. Breaks on shutdown.
    async fn wait_ready(
        client: &NodeClient,
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

impl Node for EmbeddedNode {
    type Components = EmbeddedComponents;

    /// The network of the Zakura configuration. It does not wait.
    async fn network(&mut self, _: &CancellationToken) -> Result<ControlFlow<(), Network>> {
        Ok(ControlFlow::Continue(self.config.network.network.clone()))
    }

    /// Starts Zakura on a thread of the blocking pool: the future of Zakura is not `Send`. Then
    /// waits for the services of Zakura and for [`EmbeddedNode::wait_ready`].
    async fn boot(self, tasks: &TaskSet) -> Result<ControlFlow<(), EmbeddedComponents>> {
        let Self { config } = self;
        let shutdown = tasks.shutdown();
        let p2p = Arc::new(P2pCompactService::pending());
        info!(network = %config.network.network, "starting embedded Zakura");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let services = vec![p2p.registration()];
        let zakura_shutdown = shutdown.clone();
        tasks.spawn_on_blocking_thread(
            "Zakura",
            "Zakura stopped unexpectedly",
            move || async move {
                // Keep Zakura's large startup future on the heap while polling it.
                Box::pin(zakurad::node::run_with_services_ready(
                    config,
                    services,
                    zakura_shutdown,
                    ready_tx,
                ))
                .await
                .map_err(|error| eyre!("Zakura failed: {error}"))
            },
        );

        let services = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
            result = ready_rx => result.wrap_err("Zakura stopped before readiness")?,
        };
        let client = NodeClient::from_services(services);
        if Self::wait_ready(&client, shutdown).await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(EmbeddedComponents { client, p2p }))
    }
}

/// The components of a booted embedded node.
pub(crate) struct EmbeddedComponents {
    client: NodeClient,
    /// The Zakura p2p compact service. It gets the wallet service when the index follows the
    /// head.
    p2p: Arc<P2pCompactService>,
}

impl NodeComponents for EmbeddedComponents {
    type Historical = FinalizedHistory;
    type Finalized = FinalizedDb;
    type Head = NodeClient;
    type Wallet = InProcessBackend;

    fn historical(&self) -> FinalizedHistory {
        FinalizedHistory(self.client.finalized_db())
    }

    async fn wallet(&self) -> Result<InProcessBackend> {
        Ok(InProcessBackend::with_node(self.client.clone()))
    }

    /// The finalized database and the node. Zakura serves the index tip after
    /// [`EmbeddedNode::wait_ready`]: thus it does not wait.
    async fn tail(
        &self,
        _: IndexTip,
        _: &CancellationToken,
    ) -> Result<ControlFlow<(), Tail<FinalizedDb, NodeClient>>> {
        let tail = Tail { finalized: self.client.finalized_db(), head: self.client.clone() };
        Ok(ControlFlow::Continue(tail))
    }

    /// The Zakura of this build. It does not call the node.
    async fn node_version(&self) -> Result<NodeVersion> {
        Ok(NodeClient::version())
    }

    fn id(&self) -> NodeId {
        NodeClient::embedded_id()
    }

    /// Gives `compact` to the p2p peers of Zakura.
    fn start_following(&self, compact: &CompactService) -> Result<()> {
        self.p2p.install(compact.clone()).map_err(|error| eyre!(error))
    }
}

/// The historical source of the embedded node: a block pipeline pass over the finalized
/// database of Zakura.
pub(crate) struct FinalizedHistory(FinalizedDb);

impl HistoricalSource for FinalizedHistory {
    async fn fill(self, stage: &StageContext<'_>) -> Result<ControlFlow<()>> {
        let pass = IndexPass { kind: PassKind::Historical, source: self.0 };
        Ok(pass.run(stage).await?.map_continue(|_| ()))
    }
}

/// Fails when `config` changes the `[tracing]` or the `[metrics]` section from its default.
/// `ztreamerd` installs the one subscriber and the one recorder of the process, and Zakura
/// records to them. Thus Zakura does not read these sections, and a value there has no effect.
fn reject_zakura_telemetry(config: &ZakuradConfig) -> Result<()> {
    ensure!(
        config.tracing == Default::default(),
        "the Zakura configuration sets [tracing], but ztreamerd does not read it: use \
         --log-filter, --log-format and --otlp-endpoint"
    );
    ensure!(
        config.metrics == Default::default(),
        "the Zakura configuration sets [metrics], but ztreamerd does not read it: use \
         --metrics-listen"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(toml: &str) -> Result<EmbeddedNode> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zakurad.toml");
        std::fs::write(&path, toml).unwrap();
        EmbeddedNode::load(Some(path))
    }

    #[test]
    fn loads_a_configuration_with_no_telemetry_section() {
        load("[network]\nnetwork = \"Testnet\"\n").unwrap();
    }

    #[test]
    fn rejects_a_tracing_section() {
        let error = load("[tracing]\nfilter = \"debug\"\n").unwrap_err();
        assert!(error.to_string().contains("[tracing]"), "{error:#}");
    }

    #[test]
    fn rejects_a_metrics_section() {
        let error = load("[metrics]\nendpoint_addr = \"127.0.0.1:9000\"\n").unwrap_err();
        assert!(error.to_string().contains("[metrics]"), "{error:#}");
    }
}
