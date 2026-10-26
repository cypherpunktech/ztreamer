//! The `--remote-rpc` mode: `ztreamerd` reads the chain from a remote zakura node over JSON-RPC
//! and starts no embedded node.

use std::{ops::ControlFlow, path::PathBuf, sync::Arc, time::Duration};

use clap::Args;
use eyre::{Result, WrapErr, eyre};
use http::Uri;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{info, warn};
use zakura_chain::{block::Height, parameters::Network};
use zakura_rpc::client::GetBlockchainInfoResponse;
use zakurad::components::metrics::{self, MetricsEndpoint};
use ztreamer_indexer::{
    index::{BlockId, Index},
    pipeline::RemoteTailSource,
};
use ztreamer_remote::{Auth, RpcClient, RpcCode, Secret, ZakuraClient};
use ztreamer_service::remote::{RemoteBackend, RemoteHeadSource};

use crate::{DaemonArgs, NodeSources, Readiness, lifecycle, run_application};

/// The time between two `getblockchaininfo` calls while the remote node is not ready.
const READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The sources of the remote mode. All three read the same node. Only
/// [`RemoteNode`] makes them, when the node is ready for the index.
type RemoteSources = NodeSources<RemoteTailSource, RemoteHeadSource, RemoteBackend>;

/// The command-line flags of the remote mode. All are off by default.
#[derive(Debug, Args)]
pub(crate) struct RemoteArgs {
    /// JSON-RPC URL of a remote zakura node. ztreamerd then reads the chain from this node and
    /// starts no embedded node.
    #[arg(long, value_name = "URL", conflicts_with_all = ["zakura_config", "fill_range_meta"])]
    remote_rpc: Option<Uri>,

    /// Cookie file of the --remote-rpc node.
    #[arg(long, value_name = "FILE", requires = "remote_rpc", conflicts_with = "remote_rpc_user")]
    remote_rpc_cookie: Option<PathBuf>,

    /// User for HTTP basic auth to the --remote-rpc node.
    #[arg(long, value_name = "USER", requires_all = ["remote_rpc", "remote_rpc_password"])]
    remote_rpc_user: Option<String>,

    /// Password for HTTP basic auth to the --remote-rpc node. Other users of the host can read
    /// the command line, thus prefer --remote-rpc-cookie.
    #[arg(long, value_name = "PASSWORD", requires = "remote_rpc_user")]
    remote_rpc_password: Option<Secret>,

    /// PEM-encoded CA certificates for an https --remote-rpc URL, in addition to the web PKI
    /// roots.
    #[arg(long, value_name = "FILE", requires = "remote_rpc")]
    remote_rpc_ca: Option<PathBuf>,

    /// Network of the index and of the --remote-rpc node: mainnet or testnet. Defaults to
    /// mainnet.
    #[arg(long, requires = "remote_rpc")]
    network: Option<Network>,
}

impl RemoteArgs {
    /// The node of `--remote-rpc`, or `None` without that flag. It reads the cookie file, but it
    /// does not call the node.
    pub(crate) fn node(&self) -> Result<Option<RemoteNode>> {
        let Some(url) = self.remote_rpc.clone() else {
            return Ok(None);
        };
        let auth = match (&self.remote_rpc_cookie, &self.remote_rpc_user, &self.remote_rpc_password)
        {
            (Some(cookie), ..) => Auth::Cookie(cookie.clone()),
            (None, Some(user), Some(password)) => {
                Auth::Basic { user: user.clone(), password: password.clone() }
            }
            _ => Auth::None,
        };
        let config = ZakuraClient::config(url).with_auth(auth);
        let config = match &self.remote_rpc_ca {
            Some(ca) => config.with_tls_ca_file(ca),
            None => config,
        };
        let rpc = RpcClient::new(config).wrap_err("configure the --remote-rpc client")?;
        let network = self.network.clone().unwrap_or(Network::Mainnet);
        Ok(Some(RemoteNode::new(ZakuraClient::new(rpc), network)))
    }
}

/// A remote zakura node that replaces the embedded node.
#[derive(Clone, Debug)]
pub(crate) struct RemoteNode {
    client: ZakuraClient,
    network: Network,
}

impl RemoteNode {
    /// The node behind `client`, which must run `network`.
    pub(crate) const fn new(client: ZakuraClient, network: Network) -> Self {
        Self { client, network }
    }

    /// Runs the daemon on this node until `signal`, the end of the work, or a failure. It starts
    /// the Prometheus endpoint, then calls [`Self::run_until`].
    pub(crate) async fn run(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        // No zakurad runs, thus the metrics endpoint is the only part of its configuration.
        let metrics = metrics::Config { endpoint_addr: Some(cli.metrics_listen) };
        let _metrics = MetricsEndpoint::new(&metrics).wrap_err("start Prometheus endpoint")?;
        self.run_until(cli, grpc_server, signal).await
    }

    /// Opens the index as the embedded mode does. Then runs the daemon on this node until
    /// `signal`, the end of the work (`--index-only`), or a failure.
    ///
    /// A stop during the historical pass waits for the `getblock` calls in flight. When the node
    /// stalls, each call can wait for its timeout and its retries, thus more than one minute.
    async fn run_until(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let index = cli.open_index(&self.network).await?;
        let url = self.client.rpc().url();
        info!(network = %self.network, host = url.host(), port = url.port_u16(), "using remote zakura");
        let shutdown = CancellationToken::new();
        // No node runs in this process. Thus the node half of the lifecycle only waits for the
        // shutdown, and `supervise` can drain it.
        let node = {
            let shutdown = shutdown.clone();
            async move {
                shutdown.cancelled().await;
                Ok(())
            }
        };
        let application = self.application(cli, grpc_server, index, shutdown.clone());
        lifecycle::supervise(node, application, shutdown, signal).await
    }

    /// Makes the sources when the node is ready, then runs the daemon on them until shutdown.
    async fn application(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        index: Arc<Index>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let ControlFlow::Continue(sources) = self.sources(&index, &shutdown).await? else {
            return Ok(());
        };
        run_application(cli, grpc_server, index, &self.network, None, sources, shutdown).await
    }

    /// The sources over this node, when the node is ready for `index`. Breaks on shutdown.
    ///
    /// It waits while the node is out of reach or in warm-up. Then it refuses a node that does
    /// not run the network of the index, and a node that pruned the next block that the index
    /// needs. Last, it waits until the best block of the node is at or above the durable tip of
    /// the index.
    async fn sources(
        &self,
        index: &Arc<Index>,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), RemoteSources>> {
        let ControlFlow::Continue(mut info) = self.blockchain_info_when_up(shutdown).await? else {
            return Ok(ControlFlow::Break(()));
        };
        let (backend, poller) = RemoteBackend::connect(self.client.clone(), self.network.clone())
            .await
            .wrap_err("connect to the remote zakura node")?;
        let tip = IndexTip::read(index).await?;
        while !tip.served_by(&info)? {
            info!(
                best = info.blocks().0,
                index_tip = tip.0.map(|tip| tip.height),
                "waiting for remote zakura to reach the index tip"
            );
            if shutdown.run_until_cancelled(tokio::time::sleep(READY_POLL_INTERVAL)).await.is_none()
            {
                return Ok(ControlFlow::Break(()));
            }
            let ControlFlow::Continue(next) = self.blockchain_info_when_up(shutdown).await? else {
                return Ok(ControlFlow::Break(()));
            };
            info = next;
        }
        info!(best = info.blocks().0, "remote zakura is ready");
        tokio::spawn(poller.run());
        Ok(ControlFlow::Continue(NodeSources {
            tail: RemoteTailSource::new(self.client.clone(), Handle::current()),
            head: RemoteHeadSource::new(self.client.clone()),
            backend,
        }))
    }

    /// `getblockchaininfo`, sent again after [`READY_POLL_INTERVAL`] while the node is out of
    /// reach or answers [`RpcCode::IN_WARMUP`]. Breaks on shutdown.
    async fn blockchain_info_when_up(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), GetBlockchainInfoResponse>> {
        loop {
            let Some(result) =
                shutdown.run_until_cancelled(self.client.get_blockchain_info()).await
            else {
                return Ok(ControlFlow::Break(()));
            };
            match result {
                Err(error) if error.rpc_code() == Some(RpcCode::IN_WARMUP) => {
                    info!(%error, "waiting for remote zakura to end its warm-up");
                }
                Err(error) if error.is_unsent() => {
                    warn!(%error, "waiting for remote zakura to accept connections");
                }
                result => {
                    return result
                        .map(ControlFlow::Continue)
                        .wrap_err("read the chain of the remote zakura node");
                }
            }
            if shutdown.run_until_cancelled(tokio::time::sleep(READY_POLL_INTERVAL)).await.is_none()
            {
                return Ok(ControlFlow::Break(()));
            }
        }
    }
}

impl Readiness for RemoteSources {
    /// Ready at once: [`RemoteNode`] makes these sources only when the node is ready for the
    /// index.
    async fn wait_ready(&self, _shutdown: &CancellationToken) -> Result<ControlFlow<()>> {
        Ok(ControlFlow::Continue(()))
    }
}

/// The durable tip of the index. The remote node must serve the blocks above it before the first
/// sync.
#[derive(Clone, Copy, Debug)]
struct IndexTip(Option<BlockId>);

impl IndexTip {
    /// Reads the durable tip of `index` on a blocking thread.
    async fn read(index: &Arc<Index>) -> Result<Self> {
        let index = Arc::clone(index);
        let state = tokio::task::spawn_blocking(move || index.state())
            .await
            .wrap_err("index state task panicked")??;
        Ok(Self(state.durable_tip()))
    }

    /// True when the node of `info` has the tip, false while the node is below the tip.
    ///
    /// Refuses a node that pruned the next block that the index needs. The historical pass and
    /// the head follower read the blocks above the tip, thus the first of them must be at or
    /// above the prune height of the node.
    fn served_by(self, info: &GetBlockchainInfoResponse) -> Result<bool> {
        let next = self.0.map_or(0, |tip| tip.height.saturating_add(1));
        let prune_height = info.prune_height().unwrap_or(Height::MIN).0;
        if next < prune_height {
            return Err(eyre!(
                "the index needs block {next}, but the remote zakura node keeps block bodies \
                 only from height {prune_height}: use a node that keeps these blocks, or an \
                 index with a tip at or above {}",
                prune_height - 1
            ));
        }
        Ok(self.0.is_none_or(|tip| info.blocks().0 >= tip.height))
    }
}

#[cfg(test)]
mod tests;
