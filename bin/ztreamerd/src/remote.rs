//! The `--remote-rpc` mode: `ztreamerd` reads the chain from a remote zakura node over JSON-RPC
//! and starts no embedded node.

use std::{future, ops::ControlFlow, path::PathBuf, sync::Arc, time::Duration};

use clap::Args;
use eyre::{Result, WrapErr, eyre};
use http::Uri;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;
use zakura_chain::{block::Height, parameters::Network};
use zakura_rpc::client::GetBlockchainInfoResponse;
use zakurad::components::metrics::{self, MetricsEndpoint};
use ztreamer_indexer::{index::Index, pipeline::RemoteTailSource};
use ztreamer_remote::{Auth, RpcClient, RpcCode, Secret, ZakuraClient};
use ztreamer_service::remote::{RemoteBackend, RemoteHeadSource};

use crate::{Cli, NodeSources, Readiness, lifecycle, run_application};

/// The time between two `getblockchaininfo` calls while the remote node is not ready.
const READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The sources of the remote mode. All three read the same node.
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

    /// Runs the daemon on this node until `signal` or a failure. It starts the Prometheus
    /// endpoint and opens the index as the embedded mode does.
    ///
    /// A stop during the historical pass waits for the `getblock` calls in flight. When the node
    /// stalls, each call can wait for its timeout and its retries, thus more than one minute.
    pub(crate) async fn run(
        self,
        cli: Cli,
        grpc_server: Server,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        // No zakurad runs, thus the metrics endpoint is the only part of its configuration.
        let metrics = metrics::Config { endpoint_addr: Some(cli.metrics_listen) };
        let _metrics = MetricsEndpoint::new(&metrics).wrap_err("start Prometheus endpoint")?;
        let index = cli.open_index(&self.network)?;
        let url = self.client.rpc().url();
        info!(network = %self.network, host = url.host(), port = url.port_u16(), "using remote zakura");
        let shutdown = CancellationToken::new();
        let application = self.application(cli, grpc_server, index, shutdown.clone());
        // No node runs in this process, thus the node half of the lifecycle does not end.
        lifecycle::supervise(future::pending(), application, shutdown, signal).await
    }

    /// Connects to the node, then runs the daemon on the remote sources until shutdown.
    pub(crate) async fn application(
        self,
        cli: Cli,
        grpc_server: Server,
        index: Arc<Index>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let ControlFlow::Continue(sources) = self.sources(&shutdown).await? else {
            return Ok(());
        };
        run_application(cli, grpc_server, index, &self.network, None, sources, shutdown).await
    }

    /// The sources over this node. Waits until the node is out of warm-up, then refuses a node
    /// that does not run the network of the index. Breaks on shutdown.
    async fn sources(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), RemoteSources>> {
        if self.client.blockchain_info_after_warmup(shutdown).await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
        let (backend, poller) = RemoteBackend::connect(self.client.clone(), self.network.clone())
            .await
            .wrap_err("check the network of the remote zakura node")?;
        tokio::spawn(poller.run());
        Ok(ControlFlow::Continue(NodeSources {
            tail: RemoteTailSource::new(self.client.clone(), Handle::current()),
            head: RemoteHeadSource::new(self.client.clone()),
            backend,
        }))
    }
}

impl Readiness for RemoteSources {
    /// Waits until the node is out of warm-up and its best block is at or above the durable tip
    /// of the index. Breaks on shutdown.
    ///
    /// Refuses an index that needs a block body that the node pruned. The historical pass and the
    /// head follower read the blocks above the durable tip, thus the first of them must be at or
    /// above the prune height of the node.
    async fn wait_ready(
        &self,
        index: &Arc<Index>,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<()>> {
        let state_index = Arc::clone(index);
        let durable_tip = tokio::task::spawn_blocking(move || state_index.state())
            .await
            .wrap_err("index state task panicked")??
            .durable_tip();
        let next = durable_tip.map_or(0, |tip| tip.height.saturating_add(1));
        let node = self.head.node();
        loop {
            let ControlFlow::Continue(info) = node.blockchain_info_after_warmup(shutdown).await?
            else {
                return Ok(ControlFlow::Break(()));
            };
            let prune_height = info.prune_height().unwrap_or(Height::MIN).0;
            if next < prune_height {
                return Err(eyre!(
                    "the index needs block {next}, but the remote zakura node keeps block bodies \
                     only from height {prune_height}: use a node that keeps these blocks, or an \
                     index with a tip at or above {}",
                    prune_height - 1
                ));
            }
            let best = info.blocks().0;
            if durable_tip.is_none_or(|tip| best >= tip.height) {
                info!(best, "remote zakura is ready");
                return Ok(ControlFlow::Continue(()));
            }
            info!(best, index_tip = next - 1, "waiting for remote zakura to reach the index tip");
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
                () = tokio::time::sleep(READY_POLL_INTERVAL) => {}
            }
        }
    }
}

/// The wait for the end of the warm-up of a zakura node.
trait WarmUp {
    /// `getblockchaininfo`, sent again after [`READY_POLL_INTERVAL`] while the node answers
    /// [`RpcCode::IN_WARMUP`]. Breaks on shutdown.
    async fn blockchain_info_after_warmup(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), GetBlockchainInfoResponse>>;
}

impl WarmUp for ZakuraClient {
    async fn blockchain_info_after_warmup(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), GetBlockchainInfoResponse>> {
        loop {
            let result = tokio::select! {
                _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
                result = self.get_blockchain_info() => result,
            };
            match result {
                Err(error) if error.rpc_code() == Some(RpcCode::IN_WARMUP) => {
                    info!(%error, "waiting for remote zakura to end its warm-up");
                }
                result => {
                    return result
                        .map(ControlFlow::Continue)
                        .wrap_err("read the chain of the remote zakura node");
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(ControlFlow::Break(())),
                () = tokio::time::sleep(READY_POLL_INTERVAL) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
