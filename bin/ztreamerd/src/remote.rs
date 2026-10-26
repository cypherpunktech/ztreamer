//! The `--remote-rpc` mode: `ztreamerd` reads the chain from a remote zakura node over JSON-RPC
//! and starts no embedded node.

use std::{
    fmt,
    net::IpAddr,
    ops::ControlFlow,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use clap::Args;
use eyre::{Result, WrapErr, eyre};
use http::Uri;
use percent_encoding::percent_decode_str;
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

use crate::{DaemonArgs, NodeSources, Readiness, lifecycle, replicate::Replica, run_application};

/// The time between two `getblockchaininfo` calls while the remote node is not ready.
const READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The sources of the remote mode. All three read the same node. Only
/// [`RemoteNode`] makes them, when the node is ready for the index.
type RemoteSources = NodeSources<RemoteTailSource, RemoteHeadSource, RemoteBackend>;

/// The command-line flags of the remote mode. All are off by default.
#[derive(Debug, Args)]
pub(crate) struct RemoteArgs {
    /// JSON-RPC URL of a remote zakura node. ztreamerd then reads the chain from this node and
    /// starts no embedded node. The node gives the network (mainnet or testnet), and the index
    /// must be of that network. For HTTP basic auth, put the credentials in the URL:
    /// https://user:password@host:8232. Without credentials and without --remote-rpc-cookie, a
    /// loopback URL uses the default cookie file of zakura if it exists.
    #[arg(long, value_name = "URL", conflicts_with = "zakura_config")]
    remote_rpc: Option<NodeUrl>,

    /// Cookie file of the --remote-rpc node.
    #[arg(long, value_name = "FILE", requires = "remote_rpc")]
    remote_rpc_cookie: Option<PathBuf>,
}

impl RemoteArgs {
    /// The node of `--remote-rpc`, or `None` without that flag. The default cookie file is the
    /// one of a zakura with the default configuration. It reads the cookie file, but it does
    /// not call the node.
    pub(crate) fn node(&self) -> Result<Option<RemoteNode>> {
        let rpc = zakura_rpc::config::rpc::Config::default();
        self.node_with_default_cookie(&rpc.cookie_dir.join(rpc.cookie_file_name))
    }

    /// The node of `--remote-rpc`, or `None` without that flag, with `default_cookie` as the
    /// default cookie file.
    fn node_with_default_cookie(&self, default_cookie: &Path) -> Result<Option<RemoteNode>> {
        let Some(url) = &self.remote_rpc else {
            return Ok(None);
        };
        let auth = self.auth(default_cookie)?;
        info!(?auth, "remote zakura credentials");
        let config = ZakuraClient::config(url.uri.clone()).with_auth(auth);
        let rpc = RpcClient::new(config).wrap_err("configure the --remote-rpc client")?;
        Ok(Some(RemoteNode::new(ZakuraClient::new(rpc))))
    }

    /// The credentials for the node: `--remote-rpc-cookie`, else the credentials of the URL,
    /// else `default_cookie` when the URL is a loopback URL and the file exists, else none.
    ///
    /// The default cookie applies only to a loopback URL, because the cookie of a local zakura
    /// gives access to that node: ztreamerd must not send it to another host.
    fn auth(&self, default_cookie: &Path) -> Result<Auth> {
        let Some(url) = &self.remote_rpc else {
            return Ok(Auth::None);
        };
        Ok(match (&self.remote_rpc_cookie, &url.credentials) {
            (Some(_), Some(_)) => {
                return Err(eyre!(
                    "use --remote-rpc-cookie or credentials in the --remote-rpc URL, not both"
                ));
            }
            (Some(cookie), None) => Auth::Cookie(cookie.clone()),
            (None, Some(credentials)) => credentials.clone(),
            (None, None) if url.is_loopback() && default_cookie.is_file() => {
                Auth::Cookie(default_cookie.to_owned())
            }
            (None, None) => Auth::None,
        })
    }
}

/// The URL of `--remote-rpc`. The credentials of its userinfo part
/// (`https://user:password@host:8232`) go into [`Auth::Basic`]. Thus the URL that ztreamerd keeps
/// and logs has no credentials, and the `Debug` output does not show the password.
#[derive(Clone, Debug)]
pub(crate) struct NodeUrl {
    /// The URL without its userinfo part.
    uri: Uri,
    /// [`Auth::Basic`] from the userinfo part, if the URL has one.
    credentials: Option<Auth>,
}

impl NodeUrl {
    /// True when the host is `localhost` or a loopback address.
    fn is_loopback(&self) -> bool {
        self.uri.host().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
    }
}

impl FromStr for NodeUrl {
    type Err = eyre::Report;

    /// Parses the URL and moves `user:password` from the userinfo part into [`Auth::Basic`]. It
    /// percent-decodes the user and the password.
    fn from_str(url: &str) -> Result<Self> {
        let uri: Uri = url.parse()?;
        let Some((userinfo, host)) = uri
            .authority()
            .and_then(|authority| authority.as_str().rsplit_once('@'))
            .map(|(userinfo, host)| (userinfo.to_owned(), host.to_owned()))
        else {
            return Ok(Self { uri, credentials: None });
        };
        let (user, password) = userinfo
            .split_once(':')
            .ok_or_else(|| eyre!("the userinfo part of the URL needs user:password"))?;
        let decode = |part: &str| {
            percent_decode_str(part)
                .decode_utf8()
                .map(String::from)
                .map_err(|_| eyre!("the userinfo part of the URL is not UTF-8"))
        };
        let credentials =
            Auth::Basic { user: decode(user)?, password: Secret::new(decode(password)?) };
        let mut parts = uri.into_parts();
        parts.authority = Some(host.parse()?);
        Ok(Self { uri: Uri::from_parts(parts)?, credentials: Some(credentials) })
    }
}

/// A remote zakura node that replaces the embedded node.
#[derive(Clone, Debug)]
pub(crate) struct RemoteNode {
    client: ZakuraClient,
    replica: Option<Replica>,
}

impl RemoteNode {
    /// The node behind `client`.
    pub(crate) const fn new(client: ZakuraClient) -> Self {
        Self { client, replica: None }
    }

    /// Runs `replica`, the import of `--replicate-from`, before the tail.
    pub(crate) fn with_replica(mut self, replica: Option<Replica>) -> Self {
        self.replica = replica;
        self
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

    /// Runs the daemon on this node until `signal`, the end of the work (`--index-only`), or a
    /// failure.
    ///
    /// A stop during the historical pass waits for the `getblock` calls in flight. When the node
    /// stalls, each call can wait for its timeout and its retries, thus more than one minute.
    pub(crate) async fn run_until(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        signal: impl Future<Output = Result<()>>,
    ) -> Result<()> {
        let url = self.client.rpc().url();
        info!(host = url.host(), port = url.port_u16(), "using remote zakura");
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
        let application = self.application(cli, grpc_server, shutdown.clone());
        lifecycle::supervise(node, application, shutdown, signal).await
    }

    /// Reads the network from the node and opens the index of that network, as the embedded
    /// mode does. Runs the import of `--replicate-from`, if there is one. Then makes the sources
    /// when the node is ready, and runs the daemon on them until shutdown.
    ///
    /// It waits while the node is out of reach or in warm-up. [`Index::open`] refuses an index
    /// of another network.
    async fn application(
        self,
        cli: DaemonArgs,
        grpc_server: Server,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let ControlFlow::Continue(info) = self.blockchain_info_when_up(&shutdown).await? else {
            return Ok(());
        };
        let network = Network::try_from(NodeChain(info.chain()))?;
        info!(%network, "remote zakura network");
        let index = cli.open_index(&network).await?;
        if let Some(replica) = &self.replica
            && replica.import(&self.client, &info, &network, &index, &shutdown).await?.is_break()
        {
            return Ok(());
        }
        let ControlFlow::Continue(sources) =
            self.sources(info, &network, &index, &shutdown).await?
        else {
            return Ok(());
        };
        run_application(cli, grpc_server, index, &network, None, sources, shutdown).await
    }

    /// The sources over this node, when the node is ready for `index`. Breaks on shutdown.
    ///
    /// `info` is the latest `getblockchaininfo` answer. It refuses a node whose genesis block is
    /// not the one of `network`, and a node that pruned the next block that the index needs.
    /// Then it waits until the best block of the node is at or above the durable tip of the
    /// index.
    async fn sources(
        &self,
        mut info: GetBlockchainInfoResponse,
        network: &Network,
        index: &Arc<Index>,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), RemoteSources>> {
        let (backend, poller) = RemoteBackend::connect(self.client.clone(), network.clone())
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

/// The chain name that a zakura node gives in `getblockchaininfo`. [`Network::try_from`] maps it
/// to the network of the node.
#[derive(Clone, Copy, Debug)]
struct NodeChain<'a>(&'a str);

impl TryFrom<NodeChain<'_>> for Network {
    type Error = UnknownChain;

    /// The network whose BIP70 name is the chain name: `main` is mainnet, `test` is the default
    /// testnet.
    fn try_from(chain: NodeChain<'_>) -> Result<Self, UnknownChain> {
        [Self::Mainnet, Self::new_default_testnet()]
            .into_iter()
            .find(|network| network.bip70_network_name() == chain.0)
            .ok_or_else(|| UnknownChain(chain.0.to_owned()))
    }
}

/// A chain that ztreamerd cannot serve: only mainnet and the default testnet have a
/// [`Network`] that ztreamerd can make from the chain name.
#[derive(Debug)]
pub(crate) struct UnknownChain(String);

impl fmt::Display for UnknownChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "remote zakura runs chain {:?}, ztreamerd serves only \"main\" and \"test\"",
            self.0
        )
    }
}

impl std::error::Error for UnknownChain {}

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
