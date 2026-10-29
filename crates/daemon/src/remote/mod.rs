//! The remote node: `ztreamerd` imports the sealed ranges of `--replicate-from` in the
//! historical stage, checked against the `--remote-rpc` zakura node. Then it reads the tail and
//! the head from that node over JSON-RPC. It starts no node in this process.

use std::{
    ops::ControlFlow,
    path::{Path, PathBuf},
    time::Duration,
};

use eyre::{Result, WrapErr, eyre};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakura_chain::parameters::Network;
use zakura_rpc::client::GetBlockchainInfoResponse;
use ztreamer_index::index::range_meta::NodeId;
use ztreamer_remote::{
    Auth, NodeChain, NodeSyncWait, NodeUrl, NodeWaitError, RemoteBackend, RemoteHeadSource,
    RemoteTailSource, RpcClient, ZakuraClient,
};
use ztreamer_service::CompactService;

use crate::{
    lifecycle::TaskSet,
    node::{HistoricalSource, IndexTip, Node, NodeComponents, StageContext, Tail},
    replicate::{Replica, ReplicaSource},
};

/// The time between two `getblockchaininfo` calls while the remote node is below the index tip.
const READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The configuration of the remote mode: the `--remote-rpc` node, its credentials, and the
/// import of `--replicate-from` that the node attests.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// The JSON-RPC URL of the node, without credentials.
    url: NodeUrl,
    /// The credentials of `--remote-rpc-cookie` or of the URL. With `None`, a loopback URL uses
    /// the default cookie file of zakura if it exists.
    auth: Option<Auth>,
    /// The source of the import.
    replicate_from: ReplicaSource,
}

impl RemoteConfig {
    /// The node at `url`, with the cookie file `cookie` or the credentials of the URL, and the
    /// import of `replicate_from`. Refuses a cookie file and credentials in the URL together.
    pub fn new(
        url: NodeUrl,
        cookie: Option<PathBuf>,
        replicate_from: ReplicaSource,
    ) -> Result<Self> {
        let auth = match (cookie, url.credentials()) {
            (Some(_), Some(_)) => {
                return Err(eyre!(
                    "use --remote-rpc-cookie or credentials in the --remote-rpc URL, not both"
                ));
            }
            (Some(cookie), None) => Some(Auth::Cookie(cookie)),
            (None, credentials) => credentials.cloned(),
        };
        Ok(Self { url, auth, replicate_from })
    }

    /// The node of this configuration with its import. The default cookie file is the one of a
    /// zakura with the default configuration. It reads the cookie file, but it does not call the
    /// node.
    pub(crate) fn node(&self) -> Result<RemoteNode> {
        let rpc = zakura_rpc::config::rpc::Config::default();
        let client = self.client(&rpc.cookie_dir.join(rpc.cookie_file_name))?;
        Ok(RemoteNode::new(client, self.replica()))
    }

    /// The import of `--replicate-from`.
    pub(crate) fn replica(&self) -> Replica {
        Replica::new(self.replicate_from.clone())
    }

    /// The client of the node, with `default_cookie` as the default cookie file.
    pub(crate) fn client(&self, default_cookie: &Path) -> Result<ZakuraClient> {
        let auth = self.auth(default_cookie);
        info!(?auth, "remote zakura credentials");
        let config = ZakuraClient::config(self.url.uri().clone()).with_auth(auth);
        let rpc = RpcClient::new(config).wrap_err("configure the --remote-rpc client")?;
        Ok(ZakuraClient::new(rpc))
    }

    /// The credentials for the node: `--remote-rpc-cookie`, else the credentials of the URL,
    /// else `default_cookie` when the URL is a loopback URL and the file exists, else none.
    ///
    /// The default cookie applies only to a loopback URL, because the cookie of a local zakura
    /// gives access to that node: ztreamerd must not send it to another host.
    fn auth(&self, default_cookie: &Path) -> Auth {
        match &self.auth {
            Some(auth) => auth.clone(),
            None if self.url.is_loopback() && default_cookie.is_file() => {
                Auth::Cookie(default_cookie.to_owned())
            }
            None => Auth::None,
        }
    }
}

/// A remote zakura node that replaces the embedded node, and the import that it attests.
///
/// A stop during the catch-up waits for the `getblock` calls in flight. When the node stalls,
/// each call can wait for its timeout and its retries, thus more than one minute.
#[derive(Clone, Debug)]
pub(crate) struct RemoteNode {
    client: ZakuraClient,
    /// The import of `--replicate-from`. It is the historical stage.
    replica: Replica,
    /// The network and the `getblockchaininfo` answer of [`Node::network`].
    chain: Option<(Network, GetBlockchainInfoResponse)>,
}

impl RemoteNode {
    /// The node behind `client`, with the import `replica`.
    pub(crate) const fn new(client: ZakuraClient, replica: Replica) -> Self {
        Self { client, replica, chain: None }
    }

    /// The node in `verified_by` of the `range_meta` rows that it answers: of the import, and of
    /// the filler. The URL has no credentials ([`NodeUrl`]). The version is empty: no call gives
    /// it.
    fn id(&self) -> NodeId {
        NodeId { url: self.client.rpc().url().to_string(), version: String::new() }
    }

    /// `getblockchaininfo` when the best block of the node is at or above `height`. It waits
    /// while the node is in warm-up or below `height` ([`NodeSyncWait`]). A node that is gone or
    /// refuses the call is an error, and the daemon stops: systemd starts it again. Breaks on
    /// shutdown.
    async fn blockchain_info_at_height(
        &self,
        height: u32,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), GetBlockchainInfoResponse>, NodeWaitError> {
        let sync = NodeSyncWait::new(self.client.clone());
        let wait = sync.wait_for_height(height);
        match shutdown.run_until_cancelled(wait).await {
            Some(info) => info.map(ControlFlow::Continue),
            None => Ok(ControlFlow::Break(())),
        }
    }
}

impl Node for RemoteNode {
    type Components = RemoteComponents;

    /// Reads the network from the node. It waits while the node is in warm-up, and fails when
    /// the node is gone or refuses the call.
    async fn network(&mut self, shutdown: &CancellationToken) -> Result<ControlFlow<(), Network>> {
        let url = self.client.rpc().url();
        info!(host = url.host(), port = url.port_u16(), "using remote zakura");
        // Each answer with a height is at or above height 0.
        let info = self.blockchain_info_at_height(0, shutdown).await;
        let ControlFlow::Continue(info) =
            info.wrap_err("read the chain of the remote zakura node")?
        else {
            return Ok(ControlFlow::Break(()));
        };
        let network = Network::try_from(NodeChain(info.chain()))?;
        info!(%network, "remote zakura network");
        self.chain = Some((network.clone(), info));
        Ok(ControlFlow::Continue(network))
    }

    /// No node runs in this process: it starts nothing, and the import of the historical stage
    /// checks the node.
    async fn boot(self, _: &TaskSet) -> Result<ControlFlow<(), RemoteComponents>> {
        let Self { client, replica, chain } = self;
        let (network, info) = chain.expect("the launcher reads the network before the boot");
        let node = Self::new(client, replica);
        Ok(ControlFlow::Continue(RemoteComponents { node, network, info }))
    }
}

/// The components of a booted remote node. All of them read the same node.
pub(crate) struct RemoteComponents {
    node: RemoteNode,
    /// The network of the node.
    network: Network,
    /// The `getblockchaininfo` answer of [`Node::network`].
    info: GetBlockchainInfoResponse,
}

impl NodeComponents for RemoteComponents {
    type Historical = ReplicaImport;
    type Finalized = RemoteTailSource;
    type Head = RemoteHeadSource;
    type Wallet = RemoteBackend;

    fn historical(&self) -> ReplicaImport {
        ReplicaImport { node: self.node.clone(), info: self.info.clone() }
    }

    /// The backend over the node. It refuses a node whose genesis block is not the one of the
    /// network.
    async fn wallet(&self) -> Result<RemoteBackend> {
        RemoteBackend::connect(self.node.client.clone(), self.network.clone())
            .await
            .wrap_err("connect to the remote zakura node")
    }

    /// Waits until the best block of the node is at or above `tip`. The first check uses the
    /// `getblockchaininfo` answer of [`Node::network`].
    async fn tail(
        &self,
        tip: IndexTip,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Tail<RemoteTailSource, RemoteHeadSource>>> {
        let mut info = self.info.clone();
        while !tip.is_reached_by(info.blocks().0) {
            info!(
                best = info.blocks().0,
                index_tip = tip.height(),
                "waiting for remote zakura to reach the index tip"
            );
            if shutdown.run_until_cancelled(tokio::time::sleep(READY_POLL_INTERVAL)).await.is_none()
            {
                return Ok(ControlFlow::Break(()));
            }
            let next = self.node.blockchain_info_at_height(0, shutdown).await;
            let ControlFlow::Continue(next) =
                next.wrap_err("read the chain of the remote zakura node")?
            else {
                return Ok(ControlFlow::Break(()));
            };
            info = next;
        }
        info!(best = info.blocks().0, "remote zakura is ready");
        info!(import_state = "tail");
        let client = &self.node.client;
        Ok(ControlFlow::Continue(Tail {
            finalized: RemoteTailSource::new(client.clone(), Handle::current()),
            head: RemoteHeadSource::new(client.clone()),
        }))
    }

    fn id(&self) -> NodeId {
        self.node.id()
    }

    /// Logs the import state `following`.
    fn start_following(&self, _: &CompactService) -> Result<()> {
        info!(import_state = "following");
        Ok(())
    }
}

/// The historical source of the remote node: the import of `--replicate-from`, checked against
/// the node.
pub(crate) struct ReplicaImport {
    node: RemoteNode,
    /// The `getblockchaininfo` answer of [`Node::network`].
    info: GetBlockchainInfoResponse,
}

impl HistoricalSource for ReplicaImport {
    async fn fill(self, stage: &StageContext<'_>) -> Result<ControlFlow<()>> {
        let Self { node, info } = self;
        let StageContext { index, network, shutdown, .. } = stage;
        node.replica.import(&node.client, node.id(), &info, network, index, shutdown).await
    }
}

#[cfg(test)]
mod tests;
