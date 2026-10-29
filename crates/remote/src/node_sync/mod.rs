//! A wait for a remote zakura node that syncs or warms up, until it reaches a height.
//!
//! The remote mode stops on a node that is gone or refuses a call ([`NodeState`]). It waits
//! only for a node that is syncing: the node answers [`RpcCode::IN_WARMUP`](crate::RpcCode), or
//! its `blocks` is below the height that the work needs. [`NodeSyncWait`] does this wait, with a
//! progress line in the log and a state metric. It stops when the node does not sync.

mod progress;

use std::time::Duration;

use tokio::time::Instant;
use zakura_rpc::client::GetBlockchainInfoResponse;

use self::progress::{NodeSyncMetrics, Reading, Stall, SyncProgress};
use crate::{NodeState, RpcError, ZakuraClient};

/// The time between two `getblockchaininfo` calls while the node is below the target.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The time between two progress lines in the log.
pub const LOG_INTERVAL: Duration = Duration::from_secs(60);

/// The wait stops when the height of the node does not change for this time while its
/// `headers` is above its `blocks`: the node has more headers, but it does not add blocks.
pub const STALL_LIMIT: Duration = Duration::from_secs(30 * 60);

/// Why [`NodeSyncWait::wait_for_height`] stopped before the node reached the target.
#[derive(Debug, thiserror::Error)]
pub enum NodeWaitError {
    /// A `getblockchaininfo` call failed, and the node is gone or refuses the call.
    #[error("remote zakura {url} {state}: {source}")]
    Node {
        /// The URL of the node.
        url: String,
        /// The state that the error shows: [`NodeState::Gone`] or [`NodeState::Refuses`].
        state: NodeState,
        /// The failed call.
        #[source]
        source: RpcError,
    },
    /// The node does not sync: its height did not change for [`STALL_LIMIT`], while its
    /// `headers` is above its `blocks`.
    #[error(
        "remote zakura {url} stays at height {height} below {target} for {limit:?}, with headers \
         at {headers}",
        limit = STALL_LIMIT
    )]
    Stalled {
        /// The URL of the node.
        url: String,
        /// The `blocks` of the node.
        height: u32,
        /// The `headers` of the node.
        headers: u32,
        /// The height that the wait needs.
        target: u32,
    },
}

/// Waits for a remote zakura node to reach a height.
///
/// ```no_run
/// use ztreamer_remote::{NodeSyncWait, RpcClient, ZakuraClient};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let wait = NodeSyncWait::new(ZakuraClient::new(RpcClient::new(config)?));
/// let info = wait.wait_for_height(3_000_000).await?;
/// assert!(info.blocks().0 >= 3_000_000);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct NodeSyncWait {
    node: ZakuraClient,
}

impl NodeSyncWait {
    /// A wait on `node`.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node }
    }

    /// Calls `getblockchaininfo` each [`POLL_INTERVAL`] until the `blocks` of the node is at or
    /// above `target`, and gives that answer. A node at the target gives the first answer, with
    /// no wait.
    ///
    /// While it waits, it logs a progress line at `info` each [`LOG_INTERVAL`] and sets the
    /// metric `ztreamer.remote.node.syncing` to 1. It continues on [`NodeState::Syncing`]. It
    /// stops on [`NodeState::Gone`] and [`NodeState::Refuses`], and on a node that does not sync
    /// ([`STALL_LIMIT`]). To stop the wait before, drop the future.
    pub async fn wait_for_height(
        &self,
        target: u32,
    ) -> Result<GetBlockchainInfoResponse, NodeWaitError> {
        let metrics = NodeSyncMetrics::new();
        let mut progress = SyncProgress::new(target);
        let result = loop {
            let info = match self.node.get_blockchain_info().await {
                Ok(info) if info.blocks().0 >= target => break Ok(info),
                Ok(info) => Some(info),
                Err(error) if error.node_state() == NodeState::Syncing => None,
                Err(source) => break Err(self.call_failed(source)),
            };
            let reading = info.as_ref().map_or(Reading::WarmUp, Reading::Behind);
            metrics.waiting(reading, target);
            match progress.observe(Instant::now(), reading) {
                Ok(Some(line)) => line.log(),
                Ok(None) => {}
                Err(Stall { height, headers }) => {
                    let url = self.url();
                    break Err(NodeWaitError::Stalled { url, height, headers, target });
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        };
        if let Ok(info) = &result {
            metrics.reached(info);
        }
        result
    }

    /// The error for a failed call on a node that is gone or refuses the call.
    fn call_failed(&self, source: RpcError) -> NodeWaitError {
        NodeWaitError::Node { url: self.url(), state: source.node_state(), source }
    }

    /// The URL of the node, without credentials.
    fn url(&self) -> String {
        self.node.rpc().url().to_string()
    }
}

#[cfg(test)]
mod tests;
