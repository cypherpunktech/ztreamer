//! A wait for a remote zakura node that syncs or warms up, until it reaches a height.
//!
//! The remote mode stops on a node that is gone or refuses a call ([`NodeState`]). It waits
//! only for a node that is syncing: the node answers [`RpcCode::IN_WARMUP`](crate::RpcCode), or
//! its `blocks` is below the height that the work needs. [`NodeSyncWait`] does this wait. It
//! stops when the node does not sync.

use std::time::Duration;

use tokio::time::Instant;
use tracing::info;
use zakura_rpc::client::GetBlockchainInfoResponse;

use crate::{NodeState, RpcError, ZakuraClient};

/// The time between two `getblockchaininfo` calls while the node is below the target.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The time between two progress lines in the log.
const LOG_INTERVAL: Duration = Duration::from_secs(60);

/// The wait stops when the height of the node does not change for this time while its
/// `headers` is above its `blocks`: the node has more headers, but it does not add blocks.
const STALL_LIMIT: Duration = Duration::from_secs(30 * 60);

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
    /// The node does not sync: its height did not change for 30 minutes, while its `headers`
    /// is above its `blocks`.
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

    /// Calls `getblockchaininfo` each second until the `blocks` of the node is at or above
    /// `target`, and gives that answer. A node at the target gives the first answer, with no
    /// wait.
    ///
    /// While it waits, it logs the height at `info` each minute. It continues on
    /// [`NodeState::Syncing`]. It stops on [`NodeState::Gone`] and [`NodeState::Refuses`], and
    /// when the height does not change for 30 minutes while the node has more headers. To stop
    /// the wait before, drop the future.
    pub async fn wait_for_height(
        &self,
        target: u32,
    ) -> Result<GetBlockchainInfoResponse, NodeWaitError> {
        let mut stall = StallCheck::default();
        let mut logged_at: Option<Instant> = None;
        loop {
            let info = match self.node.get_blockchain_info().await {
                Ok(info) if info.blocks().0 >= target => return Ok(info),
                Ok(info) => Some(info),
                Err(error) if error.node_state() == NodeState::Syncing => None,
                Err(source) => {
                    let state = source.node_state();
                    return Err(NodeWaitError::Node { url: self.url(), state, source });
                }
            };
            let now = Instant::now();
            if let Some(info) = &info
                && stall.is_stalled(now, info)
            {
                let (height, headers) = (info.blocks().0, info.headers().0);
                return Err(NodeWaitError::Stalled { url: self.url(), height, headers, target });
            }
            if logged_at.is_none_or(|at| now.duration_since(at) >= LOG_INTERVAL) {
                logged_at = Some(now);
                match &info {
                    Some(info) => info!(
                        height = info.blocks().0,
                        headers = info.headers().0,
                        target_height = target,
                        "waiting for remote zakura to sync"
                    ),
                    None => info!(target_height = target, "waiting for remote zakura to warm up"),
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// The URL of the node, without credentials.
    fn url(&self) -> String {
        self.node.rpc().url().to_string()
    }
}

/// The height of the node and the time when the wait first saw it.
#[derive(Debug, Default)]
struct StallCheck {
    height_since: Option<(u32, Instant)>,
}

impl StallCheck {
    /// Adds the answer at `now`. True when the height did not change for [`STALL_LIMIT`] and
    /// the answer has `headers` above `blocks`.
    fn is_stalled(&mut self, now: Instant, info: &GetBlockchainInfoResponse) -> bool {
        let (blocks, headers) = (info.blocks().0, info.headers().0);
        match self.height_since {
            Some((height, since)) if height == blocks => {
                headers > blocks && now.duration_since(since) >= STALL_LIMIT
            }
            _ => {
                self.height_since = Some((blocks, now));
                false
            }
        }
    }
}

#[cfg(test)]
mod tests;
