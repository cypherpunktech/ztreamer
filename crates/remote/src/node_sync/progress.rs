//! The progress of one [`NodeSyncWait`](super::NodeSyncWait): the progress lines, the stall
//! check and the metrics.

use std::{sync::LazyLock, time::Duration};

use metrics::{Gauge, describe_gauge, gauge};
use tokio::time::Instant;
use tracing::info;
use zakura_rpc::client::GetBlockchainInfoResponse;

use super::{LOG_INTERVAL, STALL_LIMIT};

const NODE_SYNCING: &str = "ztreamer.remote.node.syncing";
const NODE_SYNCING_HELP: &str =
    "1 while the daemon waits for the remote node to sync or to end its warm-up, else 0.";

const NODE_HEIGHT: &str = "ztreamer.remote.node.height";
const NODE_HEIGHT_HELP: &str =
    "The best height that the remote node gave last. NaN while the node is in warm-up.";

const NODE_TARGET_HEIGHT: &str = "ztreamer.remote.node.target.height";
const NODE_TARGET_HEIGHT_HELP: &str =
    "The height that the daemon waits for the remote node to reach. NaN when it does not wait.";

/// Gives the help text of each metric to the recorder. [`NodeSyncMetrics::new`] forces it.
static DESCRIBE: LazyLock<()> = LazyLock::new(|| {
    describe_gauge!(NODE_SYNCING, NODE_SYNCING_HELP);
    describe_gauge!(NODE_HEIGHT, NODE_HEIGHT_HELP);
    describe_gauge!(NODE_TARGET_HEIGHT, NODE_TARGET_HEIGHT_HELP);
});

/// One answer of a node below the target.
#[derive(Clone, Copy, Debug)]
pub(super) enum Reading<'a> {
    /// The node answered [`RpcCode::IN_WARMUP`](crate::RpcCode): it gives no height yet.
    WarmUp,
    /// The `getblockchaininfo` answer of a node with `blocks` below the target.
    Behind(&'a GetBlockchainInfoResponse),
}

/// The node did not add a block for [`STALL_LIMIT`], while its `headers` is above its `blocks`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stall {
    /// The `blocks` of the node.
    pub(super) height: u32,
    /// The `headers` of the node.
    pub(super) headers: u32,
}

/// A progress line of the log.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ProgressLine {
    /// The height that the wait needs.
    pub(super) target: u32,
    /// The `getblockchaininfo` fields: `blocks`, `headers`, `estimated_height`,
    /// `verification_progress`. `None` in warm-up.
    pub(super) node: Option<NodeHeights>,
    /// The blocks per second since the line before. `None` for the first line with a height.
    pub(super) rate: Option<f64>,
    /// The time to the target at `rate`. `None` when the rate is unknown or zero.
    pub(super) eta: Option<Duration>,
}

/// The height fields of a `getblockchaininfo` answer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct NodeHeights {
    pub(super) blocks: u32,
    pub(super) headers: u32,
    pub(super) estimated_height: u32,
    pub(super) verification_progress: f64,
}

impl From<&GetBlockchainInfoResponse> for NodeHeights {
    fn from(info: &GetBlockchainInfoResponse) -> Self {
        Self {
            blocks: info.blocks().0,
            headers: info.headers().0,
            estimated_height: info.estimated_height().0,
            verification_progress: info.verification_progress(),
        }
    }
}

impl ProgressLine {
    /// Writes the line to the log at `info`.
    pub(super) fn log(&self) {
        let Some(node) = self.node else {
            info!(target_height = self.target, "waiting for remote zakura to end its warm-up");
            return;
        };
        info!(
            height = node.blocks,
            target_height = self.target,
            headers = node.headers,
            estimated_height = node.estimated_height,
            verification_progress = node.verification_progress,
            blocks_per_second = self.rate.map(|rate| format!("{rate:.1}")),
            eta = self.eta.map(|eta| format!("{}s", eta.as_secs())),
            "waiting for remote zakura to sync"
        );
    }
}

/// The progress of a node toward `target` in one wait: when the height last changed, and the
/// last progress line.
#[derive(Debug)]
pub(super) struct SyncProgress {
    target: u32,
    /// The height of the node and the time when the wait first saw it. `None` before the first
    /// answer with a height.
    height_since: Option<(u32, Instant)>,
    /// The time of the last progress line, and the height in it. `None` before the first line.
    logged: Option<(Instant, Option<u32>)>,
}

impl SyncProgress {
    /// The progress of a wait for `target`, with no reading yet.
    pub(super) const fn new(target: u32) -> Self {
        Self { target, height_since: None, logged: None }
    }

    /// Adds the reading at `now`. Gives a progress line for the first reading and then each
    /// [`LOG_INTERVAL`]. Fails when the height did not change for [`STALL_LIMIT`] and the
    /// reading has `headers` above `blocks`.
    pub(super) fn observe(
        &mut self,
        now: Instant,
        reading: Reading<'_>,
    ) -> Result<Option<ProgressLine>, Stall> {
        let node = match reading {
            Reading::WarmUp => None,
            Reading::Behind(info) => Some(NodeHeights::from(info)),
        };
        if let Some(node) = node {
            match self.height_since {
                Some((height, since)) if height == node.blocks => {
                    if node.headers > node.blocks && now.duration_since(since) >= STALL_LIMIT {
                        return Err(Stall { height, headers: node.headers });
                    }
                }
                _ => self.height_since = Some((node.blocks, now)),
            }
        }
        Ok(self.progress_line(now, node))
    }

    /// The progress line at `now`, when the first line or [`LOG_INTERVAL`] since the last line.
    fn progress_line(&mut self, now: Instant, node: Option<NodeHeights>) -> Option<ProgressLine> {
        let rate = match self.logged {
            Some((at, _)) if now.duration_since(at) < LOG_INTERVAL => return None,
            Some((at, Some(logged))) => node.map(|node| {
                f64::from(node.blocks.saturating_sub(logged)) / now.duration_since(at).as_secs_f64()
            }),
            Some((_, None)) | None => None,
        };
        self.logged = Some((now, node.map(|node| node.blocks)));
        let eta = rate.zip(node).filter(|(rate, _)| *rate > 0.0).map(|(rate, node)| {
            Duration::from_secs_f64(f64::from(self.target.saturating_sub(node.blocks)) / rate)
        });
        Some(ProgressLine { target: self.target, node, rate, eta })
    }
}

/// The metric handles of one wait.
pub(super) struct NodeSyncMetrics {
    syncing: Gauge,
    height: Gauge,
    target: Gauge,
}

impl NodeSyncMetrics {
    pub(super) fn new() -> Self {
        LazyLock::force(&DESCRIBE);
        Self {
            syncing: gauge!(NODE_SYNCING),
            height: gauge!(NODE_HEIGHT),
            target: gauge!(NODE_TARGET_HEIGHT),
        }
    }

    /// Records a reading of a node below `target`.
    pub(super) fn waiting(&self, reading: Reading<'_>, target: u32) {
        self.syncing.set(1.0);
        self.target.set(f64::from(target));
        self.height.set(match reading {
            Reading::WarmUp => f64::NAN,
            Reading::Behind(info) => f64::from(info.blocks().0),
        });
    }

    /// Records the answer of a node at or above the target. The wait ends.
    pub(super) fn reached(&self, info: &GetBlockchainInfoResponse) {
        self.syncing.set(0.0);
        self.target.set(f64::NAN);
        self.height.set(f64::from(info.blocks().0));
    }
}
