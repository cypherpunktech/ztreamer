//! The node of the daemon. The stages of the daemon are fixed: boot, historical, catch up and
//! serve ([`Launcher`](crate::launcher::Launcher)). A node plugs its components into them. A
//! [`Node`] boots, and then gives its [`NodeComponents`].

use std::{ops::ControlFlow, sync::Arc};

use eyre::{Result, WrapErr};
use tokio_util::sync::CancellationToken;
use zakura_chain::parameters::Network;
use ztreamer_backend::StateBackend;
use ztreamer_index::index::{BlockId, Index, range_meta::NodeId};
use ztreamer_service::{CompactService, NodeVersion};
use ztreamer_sync::{head::CanonicalBlockSource, pipeline::FinalizedBlockSource};

use crate::{SyncConfig, lifecycle::TaskSet};

/// A node before boot: an embedded Zakura node, or a remote zakura node.
///
/// # Implementation guide
///
/// The launcher calls [`Self::network`], opens the index of that network, then calls
/// [`Self::boot`]. Thus [`Self::network`] must not start the node. [`Self::boot`] starts the
/// node and waits until it is ready for the historical stage. A node that runs in this process
/// adds its tasks to the [`TaskSet`]. Both methods break on shutdown.
pub(crate) trait Node {
    /// The components of the node after boot.
    type Components: NodeComponents;

    /// The network of the node.
    async fn network(&mut self, shutdown: &CancellationToken) -> Result<ControlFlow<(), Network>>;

    /// Starts the node, and gives its components when it is ready for the historical stage.
    async fn boot(self, tasks: &TaskSet) -> Result<ControlFlow<(), Self::Components>>;
}

/// What a booted node is composed of: the source of each stage, the wallet backend, and the id
/// and the version of the node.
///
/// # Implementation guide
///
/// The launcher calls the methods in this order: [`Self::historical`] in the historical stage;
/// [`Self::wallet`], [`Self::tail`] and [`Self::node_version`] in the catch-up stage; [`Self::id`]
/// and [`Self::start_following`] in the serve stage.
pub(crate) trait NodeComponents {
    /// The source of the historical stage.
    type Historical: HistoricalSource;
    /// The finalized blocks of the catch-up stage.
    type Finalized: FinalizedBlockSource + Send + 'static;
    /// The canonical head, for the head reconcile and the head follower.
    type Head: CanonicalBlockSource + Clone + 'static;
    /// The chain state and the node requests of the wallet service.
    type Wallet: StateBackend;

    /// The source of the historical stage.
    fn historical(&self) -> Self::Historical;

    /// The backend of the wallet service.
    async fn wallet(&self) -> Result<Self::Wallet>;

    /// The tail of the node, when the node serves the blocks above `tip`. Breaks on shutdown.
    async fn tail(
        &self,
        tip: IndexTip,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Tail<Self::Finalized, Self::Head>>>;

    /// The version of the node that `GetLightdInfo` reports.
    async fn node_version(&self) -> Result<NodeVersion>;

    /// The node in `verified_by` of the `range_meta` rows that the wallet backend answers.
    fn id(&self) -> NodeId;

    /// Called when the index follows the head of the node, with the wallet service.
    fn start_following(&self, compact: &CompactService) -> Result<()>;
}

/// The source of the historical stage: it fills the index up to where the tail starts.
///
/// # Implementation guide
///
/// [`Self::fill`] skips the work that the index does not need, for example when the index is
/// already at or above the source. A stop on shutdown completes the work in progress, writes it,
/// and breaks.
pub(crate) trait HistoricalSource {
    /// Fills the index of `stage`. Breaks on shutdown.
    async fn fill(self, stage: &StageContext<'_>) -> Result<ControlFlow<()>>;
}

/// The tail of a node: the finalized blocks of the catch-up stage, and the canonical head.
pub(crate) struct Tail<F, H> {
    /// The finalized blocks.
    pub(crate) finalized: F,
    /// The canonical head.
    pub(crate) head: H,
}

/// What each stage after boot works on.
pub(crate) struct StageContext<'a> {
    /// The compact index.
    pub(crate) index: &'a Arc<Index>,
    /// The network of the node and of the index.
    pub(crate) network: &'a Network,
    /// The block pipeline and the `--index-only` stop.
    pub(crate) sync: SyncConfig,
    pub(crate) shutdown: &'a CancellationToken,
}

/// The durable tip of the index. The node must serve the blocks above it before the catch-up.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexTip(Option<BlockId>);

impl IndexTip {
    /// Reads the durable tip of `index` on a blocking thread.
    pub(crate) async fn read(index: &Arc<Index>) -> Result<Self> {
        let index = Arc::clone(index);
        let state = tokio::task::spawn_blocking(move || index.state())
            .await
            .wrap_err("index state task panicked")??;
        Ok(Self(state.durable_tip()))
    }

    /// The height of the tip. `None` for an empty index.
    pub(crate) fn height(self) -> Option<u32> {
        self.0.map(|tip| tip.height)
    }

    /// True when a node with the best block at `best` has the tip.
    pub(crate) fn is_reached_by(self, best: u32) -> bool {
        self.0.is_none_or(|tip| best >= tip.height)
    }
}
