//! The node modes of the daemon: [`EmbeddedMode`] runs a Zakura node in this process, and
//! [`RemoteMode`] imports sealed ranges and then reads a remote Zakura node.
//!
//! [`EmbeddedMode`]: crate::embedded::EmbeddedMode
//! [`RemoteMode`]: crate::remote::RemoteMode

use std::{ops::ControlFlow, sync::Arc};

use eyre::Result;
use tokio_util::sync::CancellationToken;
use zakura_chain::parameters::Network;
use ztreamer_backend::StateBackend;
use ztreamer_index::index::{Index, range_meta::NodeId};
use ztreamer_sync::{head::CanonicalBlockSource, pipeline::HistoricalSource};

use crate::{
    IndexConfig,
    embedded::Embedded,
    lifecycle::{Supervisor, Tasks},
};

/// How the daemon gets its chain: from a node in this process, or from a remote node. There are
/// two modes, [`EmbeddedMode`](crate::embedded::EmbeddedMode) and
/// [`RemoteMode`](crate::remote::RemoteMode).
///
/// # Implementation guide
///
/// [`Self::start`] does the work that must complete before the daemon supervises the node, and
/// that stops the daemon when it fails. For example, it opens the index when the network is
/// known before the node runs. It gives a [`Started`]:
///
/// - [`Started::node`] has the tasks that run the node in this process until shutdown. A mode
///   with no node in this process gives `Supervisor::new()`.
/// - [`Started::startup`] waits until the node is ready for the historical index pass, and gives
///   the [`ReadyNode`]. It breaks on shutdown. It runs concurrently with the node tasks.
pub(crate) trait NodeMode {
    /// Finalized blocks for the historical index pass.
    type Tail: HistoricalSource + Send + 'static;
    /// The canonical head, for head reconciliation and the head follower.
    type Head: CanonicalBlockSource + Clone + 'static;
    /// Chain state and node requests for the gRPC service.
    type Backend: StateBackend;

    /// Starts the node of this mode with the index of `index`. The node tasks and the startup
    /// stop on `shutdown`.
    async fn start(
        self,
        index: &IndexConfig,
        shutdown: &CancellationToken,
    ) -> Result<
        Started<
            impl Tasks,
            impl Future<
                Output = Result<ControlFlow<(), ReadyNode<Self::Tail, Self::Head, Self::Backend>>>,
            >,
        >,
    >;
}

/// A node mode after [`NodeMode::start`].
pub(crate) struct Started<N, S> {
    /// The tasks that run the node in this process until shutdown.
    pub(crate) node: Supervisor<N>,
    /// Waits until the node is ready, and gives the [`ReadyNode`]. Breaks on shutdown.
    pub(crate) startup: S,
}

/// A node that is ready for the historical index pass.
pub(crate) struct ReadyNode<T, H, B> {
    /// The network of the chain of the node.
    pub(crate) network: Network,
    /// The index of that network.
    pub(crate) index: Arc<Index>,
    /// The chain access of the node.
    pub(crate) sources: NodeSources<T, H, B>,
    /// Set when the node runs in this process.
    pub(crate) embedded: Option<Embedded>,
}

/// The chain access that one node mode gives the daemon.
pub(crate) struct NodeSources<T, H, B> {
    /// Finalized blocks for the historical index pass.
    pub(crate) tail: T,
    /// The canonical head, for head reconciliation and the head follower.
    pub(crate) head: H,
    /// Chain state and node requests for the gRPC service.
    pub(crate) backend: B,
    /// The node that answers the `range_meta` filler through `backend`. Each row that the filler
    /// writes names it in `verified_by`.
    pub(crate) verified_by: NodeId,
}
