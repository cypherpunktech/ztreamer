//! [`NodeError`]: why a [`NodeClient`](crate::NodeClient) request failed.

use tokio::sync::{oneshot, watch};
use zakura_node_services::BoxError;

/// An error returned by a [`NodeClient`](crate::NodeClient) request.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The sync status channel closed before the node reached the tip.
    #[error("sync status stopped: {0}")]
    SyncStopped(#[source] watch::error::RecvError),
    /// The state service failed a block query.
    #[error("state block query failed: {0}")]
    State(#[source] BoxError),
    /// The state service answered with a response of the wrong kind.
    #[error("state returned an unexpected response: {0}")]
    UnexpectedState(String),
    /// The mempool service failed a request.
    #[error("mempool request failed: {0}")]
    Mempool(#[source] BoxError),
    /// The mempool service answered with a response of the wrong kind.
    #[error("mempool returned an unexpected response: {0}")]
    UnexpectedMempool(String),
    /// The mempool answered a single-transaction queue request with the wrong number of results.
    #[error("mempool returned {0} results for one transaction")]
    QueueResults(usize),
    /// The mempool refused the transaction before verifying it.
    #[error("mempool rejected the transaction before verification: {0}")]
    Rejected(#[source] BoxError),
    /// The mempool dropped the transaction before verification finished.
    #[error("mempool stopped while verifying the transaction: {0}")]
    VerifierStopped(#[source] oneshot::error::RecvError),
    /// The transaction failed verification.
    #[error("transaction verification failed: {0}")]
    Verification(#[source] BoxError),
}
