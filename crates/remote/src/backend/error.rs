//! The errors of the remote backend.

use crate::RpcError;
use zakura_chain::block;
use ztreamer_backend::BackendError;

/// Why [`RemoteBackend::connect`](crate::RemoteBackend::connect) refused the node.
#[derive(Debug, thiserror::Error)]
pub enum RemoteConnectError {
    /// The node did not answer.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The node runs a different chain.
    #[error("remote zakura runs chain {found:?}, the index needs {expected:?}")]
    WrongChain {
        /// The BIP70 name of the network of the index.
        expected: String,
        /// The chain name that the node gave.
        found: String,
    },
    /// The node has a different genesis block.
    #[error("remote zakura has genesis block {found}, the index needs {expected}")]
    WrongGenesis {
        /// The genesis hash of the network of the index.
        expected: block::Hash,
        /// The genesis hash that the node gave.
        found: block::Hash,
    },
}

/// A failed call to the remote node. It converts to the [`BackendError`] that the call gives.
pub(crate) struct NodeCallError(pub(crate) RpcError);

impl From<NodeCallError> for BackendError {
    fn from(NodeCallError(error): NodeCallError) -> Self {
        match error {
            RpcError::Protocol { .. } | RpcError::Decode { .. } => {
                Self::Unexpected(format!("remote zakura: {error}"))
            }
            _ => Self::Unavailable(format!("remote zakura: {error}")),
        }
    }
}
