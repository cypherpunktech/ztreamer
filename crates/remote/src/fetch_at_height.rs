//! Why [`ZakuraClient::block_at_height`](crate::ZakuraClient::block_at_height) gave no block.

use ztreamer_blocking::BlockingTaskCancelled;
use ztreamer_chain::types::BlockAtHeightError;
use ztreamer_jsonrpc::{NodeState, RpcError};
use ztreamer_sync::head::HeadError;

/// Why [`ZakuraClient::block_at_height`](crate::ZakuraClient::block_at_height) gave no block.
#[derive(Debug, thiserror::Error)]
pub enum FetchAtHeightError {
    /// The `getblock` call failed. [`RpcError::is_block_not_found`] is `true` when the node has
    /// no block at the height.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The bytes do not give the block at the height.
    #[error(transparent)]
    Block(#[from] BlockAtHeightError),
    /// The blocking pool stopped before the decode ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
}

/// [`RemoteHeadSource`](crate::RemoteHeadSource) maps [`FetchAtHeightError::Rpc`] itself, with
/// the URL of the node. Here it is [`HeadError::RemoteFatal`] when the node is gone or refuses
/// the call, and [`HeadError::Remote`] when the node is syncing.
impl From<FetchAtHeightError> for HeadError {
    fn from(error: FetchAtHeightError) -> Self {
        match error {
            FetchAtHeightError::Rpc(error) if error.node_state() == NodeState::Syncing => {
                Self::Remote(error.into())
            }
            FetchAtHeightError::Rpc(error) => Self::RemoteFatal(error.into()),
            FetchAtHeightError::Block(error) => error.into(),
            FetchAtHeightError::Cancelled(error) => Self::Cancelled(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fetch_error_keeps_its_cause_in_the_head_error() {
        let cancelled = HeadError::from(FetchAtHeightError::Cancelled(BlockingTaskCancelled));
        assert!(matches!(cancelled, HeadError::Cancelled(BlockingTaskCancelled)));
        let height = BlockAtHeightError::Height { expected: 3, actual: 4 };
        let height = HeadError::from(FetchAtHeightError::Block(height));
        assert!(matches!(height, HeadError::Height { expected: 3, actual: 4 }));
    }
}
