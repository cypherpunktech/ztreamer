//! Why [`ZakuraClient::block_at_height`](crate::ZakuraClient::block_at_height) gave no block.

use ztreamer_indexer::{
    blocking::BlockingTaskCancelled, head::HeadError, types::BlockAtHeightError,
};
use ztreamer_jsonrpc::RpcError;

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

impl From<FetchAtHeightError> for HeadError {
    fn from(error: FetchAtHeightError) -> Self {
        match error {
            FetchAtHeightError::Rpc(error) => Self::Remote(error.into()),
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
