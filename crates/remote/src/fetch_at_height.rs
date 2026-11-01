//! The error of [`ZakuraClient::block_at_height`]. A [`From`] impl changes this error to the
//! [`HeadError`] of the head poll.
//!
//! [`ZakuraClient::block_at_height`]: crate::ZakuraClient::block_at_height

use ztreamer_blocking::BlockingTaskCancelled;
use ztreamer_chain::types::BlockAtHeightError;
use ztreamer_jsonrpc::RpcError;
use ztreamer_sync::head::HeadError;

/// The error of [`ZakuraClient::block_at_height`]. The variant shows the step of the fetch
/// that failed. The fetch has three steps:
///
/// 1. A `getblock` call with verbosity 0 reads the bytes of the block from the node. If the
///    call fails, the error is [`Self::Rpc`].
/// 2. A task on the blocking pool decodes the bytes. If the pool stops before the task runs,
///    the error is [`Self::Cancelled`].
/// 3. The decode checks the bytes and the coinbase height. If a check fails, the error is
///    [`Self::Block`].
///
/// A missing block is an [`Self::Rpc`] error. For this error,
/// [`RpcError::is_block_not_found`] is `true`. If a caller accepts a missing block, it must
/// examine each [`Self::Rpc`] error first:
///
/// ```
/// use ztreamer_remote::FetchAtHeightError;
///
/// fn found<T>(fetch: Result<T, FetchAtHeightError>) -> Result<Option<T>, FetchAtHeightError> {
///     match fetch {
///         Ok(block) => Ok(Some(block)),
///         Err(FetchAtHeightError::Rpc(error)) if error.is_block_not_found() => Ok(None),
///         Err(error) => Err(error),
///     }
/// }
/// ```
///
/// [`ZakuraClient::block_at_height`]: crate::ZakuraClient::block_at_height
#[derive(Debug, thiserror::Error)]
pub enum FetchAtHeightError {
    /// The `getblock` call failed, for example because the node has no block at the height.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The bytes do not decode, or the block is at a different height.
    #[error(transparent)]
    Block(#[from] BlockAtHeightError),
    /// The blocking pool stopped before the decode ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
}

/// Keeps the cause of the error. An RPC failure becomes [`HeadError::Remote`]. A wrong height
/// becomes [`HeadError::Height`]. A decode failure becomes [`HeadError::Decode`].
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
