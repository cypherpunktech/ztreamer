//! Values that the node sends in the Zcash serialization.

use zakura_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashDeserializeInto as _},
    transaction::Transaction,
};
use ztreamer_backend::BackendError;

/// A value that the node sends in the Zcash serialization: a block or a transaction.
pub(crate) trait FromNodeBytes: ZcashDeserialize + Send + 'static {
    /// The name of the value in errors.
    const WHAT: &'static str;

    /// Deserializes `bytes` on a blocking thread, because a large block takes milliseconds. The
    /// thread runs in the tracing span of the caller.
    fn from_node_bytes(bytes: Vec<u8>) -> impl Future<Output = Result<Self, BackendError>> + Send {
        let span = tracing::Span::current();
        async move {
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| bytes.zcash_deserialize_into::<Self>())
            })
            .await
            .map_err(|error| BackendError::Unavailable(format!("decode task failed: {error}")))?
            .map_err(|error| {
                BackendError::Unexpected(format!(
                    "remote zakura sent a bad {}: {error}",
                    Self::WHAT
                ))
            })
        }
    }
}

impl FromNodeBytes for Block {
    const WHAT: &'static str = "block";
}

impl FromNodeBytes for Transaction {
    const WHAT: &'static str = "transaction";
}
