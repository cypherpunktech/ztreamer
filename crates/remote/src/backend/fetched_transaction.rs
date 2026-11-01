//! A verbose `getrawtransaction` answer.

use zakura_chain::{
    block,
    transaction::{self, Transaction},
};
use zakura_rpc::client::{GetRawTransactionResponse, TransactionObject};
use ztreamer_backend::BackendError;

use super::node_bytes::FromNodeBytes;

/// A verbose `getrawtransaction` answer for one transaction id.
pub(crate) struct FetchedTransaction {
    id: transaction::Hash,
    object: Box<TransactionObject>,
}

impl FetchedTransaction {
    /// Reads the answer of the node for `id`. The answer must be verbose.
    pub(crate) fn from_response(
        id: transaction::Hash,
        response: GetRawTransactionResponse,
    ) -> Result<Self, BackendError> {
        let GetRawTransactionResponse::Object(object) = response else {
            return Err(BackendError::Unexpected(
                "remote zakura sent a raw transaction for a verbose request".into(),
            ));
        };
        Ok(Self { id, object })
    }

    /// The best-chain height of the transaction. `None` for a mempool transaction. A height of
    /// -1 is a side chain, which is not the best chain either.
    pub(crate) fn height(&self) -> Option<block::Height> {
        self.object.height().and_then(|height| u32::try_from(height).ok()).map(block::Height)
    }

    /// Decodes the transaction. Its id must be the id of the request.
    pub(crate) async fn decode(self) -> Result<Transaction, BackendError> {
        let transaction = Transaction::from_node_bytes(self.object.hex().as_ref().to_vec()).await?;
        if transaction.hash() != self.id {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent transaction {} for {}",
                transaction.hash(),
                self.id
            )));
        }
        Ok(transaction)
    }
}
