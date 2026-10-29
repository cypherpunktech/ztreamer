//! [`NodeClient`]: the application client of the in-process Zakura node.

use std::sync::Arc;
use tower::ServiceExt as _;
use zakura_chain::{
    block::{Block, Hash as BlockHash, Height as BlockHeight},
    chain_tip::ChainTip as _,
    transaction::{UnminedTx, UnminedTxId},
};
use zakura_state::{ChainTipChange, HashOrHeight as BlockId};
use ztreamer_index::index::range_meta::NodeId;
use ztreamer_service::NodeVersion;

use crate::{FinalizedDb, NodeError};

/// A cloneable application client for a running node.
#[derive(Clone)]
pub struct NodeClient {
    services: zakurad::node::NodeServices,
}

impl std::fmt::Debug for NodeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClient").finish_non_exhaustive()
    }
}

impl NodeClient {
    /// Wraps the handles delivered by Zakura's startup readiness hook.
    pub const fn from_services(services: zakurad::node::NodeServices) -> Self {
        Self { services }
    }

    /// The node that the `range_meta` rows of the embedded Zakura name: `embedded`, with the
    /// Zakura version.
    pub fn embedded_id() -> NodeId {
        NodeId {
            url: "embedded".into(),
            version: zakurad::application::build_version().to_string(),
        }
    }

    /// The Zakura build and user agent that `GetLightdInfo` reports.
    pub fn version() -> NodeVersion {
        NodeVersion {
            build: format!("v{}", zakurad::application::build_version()),
            subversion: zakurad::application::user_agent(),
        }
    }

    /// Returns the current best chain tip, if the state is not empty.
    pub fn tip(&self) -> Option<(BlockHeight, BlockHash)> {
        self.services.latest_chain_tip.best_tip_height_and_hash()
    }

    /// Returns a shared handle for state queries.
    pub fn read_state(&self) -> zakura_state::ReadStateService {
        self.services.read_state.clone()
    }

    /// Returns the shared database handle.
    ///
    /// Can be used to modify the database without doing any consensus checks.
    pub fn database(&self) -> zakura_state::ZakuraDb {
        self.services.read_state.db().clone()
    }

    /// Returns the finalized database as the source of the historical pass.
    pub fn finalized_db(&self) -> FinalizedDb {
        FinalizedDb(self.database())
    }

    /// Waits until the synchronizer is likely within its recent-tip window.
    pub async fn wait_until_close_to_tip(&self) -> Result<(), NodeError> {
        self.services
            .sync_status
            .clone()
            .wait_until_close_to_tip()
            .await
            .map_err(NodeError::SyncStopped)
    }

    /// Returns a block from the best chain by hash or height.
    pub async fn block(
        &self,
        hash_or_height: impl Into<BlockId>,
    ) -> Result<Option<Arc<Block>>, NodeError> {
        let response = self
            .services
            .read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::Block(hash_or_height.into()))
            .await
            .map_err(NodeError::State)?;

        match response {
            zakura_state::ReadResponse::Block(block) => Ok(block),
            response => Err(NodeError::UnexpectedState(format!("{response:?}"))),
        }
    }

    /// Verifies and queues a transaction in the mempool.
    pub async fn submit_transaction(
        &self,
        transaction: impl Into<UnminedTx>,
    ) -> Result<UnminedTxId, NodeError> {
        let transaction = transaction.into();
        let transaction_id = transaction.id();
        let response = self
            .services
            .mempool
            .clone()
            .oneshot(zakura_node_services::mempool::Request::Queue(vec![transaction.into()]))
            .await
            .map_err(NodeError::Mempool)?;
        let zakura_node_services::mempool::Response::Queued(mut results) = response else {
            return Err(NodeError::UnexpectedMempool(format!("{response:?}")));
        };
        if results.len() != 1 {
            return Err(NodeError::QueueResults(results.len()));
        }

        results
            .pop()
            .expect("one result exists because its length was checked")
            .map_err(NodeError::Rejected)?
            .await
            .map_err(NodeError::VerifierStopped)?
            .map_err(NodeError::Verification)?;

        Ok(transaction_id)
    }

    /// Returns all transactions currently in the mempool.
    pub async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, NodeError> {
        let response = self
            .services
            .mempool
            .clone()
            .oneshot(zakura_node_services::mempool::Request::FullTransactions)
            .await
            .map_err(NodeError::Mempool)?;
        let zakura_node_services::mempool::Response::FullTransactions { transactions, .. } =
            response
        else {
            return Err(NodeError::UnexpectedMempool(format!("{response:?}")));
        };

        Ok(transactions.into_iter().map(|transaction| transaction.transaction).collect())
    }

    /// Returns an independent listener for best chain tip changes.
    pub fn subscribe_chain_tip(&self) -> ChainTipChange {
        self.services.chain_tip_change.clone()
    }
}
