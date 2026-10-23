//! Transport-independent requests, serving snapshots, readiness, and canonical-head following.

use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Semaphore, watch};
use tokio_stream::Stream;
use tonic::Status;
use tower::ServiceExt;
use zakura_chain::{
    block,
    parameters::{ConsensusBranchId, NetworkUpgrade},
    serialization::ZcashSerialize,
    transaction,
};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};
use ztreamer_node::NodeClient;

use crate::telemetry::ServiceMetrics;

use ztreamer_indexer::{
    codec::{CompactBlockRecord, StoredBlock},
    index::{BlockId, Index, IndexError, IndexState},
};
use ztreamer_protocol::proto;

mod address;
mod blocks;
mod head;
mod mempool;
#[cfg(test)]
mod test_utils;
mod tree;

pub use head::{HeadFollowerConfig, HeadFollowerError};

pub(crate) type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const MAX_RANGE_STREAMS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingSnapshot {
    pub generation: u64,
    pub durable_tip: Option<BlockId>,
    pub visible_tip: Option<BlockId>,
    pub volatile_head: Arc<[CompactBlockRecord]>,
    pub ready: bool,
    pub tip_fresh: bool,
    pub last_source_success: Option<Instant>,
    pub source_error: Option<Arc<str>>,
}

impl From<IndexState> for ServingSnapshot {
    fn from(state: IndexState) -> Self {
        Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip: state.durable_tip(),
            volatile_head: Arc::default(),
            ready: true,
            tip_fresh: false,
            last_source_success: None,
            source_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("volatile head does not connect to the durable index")]
pub struct SnapshotError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Readiness {
    pub historical: bool,
    pub tip: bool,
    pub recovering: bool,
    pub source_error: Option<Arc<str>>,
}

impl ServingSnapshot {
    fn with_head(
        state: IndexState,
        volatile_head: Vec<CompactBlockRecord>,
    ) -> Result<Self, SnapshotError> {
        let mut expected_height = state.durable_tip().map_or(0, |tip| tip.height.saturating_add(1));
        let mut previous_hash = state.durable_tip().map(|tip| tip.hash);
        for block in &volatile_head {
            if block.height != expected_height
                || previous_hash.is_some_and(|hash| block.previous_hash != hash)
            {
                return Err(SnapshotError);
            }
            expected_height = expected_height.checked_add(1).ok_or(SnapshotError)?;
            previous_hash = Some(block.hash);
        }
        let visible_tip = volatile_head
            .last()
            .map(|block| BlockId::new(block.height, block.hash))
            .or(state.durable_tip());
        Ok(Self {
            generation: state.generation(),
            durable_tip: state.durable_tip(),
            visible_tip,
            volatile_head: volatile_head.into(),
            ready: true,
            tip_fresh: true,
            last_source_success: Some(Instant::now()),
            source_error: None,
        })
    }

    fn volatile(&self, height: u32) -> Option<&CompactBlockRecord> {
        self.volatile_head
            .binary_search_by_key(&height, |block| block.height)
            .ok()
            .map(|index| &self.volatile_head[index])
    }
}

/// Restricted CompactTxStreamer service over the compact index.
#[derive(Clone)]
pub struct CompactService {
    index: Arc<Index>,
    snapshot: watch::Sender<ServingSnapshot>,
    chain_name: Arc<str>,
    zakura: ReadStateService,
    node: Option<NodeClient>,
    range_streams: Arc<Semaphore>,
    ping_enabled: bool,
    metrics: Arc<ServiceMetrics>,
}

impl std::fmt::Debug for CompactService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactService")
            .field("chain_name", &self.chain_name)
            .field("ping_enabled", &self.ping_enabled)
            .finish_non_exhaustive()
    }
}

impl CompactService {
    pub fn new(
        index: Arc<Index>,
        state: IndexState,
        chain_name: impl Into<Arc<str>>,
        zakura: ReadStateService,
    ) -> Self {
        let chain_name = chain_name.into();
        let (snapshot, _) = watch::channel(state.into());
        let metrics = Arc::new(ServiceMetrics::new());
        metrics.snapshot(&snapshot.borrow());
        Self {
            index,
            snapshot,
            chain_name,
            zakura,
            node: None,
            range_streams: Arc::new(Semaphore::new(MAX_RANGE_STREAMS)),
            ping_enabled: false,
            metrics,
        }
    }

    pub fn with_node(
        index: Arc<Index>,
        state: IndexState,
        chain_name: impl Into<Arc<str>>,
        node: NodeClient,
    ) -> Self {
        let mut service = Self::new(index, state, chain_name, node.read_state());
        service.node = Some(node);
        service
    }

    pub const fn with_ping_enabled(mut self, enabled: bool) -> Self {
        self.ping_enabled = enabled;
        self
    }

    pub fn publish(&self, state: IndexState) {
        self.snapshot.send_replace(state.into());
    }

    pub fn publish_head(
        &self,
        state: IndexState,
        volatile_head: Vec<CompactBlockRecord>,
    ) -> Result<(), SnapshotError> {
        let snapshot = ServingSnapshot::with_head(state, volatile_head)?;
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
        Ok(())
    }

    /// Stops new chain-data requests while a deep replacement is staged.
    pub fn begin_recovery(&self) {
        let mut snapshot = self.snapshot();
        snapshot.ready = false;
        snapshot.tip_fresh = false;
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
    }

    pub fn readiness(&self) -> Readiness {
        let snapshot = self.snapshot();
        Readiness {
            historical: snapshot.durable_tip.is_some(),
            tip: snapshot.ready && snapshot.tip_fresh,
            recovering: !snapshot.ready,
            source_error: snapshot.source_error,
        }
    }

    fn mark_source_failure(&self, error: String, freshness_timeout: Duration) {
        let mut snapshot = self.snapshot();
        snapshot.tip_fresh =
            snapshot.last_source_success.is_some_and(|last| last.elapsed() < freshness_timeout);
        snapshot.source_error = Some(error.into());
        self.metrics.snapshot(&snapshot);
        self.snapshot.send_replace(snapshot);
    }

    fn snapshot(&self) -> ServingSnapshot {
        self.snapshot.borrow().clone()
    }

    async fn record(&self, request: proto::BlockId) -> Result<CompactBlockRecord, Status> {
        self.record_as(request).await
    }

    async fn record_as<T: StoredBlock + Send + 'static>(
        &self,
        request: proto::BlockId,
    ) -> Result<T, Status> {
        let snapshot = self.snapshot();
        ensure_ready(&snapshot)?;
        if request.hash.is_empty() {
            let height = u32::try_from(request.height)
                .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
            if let Some(record) = snapshot.volatile(height) {
                ensure_tip_ready(&snapshot)?;
                return Ok(T::from_record(record));
            }
        } else {
            let hash = request
                .hash
                .as_slice()
                .try_into()
                .map(block::Hash)
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            if let Some(record) = snapshot.volatile_head.iter().find(|record| record.hash == hash) {
                ensure_tip_ready(&snapshot)?;
                return Ok(T::from_record(record));
            }
        }
        let index = Arc::clone(&self.index);
        let generation = snapshot.generation;
        let lookup = if request.hash.is_empty() {
            let height = u32::try_from(request.height)
                .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
            tokio::task::spawn_blocking(move || index.read_block_as(generation, height).map(Some))
        } else {
            let hash = request
                .hash
                .try_into()
                .map(block::Hash)
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            tokio::task::spawn_blocking(move || index.read_block_by_hash_as(generation, hash))
        };
        lookup
            .await
            .map_err(|error| Status::unavailable(format!("LMDB reader failed: {error}")))?
            .map_err(index_status)?
            .ok_or_else(|| Status::not_found("block is not in the indexed canonical chain"))
    }

    pub(crate) fn latest_block(&self) -> Result<proto::BlockId, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        Ok(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.0.to_vec() })
    }

    pub(crate) fn lightd_info(&self) -> proto::LightdInfo {
        let tip = self.snapshot().visible_tip;
        let tip_height = tip.map_or(0, |tip| tip.height);
        let network = self.zakura.db().network();
        let next_upgrade =
            network.activation_list().into_iter().find(|(height, _)| height.0 > tip_height);
        proto::LightdInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            vendor: "Ztreamer".to_owned(),
            taddr_support: true,
            chain_name: self.chain_name.to_string(),
            sapling_activation_height: NetworkUpgrade::Sapling
                .activation_height(&network)
                .map_or(0, |height| u64::from(height.0)),
            consensus_branch_id: ConsensusBranchId::current(&network, block::Height(tip_height))
                .unwrap_or(ConsensusBranchId::RPC_MISSING_ID)
                .to_string(),
            block_height: u64::from(tip_height),
            estimated_height: u64::from(tip_height),
            zcashd_build: format!("v{}", zakurad::application::build_version()),
            zcashd_subversion: zakurad::application::user_agent(),
            upgrade_name: next_upgrade.map_or_else(String::new, |(_, upgrade)| upgrade.to_string()),
            upgrade_height: next_upgrade.map_or(0, |(height, _)| u64::from(height.0)),
            lightwallet_protocol_version: "v0.5.0".to_owned(),
            ..Default::default()
        }
    }

    pub(crate) fn ping(&self) -> Result<proto::PingResponse, Status> {
        if !self.ping_enabled {
            return Err(Status::unimplemented(
                "Ping is disabled; start ztreamerd with --ping-very-insecure to enable it",
            ));
        }
        Ok(proto::PingResponse { entry: 1, exit: 0 })
    }

    pub(crate) async fn transaction(
        &self,
        request: proto::TxFilter,
    ) -> Result<proto::RawTransaction, Status> {
        let hash = request
            .hash
            .try_into()
            .map(transaction::Hash)
            .map_err(|_| Status::invalid_argument("transaction hash must be 32 bytes"))?;
        if let Some(transaction) = self.mined_transaction(hash).await? {
            return Ok(transaction);
        }

        // todo(@distractedm1nd): this sucks and should be replaced with a direct mempool query
        Self::mempool_transactions(self.node()?.clone())
            .await?
            .into_iter()
            .find(|transaction| transaction.id().mined_id() == hash)
            .map(|transaction| {
                transaction
                    .transaction()
                    .zcash_serialize_to_vec()
                    .map(|data| proto::RawTransaction { data, height: 0 })
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .transpose()?
            .ok_or_else(|| Status::not_found("transaction not found"))
    }

    async fn mined_transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<proto::RawTransaction>, Status> {
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::Transaction(hash))
            .await
            .map_err(source_status)?;
        match response {
            ReadResponse::Transaction(Some(transaction)) => Ok(Some(proto::RawTransaction {
                data: transaction
                    .tx
                    .zcash_serialize_to_vec()
                    .map_err(|error| Status::internal(error.to_string()))?,
                height: u64::from(transaction.height.0),
            })),
            ReadResponse::Transaction(None) => Ok(None),
            _ => Err(Status::internal("unexpected transaction response")),
        }
    }

    fn node(&self) -> Result<&NodeClient, Status> {
        self.node
            .as_ref()
            .ok_or_else(|| Status::unavailable("embedded Zakura node handle is unavailable"))
    }
}

fn ensure_ready(snapshot: &ServingSnapshot) -> Result<(), Status> {
    if snapshot.ready { Ok(()) } else { Err(Status::unavailable("deep reorg recovery is active")) }
}

fn ensure_tip_ready(snapshot: &ServingSnapshot) -> Result<(), Status> {
    ensure_ready(snapshot)?;
    if snapshot.tip_fresh {
        Ok(())
    } else {
        Err(Status::unavailable("canonical head source is stale"))
    }
}

fn source_status(error: zakura_state::BoxError) -> Status {
    Status::unavailable(format!("Zakura read failed: {error}"))
}

fn reorganized(height: u32) -> Status {
    Status::unavailable(format!(
        "chain reorganized while streaming at height {height}; restart from a fresh tip"
    ))
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::Coverage { .. } => Status::out_of_range(error.to_string()),
        IndexError::Generation { .. } => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{block, parameters::Network};
    use zakura_state::Config;

    #[test]
    fn ping_answers_only_when_enabled() {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let index = Arc::new(
                Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap(),
            );
            let (_state_service, read_service, _tip, _change) =
                zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                    .await
                    .expect("ephemeral state initializes");
            let service = CompactService::new(index, IndexState::default(), "main", read_service);

            assert_eq!(
                service.clone().with_ping_enabled(false).ping().unwrap_err().code(),
                tonic::Code::Unimplemented
            );
            assert_eq!(
                service.with_ping_enabled(true).ping().unwrap(),
                proto::PingResponse { entry: 1, exit: 0 }
            );
        });
    }
}
