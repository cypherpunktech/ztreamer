//! Transport-independent requests over the serving snapshot of the head follower.

use std::sync::Arc;

use tokio::sync::{Semaphore, watch};
use tonic::Status;
use zakura_chain::{
    block,
    parameters::{ConsensusBranchId, NetworkUpgrade},
    serialization::ZcashSerialize,
    transaction,
};

use ztreamer_backend::StateBackend;

use crate::telemetry::ServiceMetrics;

use ztreamer_chain::codec::{CompactBlockRecord, StoredBlock};
use ztreamer_indexer::{head::ServingSnapshot, index::Index};
use ztreamer_protocol::{RpcStream, proto};

mod address;
mod blocks;
mod mempool;
mod tree;

const MAX_RANGE_STREAMS: usize = 16;

/// The node version that `GetLightdInfo` reports.
///
/// The default is empty. ztreamerd sets the version of its Zakura build.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeVersion {
    /// The node build. `GetLightdInfo` reports it as `zcashd_build`.
    pub build: String,
    /// The node user agent. `GetLightdInfo` reports it as `zcashd_subversion`.
    pub subversion: String,
}

/// Restricted CompactTxStreamer service over the compact index.
#[derive(Clone)]
pub struct CompactService {
    index: Arc<Index>,
    snapshots: watch::Receiver<ServingSnapshot>,
    chain_name: Arc<str>,
    backend: Arc<dyn StateBackend>,
    node_version: Arc<NodeVersion>,
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
    /// Serves chain state and node requests through any [`StateBackend`], at the generation of
    /// the newest snapshot in `snapshots`.
    pub fn with_backend(
        index: Arc<Index>,
        snapshots: watch::Receiver<ServingSnapshot>,
        chain_name: impl Into<Arc<str>>,
        backend: impl StateBackend,
    ) -> Self {
        let chain_name = chain_name.into();
        let metrics = Arc::new(ServiceMetrics::new());
        Self {
            index,
            snapshots,
            chain_name,
            backend: Arc::new(backend),
            node_version: Arc::default(),
            range_streams: Arc::new(Semaphore::new(MAX_RANGE_STREAMS)),
            ping_enabled: false,
            metrics,
        }
    }

    pub const fn with_ping_enabled(mut self, enabled: bool) -> Self {
        self.ping_enabled = enabled;
        self
    }

    /// Sets the node version that `GetLightdInfo` reports.
    pub fn with_node_version(mut self, version: NodeVersion) -> Self {
        self.node_version = Arc::new(version);
        self
    }

    /// Returns the backend that answers the chain state requests and the node requests.
    pub fn backend(&self) -> Arc<dyn StateBackend> {
        Arc::clone(&self.backend)
    }

    /// A receiver for each serving snapshot that the head follower publishes after this call.
    pub(crate) fn subscribe(&self) -> watch::Receiver<ServingSnapshot> {
        let mut snapshots = self.snapshots.clone();
        snapshots.mark_unchanged();
        snapshots
    }

    pub(crate) fn snapshot(&self) -> ServingSnapshot {
        self.snapshots.borrow().clone()
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
            tokio::task::spawn_blocking(move || index.snapshot(generation)?.block(height).map(Some))
        } else {
            let hash = request
                .hash
                .try_into()
                .map(block::Hash)
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            tokio::task::spawn_blocking(move || index.snapshot(generation)?.block_by_hash(hash))
        };
        lookup
            .await
            .map_err(|error| Status::unavailable(format!("LMDB reader failed: {error}")))?
            .map_err(Status::from)?
            .ok_or_else(|| Status::not_found("block is not in the indexed canonical chain"))
    }

    /// Answers `GetLatestBlock`.
    pub fn latest_block(&self) -> Result<proto::BlockId, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        Ok(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.0.to_vec() })
    }

    /// Answers `GetLightdInfo`.
    pub fn lightd_info(&self) -> proto::LightdInfo {
        let tip = self.snapshot().visible_tip;
        let tip_height = tip.map_or(0, |tip| tip.height);
        let network = self.backend.network();
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
            zcashd_build: self.node_version.build.clone(),
            zcashd_subversion: self.node_version.subversion.clone(),
            upgrade_name: next_upgrade.map_or_else(String::new, |(_, upgrade)| upgrade.to_string()),
            upgrade_height: next_upgrade.map_or(0, |(height, _)| u64::from(height.0)),
            lightwallet_protocol_version: "v0.5.0".to_owned(),
            ..Default::default()
        }
    }

    /// Answers `Ping`. Fails with `UNIMPLEMENTED` unless ping is enabled.
    pub fn ping(&self) -> Result<proto::PingResponse, Status> {
        if !self.ping_enabled {
            return Err(Status::unimplemented(
                "Ping is disabled; start ztreamerd with --ping-very-insecure to enable it",
            ));
        }
        Ok(proto::PingResponse { entry: 1, exit: 0 })
    }

    /// Answers `GetTransaction`.
    pub async fn transaction(
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
        self.backend
            .mempool_transactions()
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
        self.backend
            .transaction(hash)
            .await?
            .map(|mined| {
                Ok(proto::RawTransaction {
                    data: mined
                        .transaction
                        .zcash_serialize_to_vec()
                        .map_err(|error| Status::internal(error.to_string()))?,
                    height: u64::from(mined.height.0),
                })
            })
            .transpose()
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

fn reorganized(height: u32) -> Status {
    Status::unavailable(format!(
        "chain reorganized while streaming at height {height}; restart from a fresh tip"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::EphemeralService;

    #[tokio::test]
    async fn ping_answers_only_when_enabled() {
        let ctx = EphemeralService::empty().await;

        assert_eq!(
            ctx.service.clone().with_ping_enabled(false).ping().unwrap_err().code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            ctx.service.with_ping_enabled(true).ping().unwrap(),
            proto::PingResponse { entry: 1, exit: 0 }
        );
    }
}
