//! Transport-independent requests, serving snapshots, readiness, and canonical-head following.

use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Semaphore, watch};
use tokio_stream::{Stream, StreamExt};
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

use crate::telemetry::{ObservedSource, ServiceMetrics};

use crate::project::{PoolSelection, add_transparent_txs, project_block};
use ztreamer_indexer::{
    Digest,
    codec::{CompactBlockRecord, EncodedBlockRecord, ProtobufBlockRecord, StoredBlock},
    head::{CanonicalBlockSource, HeadSyncError, recover_deep_reorg},
    index::{BlockId, Index, IndexError, IndexState},
    pipeline::PipelineConfig,
};
use ztreamer_protocol::{EncodedCompactBlock, proto};

mod address;
mod mempool;
#[cfg(test)]
mod test_utils;
mod tree;

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

#[derive(Clone, Copy, Debug)]
pub struct HeadFollowerConfig {
    pub poll_interval: Duration,
    pub attempt_timeout: Duration,
    pub freshness_timeout: Duration,
}

impl Default for HeadFollowerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            attempt_timeout: Duration::from_secs(30),
            freshness_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum HeadFollowerError {
    #[error("head follower durations are invalid")]
    Config,
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

    /// Reconciles one head view and fails closed when it detects a deep reorg.
    pub async fn sync_head(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        let started = Instant::now();
        let snapshot = self.snapshot();
        let mut observed = ObservedSource {
            source,
            previous: snapshot.visible_tip,
            observed: None,
            metrics: &self.metrics,
        };
        let result = ztreamer_indexer::head::sync_head_once(
            &self.index,
            &mut observed,
            &snapshot.volatile_head,
            config,
        )
        .await;
        if matches!(result, Err(HeadSyncError::DeepReorg { .. })) {
            self.begin_recovery();
        }
        let (state, head) = result.inspect_err(|_| {
            self.metrics.head_error.record(started.elapsed().as_secs_f64());
        })?;
        self.publish_head(state, head)
            .expect("head reconciliation must produce a connected snapshot");
        self.metrics.published(observed.observed, self.snapshot().visible_tip);
        self.metrics.head_success.record(started.elapsed().as_secs_f64());
        Ok(state)
    }

    /// Stages and publishes a deep replacement while new requests remain disabled.
    pub async fn recover_deep_reorg(
        &self,
        source: &mut impl CanonicalBlockSource,
        config: PipelineConfig,
    ) -> Result<IndexState, HeadSyncError> {
        self.begin_recovery();
        let snapshot = self.snapshot();
        let (state, head) =
            recover_deep_reorg(&self.index, source, &snapshot.volatile_head, config).await?;
        self.publish_head(state, head).expect("deep recovery must produce a connected snapshot");
        Ok(state)
    }

    /// Polls Zakura until `shutdown` becomes true. Source errors are retried.
    pub async fn follow_head(
        &self,
        mut source: impl CanonicalBlockSource,
        pipeline: PipelineConfig,
        config: HeadFollowerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), HeadFollowerError> {
        if config.poll_interval.is_zero()
            || config.attempt_timeout.is_zero()
            || config.freshness_timeout.is_zero()
            || config.poll_interval > config.freshness_timeout
            || config.attempt_timeout > config.freshness_timeout
        {
            return Err(HeadFollowerError::Config);
        }

        let mut interval = tokio::time::interval(config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            }

            let attempt = async {
                if self.snapshot().ready {
                    match self.sync_head(&mut source, pipeline).await {
                        Err(HeadSyncError::DeepReorg { .. }) => {
                            self.recover_deep_reorg(&mut source, pipeline).await
                        }
                        result => result,
                    }
                } else {
                    self.recover_deep_reorg(&mut source, pipeline).await
                }
            };
            let result = tokio::select! {
                result = tokio::time::timeout(config.attempt_timeout, attempt) => {
                    result
                        .map_err(|_| "Zakura head attempt timed out".to_owned())
                        .and_then(|result| result.map_err(|error| error.to_string()))
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            if let Err(error) = result {
                self.mark_source_failure(error, config.freshness_timeout);
            }
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
            let hash: Digest = request
                .hash
                .as_slice()
                .try_into()
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
            let hash: Digest = request
                .hash
                .try_into()
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            tokio::task::spawn_blocking(move || index.read_block_by_hash_as(generation, hash))
        };
        lookup
            .await
            .map_err(|error| Status::unavailable(format!("LMDB reader failed: {error}")))?
            .map_err(index_status)?
            .ok_or_else(|| Status::not_found("block is not in the indexed canonical chain"))
    }

    /// Returns a decoded block, optionally projected to nullifiers.
    pub async fn block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<proto::CompactBlock, Status> {
        let record = self.record_as::<ProtobufBlockRecord>(request).await?;
        let pools = PoolSelection::from_request(&[]).expect("empty pool selection is valid");
        Ok(project_block(record.0, pools, nullifiers))
    }

    pub(crate) async fn encoded_block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<EncodedCompactBlock, Status> {
        if nullifiers {
            return self.block(request, true).await.map(Into::into);
        }
        Ok(self.record_as::<EncodedBlockRecord>(request).await?.block)
    }

    pub(crate) async fn encoded_range(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<EncodedCompactBlock>, Status> {
        let guard = self.metrics.range();
        guard.response(self.encoded_range_inner(request, nullifiers).await)
    }

    async fn encoded_range_inner(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<EncodedCompactBlock>, Status> {
        let pools = PoolSelection::from_request(&request.pool_types)?;
        if !nullifiers && pools.is_all() {
            self.range_as(request, |record: EncodedBlockRecord| record.block).await
        } else {
            Ok(Box::pin(
                self.range_inner(request, nullifiers).await?.map(|block| block.map(Into::into)),
            ))
        }
    }

    /// Streams decoded blocks with the requested pool/nullifier projection.
    pub async fn range(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<proto::CompactBlock>, Status> {
        let guard = self.metrics.range();
        guard.response(self.range_inner(request, nullifiers).await)
    }

    async fn range_inner(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<proto::CompactBlock>, Status> {
        let pools = PoolSelection::from_request(&request.pool_types)?;
        let blocks = self
            .range_as(request, move |record: ProtobufBlockRecord| {
                project_block(record.0, pools, nullifiers)
            })
            .await?;
        if !pools.transparent {
            return Ok(blocks);
        }
        let source = self.zakura.clone();
        // Transparent data is deliberately fetched on demand, outside the compact index.
        // Obviously this is extremely slow. Transparent scanning is not a recommended use case.
        Ok(Box::pin(blocks.then(move |block| {
            let source = source.clone();
            async move {
                let mut block = block?;
                let hash = block::Hash(
                    block
                        .hash
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid compact block hash"))?,
                );
                let response =
                    source.oneshot(ReadRequest::Block(hash.into())).await.map_err(source_status)?;
                let full = match response {
                    ReadResponse::Block(Some(block)) => block,
                    ReadResponse::Block(None) => return Err(reorganized(block.height as u32)),
                    _ => return Err(Status::internal("unexpected block response")),
                };
                add_transparent_txs(&mut block, &full.transactions);
                Ok(block)
            }
        })))
    }

    async fn range_as<T: StoredBlock + Send + 'static, B: Send + 'static>(
        &self,
        request: proto::BlockRange,
        project: impl Fn(T) -> B + Send + 'static,
    ) -> Result<RpcStream<B>, Status> {
        let (start, end) = range_heights(&request)?;
        let snapshot = self.snapshot();
        ensure_ready(&snapshot)?;
        let visible_tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        if start > visible_tip.height || end > visible_tip.height {
            return Err(Status::out_of_range(format!(
                "range exceeds compact tip {}",
                visible_tip.height
            )));
        }
        if !snapshot.tip_fresh
            && snapshot
                .durable_tip
                .is_none_or(|durable| start > durable.height || end > durable.height)
        {
            return Err(Status::unavailable("canonical head source is stale"));
        }
        let admission_started = Instant::now();
        let permit = Arc::clone(&self.range_streams).acquire_owned().await;
        self.metrics.admission.record(admission_started.elapsed().as_secs_f64());
        let permit = permit.map_err(|_| Status::unavailable("range stream pool is closed"))?;
        let index = Arc::clone(&self.index);
        let ascending = start <= end;
        let durable_tip = snapshot.durable_tip.map(|tip| tip.height);
        let mut cursor = Some(start);
        // Bound active streams so large ranges do not all compete for throughput.
        // The permit lasts until the response stream is dropped, including transport
        // backpressure. Each stream buffers one chunk; its LMDB transaction ends
        // inside read_range_latest_as before yielding to the transport.
        let mut records = Vec::<T>::new().into_iter();
        // (hash, previous_hash) of the last record sent; every chunk must chain onto it.
        let mut last: Option<(Digest, Digest)> = None;
        Ok(Box::pin(tokio_stream::iter(std::iter::from_fn(move || {
            loop {
                let _ = &permit;
                if let Some(record) = records.next() {
                    last = Some((record.hash(), record.previous_hash()));
                    return Some(Ok(project(record)));
                }
                let height = cursor?;
                let mut chunk_end = if ascending {
                    height.saturating_add(63).min(end)
                } else {
                    height.saturating_sub(63).max(end)
                };
                let durable = durable_tip.is_some_and(|tip| height <= tip);
                if let Some(tip) = durable_tip {
                    if ascending && durable {
                        chunk_end = chunk_end.min(tip);
                    } else if !ascending && !durable {
                        chunk_end = chunk_end.max(tip.saturating_add(1));
                    }
                }
                let mut chunk = Vec::with_capacity(64);
                let result = if durable {
                    index.read_range_latest_as(height, chunk_end, |record| {
                        chunk.push(record);
                        true
                    })
                } else {
                    (0..=height.abs_diff(chunk_end)).try_for_each(|offset| {
                        let height = if ascending { height + offset } else { height - offset };
                        chunk.push(T::from_record(
                            snapshot.volatile(height).ok_or(IndexError::Coverage { height })?,
                        ));
                        Ok(())
                    })
                };
                if let Err(error) = result {
                    cursor = None;
                    return Some(Err(match error {
                        // Validation saw this height; if the index no longer has it, the chain shrank.
                        IndexError::Coverage { height } if durable => reorganized(height),
                        other => index_status(other),
                    }));
                }
                if let Some((last_hash, last_previous)) = last
                    && let Some(first) = chunk.first()
                {
                    let continuous = if ascending {
                        first.previous_hash() == last_hash
                    } else {
                        last_previous == first.hash()
                    };
                    if !continuous {
                        cursor = None;
                        return Some(Err(reorganized(first.height())));
                    }
                }
                cursor = (chunk_end != end).then_some(if ascending {
                    chunk_end + 1
                } else {
                    chunk_end - 1
                });
                records = chunk.into_iter();
            }
        }))))
    }

    pub(crate) fn latest_block(&self) -> Result<proto::BlockId, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        Ok(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.to_vec() })
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
        let hash: Digest = request
            .hash
            .try_into()
            .map_err(|_| Status::invalid_argument("transaction hash must be 32 bytes"))?;
        let hash = transaction::Hash(hash);
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

fn range_heights(request: &proto::BlockRange) -> Result<(u32, u32), Status> {
    let endpoint = |name, endpoint: &Option<proto::BlockId>| {
        let endpoint = endpoint
            .as_ref()
            .ok_or_else(|| Status::invalid_argument(format!("range.{name} is required")))?;
        if !endpoint.hash.is_empty() {
            return Err(Status::invalid_argument("range endpoints must be heights"));
        }
        u32::try_from(endpoint.height)
            .map_err(|_| Status::invalid_argument("range height exceeds u32"))
    };
    Ok((endpoint("start", &request.start)?, endpoint("end", &request.end)?))
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
    use tokio_stream::StreamExt;
    use tonic::Request;
    use zakura_chain::{block, parameters::Network};
    use zakura_state::Config;
    use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamer;

    use ztreamer_indexer::{
        head::HeadError, ingest::OrderedBuilder, parser::RawIndexBlock, pipeline::PipelineConfig,
    };

    use super::test_utils::{hash, index_through, parsed, range, raw_block, record};

    struct HeadSource(Vec<RawIndexBlock>);

    #[tonic::async_trait]
    impl CanonicalBlockSource for HeadSource {
        async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
            Ok(self.0.get(height as usize).cloned())
        }
    }

    #[test]
    fn follower_polls_until_shutdown() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service =
                    CompactService::new(index, IndexState::default(), "main", read_service);
                let (shutdown, receiver) = watch::channel(false);
                let follower = {
                    let service = service.clone();
                    tokio::spawn(async move {
                        service
                            .follow_head(
                                HeadSource((0..=12).map(raw_block).collect()),
                                PipelineConfig::default(),
                                HeadFollowerConfig {
                                    poll_interval: Duration::from_millis(1),
                                    attempt_timeout: Duration::from_millis(100),
                                    freshness_timeout: Duration::from_millis(100),
                                },
                                receiver,
                            )
                            .await
                    })
                };

                tokio::time::timeout(Duration::from_secs(1), async {
                    while !service.readiness().tip {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(service.snapshot().visible_tip.unwrap().height, 12);
                shutdown.send(true).unwrap();
                follower.await.unwrap().unwrap();
            },
        );
    }

    #[test]
    fn ping_answers_only_when_enabled() {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let index =
                Arc::new(Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
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

    #[test]
    fn streams_cross_range_descending_and_fetches_transparent_data() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 1_005);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(index, state, "main", read_service);
                assert_eq!(
                    service.readiness(),
                    Readiness {
                        historical: true,
                        tip: false,
                        recovering: false,
                        source_error: None,
                    }
                );
                let descending = range(1_002, 998);
                let mut stream = service
                    .get_block_range(Request::new(descending.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_002, 1_001, 1_000, 999, 998]);

                service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();
                assert!(service.readiness().tip);
                let mut stream = service
                    .get_block_range(Request::new(range(1_004, 1_007)))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);

                let recovery_range = range(1_004, 1_007);
                let mut pinned = service
                    .get_block_range(Request::new(recovery_range.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                service.begin_recovery();
                assert_eq!(
                    service
                        .get_block_range(Request::new(recovery_range))
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unavailable
                );
                let mut heights = Vec::new();
                while let Some(block) = pinned.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);
                service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();
                service.mark_source_failure("offline".to_owned(), Duration::ZERO);
                assert!(!service.readiness().tip);
                assert_eq!(
                    service
                        .get_latest_block(Request::new(proto::ChainSpec::default()))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );
                assert!(
                    service
                        .get_block(Request::new(proto::BlockId { height: 1_000, hash: Vec::new() }))
                        .await
                        .is_ok()
                );
                assert_eq!(
                    service
                        .get_transaction(Request::new(proto::TxFilter {
                            hash: vec![0; 32],
                            ..Default::default()
                        }))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );

                let mut transparent = descending;
                transparent.pool_types = vec![proto::PoolType::Transparent as i32];
                assert_eq!(
                    service
                        .get_block_range(Request::new(transparent))
                        .await
                        .unwrap()
                        .into_inner()
                        .next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::send_transaction(
                        &service,
                        Request::new(proto::RawTransaction::default()),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                    tonic::Code::InvalidArgument
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_tx(
                        &service,
                        Request::new(proto::GetMempoolTxRequest {
                            pool_types: vec![proto::PoolType::Transparent as i32],
                            ..Default::default()
                        }),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_tx(
                        &service,
                        Request::new(proto::GetMempoolTxRequest::default()),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_stream(&service, Request::new(proto::Empty {}),)
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unavailable
                );

                service.snapshot.send_modify(|snapshot| {
                    snapshot.visible_tip = Some(BlockId::new(3_464_754, [0; 32]));
                });
                let info = service.lightd_info();
                assert_eq!(info.sapling_activation_height, 419_200);
                assert_eq!(info.consensus_branch_id, "37a5165b");
                assert_eq!(info.block_height, 3_464_754);
                assert!(!info.zcashd_build.is_empty());
                assert!(!info.zcashd_subversion.is_empty());
            },
        );
    }

    #[test]
    fn range_admission_is_shared_and_releases_on_drop() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(index, state, "main", read_service);
                let clone = service.clone();
                let mut streams = Vec::new();
                for _ in 0..MAX_RANGE_STREAMS {
                    streams.push(service.encoded_range(range(0, 200), false).await.unwrap());
                }
                // Poll once to prove the limit applies across service clones.
                let waiting = clone.encoded_range(range(0, 200), false);
                tokio::pin!(waiting);
                assert!(
                    std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(
                            std::future::Future::poll(waiting.as_mut(), cx).is_pending(),
                        )
                    })
                    .await
                );
                drop(streams.pop());
                let mut admitted = tokio::time::timeout(Duration::from_secs(1), waiting)
                    .await
                    .expect("dropping a response frees a slot")
                    .unwrap();
                let mut count = 0;
                while let Some(block) = admitted.next().await {
                    block.unwrap();
                    count += 1;
                }
                assert_eq!(count, 201);
                drop(admitted);
                let _next_stream = tokio::time::timeout(
                    Duration::from_secs(1),
                    service.encoded_range(range(0, 0), false),
                )
                .await
                .expect("completed response frees a slot")
                .unwrap();
            },
        );
    }

    #[test]
    fn range_stream_survives_a_durable_advance_mid_flight() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);

                let mut stream = service
                    .encoded_range(range(0, 200), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 0);

                // A durable advance lands under the stream.
                let mut builder = OrderedBuilder::new(state, 1024 * 1024).unwrap();
                for height in 201..=205 {
                    builder.push(parsed(height)).unwrap();
                }
                let batch = builder
                    .build_batch(Some(205), Some(205), 1024 * 1024)
                    .unwrap()
                    .expect("the builder holds five blocks");
                let advanced = index.write(batch).unwrap();
                assert_eq!(advanced.generation(), state.generation() + 1);

                let mut heights = vec![0];
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, (0..=200).collect::<Vec<_>>());
            },
        );
    }

    #[test]
    fn range_stream_refuses_a_reorg_it_cannot_chain_onto() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);

                let mut stream = service
                    .encoded_range(range(0, 200), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 0);

                // A competing chain replaces everything above 30; the next chunk cannot link onto 63.
                let fork_hash = |height: u32| {
                    let mut digest = hash(height);
                    digest[31] = 0xff;
                    digest
                };
                let fork: Vec<CompactBlockRecord> = (31..=205)
                    .map(|height| {
                        let mut forked = record(height);
                        forked.hash = fork_hash(height);
                        if height > 31 {
                            forked.previous_hash = fork_hash(height - 1);
                        }
                        forked
                    })
                    .collect();
                index
                    .replace_mutable_suffix(
                        state.generation(),
                        BlockId::new(30, hash(30)),
                        fork,
                        None,
                    )
                    .unwrap();

                let mut heights = vec![0];
                let mut refusal = None;
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(block) => heights.push(block.height),
                        Err(status) => {
                            refusal = Some(status);
                            break;
                        }
                    }
                }
                assert_eq!(heights, (0..=63).collect::<Vec<_>>());
                assert_eq!(
                    refusal.expect("the boundary is refused").code(),
                    tonic::Code::Unavailable
                );
            },
        );
    }

    #[test]
    fn descending_range_from_the_volatile_head_survives_an_advance() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);
                service.publish_head(state, vec![record(201), record(202)]).unwrap();

                // The first chunk is volatile, the rest is durable and read after an advance.
                let mut stream = service
                    .encoded_range(range(202, 100), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 202);
                let mut builder = OrderedBuilder::new(state, 1024 * 1024).unwrap();
                for height in 201..=205 {
                    builder.push(parsed(height)).unwrap();
                }
                let batch = builder
                    .build_batch(Some(205), Some(205), 1024 * 1024)
                    .unwrap()
                    .expect("the builder holds five blocks");
                index.write(batch).unwrap();

                let mut heights = vec![202];
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, (100..=202).rev().collect::<Vec<_>>());
            },
        );
    }
}
