//! Transport-independent requests, serving snapshots, readiness, and canonical-head following.

use std::{
    collections::HashSet,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Semaphore, mpsc, watch};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::Status;
use tower::ServiceExt;
use zakura_chain::{
    block,
    parameters::{ConsensusBranchId, NetworkUpgrade},
    serialization::{ZcashDeserialize, ZcashSerialize},
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{self, Transaction},
    transparent,
};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};
use ztreamer_node::NodeClient;

use crate::telemetry::{ObservedSource, ServiceMetrics};

use crate::project::{
    PoolSelection, add_transparent_txs, compact_mempool_txs, excluded_txids, project_block,
};
use ztreamer_indexer::{
    Digest,
    codec::{CompactBlockRecord, EncodedBlockRecord, ProtobufBlockRecord, StoredBlock},
    head::{CanonicalBlockSource, HeadSyncError, recover_deep_reorg},
    index::{BlockId, Index, IndexError, IndexState},
    pipeline::PipelineConfig,
};
use ztreamer_protocol::{EncodedCompactBlock, proto};

pub(crate) type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const MAX_RANGE_STREAMS: usize = 16;
const MAX_UTXO_ADDRESSES: usize = 1_000;

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

    pub(crate) async fn tree_state(
        &self,
        request: proto::BlockId,
    ) -> Result<proto::TreeState, Status> {
        let record = self.record(request).await?;
        let source = self.zakura.clone();
        let hash = block::Hash(record.hash);
        let (sapling, orchard, ironwood) = tokio::try_join!(
            source.clone().oneshot(ReadRequest::SaplingTree(hash.into())),
            source.clone().oneshot(ReadRequest::OrchardTree(hash.into())),
            source.oneshot(ReadRequest::IronwoodTree(hash.into())),
        )
        .map_err(source_status)?;

        let sapling_tree = match sapling {
            ReadResponse::SaplingTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Sapling tree response")),
        };
        let orchard_tree = match orchard {
            ReadResponse::OrchardTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Orchard tree response")),
        };
        let ironwood_tree = match ironwood {
            ReadResponse::IronwoodTree(tree) => {
                tree.map(|tree| hex::encode(tree.to_rpc_bytes())).unwrap_or_default()
            }
            _ => return Err(Status::internal("unexpected Ironwood tree response")),
        };

        Ok(proto::TreeState {
            network: self.chain_name.to_string(),
            height: u64::from(record.height),
            hash: hash.to_string(),
            time: record.time,
            sapling_tree,
            orchard_tree,
            ironwood_tree,
        })
    }

    fn subtree_stream(
        &self,
        roots: Vec<(Vec<u8>, u32)>,
        snapshot: ServingSnapshot,
    ) -> RpcStream<proto::SubtreeRoot> {
        let index = Arc::clone(&self.index);
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn_blocking(move || {
            for (root_hash, height) in roots {
                let block = match snapshot.volatile(height) {
                    Some(block) => block.clone(),
                    None => match index.read_block(snapshot.generation, height) {
                        Ok(block) => block,
                        Err(error) => {
                            let _ = sender.blocking_send(Err(index_status(error)));
                            return;
                        }
                    },
                };
                if sender
                    .blocking_send(Ok(proto::SubtreeRoot {
                        root_hash,
                        completing_block_hash: block.hash.to_vec(),
                        completing_block_height: u64::from(height),
                    }))
                    .is_err()
                {
                    return;
                }
            }
        });
        Box::pin(ReceiverStream::new(receiver))
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

    pub(crate) async fn latest_tree_state(&self) -> Result<proto::TreeState, Status> {
        let snapshot = self.snapshot();
        ensure_tip_ready(&snapshot)?;
        let tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        self.tree_state(proto::BlockId { height: u64::from(tip.height), hash: tip.hash.to_vec() })
            .await
    }

    pub(crate) async fn subtree_roots(
        &self,
        request: proto::GetSubtreeRootsArg,
    ) -> Result<RpcStream<proto::SubtreeRoot>, Status> {
        let start_index = NoteCommitmentSubtreeIndex(
            request
                .start_index
                .try_into()
                .map_err(|_| Status::invalid_argument("subtree start index exceeds u16"))?,
        );
        let limit = match request.max_entries {
            0 => None,
            limit => Some(NoteCommitmentSubtreeIndex(
                limit
                    .try_into()
                    .map_err(|_| Status::invalid_argument("subtree entry limit exceeds u16"))?,
            )),
        };
        let source = self.zakura.clone();
        let response = match proto::ShieldedProtocol::try_from(request.shielded_protocol) {
            Ok(proto::ShieldedProtocol::Sapling) => {
                source.oneshot(ReadRequest::SaplingSubtrees { start_index, limit }).await
            }
            Ok(proto::ShieldedProtocol::Orchard) => {
                source.oneshot(ReadRequest::OrchardSubtrees { start_index, limit }).await
            }
            Ok(proto::ShieldedProtocol::Ironwood) => {
                source.oneshot(ReadRequest::IronwoodSubtrees { start_index, limit }).await
            }
            Err(_) => return Err(Status::invalid_argument("invalid shielded protocol")),
        }
        .map_err(source_status)?;
        let roots = match response {
            ReadResponse::SaplingSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_bytes().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::OrchardSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            ReadResponse::IronwoodSubtrees(subtrees) => subtrees
                .into_values()
                .map(|subtree| (subtree.root.to_repr().to_vec(), subtree.end_height.0))
                .collect(),
            _ => return Err(Status::internal("unexpected subtree response")),
        };
        Ok(self.subtree_stream(roots, self.snapshot()))
    }

    pub(crate) async fn send_transaction(
        &self,
        request: proto::RawTransaction,
    ) -> Result<proto::SendResponse, Status> {
        let transaction = Transaction::zcash_deserialize(request.data.as_slice())
            .map_err(|error| Status::invalid_argument(format!("invalid transaction: {error}")))?;
        let node = self.node()?.clone();
        let runtime = tokio::runtime::Handle::current();
        // zakura's submit_transaction future is not Send
        let txid = tokio::task::spawn_blocking(move || {
            runtime
                .block_on(node.submit_transaction(transaction))
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| Status::unavailable(format!("Zakura task failed: {error}")))?
        .map_err(|error| Status::invalid_argument(format!("Zakura rejected transaction: {error}")))?
        .mined_id();
        Ok(proto::SendResponse { error_code: 0, error_message: txid.to_string() })
    }

    pub(crate) async fn mempool_stream(&self) -> Result<RpcStream<proto::RawTransaction>, Status> {
        let node = self.node()?.clone();
        let mut tip_changes = node.subscribe_chain_tip();
        let _ = tip_changes.last_tip_change();
        let mut transactions = Self::mempool_transactions(node.clone()).await?;
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut seen = HashSet::new();
            loop {
                for transaction in transactions.drain(..) {
                    if !seen.insert(transaction.id()) {
                        continue;
                    }
                    let transaction = match transaction.transaction().zcash_serialize_to_vec() {
                        Ok(data) => proto::RawTransaction { data, height: 0 },
                        Err(error) => {
                            let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    tokio::select! {
                        biased;
                        change = tip_changes.wait_for_tip_change() => {
                            if let Err(error) = change {
                                let _ = sender.send(Err(Status::unavailable(format!(
                                    "Zakura chain-tip listener failed: {error}"
                                )))).await;
                            }
                            return;
                        }
                        result = sender.send(Ok(transaction)) => {
                            if result.is_err() {
                                return;
                            }
                        }
                    }
                }

                transactions = tokio::select! {
                    biased;
                    change = tip_changes.wait_for_tip_change() => {
                        if let Err(error) = change {
                            let _ = sender.send(Err(Status::unavailable(format!(
                                "Zakura chain-tip listener failed: {error}"
                            )))).await;
                        }
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        match Self::mempool_transactions(node.clone()).await {
                            Ok(transactions) => transactions,
                            Err(error) => {
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        }
                    }
                };
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    pub(crate) async fn mempool_tx(
        &self,
        request: proto::GetMempoolTxRequest,
    ) -> Result<RpcStream<proto::CompactTx>, Status> {
        PoolSelection::from_mempool_request(&request.pool_types)?;
        let node = self.node()?.clone();
        let transactions = Self::mempool_transactions(node).await?;
        let compact = tokio::task::spawn_blocking(move || mempool_compact(transactions, &request))
            .await
            .map_err(|error| Status::internal(format!("mempool conversion failed: {error}")))??;
        Ok(Box::pin(tokio_stream::iter(compact.into_iter().map(Ok))))
    }

    async fn mempool_transactions(node: NodeClient) -> Result<Vec<transaction::UnminedTx>, Status> {
        let runtime = tokio::runtime::Handle::current();
        // Zakura's mempool service future is not Send.
        tokio::task::spawn_blocking(move || {
            runtime.block_on(node.mempool_transactions()).map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| Status::unavailable(format!("Zakura task failed: {error}")))?
        .map_err(|error| Status::unavailable(format!("Zakura mempool query failed: {error}")))
    }

    pub(crate) async fn taddress_transactions(
        &self,
        request: proto::TransparentAddressBlockFilter,
    ) -> Result<RpcStream<proto::RawTransaction>, Status> {
        let address = self.parse_address(&request.address)?;
        let range =
            request.range.ok_or_else(|| Status::invalid_argument("block range is required"))?;
        let tip = self.zakura_tip().await?;
        let endpoint = |endpoint: Option<proto::BlockId>| -> Result<Option<u32>, Status> {
            endpoint
                .map(|block| {
                    u32::try_from(block.height)
                        .map(|height| height.min(tip))
                        .map_err(|_| Status::invalid_argument("block height exceeds u32"))
                })
                .transpose()
        };
        let mut start = endpoint(range.start)?.unwrap_or(0);
        let mut end = endpoint(range.end)?.unwrap_or(tip);
        if end == 0 {
            end = tip;
        }
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::TransactionIdsByAddresses {
                addresses: HashSet::from([address]),
                height_range: block::Height(start)..=block::Height(end),
            })
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressesTransactionIds(transactions) = response else {
            return Err(Status::internal("unexpected address transaction response"));
        };

        let service = self.clone();
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            for hash in transactions.into_values() {
                match service.mined_transaction(hash).await {
                    Ok(Some(transaction)) => {
                        if sender.send(Ok(transaction)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = sender
                            .send(Err(Status::unavailable(
                                "Zakura address index referenced a missing transaction",
                            )))
                            .await;
                        return;
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    pub(crate) async fn taddress_balance(
        &self,
        addresses: Vec<String>,
    ) -> Result<proto::Balance, Status> {
        self.taddress_balance_parsed(self.parse_addresses(addresses)?).await
    }

    pub(crate) async fn taddress_balance_parsed(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<proto::Balance, Status> {
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::AddressBalance(addresses))
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressBalance { balance, .. } = response else {
            return Err(Status::internal("unexpected address balance response"));
        };
        Ok(proto::Balance {
            value_zat: i64::try_from(u64::from(balance))
                .map_err(|_| Status::out_of_range("transparent balance exceeds i64"))?,
        })
    }

    pub(crate) async fn address_utxos(
        &self,
        request: proto::GetAddressUtxosArg,
    ) -> Result<Vec<proto::GetAddressUtxosReply>, Status> {
        if request.addresses.len() > MAX_UTXO_ADDRESSES {
            return Err(Status::invalid_argument(format!(
                "too many addresses: {} exceeds {MAX_UTXO_ADDRESSES}",
                request.addresses.len()
            )));
        }
        let addresses = self.parse_addresses(request.addresses)?;
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::UtxosByAddresses(addresses))
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressUtxos(utxos) = response else {
            return Err(Status::internal("unexpected address UTXO response"));
        };
        let limit = usize::try_from(request.max_entries).unwrap_or(usize::MAX);
        utxos
            .utxos()
            .filter(|(_, _, location, _)| u64::from(location.height().0) >= request.start_height)
            .take(if limit == 0 { usize::MAX } else { limit })
            .map(|(address, txid, location, output)| {
                Ok(proto::GetAddressUtxosReply {
                    address: address.to_string(),
                    txid: txid.0.to_vec(),
                    index: i32::try_from(location.output_index().index()).map_err(|_| {
                        Status::out_of_range("transparent output index exceeds i32")
                    })?,
                    script: output.lock_script.as_raw_bytes().to_vec(),
                    value_zat: i64::try_from(u64::from(output.value)).map_err(|_| {
                        Status::out_of_range("transparent output value exceeds i64")
                    })?,
                    height: u64::from(location.height().0),
                })
            })
            .collect()
    }

    fn parse_addresses(
        &self,
        addresses: impl IntoIterator<Item = String>,
    ) -> Result<HashSet<transparent::Address>, Status> {
        addresses.into_iter().map(|address| self.parse_address(&address)).collect()
    }

    pub(crate) fn parse_address(&self, address: &str) -> Result<transparent::Address, Status> {
        let address: transparent::Address = address
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid address: {error}")))?;
        Ok(address)
    }

    async fn zakura_tip(&self) -> Result<u32, Status> {
        let response =
            self.zakura.clone().oneshot(ReadRequest::Tip).await.map_err(source_status)?;
        match response {
            ReadResponse::Tip(Some((height, _))) => Ok(height.0),
            ReadResponse::Tip(None) => Err(Status::unavailable("Zakura chain is empty")),
            _ => Err(Status::internal("unexpected tip response")),
        }
    }

    fn node(&self) -> Result<&NodeClient, Status> {
        self.node
            .as_ref()
            .ok_or_else(|| Status::unavailable("embedded Zakura node handle is unavailable"))
    }
}

fn mempool_compact(
    transactions: Vec<transaction::UnminedTx>,
    request: &proto::GetMempoolTxRequest,
) -> Result<Vec<proto::CompactTx>, Status> {
    let pools = PoolSelection::from_mempool_request(&request.pool_types)?;
    let excluded = excluded_txids(
        transactions.iter().map(|tx| tx.id().mined_id().0),
        &request.exclude_txid_suffixes,
    );
    let transactions = transactions
        .into_iter()
        .filter(|tx| !excluded.contains(&tx.id().mined_id().0))
        .map(|transaction| {
            transaction
                .transaction()
                .zcash_serialize_to_vec()
                .map(|bytes| (bytes, transaction.id().mined_id()))
                .map_err(|error| Status::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    compact_mempool_txs(&transactions, pools, &[])
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
    use zakura_chain::{block, parameters::Network, transaction};
    use zakura_state::Config;
    use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamer;

    use zakura_test::vectors::{BLOCK_MAINNET_949496_BYTES, BLOCK_TESTNET_1842421_BYTES};
    use ztreamer_indexer::{
        codec::CompactBlockRecord,
        head::HeadError,
        ingest::OrderedBuilder,
        parser::{ParsedCompactBlock, RawIndexBlock},
        pipeline::PipelineConfig,
    };

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
    fn subtree_stream_resolves_volatile_completion_blocks() {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let index =
                Arc::new(Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
            let state = index_through(&index, 1_005);
            let (_state_service, read_service, _tip, _change) =
                zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                    .await
                    .expect("ephemeral state initializes");
            let service = CompactService::new(index, state, "main", read_service);
            service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();

            let mut stream = service.subtree_stream(vec![(vec![7; 32], 1_007)], service.snapshot());
            let root = stream.next().await.unwrap().unwrap();

            assert_eq!(root.root_hash, vec![7; 32]);
            assert_eq!(root.completing_block_hash, hash(1_007));
            assert_eq!(root.completing_block_height, 1_007);
        });
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

    #[test]
    fn mempool_requests_select_their_pools_and_exclusions() {
        let sapling = shielded_mempool_transaction(&BLOCK_MAINNET_949496_BYTES, |transaction| {
            transaction.sapling_outputs().next().is_some()
        });
        let orchard = shielded_mempool_transaction(&BLOCK_TESTNET_1842421_BYTES, |transaction| {
            transaction.orchard_actions().next().is_some()
        });
        let sapling_txid = sapling.id().mined_id().0.to_vec();
        let orchard_txid = orchard.id().mined_id().0.to_vec();
        let mempool = vec![sapling, orchard];
        let txids = |compact: Vec<proto::CompactTx>| {
            compact.into_iter().map(|transaction| transaction.txid).collect::<Vec<_>>()
        };

        let compact =
            mempool_compact(mempool.clone(), &proto::GetMempoolTxRequest::default()).unwrap();
        assert_eq!(txids(compact.clone()), [sapling_txid.clone(), orchard_txid.clone()]);
        assert!(!compact[0].outputs.is_empty());
        assert!(!compact[1].actions.is_empty());

        let orchard_only = proto::GetMempoolTxRequest {
            pool_types: vec![proto::PoolType::Orchard as i32],
            ..Default::default()
        };
        assert_eq!(
            txids(mempool_compact(mempool.clone(), &orchard_only).unwrap()),
            vec![orchard_txid.clone()]
        );

        let excluded = proto::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![orchard_txid[30..].to_vec()],
            ..Default::default()
        };
        assert_eq!(txids(mempool_compact(mempool.clone(), &excluded).unwrap()), [sapling_txid]);

        let transparent = proto::GetMempoolTxRequest {
            pool_types: vec![proto::PoolType::Transparent as i32],
            ..Default::default()
        };
        let compact = mempool_compact(mempool, &transparent).unwrap();
        assert!(!compact.is_empty());
        assert!(compact.iter().all(|tx| tx.spends.is_empty()
            && tx.outputs.is_empty()
            && tx.actions.is_empty()
            && tx.ironwood_actions.is_empty()));
    }

    fn shielded_mempool_transaction(
        block: &[u8],
        shielded: impl Fn(&transaction::Transaction) -> bool,
    ) -> transaction::UnminedTx {
        block::Block::zcash_deserialize(block)
            .unwrap()
            .transactions
            .iter()
            .find(|candidate| shielded(candidate))
            .cloned()
            .expect("the block vector holds a shielded transaction")
            .into()
    }

    fn range(start: u32, end: u32) -> proto::BlockRange {
        proto::BlockRange {
            start: Some(proto::BlockId { height: u64::from(start), hash: Vec::new() }),
            end: Some(proto::BlockId { height: u64::from(end), hash: Vec::new() }),
            pool_types: Vec::new(),
        }
    }

    fn parsed(height: u32) -> ParsedCompactBlock {
        ParsedCompactBlock {
            height,
            hash: hash(height),
            previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
            time: height,
            transactions: Vec::new(),
            sapling_additions: 0,
            orchard_additions: 0,
            ironwood_additions: 0,
        }
    }

    fn raw_block(height: u32) -> RawIndexBlock {
        let mut bytes = vec![0; 140];
        bytes[4..36].copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or([0; 32]));
        bytes[100..104].copy_from_slice(&height.to_le_bytes());
        bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        RawIndexBlock {
            height: block::Height(height),
            hash: block::Hash(hash(height)),
            bytes,
            txids: vec![transaction::Hash(hash(height))],
        }
    }

    fn record(height: u32) -> CompactBlockRecord {
        CompactBlockRecord {
            height,
            hash: hash(height),
            previous_hash: hash(height - 1),
            time: height,
            transactions: Vec::new(),
            end_tree_sizes: ztreamer_indexer::codec::TreeSizes::default(),
        }
    }

    fn index_through(index: &Index, tip: u32) -> IndexState {
        let mut builder = OrderedBuilder::new(IndexState::default(), 1024 * 1024).unwrap();
        for height in 0..=tip {
            builder.push(parsed(height)).unwrap();
        }
        let mut state = IndexState::default();
        while let Some(batch) = builder.build_batch(Some(tip), Some(tip), 1024 * 1024).unwrap() {
            state = index.write(batch).unwrap();
        }
        state
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
