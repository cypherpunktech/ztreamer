//! A synthetic indexed [`CompactService`](crate::CompactService), a [`StateBackend`] over an empty
//! chain, and a local gRPC server, shared by tests, benchmarks and examples.
//!
//! Compiled for this crate's own tests and for targets that enable the `test-utils` feature.

use crate::{CompactService, NodeVersion};
use prost::Message as _;
use sapling_crypto::Node as SaplingNode;
use std::{
    collections::HashSet,
    ops::RangeInclusive,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use ztreamer_backend::{
    AddressUtxo, BackendError, MinedTransaction, StateBackend, Subtrees, TreeState,
};
use ztreamer_chain::codec::CompactBlockRecord;
use ztreamer_indexer::index::{Index, IndexState};
use ztreamer_protocol::proto::{self, compact_tx_streamer_client::CompactTxStreamerClient};
use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamerServer;
use ztreamer_sync::head::HeadFollower;

/// A gRPC client for the CompactTxStreamer service.
pub type Client = CompactTxStreamerClient<Channel>;
/// The error type returned by [`consume`].
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A [`StateBackend`] over an empty Mainnet chain with no mempool.
///
/// It gives the answers of the embedded Zakura over an empty ephemeral state, read with no node
/// handle: no tip, no blocks, no transactions, empty trees and subtrees, a zero balance, no
/// UTXOs. Mempool requests fail with [`BackendError::Unavailable`].
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyChain;

impl EmptyChain {
    /// The node version that [`EphemeralService`] reports.
    pub fn node_version() -> NodeVersion {
        NodeVersion { build: "v0.0.0-test".into(), subversion: "/ztreamer-test/".into() }
    }

    fn no_mempool<T>() -> Result<T, BackendError> {
        Err(BackendError::Unavailable("the empty chain has no mempool".into()))
    }
}

#[tonic::async_trait]
impl StateBackend for EmptyChain {
    fn network(&self) -> Network {
        Network::Mainnet
    }

    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError> {
        Ok(None)
    }

    async fn block(&self, _hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        Ok(None)
    }

    async fn transaction(
        &self,
        _hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError> {
        Ok(None)
    }

    async fn tree_state(&self, _hash: block::Hash) -> Result<TreeState, BackendError> {
        Ok(TreeState::default())
    }

    async fn sapling_subtrees(
        &self,
        _start: NoteCommitmentSubtreeIndex,
        _limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError> {
        Ok(Subtrees::new())
    }

    async fn orchard_subtrees(
        &self,
        _start: NoteCommitmentSubtreeIndex,
        _limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError> {
        Ok(Subtrees::new())
    }

    async fn ironwood_subtrees(
        &self,
        _start: NoteCommitmentSubtreeIndex,
        _limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError> {
        Ok(Subtrees::new())
    }

    async fn address_transaction_ids(
        &self,
        _address: transparent::Address,
        _heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        Ok(Vec::new())
    }

    async fn address_balance(
        &self,
        _addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        Ok(Amount::zero())
    }

    async fn address_utxos(
        &self,
        _addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        Ok(Vec::new())
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        Self::no_mempool()
    }

    async fn submit_transaction(
        &self,
        _transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        Self::no_mempool()
    }
}

/// A [`CompactService`] over a temporary index and an [`EmptyChain`], and the [`HeadFollower`]
/// that publishes its snapshots.
///
/// A test keeps the whole value, for example as `ctx`, and reads its fields. It owns the index
/// directory, so the directory stays until the test ends.
#[derive(Debug)]
pub struct EphemeralService {
    pub service: CompactService,
    pub follower: HeadFollower,
    pub index: Arc<Index>,
    /// The durable index state that the service starts from.
    pub state: IndexState,
    /// The index directory. Keep it alive until the service has stopped.
    pub dir: tempfile::TempDir,
}

impl EphemeralService {
    /// Writes `records` into a new temporary index, seals the ranges through `seal_through`, and
    /// builds the service over it. With no records, the index stays empty.
    pub async fn with_records(records: &[CompactBlockRecord], seal_through: Option<u32>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(
            Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap(),
        );
        let state = match records {
            [] => index.state().unwrap(),
            records => ztreamer_indexer::test_utils::populate(&index, records, seal_through),
        };
        let follower = HeadFollower::new(Arc::clone(&index), state);
        let service = CompactService::with_backend(
            Arc::clone(&index),
            follower.subscribe(),
            "main",
            EmptyChain,
        )
        .with_node_version(EmptyChain::node_version());
        Self { service, follower, index, state, dir }
    }

    /// A service over an empty index.
    pub async fn empty() -> Self {
        Self::with_records(&[], None).await
    }

    /// A service over empty blocks from 0 to `tip`, sealed through `tip`.
    pub async fn sealed_through(tip: u32) -> Self {
        let records: Vec<_> = (0..=tip)
            .map(|height| ztreamer_chain::test_utils::record(height, Vec::new()))
            .collect();
        Self::with_records(&records, Some(tip)).await
    }
}

/// A [`CompactService`] over a temporary index with 2,200 durable blocks and 10 volatile ones.
#[derive(Debug)]
pub struct Fixture {
    pub service: CompactService,
    // Keep the snapshot channel open and the index directory alive until the service has
    // stopped.
    _follower: HeadFollower,
    _dir: tempfile::TempDir,
}

impl Fixture {
    /// Builds the index and service.
    pub async fn new() -> Self {
        let records = ztreamer_chain::test_utils::mixed_records(2_210);
        let EphemeralService { service, follower, state, dir, .. } =
            EphemeralService::with_records(&records[..2_200], Some(1_999)).await;
        follower.publish_head(state, records[2_200..].to_vec()).unwrap();
        Self { service, _follower: follower, _dir: dir }
    }
}

/// A [`CompactService`] served over gRPC on a local TCP port.
#[derive(Debug)]
pub struct LocalServer {
    pub endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    _fixture: Option<Fixture>,
}

impl LocalServer {
    /// Builds a [`Fixture`] and serves it on `127.0.0.1` at a free port.
    pub async fn start() -> Self {
        let fixture = Fixture::new().await;
        let server = Self::serve(fixture.service.clone()).await;
        Self { _fixture: Some(fixture), ..server }
    }

    /// Serves `service` on `127.0.0.1` at a free port. The caller keeps the index and the state of
    /// `service` alive until [`stop`](Self::stop).
    pub async fn serve(service: CompactService) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(CompactTxStreamerServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
        });
        Self { endpoint, shutdown, task, _fixture: None }
    }

    /// Shuts the server down and waits for it to stop.
    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task).await.unwrap().unwrap().unwrap();
    }
}

/// A block range from `start` to `end`, by height, with the default pools.
pub fn range(start: u32, end: u32) -> proto::BlockRange {
    let id = |height| Some(proto::BlockId { height: u64::from(height), hash: Vec::new() });
    proto::BlockRange { start: id(start), end: id(end), pool_types: Vec::new() }
}

/// Timings and sizes from one [`consume`] call.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    pub first: Duration,
    pub complete: Duration,
    pub bytes: u64,
    pub blocks: u64,
}

/// Measures request-to-first-message and request-to-EOF, including decoding and
/// consumer delay. Validates complete, ordered delivery for synthetic or live data.
pub async fn consume(
    client: &mut Client,
    range: proto::BlockRange,
    delay: Duration,
) -> Result<Observation, Error> {
    let start = range.start.as_ref().unwrap().height;
    let end = range.end.as_ref().unwrap().height;
    let expected = start.abs_diff(end) + 1;
    let started = Instant::now();
    let mut stream = client.get_block_range(range).await?.into_inner();
    let mut first = None;
    let mut blocks = 0;
    let mut bytes = 0;
    while let Some(block) = stream.message().await? {
        first.get_or_insert_with(|| started.elapsed());
        let height =
            if start <= end { start.checked_add(blocks) } else { start.checked_sub(blocks) };
        if blocks >= expected || Some(block.height) != height {
            return Err("range returned an unexpected height".into());
        }
        blocks += 1;
        bytes += block.encoded_len() as u64;
        std::hint::black_box(block);
        if !delay.is_zero() && blocks % 64 == 0 && blocks < expected {
            tokio::time::sleep(delay).await;
        }
    }
    if blocks != expected {
        return Err("range ended before all blocks arrived".into());
    }
    Ok(Observation { first: first.unwrap(), complete: started.elapsed(), bytes, blocks })
}

#[cfg(test)]
pub(crate) use ztreamer_chain::test_utils::hash;

/// An empty block at `height` that chains onto [`hash`] of the previous height.
#[cfg(test)]
pub(crate) fn parsed(height: u32) -> ztreamer_chain::types::ParsedCompactBlock {
    ztreamer_chain::test_utils::parsed(ztreamer_chain::test_utils::record(height, Vec::new()))
}

#[cfg(test)]
pub(crate) fn record(height: u32) -> ztreamer_chain::codec::CompactBlockRecord {
    ztreamer_chain::codec::CompactBlockRecord {
        height,
        hash: hash(height),
        previous_hash: hash(height - 1),
        time: height,
        transactions: Vec::new(),
        end_tree_sizes: ztreamer_chain::codec::TreeSizes::default(),
    }
}
