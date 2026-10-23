//! A synthetic indexed [`CompactService`](crate::CompactService) and a local gRPC server, shared
//! by tests, benchmarks and examples.
//!
//! Compiled for this crate's own tests and for targets that enable the `test-utils` feature.

use crate::CompactService;
use prost::Message as _;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use zakura_chain::{block, parameters::Network};
use zakura_state::Config;
use ztreamer_indexer::index::Index;
use ztreamer_protocol::proto::{self, compact_tx_streamer_client::CompactTxStreamerClient};
use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamerServer;

pub use ztreamer_indexer::test_utils as fixtures;

/// A gRPC client for the CompactTxStreamer service.
pub type Client = CompactTxStreamerClient<Channel>;
/// The error type returned by [`consume`].
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A [`CompactService`] over a temporary index with 2,200 durable blocks and 10 volatile ones.
#[derive(Debug)]
pub struct Fixture {
    pub service: CompactService,
    // Keep the index directory alive until the service has stopped.
    _dir: tempfile::TempDir,
}

impl Fixture {
    /// Builds the index and service.
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(
            Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap(),
        );
        let records = fixtures::mixed_records(2_210);
        let state = fixtures::populate(&index, &records[..2_200], Some(1_999));
        let (_state, read, _tip, _change) =
            zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                .await
                .unwrap();
        let service = CompactService::new(index, state, "main", read);
        service.publish_head(state, records[2_200..].to_vec()).unwrap();
        Self { service, _dir: dir }
    }
}

/// A [`Fixture`] served over gRPC on a local TCP port.
#[derive(Debug)]
pub struct LocalServer {
    pub endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    _fixture: Fixture,
}

impl LocalServer {
    /// Builds a [`Fixture`] and serves it on `127.0.0.1` at a free port.
    pub async fn start() -> Self {
        let fixture = Fixture::new().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = oneshot::channel();
        let service = fixture.service.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(CompactTxStreamerServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
        });
        Self { endpoint, shutdown, task, _fixture: fixture }
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
