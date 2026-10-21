#![allow(dead_code)]

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
use ztreamer_service::CompactService;

#[path = "../../../../benchmarks/fixtures.rs"]
pub mod fixtures;

pub type Client = CompactTxStreamerClient<Channel>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Fixture {
    pub service: CompactService,
    // Keep the index directory alive until the service has stopped.
    _dir: tempfile::TempDir,
}

impl Fixture {
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Arc::new(Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
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

pub struct LocalServer {
    pub endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    _fixture: Fixture,
}

impl LocalServer {
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

    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task).await.unwrap().unwrap().unwrap();
    }
}

pub fn range(start: u32, end: u32) -> proto::BlockRange {
    let id = |height| Some(proto::BlockId { height: u64::from(height), hash: Vec::new() });
    proto::BlockRange { start: id(start), end: id(end), pool_types: Vec::new() }
}

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
