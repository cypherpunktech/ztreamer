use super::*;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Server},
};
use ztreamer_indexer::{
    index::{RANGE_SIZE, range_meta::RangeMeta},
    manifest_entry::{Frontiers, ManifestEntry as Entry},
    test_utils::{range_meta_row, sealed_range_blob},
};
use ztreamer_protocol::{
    proto::{BlockId, compact_tx_streamer_client::CompactTxStreamerClient},
    replicate::{replicator_client::ReplicatorClient, replicator_server::ReplicatorServer},
    wire::compact_tx_streamer_server::CompactTxStreamerServer,
};
use ztreamer_service::{
    CompactService,
    test_utils::{EmptyChain, fixtures},
};

/// Five sealed ranges. Ranges 0, 1,000 and 2,000 have rows, range 3,000 has none, and the
/// row of range 4,000 is rejected.
struct Fixture {
    index: Arc<Index>,
    service: ReplicatorService,
    snapshots: watch::Sender<ServingSnapshot>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let network = Network::Mainnet;
        let index = Arc::new(
            Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", network.genesis_hash()).unwrap(),
        );
        let records = fixtures::mixed_records(5_010);
        let state = fixtures::populate(&index, &records, Some(4_999));
        for (start, state) in [
            (0, VerifyState::Verified),
            (1_000, VerifyState::Verified),
            (2_000, VerifyState::Unverified),
            (4_000, VerifyState::Rejected),
        ] {
            let blob = sealed_range_blob(&index, start);
            let range = &records[start as usize..][..RANGE_SIZE as usize];
            let entry = Entry {
                frontiers: Frontiers { sapling: vec![0xab; 40], ..Frontiers::default() },
                epoch_end: start == 0,
                ..Entry::from_records_and_blob(range, &blob)
            };
            let meta = RangeMeta {
                entry,
                state,
                verified_by: None,
                verified_at: None,
                attested: None,
                sampled_heights: Vec::new(),
            };
            index.put_range_meta(start, &meta).unwrap();
        }
        let (snapshots, receiver) = watch::channel(ServingSnapshot::from(state));
        let service = ReplicatorService::new(Arc::clone(&index), receiver, &network);
        Self { index, service, snapshots, _dir: dir }
    }

    /// Serves CompactTxStreamer and the fixture's Replicator on one free local port, as
    /// `ztreamerd` does, and returns the endpoint.
    async fn serve(&self) -> String {
        let compact = CompactService::with_backend(
            Arc::clone(&self.index),
            self.index.state().unwrap(),
            "main",
            EmptyChain,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = Server::builder()
            .add_service(CompactTxStreamerServer::new(compact))
            .add_service(ReplicatorServer::new(self.service.clone()));
        tokio::spawn(server.serve_with_incoming(TcpListenerStream::new(listener)));
        endpoint
    }

    async fn client(&self) -> ReplicatorClient<Channel> {
        ReplicatorClient::connect(self.serve().await).await.unwrap()
    }
}

async fn range_bytes(client: &mut ReplicatorClient<Channel>, start: u32, offset: u64) -> Vec<u8> {
    let mut stream = client
        .get_range(GetRangeRequest { start_height: start, offset })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        assert!(chunk.data.len() <= CHUNK_BYTES);
        bytes.extend_from_slice(&chunk.data);
    }
    bytes
}

#[tokio::test]
async fn export_info_describes_the_index() {
    let fixture = Fixture::new();
    let info = fixture.client().await.get_export_info(Empty {}).await.unwrap().into_inner();
    assert_eq!(
        info,
        ExportInfo {
            network: "Mainnet".to_owned(),
            genesis_hash: Network::Mainnet.genesis_hash().0.to_vec(),
            manifest_version: 1,
            sealed_through: Some(4_999),
        }
    );
}

#[tokio::test]
async fn one_listener_serves_both_services() {
    let fixture = Fixture::new();
    let endpoint = fixture.serve().await;
    let mut streamer = CompactTxStreamerClient::connect(endpoint.clone()).await.unwrap();
    let block =
        streamer.get_block(BlockId { height: 4_000, hash: Vec::new() }).await.unwrap().into_inner();
    assert_eq!(block.height, 4_000);
    let mut replicator = ReplicatorClient::connect(endpoint).await.unwrap();
    let info = replicator.get_export_info(Empty {}).await.unwrap().into_inner();
    assert_eq!(info.sealed_through, Some(4_999));
}

#[tokio::test]
async fn list_ranges_starts_at_the_given_height_and_skips_unlisted_ranges() {
    let fixture = Fixture::new();
    let mut client = fixture.client().await;
    let mut stream = client
        .list_ranges(ListRangesRequest { from_start_height: 1_000 })
        .await
        .unwrap()
        .into_inner();
    let mut listed = Vec::new();
    while let Some(entry) = stream.message().await.unwrap() {
        listed.push(Entry::try_from(entry).unwrap());
    }
    let expected = [1_000, 2_000].map(|start| range_meta_row(&fixture.index, start).entry);
    assert_eq!(listed, expected);
}

#[tokio::test]
async fn get_range_sends_the_blob_and_resumes_from_an_offset() {
    let fixture = Fixture::new();
    let mut client = fixture.client().await;
    let blob = sealed_range_blob(&fixture.index, 1_000);
    assert_eq!(range_bytes(&mut client, 1_000, 0).await, blob);
    let offset = blob.len() as u64 / 3;
    assert_eq!(range_bytes(&mut client, 1_000, offset).await, blob[offset as usize..]);
    assert!(range_bytes(&mut client, 1_000, blob.len() as u64).await.is_empty());

    let refused = |start, offset| {
        let mut client = client.clone();
        async move {
            client
                .get_range(GetRangeRequest { start_height: start, offset })
                .await
                .unwrap_err()
                .code()
        }
    };
    assert_eq!(refused(1_000, blob.len() as u64 + 1).await, Code::OutOfRange);
    // No row, a rejected row, not a range start, and not sealed.
    for start in [3_000, 4_000, 1_001, 5_000] {
        assert_eq!(refused(start, 0).await, Code::NotFound);
    }
}

/// The cap counts streams until they are dropped. The responses are held here, unread, so
/// the test does not depend on how fast a transport drains a small blob.
#[tokio::test]
async fn the_fifth_concurrent_stream_is_refused() {
    let fixture = Fixture::new();
    let request = || Request::new(GetRangeRequest { start_height: 0, offset: 0 });
    let mut streams = Vec::new();
    for _ in 0..MAX_RANGE_STREAMS {
        streams.push(fixture.service.get_range(request()).await.unwrap());
    }
    let refused = fixture.service.get_range(request()).await.err().unwrap();
    assert_eq!(refused.code(), Code::ResourceExhausted);
    drop(streams.pop());
    fixture.service.get_range(request()).await.unwrap();
}

#[tokio::test]
async fn a_moved_generation_is_unavailable() {
    let fixture = Fixture::new();
    let mut client = fixture.client().await;
    fixture.snapshots.send_modify(|snapshot| snapshot.generation += 1);
    let status = client.get_export_info(Empty {}).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
    let status =
        client.get_range(GetRangeRequest { start_height: 0, offset: 0 }).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
}
