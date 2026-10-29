use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use tokio::sync::Notify;
use zakura_chain::subtree::NoteCommitmentSubtreeIndex;
use ztreamer_chain::{
    codec::{RangeDecoder, encode_range},
    test_utils::raw_block,
    types::RawIndexBlock,
};
use ztreamer_indexer::{
    index::range_meta::{Attested, VerifyState},
    test_utils::{range_meta_row, sealed_range_blob},
};
use ztreamer_snapshot::{
    manifest_entry::{MAX_RANGES, ManifestEntry},
    pool::{Pool, PoolRoots, SubtreeNode},
};

use super::*;
use crate::{
    attest::NodeError,
    fetch::{HttpSource, PEER_RANGE_STREAMS, PeerSource},
    range::{Check, Reject},
    test_utils::{FileServer, PeerServer, SEED, Snapshot, SourceIndex},
};

/// Ranges of empty blocks: the epochs end at 9,999, 19,999 and 24,999.
const RANGES: u32 = 25;

/// The `tree_roots` call at the first height waits until the call at the second height
/// answers. With no second height, it waits forever.
type HeldTreeRoots = (u32, Option<u32>);

/// The answers in the `attested` records that the filler wrote in the source index.
struct FakeAttestor {
    answers: BTreeMap<u32, Attested>,
    held_roots: Option<HeldTreeRoots>,
    release_signal: Notify,
    /// The heights of the answered `tree_roots` calls, in order.
    answered_heights: Mutex<Vec<u32>>,
}

impl FakeAttestor {
    fn answer(&self, height: u32) -> Result<&Attested, NodeError> {
        self.answers.get(&height).ok_or(NodeError::Height { expected: height, actual: 0 })
    }
}

#[async_trait]
impl Attestor for FakeAttestor {
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
        Ok(self.answer(height)?.end_hash)
    }

    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
        if let Some((held_height, release_height)) = self.held_roots {
            if height == held_height {
                self.release_signal.notified().await;
            }
            if Some(height) == release_height {
                self.release_signal.notify_one();
            }
        }
        self.answered_heights.lock().unwrap().push(height);
        Ok(self.answer(height)?.roots)
    }

    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        _count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError> {
        Err(NodeError::MissingSubtree { pool, index: start })
    }
}

/// The node blocks of the source index.
struct FakeSampler;

#[async_trait]
impl Sampler for FakeSampler {
    async fn full_block(&self, height: u32) -> Result<RawIndexBlock, NodeError> {
        Ok(raw_block(height))
    }
}

/// A source index, a signed snapshot of it over HTTP, and an empty target index.
struct Import {
    source: SourceIndex,
    snapshot: Snapshot,
    server: FileServer,
    target: Arc<Index>,
    _dir: TempDir,
}

impl Import {
    async fn new(ranges: u32) -> Self {
        let source = SourceIndex::new(ranges).await;
        let snapshot = Snapshot::export(&source);
        let server = FileServer::start(snapshot.dir.path()).await;
        let dir = tempfile::tempdir().unwrap();
        let target = Arc::new(SourceIndex::open(&dir));
        Self { source, snapshot, server, target, _dir: dir }
    }

    /// The blob at `start` with a wrong tree size at its record 500.
    fn blob_with_wrong_tree_size(&self, start: u32) -> Vec<u8> {
        let blob = sealed_range_blob(&self.source.index, start);
        let decoder = RangeDecoder::new(&blob).unwrap();
        let mut records: Vec<_> =
            (0..RANGE_SIZE as usize).map(|at| decoder.record(at).unwrap()).collect();
        records[500].end_tree_sizes.sapling = 1;
        encode_range(&records).unwrap()
    }

    /// Changes one byte of the blob file of `entry`, and keeps the manifest.
    fn flip_byte_in_blob_file(&self, entry: &ManifestEntry) {
        let path = self.snapshot.range_path(entry);
        let mut blob = std::fs::read(&path).unwrap();
        blob[100] ^= 1;
        std::fs::write(path, blob).unwrap();
    }

    fn fake_attestor(&self, held_roots: Option<HeldTreeRoots>) -> Arc<FakeAttestor> {
        let answers = self.snapshot.manifest.entries().iter().map(|entry| {
            let meta = range_meta_row(&self.source.index, entry.start_height);
            (entry.end_height, meta.attested.unwrap())
        });
        let (release_signal, answered_heights) = (Notify::new(), Mutex::default());
        Arc::new(FakeAttestor {
            answers: answers.collect(),
            held_roots,
            release_signal,
            answered_heights,
        })
    }

    fn importer(&self, attestor: Arc<FakeAttestor>, network: Network) -> Importer {
        let key = SigningKey::from_bytes(&SEED).verifying_key();
        let source = HttpSource::new(self.server.base.clone(), key).unwrap();
        self.importer_with_source(Box::new(source), attestor, network)
    }

    fn importer_with_source(
        &self,
        source: Box<dyn RangeSource>,
        attestor: Arc<FakeAttestor>,
        network: Network,
    ) -> Importer {
        let node = TrustedNode {
            attestor,
            sampler: Arc::new(FakeSampler),
            id: NodeId { url: "http://node.test".to_owned(), version: "test".to_owned() },
        };
        let config = ImportConfig {
            network,
            verify_sample: SamplePlan { per_range: 2, seed: 7 },
            import_workers: NonZeroUsize::new(4).unwrap(),
        };
        Importer::new(source, node, Arc::clone(&self.target), config)
    }

    async fn run(&self) -> Result<ImportReport, ImportError> {
        let (_stop, shutdown) = watch::channel(false);
        self.importer(self.fake_attestor(None), Network::Mainnet).run(shutdown).await
    }

    /// The blob requests that the snapshot server got.
    fn blob_requests(&self) -> usize {
        let requests = self.server.requests();
        requests.iter().filter(|(path, _)| path.starts_with("ranges/")).count()
    }

    /// Checks that the target holds the blobs of the source, byte for byte, and is verified.
    fn assert_target_has_source_blobs(&self) {
        let sealed = self.snapshot.manifest.sealed_through();
        for start in (0..=sealed / RANGE_SIZE).map(|range| range * RANGE_SIZE) {
            let blob = self.target.latest().unwrap().sealed_range(start).unwrap();
            assert_eq!(blob, self.source.index.latest().unwrap().sealed_range(start).unwrap());
        }
        let state = self.target.state().unwrap();
        assert_eq!(
            (state.sealed_through(), state.verified_through()),
            (Some(sealed), Some(sealed))
        );
    }
}

#[tokio::test]
async fn an_import_installs_the_same_ranges_and_verifies_them() {
    let import = Import::new(RANGES).await;

    let report = import.run().await.unwrap();

    let sealed = import.snapshot.manifest.sealed_through();
    let expected = ImportReport {
        epochs: 3,
        ranges: 25,
        sealed_through: Some(sealed),
        verified_through: Some(sealed),
        stopped: false,
    };
    assert_eq!(report, expected);
    import.assert_target_has_source_blobs();
    let meta = range_meta_row(&import.target, 12_000);
    assert_eq!(meta.state, VerifyState::Verified);
    assert_eq!(meta.entry, import.snapshot.manifest.entries()[12]);
    assert_eq!(meta.verified_by.unwrap().url, "http://node.test");
    assert_eq!(meta.sampled_heights.len(), 2);
    assert!(meta.sampled_heights.iter().all(|height| (12_000..13_000).contains(height)));
}

/// The manifest closes its last epoch at 24,999, but the epoch rule does not: that epoch has
/// 5 ranges. Each row keeps the entry of the source row, with the flag of the rule.
#[tokio::test]
async fn imported_rows_keep_the_epoch_end_of_the_rule() {
    let import = Import::new(RANGES).await;

    import.run().await.unwrap();

    assert!(import.snapshot.manifest.entries()[24].epoch_end);
    for start in (0..RANGES).map(|range| range * RANGE_SIZE) {
        let imported = range_meta_row(&import.target, start).entry;
        let source = range_meta_row(&import.source.index, start).entry;
        assert_eq!(imported, source, "range {start}");
    }
    assert!(range_meta_row(&import.target, 19_000).entry.epoch_end);
    assert!(!range_meta_row(&import.target, 24_000).entry.epoch_end);
}

#[tokio::test]
async fn four_workers_install_twelve_epochs_byte_for_byte() {
    let import = Import::new(120).await;

    let report = import.run().await.unwrap();

    assert_eq!((report.epochs, report.ranges), (12, 120));
    import.assert_target_has_source_blobs();
    assert_eq!(import.blob_requests(), 120);
}

/// Four workers fetch up to eight epochs at a time, and the peer sends one stream at a time.
#[tokio::test]
async fn an_import_from_a_peer_with_one_stream_waits_for_the_peer_and_completes() {
    let import = Import::new(RANGES).await;
    let server = PeerServer::start(&import.source, 1).await;
    let peer = PeerSource::connect(server.endpoint.clone()).await.unwrap();
    let (_stop, shutdown) = watch::channel(false);
    let importer =
        import.importer_with_source(Box::new(peer), import.fake_attestor(None), Network::Mainnet);

    let report = importer.run(shutdown).await.unwrap();

    // The manifest of the peer closes the short last epoch at range 24, as an export does.
    assert_eq!((report.epochs, report.ranges), (3, 25));
    import.assert_target_has_source_blobs();
    assert!(server.refusals() > 0);
    assert_eq!(server.requests().len(), 25);
}

/// One epoch of ten ranges: only its own blob fetches can overlap.
#[tokio::test]
async fn an_http_import_fetches_the_blobs_of_an_epoch_at_the_same_time() {
    let mut import = Import::new(10).await;
    import.server = FileServer::start_with_answer_wait(import.snapshot.dir.path()).await;

    let report = import.run().await.unwrap();

    assert_eq!((report.epochs, report.ranges), (1, 10));
    import.assert_target_has_source_blobs();
    let most = import.server.most_open_requests();
    assert!((2..=MAX_RANGES).contains(&most), "{most} requests at one time");
}

/// One epoch of ten ranges, from a peer with more streams than the limit of the source.
#[tokio::test]
async fn a_peer_import_fetches_the_blobs_of_an_epoch_up_to_the_stream_limit() {
    let import = Import::new(10).await;
    let server = PeerServer::start(&import.source, 64).await;
    let peer = PeerSource::connect(server.endpoint.clone()).await.unwrap();
    let (_stop, shutdown) = watch::channel(false);
    let importer =
        import.importer_with_source(Box::new(peer), import.fake_attestor(None), Network::Mainnet);

    let report = importer.run(shutdown).await.unwrap();

    assert_eq!((report.epochs, report.ranges), (1, 10));
    import.assert_target_has_source_blobs();
    let most = server.most_open_streams();
    assert!((2..=PEER_RANGE_STREAMS).contains(&most), "{most} streams at one time");
    assert_eq!(server.refusals(), 0);
}

#[tokio::test]
async fn a_slow_epoch_verifies_last_and_installs_in_order() {
    let import = Import::new(RANGES).await;
    // The first epoch gets its roots only after the last epoch.
    let attestor = import.fake_attestor(Some((9_999, Some(24_999))));
    let (_stop, shutdown) = watch::channel(false);

    let importer = import.importer(Arc::clone(&attestor), Network::Mainnet);
    let report = importer.run(shutdown).await.unwrap();

    assert_eq!(report.ranges, 25);
    import.assert_target_has_source_blobs();
    let answered_heights = attestor.answered_heights.lock().unwrap().clone();
    let answer_order =
        |height| answered_heights.iter().position(|&answered| answered == height).unwrap();
    assert!(answer_order(24_999) < answer_order(9_999), "{answered_heights:?}");
}

#[tokio::test]
async fn a_stopped_import_keeps_a_prefix_and_a_restart_fetches_only_the_rest() {
    let import = Import::new(RANGES).await;
    // The second epoch never gets its roots.
    let attestor = import.fake_attestor(Some((19_999, None)));
    let (stop, shutdown) = watch::channel(false);
    let importer = import.importer(attestor, Network::Mainnet);
    let task = tokio::spawn(importer.run(shutdown));
    while import.target.state().unwrap().sealed_through() != Some(9_999) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    stop.send(true).unwrap();

    let report = task.await.unwrap().unwrap();
    assert_eq!((report.epochs, report.ranges, report.stopped), (1, 10, true));
    assert_eq!(report.verified_through, Some(9_999));
    assert_eq!(import.target.latest().unwrap().range_meta(10_000).unwrap(), None);
    let requests_before_restart = import.blob_requests();
    assert!(requests_before_restart >= 10);
    let report = import.run().await.unwrap();
    assert_eq!((report.epochs, report.ranges), (2, 15));
    assert_eq!(import.blob_requests() - requests_before_restart, 15);
    import.assert_target_has_source_blobs();
}

#[tokio::test]
async fn a_mutated_blob_stops_the_import_at_its_epoch() {
    let mut import = Import::new(RANGES).await;
    let blob = import.blob_with_wrong_tree_size(15_000);
    import.snapshot.replace_blob(15_000, &blob);

    let error = import.run().await.unwrap_err();

    let expected = Reject::Rejected { check: Check::TreeSizes, height: 15_500 };
    assert!(
        matches!(error, ImportError::Epoch { epoch: 19_999, source: EpochError::Reject(reject) } if reject == expected),
        "{error:?}"
    );
    let state = import.target.state().unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(9_999), Some(9_999)));
    assert_eq!(import.target.latest().unwrap().range_meta(10_000).unwrap(), None);
}

/// A blob with a wrong digest stops the import. The index keeps the verified prefix before
/// the bad blob.
#[tokio::test]
async fn a_blob_with_a_wrong_digest_stops_the_import() {
    let import = Import::new(RANGES).await;
    import.flip_byte_in_blob_file(&import.snapshot.manifest.entries()[15]);

    let error = import.run().await.unwrap_err();

    assert!(
        matches!(error, ImportError::Fetch(FetchError::Digest { start_height: 15_000, .. })),
        "{error:?}"
    );
    let state = import.target.state().unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(9_999), Some(9_999)));
}

#[tokio::test]
async fn an_index_at_the_manifest_tip_or_on_another_network_is_refused() {
    let import = Import::new(RANGES).await;
    let (stop, shutdown) = watch::channel(true);
    let importer = import.importer(import.fake_attestor(None), Network::Mainnet);
    let report = importer.run(shutdown).await.unwrap();
    assert_eq!((report.epochs, report.stopped), (0, true));
    drop(stop);
    import.run().await.unwrap();

    let error = import.run().await.unwrap_err();

    assert!(matches!(error, ImportError::Ahead { index: 24_999, manifest: 24_999 }), "{error:?}");
    let (_stop, shutdown) = watch::channel(false);
    let testnet = Network::new_default_testnet();
    let error =
        import.importer(import.fake_attestor(None), testnet).run(shutdown).await.unwrap_err();
    assert!(matches!(error, ImportError::Network { .. }), "{error:?}");
}

/// A cancel in the node work of an epoch is a stop, the same as a cancel in an index read.
#[test]
fn a_cancel_in_an_epoch_is_a_cancel_of_the_import() {
    let cancelled =
        EpochError::Node { height: 2_999, source: NodeError::Cancelled(BlockingTaskCancelled) };
    let error = ImportError::from_epoch_error(2_999, cancelled);
    assert!(matches!(error, ImportError::Cancelled(BlockingTaskCancelled)), "{error:?}");
    let reject = Reject::BlobCount { entries: 1, blobs: 0 };
    let error = ImportError::from_epoch_error(2_999, reject.into());
    assert!(matches!(error, ImportError::Epoch { epoch: 2_999, .. }), "{error:?}");
}
