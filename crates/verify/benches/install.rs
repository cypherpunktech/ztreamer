//! The work of the importer on one range after its fetch: the BLAKE3 digest of the blob, the
//! check of [`RangeBlob::check`], and the install into a fresh index. The importer does the
//! install on one writer thread, in height order. Thus the install time of all ranges adds up.
//!
//! The importer installs with [`Index::install_checked_range`]. The bench also measures
//! [`Index::install_sealed_range`], which checks the blob again, for comparison.
//!
//! By default, the bench uses a synthetic peak range at genesis: 1,060,000 Sapling outputs in
//! 1,000 blocks. To use real ranges, set `ZTREAMER_BENCH_INDEX` to an index with sealed ranges
//! and `range_meta` rows. `ZTREAMER_BENCH_RANGES` gives the start heights, separated by commas.
//! The default is the heavy Sandblast ranges `1735000,1785000,1788000`.
//!
//! The fresh index is in the temporary directory. Set `TMPDIR` to a disk, not to a tmpfs, to
//! include the sync of the LMDB commit.

use std::{env, hint::black_box, path::PathBuf};

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use zakura_chain::block;
use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes, encode_range},
    index::{
        BlockId, CheckedRange, Index, RANGE_SIZE,
        range_meta::{ManifestEntry, RangeMeta, VerifyState},
    },
    test_utils::{hash, record},
    types::{CompactSaplingOutput, CompactTransaction},
};
use ztreamer_verify::range::{RangeBlob, RangeLink};

/// Sapling outputs per block of the synthetic range: 1,060,000 in the range.
const OUTPUTS_PER_BLOCK: u32 = 1_060;

/// The map size of the fresh index. The largest Sandblast blob is far below it.
const FRESH_MAP_BYTES: usize = 4 << 30;

/// One range to install: its blob, its row, and the end of the range before it.
struct BenchRange {
    blob: Bytes,
    meta: RangeMeta,
    previous: RangeLink,
}

impl BenchRange {
    const fn start(&self) -> u32 {
        self.meta.entry.start_height
    }

    /// The synthetic range at genesis with [`OUTPUTS_PER_BLOCK`] outputs in each block.
    fn synthetic_peak() -> Self {
        let records: Vec<CompactBlockRecord> = (0..RANGE_SIZE)
            .map(|height| {
                let sapling_outputs = (0..OUTPUTS_PER_BLOCK)
                    .map(|i| {
                        let mut cmu = [0; 32];
                        cmu[..4].copy_from_slice(&height.to_le_bytes());
                        cmu[4..8].copy_from_slice(&i.to_le_bytes());
                        CompactSaplingOutput { cmu, ephemeral_key: [1; 32], ciphertext: [2; 52] }
                    })
                    .collect();
                let transaction = CompactTransaction {
                    index: 1,
                    txid: zakura_chain::transaction::Hash(hash(height).0),
                    sapling_spends: Vec::new(),
                    sapling_outputs,
                    orchard_actions: Vec::new(),
                    ironwood_actions: Vec::new(),
                };
                let mut record = record(height, vec![transaction]);
                record.end_tree_sizes =
                    TreeSizes { sapling: (height + 1) * OUTPUTS_PER_BLOCK, ..TreeSizes::default() };
                record
            })
            .collect();
        let blob = Bytes::from(encode_range(&records).unwrap());
        let meta = RangeMeta {
            entry: ManifestEntry::from_records_and_blob(&records, &blob),
            state: VerifyState::Verified,
            verified_by: None,
            verified_at: None,
            attested: None,
            sampled_heights: Vec::new(),
        };
        Self { blob, meta, previous: RangeLink::GENESIS }
    }

    /// The range at `start` of `source`, with its row and the end of the row before it.
    fn read_from(source: &Index, start: u32) -> Self {
        let blob = source.read_sealed_range_bytes(start).unwrap().expect("no range at the start");
        let blob = Bytes::from(blob);
        let meta = source.range_meta(start).unwrap().expect("the range has no row");
        let previous = start.checked_sub(RANGE_SIZE).map_or(RangeLink::GENESIS, |before| {
            RangeLink::from(&source.range_meta(before).unwrap().expect("no row before").entry)
        });
        Self { blob, meta, previous }
    }

    /// The ranges that the environment gives, or the synthetic peak range.
    fn from_env() -> Vec<Self> {
        let Some(path) = env::var_os("ZTREAMER_BENCH_INDEX").map(PathBuf::from) else {
            return vec![Self::synthetic_peak()];
        };
        let source = Index::open_existing(&path, 64 << 30).unwrap();
        env::var("ZTREAMER_BENCH_RANGES")
            .unwrap_or_else(|_| "1735000,1785000,1788000".to_owned())
            .split(',')
            .map(|start| Self::read_from(&source, start.trim().parse().unwrap()))
            .collect()
    }

    /// A fresh index whose tip is the end of the range before this range.
    fn fresh_index(&self) -> (TempDir, Index) {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), FRESH_MAP_BYTES, "Mainnet", block::Hash([1; 32])).unwrap();
        if let Some(tip_height) = self.start().checked_sub(1) {
            let tip = BlockId::new(tip_height, self.previous.end_hash);
            index.set_sealed_tip_of_empty_index(tip, self.previous.end_tree_sizes).unwrap();
        }
        (dir, index)
    }
}

fn bench_install_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_install");
    group.sample_size(10);
    for range in BenchRange::from_env() {
        let (start, entry) = (range.start(), &range.meta.entry);
        println!(
            "range {start}: {} bytes, {} Sapling and {} Orchard commitments",
            range.blob.len(),
            entry.end_tree_sizes.sapling - range.previous.end_tree_sizes.sapling,
            entry.end_tree_sizes.orchard - range.previous.end_tree_sizes.orchard,
        );
        RangeBlob::new(&range.blob, entry).check(&range.previous).unwrap();

        group.bench_function(format!("digest/{start}"), |b| {
            b.iter(|| black_box(entry).check_blob(black_box(&range.blob)).unwrap())
        });
        group.bench_function(format!("range_check/{start}"), |b| {
            b.iter(|| RangeBlob::new(black_box(&range.blob), entry).check(&range.previous).unwrap())
        });
        group.bench_function(format!("install_sealed_range/{start}"), |b| {
            b.iter_batched(
                || range.fresh_index(),
                |(dir, index)| {
                    index.install_sealed_range(start, black_box(&range.blob), &range.meta).unwrap();
                    (dir, index)
                },
                BatchSize::PerIteration,
            )
        });
        let checked = CheckedRange::check(range.blob.clone(), entry).unwrap();
        group.bench_function(format!("install_checked_range/{start}"), |b| {
            b.iter_batched(
                || range.fresh_index(),
                |(dir, index)| {
                    index.install_checked_range(black_box(&checked), &range.meta).unwrap();
                    (dir, index)
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_install_path);
criterion_main!(benches);
