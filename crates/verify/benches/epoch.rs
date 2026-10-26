//! Verification of one epoch that holds a synthetic peak range: 1,060,000 Sapling commitments in
//! 1,000 blocks, as the 2026-10-22 run measured for the range at 1,785,000.
//!
//! The gate is 45 s on one core. Run it with `RAYON_NUM_THREADS=1`, because `append_batch`
//! uses rayon.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use zakura_chain::sapling;
use ztreamer_indexer::{
    codec::{TreeSizes, encode_range},
    index::{
        RANGE_SIZE,
        range_meta::{BlobDigest, Frontiers, ManifestEntry, SubtreeNode, SubtreeRoot},
    },
    test_utils::{hash, record},
    types::{CompactSaplingOutput, CompactTransaction},
};
use ztreamer_verify::epoch::Epochs;

/// Sapling commitments per block: 1,060,000 in the range.
const OUTPUTS_PER_BLOCK: u32 = 1_060;

/// The range at genesis with [`OUTPUTS_PER_BLOCK`] commitments in each block, its blob, and the
/// entry that describes it.
fn peak_range() -> (ManifestEntry, Vec<u8>) {
    let mut tree = sapling::tree::NoteCommitmentTree::default();
    let mut subtree_roots = Vec::new();
    let records: Vec<_> = (0..RANGE_SIZE)
        .map(|height| {
            let outputs: Vec<_> = (0..OUTPUTS_PER_BLOCK)
                .map(|i| {
                    let mut cmu = [0; 32];
                    cmu[..4].copy_from_slice(&height.to_le_bytes());
                    cmu[4..8].copy_from_slice(&i.to_le_bytes());
                    CompactSaplingOutput { cmu, ephemeral_key: [1; 32], ciphertext: [2; 52] }
                })
                .collect();
            let leaves: Vec<_> = outputs
                .iter()
                .map(|output| sapling::tree::NoteCommitmentUpdate::from_bytes(&output.cmu).unwrap())
                .collect();
            if let Some((index, node)) = tree.append_batch(&leaves).unwrap() {
                subtree_roots.push(SubtreeRoot { index, root: SubtreeNode::Sapling(node) });
            }
            let transaction = CompactTransaction {
                index: 1,
                txid: zakura_chain::transaction::Hash(hash(height).0),
                sapling_spends: Vec::new(),
                sapling_outputs: outputs,
                orchard_actions: Vec::new(),
                ironwood_actions: Vec::new(),
            };
            let mut record = record(height, vec![transaction]);
            record.end_tree_sizes =
                TreeSizes { sapling: (height + 1) * OUTPUTS_PER_BLOCK, ..TreeSizes::default() };
            record
        })
        .collect();
    let blob = encode_range(&records).unwrap();
    let last = records.last().unwrap();
    let entry = ManifestEntry {
        range_version: blob[0],
        start_height: 0,
        end_height: last.height,
        end_hash: last.hash,
        end_tree_sizes: last.end_tree_sizes,
        frontiers: Frontiers { sapling: tree.to_rpc_bytes(), ..Frontiers::default() },
        subtree_roots,
        blob_digest: BlobDigest(*blake3::hash(&blob).as_bytes()),
        blob_len: blob.len() as u64,
        epoch_end: true,
    };
    (entry, blob)
}

fn bench_peak_range(c: &mut Criterion) {
    let (entry, blob) = peak_range();
    let entries = [entry];
    let epoch = Epochs::new(&entries).next().unwrap();
    let blobs = [blob];
    let start = Frontiers::default();
    assert_eq!(epoch.verify(&blobs, &start).unwrap().completed.len(), 16);

    let mut group = c.benchmark_group("epoch_verify");
    group.sample_size(10);
    group.bench_function("peak_range_1_060_000_sapling", |b| {
        b.iter(|| black_box(&epoch).verify(black_box(&blobs), black_box(&start)).unwrap())
    });
    group.finish();
}

criterion_group!(benches, bench_peak_range);
criterion_main!(benches);
