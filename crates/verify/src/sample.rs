//! Sampling: the node gives full blocks, and the replica compares their records with the ranges.
//!
//! [`Epoch::verify`](crate::epoch::Epoch::verify) and [`Epoch::attest`](crate::epoch::Epoch::attest)
//! prove the hash chain and the note commitment trees. They do not prove the fields that no root
//! commits to, for example the ciphertexts. [`sample_epoch`] picks some heights of each range,
//! asks the node for the full block at each height, parses it with the parser of the indexer, and
//! compares the record byte for byte with the record in the blob.
//!
//! A pruned node has no block bodies below its prune height. The sampler skips these heights.

use async_trait::async_trait;
use rand::{SeedableRng, rngs::StdRng, seq::index};
use zakura_chain::{
    block::{self, Block},
    serialization::ZcashDeserialize,
};
use zakura_state::HashOrHeight;
use ztreamer_indexer::{
    codec::{CompactBlockRecord, RangeDecoder, TreeSizes},
    index::{RANGE_SIZE, range_meta::ManifestEntry},
    parser::parse_block,
    types::RawIndexBlock,
};
use ztreamer_remote::ZakuraClient;

use crate::{
    attest::{EpochError, NodeError},
    epoch::Epoch,
    range::{Check, Reject},
};

/// A source of full blocks, usually a node.
///
/// # Implementation
///
/// Give the best-chain block at a height. Return `None` when the source does not store the body
/// of the block, and an error when a call fails. [`Sampler::prune_height`] is the lowest height
/// whose body the source stores: [`sample_epoch`] does not ask below it.
#[async_trait]
pub trait Sampler: Send + Sync {
    /// The full block at `height`, or `None` when the source does not store its body.
    async fn full_block(&self, height: u32) -> Result<Option<RawIndexBlock>, NodeError>;

    /// The lowest height whose block body the source stores.
    fn prune_height(&self) -> u32;
}

/// Compares the records at sampled heights of `epoch` with the full blocks of `sampler`.
///
/// `blobs` holds one blob per entry, in order; another count is [`Reject::BlobCount`]. For each
/// range, the function picks `per_range` different heights with a choice that only `seed` and
/// the range start control, and skips the heights below [`Sampler::prune_height`]. It parses
/// each full block with [`parse_block`], adds the tree sizes before the block, and compares the
/// encoded record with the record in the blob. It returns the compared heights in ascending
/// order, for `sampled_heights` in `range_meta`.
///
/// Run it after [`Epoch::verify`](crate::epoch::Epoch::verify): it does not check the blob digests.
pub async fn sample_epoch(
    epoch: &Epoch<'_>,
    blobs: &[impl AsRef<[u8]>],
    sampler: &dyn Sampler,
    per_range: usize,
    seed: u64,
) -> Result<Vec<u32>, EpochError> {
    if blobs.len() != epoch.entries().len() {
        return Err(Reject::BlobCount { entries: epoch.entries().len(), blobs: blobs.len() }.into());
    }
    let prune_height = sampler.prune_height();
    let mut before = epoch.previous().map_or_else(TreeSizes::default, |entry| entry.end_tree_sizes);
    let mut sampled = Vec::new();
    for (entry, blob) in epoch.entries().iter().zip(blobs) {
        let start = entry.start_height;
        let decoder = RangeDecoder::new(blob.as_ref()).map_err(|_| Check::Decode.at(start))?;
        for offset in offsets(entry, per_range, seed) {
            let height = start.saturating_add(offset);
            if height < prune_height {
                continue;
            }
            let decode = |offset| decoder.record(offset).map_err(|_| Check::Decode.at(height));
            let record = decode(offset as usize)?;
            let sizes = match offset {
                0 => before,
                _ => decode(offset as usize - 1)?.end_tree_sizes,
            };
            let node = |source| EpochError::Node { height, source };
            let Some(block) = sampler.full_block(height).await.map_err(node)? else {
                continue;
            };
            let expected = node_record(&block, sizes).map_err(node)?;
            if record.encode().ok().as_ref() != Some(&expected) {
                return Err(Check::SampledBlock.at(height).into());
            }
            sampled.push(height);
        }
        before = entry.end_tree_sizes;
    }
    Ok(sampled)
}

/// The offsets in the range of `entry` to sample, in ascending order.
fn offsets(entry: &ManifestEntry, per_range: usize, seed: u64) -> Vec<u32> {
    let mut rng = StdRng::seed_from_u64(seed ^ u64::from(entry.start_height));
    let length = RANGE_SIZE as usize;
    let mut offsets: Vec<_> = index::sample(&mut rng, length, per_range.min(length))
        .into_iter()
        // An offset is below `RANGE_SIZE`, which is a `u32`.
        .map(|offset| offset as u32)
        .collect();
    offsets.sort_unstable();
    offsets
}

/// The encoded record of `block`, with the tree sizes `before` the block. A tree size that
/// overflows saturates, and thus does not match the blob.
fn node_record(block: &RawIndexBlock, before: TreeSizes) -> Result<Vec<u8>, NodeError> {
    let parsed = parse_block(block)?;
    let end_tree_sizes = TreeSizes {
        sapling: before.sapling.saturating_add(parsed.sapling_additions),
        orchard: before.orchard.saturating_add(parsed.orchard_additions),
        ironwood: before.ironwood.saturating_add(parsed.ironwood_additions),
    };
    Ok(CompactBlockRecord {
        height: parsed.height,
        hash: parsed.hash,
        previous_hash: parsed.previous_hash,
        time: parsed.time,
        transactions: parsed.transactions,
        end_tree_sizes,
    }
    .encode()?)
}

/// A [`Sampler`] that asks a zakura node over JSON-RPC.
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_verify::sample::{JsonRpcSampler, Sampler};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let sampler = JsonRpcSampler::connect(ZakuraClient::new(RpcClient::new(config)?)).await?;
/// let block = sampler.full_block(3_524_999).await?;
/// # let _ = block;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct JsonRpcSampler {
    node: ZakuraClient,
    prune_height: u32,
}

impl JsonRpcSampler {
    /// Reads the prune height of the node with `getblockchaininfo`. A node that does not prune
    /// has the prune height 0.
    ///
    /// A pruning node raises its prune height while it runs. Connect again to read the new
    /// height.
    pub async fn connect(node: ZakuraClient) -> Result<Self, NodeError> {
        let info = node.get_blockchain_info().await?;
        Ok(Self { prune_height: info.prune_height().map_or(0, |height| height.0), node })
    }
}

#[async_trait]
impl Sampler for JsonRpcSampler {
    async fn full_block(&self, height: u32) -> Result<Option<RawIndexBlock>, NodeError> {
        if height < self.prune_height {
            return Ok(None);
        }
        let bytes = self.node.get_block_raw(HashOrHeight::Height(block::Height(height))).await?.0;
        let block = Block::zcash_deserialize(bytes.as_slice())?;
        let actual = block.coinbase_height().ok_or(NodeError::CoinbaseHeight)?;
        if actual.0 != height {
            return Err(NodeError::Height { expected: height, actual: actual.0 });
        }
        Ok(Some(RawIndexBlock {
            height: actual,
            hash: block.hash(),
            txids: block.transactions.iter().map(|transaction| transaction.hash()).collect(),
            bytes,
        }))
    }

    fn prune_height(&self) -> u32 {
        self.prune_height
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use zakura_rpc::client::GetBlockchainInfoResponse;
    use zakura_test::vectors::BLOCK_MAINNET_949496_BYTES;
    use ztreamer_indexer::{
        codec::encode_range,
        test_utils::{hash, raw_block},
    };
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockNode, Reply},
    };

    use super::*;
    use crate::{epoch::Epochs, range::entry_for};

    /// The height of the one block with shielded data.
    const SHIELDED: u32 = 500;

    /// Full blocks from a table. A call below the prune height panics.
    struct FakeSampler {
        blocks: BTreeMap<u32, RawIndexBlock>,
        prune_height: u32,
    }

    #[async_trait]
    impl Sampler for FakeSampler {
        async fn full_block(&self, height: u32) -> Result<Option<RawIndexBlock>, NodeError> {
            assert!(height >= self.prune_height, "call for {height} below the prune height");
            Ok(self.blocks.get(&height).cloned())
        }

        fn prune_height(&self) -> u32 {
            self.prune_height
        }
    }

    /// Mainnet block 949,496 at `height`, with the header linked to [`hash`] of the height
    /// before.
    fn shielded_block(height: u32) -> RawIndexBlock {
        let mut bytes = BLOCK_MAINNET_949496_BYTES.to_vec();
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        bytes[4..36].copy_from_slice(&hash(height - 1).0);
        RawIndexBlock {
            height: block::Height(height),
            hash: hash(height),
            txids: block.transactions.iter().map(|transaction| transaction.hash()).collect(),
            bytes,
        }
    }

    /// The node blocks 0 to 999: empty blocks, and [`shielded_block`] at [`SHIELDED`].
    fn blocks() -> BTreeMap<u32, RawIndexBlock> {
        (0..RANGE_SIZE)
            .map(|height| {
                let block =
                    if height == SHIELDED { shielded_block(height) } else { raw_block(height) };
                (height, block)
            })
            .collect()
    }

    /// The records of [`blocks`] with running tree sizes, as the indexer stores them.
    fn records() -> Vec<CompactBlockRecord> {
        let mut sizes = TreeSizes::default();
        blocks()
            .values()
            .map(|block| {
                let parsed = parse_block(block).unwrap();
                sizes = TreeSizes {
                    sapling: sizes.sapling + parsed.sapling_additions,
                    orchard: sizes.orchard + parsed.orchard_additions,
                    ironwood: sizes.ironwood + parsed.ironwood_additions,
                };
                CompactBlockRecord {
                    height: parsed.height,
                    hash: parsed.hash,
                    previous_hash: parsed.previous_hash,
                    time: parsed.time,
                    transactions: parsed.transactions,
                    end_tree_sizes: sizes,
                }
            })
            .collect()
    }

    /// Samples the range of `records` against [`blocks`].
    async fn sample(
        records: &[CompactBlockRecord],
        prune_height: u32,
        per_range: usize,
        seed: u64,
    ) -> Result<Vec<u32>, EpochError> {
        let blob = encode_range(records).unwrap();
        let entries = [entry_for(records, &blob)];
        let sampler = FakeSampler { blocks: blocks(), prune_height };
        sample_epoch(&Epochs::new(&entries).next().unwrap(), &[blob], &sampler, per_range, seed)
            .await
    }

    #[tokio::test]
    async fn matching_records_return_the_sampled_heights() {
        let records = records();

        let heights = sample(&records, 0, 5, 7).await.unwrap();

        assert_eq!(heights.len(), 5);
        assert!(heights.windows(2).all(|pair| pair[0] < pair[1]), "{heights:?}");
        assert!(heights.iter().all(|&height| height < RANGE_SIZE), "{heights:?}");
        assert_eq!(sample(&records, 0, 5, 7).await.unwrap(), heights);
        assert_ne!(sample(&records, 0, 5, 8).await.unwrap(), heights);
        let all = sample(&records, 0, 2 * RANGE_SIZE as usize, 7).await.unwrap();
        assert_eq!(all, (0..RANGE_SIZE).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_mutated_output_rejects_at_its_height() {
        let mut records = records();
        let transactions = &mut records[SHIELDED as usize].transactions;
        let output = transactions.iter_mut().flat_map(|tx| &mut tx.sapling_outputs).next().unwrap();
        output.ciphertext[0] ^= 1;

        let result = sample(&records, 0, RANGE_SIZE as usize, 7).await;

        assert!(
            matches!(
                result,
                Err(EpochError::Reject(Reject::Rejected {
                    check: Check::SampledBlock,
                    height: SHIELDED,
                }))
            ),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn heights_below_the_prune_height_are_skipped() {
        let heights = sample(&records(), 600, RANGE_SIZE as usize, 7).await.unwrap();

        assert_eq!(heights, (600..RANGE_SIZE).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_missing_blob_is_a_blob_count_reject() {
        let records = records();
        let entries = [entry_for(&records, &encode_range(&records).unwrap())];
        let sampler = FakeSampler { blocks: blocks(), prune_height: 0 };

        let result =
            sample_epoch(&Epochs::new(&entries).next().unwrap(), &[] as &[Vec<u8>], &sampler, 5, 7)
                .await;

        assert!(
            matches!(result, Err(EpochError::Reject(Reject::BlobCount { entries: 1, blobs: 0 }))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn json_rpc_sampler_reads_the_prune_height_and_the_block() {
        let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
        info["pruneheight"] = json!(949_000);
        let node = MockNode::start(move |_, seen| match seen.body["method"].as_str().unwrap() {
            "getblockchaininfo" => Reply::Result(info.clone()),
            "getblock" => Reply::Result(json!(hex::encode(&*BLOCK_MAINNET_949496_BYTES))),
            method => panic!("unexpected method {method}"),
        })
        .await;
        let client = ZakuraClient::new(RpcClient::new(node.config()).unwrap());

        let sampler = JsonRpcSampler::connect(client).await.unwrap();
        assert_eq!(sampler.prune_height(), 949_000);
        assert!(sampler.full_block(948_999).await.unwrap().is_none());
        let block = sampler.full_block(949_496).await.unwrap().unwrap();
        let error = sampler.full_block(949_497).await.unwrap_err();

        assert_eq!(block.height, block::Height(949_496));
        assert_eq!(block.bytes, *BLOCK_MAINNET_949496_BYTES);
        assert!(!parse_block(&block).unwrap().transactions.is_empty());
        assert!(
            matches!(error, NodeError::Height { expected: 949_497, actual: 949_496 }),
            "{error:?}"
        );
        assert_eq!(node.seen().len(), 3);
    }
}
