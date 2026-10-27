//! Sampling: the node gives full blocks, and the replica compares their records with the ranges.
//!
//! [`Epoch::verify`](crate::epoch::Epoch::verify) and [`Epoch::attest`](crate::epoch::Epoch::attest)
//! prove the hash chain and the note commitment trees. They do not prove the fields that no root
//! commits to, for example the ciphertexts. [`Epoch::sample`] picks some heights of each range,
//! asks the node for the full block at each height, parses it with the parser of the indexer, and
//! compares the record byte for byte with the record in the blob.

use std::panic;

use async_trait::async_trait;
use rand::{SeedableRng, rngs::StdRng, seq::index};
use zakura_chain::block;
use zakura_state::HashOrHeight;
use ztreamer_indexer::{
    codec::RangeDecoder, index::RANGE_SIZE, parser::parse_block, types::RawIndexBlock,
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
/// Give the best-chain block at a height, and an error when a call fails or the source has no
/// block at the height.
#[async_trait]
pub trait Sampler: Send + Sync {
    /// The full block at `height`.
    async fn full_block(&self, height: u32) -> Result<RawIndexBlock, NodeError>;
}

/// How [`Epoch::sample`] picks the heights to compare.
///
/// The seed must be random and secret for each replica. A snapshot source that knows the seed
/// knows the sampled heights, and can change the records at all other heights.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SamplePlan {
    /// The heights to compare in each range. A value above [`RANGE_SIZE`] compares each height.
    pub per_range: usize,
    /// The seed of the choice.
    pub seed: u64,
}

impl SamplePlan {
    /// The heights in each range that a replica compares.
    pub const PER_RANGE: usize = 2;

    /// The offsets in the range that starts at `start_height` to compare, in ascending order.
    /// Only the seed and `start_height` control the choice.
    fn offsets(self, start_height: u32) -> Vec<u32> {
        let mut rng = StdRng::seed_from_u64(self.seed ^ u64::from(start_height));
        let length = RANGE_SIZE as usize;
        let mut offsets: Vec<_> = index::sample(&mut rng, length, self.per_range.min(length))
            .into_iter()
            // An offset is below `RANGE_SIZE`, which is a `u32`.
            .map(|offset| offset as u32)
            .collect();
        offsets.sort_unstable();
        offsets
    }
}

impl Epoch<'_> {
    /// Compares the records at the heights that `plan` picks with the full blocks of `sampler`.
    ///
    /// `blobs` holds one blob per entry, in order; another count is [`Reject::BlobCount`]. The
    /// method skips the heights for which the sampler has no block. It parses each full block
    /// with [`parse_block`], adds the tree sizes before the block, and compares the encoded
    /// record with the record in the blob. It returns the compared heights in ascending order,
    /// for `sampled_heights` in `range_meta`.
    ///
    /// Run it after [`Epoch::verify`] on a Tokio runtime. It does not check the blob digests,
    /// and it parses and encodes the blocks on the blocking thread pool of the runtime.
    ///
    /// ```
    /// use ztreamer_verify::{
    ///     attest::EpochError,
    ///     epoch::Epoch,
    ///     sample::{SamplePlan, Sampler},
    /// };
    ///
    /// # async fn run(
    /// #     epoch: Epoch<'_>,
    /// #     blobs: Vec<Vec<u8>>,
    /// #     node: &dyn Sampler,
    /// #     seed: u64,
    /// # ) -> Result<Vec<u32>, EpochError> {
    /// epoch.sample(&blobs, node, SamplePlan { per_range: 5, seed }).await
    /// # }
    /// ```
    pub async fn sample(
        &self,
        blobs: &[impl AsRef<[u8]>],
        sampler: &dyn Sampler,
        plan: SamplePlan,
    ) -> Result<Vec<u32>, EpochError> {
        if blobs.len() != self.entries().len() {
            let (entries, blobs) = (self.entries().len(), blobs.len());
            return Err(Reject::BlobCount { entries, blobs }.into());
        }
        let mut sampled = Vec::new();
        for (entry, blob) in self.entries().iter().zip(blobs) {
            let start = entry.start_height;
            let decoder = RangeDecoder::new(blob.as_ref()).map_err(|_| Check::Decode.at(start))?;
            for offset in plan.offsets(start) {
                let height = start.saturating_add(offset);
                let record =
                    decoder.record(offset as usize).map_err(|_| Check::Decode.at(height))?;
                let node = |source| EpochError::Node { height, source };
                let block = sampler.full_block(height).await.map_err(node)?;
                let same = tokio::task::spawn_blocking(move || -> Result<bool, NodeError> {
                    // `Epoch::verify` checked that each record adds its own commitments. Thus
                    // the tree sizes before the block are the end sizes minus the additions.
                    let Some(before) = record.end_tree_sizes.checked_sub(record.additions()) else {
                        return Ok(false);
                    };
                    let Some(expected) = parse_block(&block)?.into_record(before) else {
                        return Ok(false);
                    };
                    let expected = expected.encode()?;
                    Ok(record.encode().is_ok_and(|bytes| bytes == expected))
                })
                .await
                .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
                .map_err(node)?;
                if !same {
                    return Err(Check::SampledBlock.at(height).into());
                }
                sampled.push(height);
            }
        }
        Ok(sampled)
    }
}

/// A [`Sampler`] that asks a zakura node over JSON-RPC.
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_verify::sample::{JsonRpcSampler, Sampler};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let sampler = JsonRpcSampler::new(ZakuraClient::new(RpcClient::new(config)?));
/// let block = sampler.full_block(3_524_999).await?;
/// # let _ = block;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct JsonRpcSampler {
    node: ZakuraClient,
}

impl JsonRpcSampler {
    /// The sampler over `node`.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node }
    }
}

#[async_trait]
impl Sampler for JsonRpcSampler {
    /// The block at `height`. Deserializes it on the blocking thread pool of the runtime.
    async fn full_block(&self, height: u32) -> Result<RawIndexBlock, NodeError> {
        let bytes = self.node.get_block_raw(HashOrHeight::Height(block::Height(height))).await?.0;
        let block = tokio::task::spawn_blocking(move || RawIndexBlock::from_bytes(bytes))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))?;
        if block.height.0 != height {
            return Err(NodeError::Height { expected: height, actual: block.height.0 });
        }
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use zakura_test::vectors::BLOCK_MAINNET_949496_BYTES;
    use ztreamer_indexer::{
        codec::{CompactBlockRecord, TreeSizes, encode_range},
        index::range_meta::ManifestEntry,
        test_utils::{hash, raw_block},
    };
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockNode, Reply},
    };

    use super::*;
    use crate::epoch::Epochs;

    /// The height of the one block with shielded data.
    const SHIELDED: u32 = 500;

    /// Full blocks from a table.
    struct FakeSampler {
        blocks: BTreeMap<u32, RawIndexBlock>,
    }

    #[async_trait]
    impl Sampler for FakeSampler {
        async fn full_block(&self, height: u32) -> Result<RawIndexBlock, NodeError> {
            Ok(self.blocks[&height].clone())
        }
    }

    /// Mainnet block 949,496 at `height`, with the header linked to [`hash`] of the height
    /// before.
    fn shielded_block(height: u32) -> RawIndexBlock {
        let mut block = RawIndexBlock::from_bytes(BLOCK_MAINNET_949496_BYTES.to_vec()).unwrap();
        block.bytes[4..36].copy_from_slice(&hash(height - 1).0);
        RawIndexBlock { height: block::Height(height), hash: hash(height), ..block }
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
                let record = parse_block(block).unwrap().into_record(sizes).unwrap();
                sizes = record.end_tree_sizes;
                record
            })
            .collect()
    }

    /// Samples the range of `records` against [`blocks`].
    async fn sample(
        records: &[CompactBlockRecord],
        per_range: usize,
        seed: u64,
    ) -> Result<Vec<u32>, EpochError> {
        let blob = encode_range(records).unwrap();
        let entries = [ManifestEntry::from_records_and_blob(records, &blob)];
        let sampler = FakeSampler { blocks: blocks() };
        let plan = SamplePlan { per_range, seed };
        Epochs::new(&entries).next().unwrap().sample(&[blob], &sampler, plan).await
    }

    #[tokio::test]
    async fn matching_records_return_the_sampled_heights() {
        let records = records();

        let heights = sample(&records, 5, 7).await.unwrap();

        assert_eq!(heights.len(), 5);
        assert!(heights.windows(2).all(|pair| pair[0] < pair[1]), "{heights:?}");
        assert!(heights.iter().all(|&height| height < RANGE_SIZE), "{heights:?}");
        assert_eq!(sample(&records, 5, 7).await.unwrap(), heights);
        assert_ne!(sample(&records, 5, 8).await.unwrap(), heights);
        let all = sample(&records, 2 * RANGE_SIZE as usize, 7).await.unwrap();
        assert_eq!(all, (0..RANGE_SIZE).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_mutated_output_rejects_at_its_height() {
        let mut records = records();
        let transactions = &mut records[SHIELDED as usize].transactions;
        let output = transactions.iter_mut().flat_map(|tx| &mut tx.sapling_outputs).next().unwrap();
        output.ciphertext[0] ^= 1;

        let result = sample(&records, RANGE_SIZE as usize, 7).await;

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
    async fn a_missing_blob_is_a_blob_count_reject() {
        let records = records();
        let entries =
            [ManifestEntry::from_records_and_blob(&records, &encode_range(&records).unwrap())];
        let sampler = FakeSampler { blocks: blocks() };
        let plan = SamplePlan { per_range: 5, seed: 7 };

        let result =
            Epochs::new(&entries).next().unwrap().sample(&[] as &[Vec<u8>], &sampler, plan).await;

        assert!(
            matches!(result, Err(EpochError::Reject(Reject::BlobCount { entries: 1, blobs: 0 }))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn json_rpc_sampler_reads_the_block() {
        let node = MockNode::start(move |_, seen| match seen.body["method"].as_str().unwrap() {
            "getblock" => Reply::Result(json!(hex::encode(&*BLOCK_MAINNET_949496_BYTES))),
            method => panic!("unexpected method {method}"),
        })
        .await;
        let client = ZakuraClient::new(RpcClient::new(node.config()).unwrap());

        let sampler = JsonRpcSampler::new(client);
        let block = sampler.full_block(949_496).await.unwrap();
        let error = sampler.full_block(949_497).await.unwrap_err();

        assert_eq!(block.height, block::Height(949_496));
        assert_eq!(block.bytes, *BLOCK_MAINNET_949496_BYTES);
        assert!(!parse_block(&block).unwrap().transactions.is_empty());
        assert!(
            matches!(error, NodeError::Height { expected: 949_497, actual: 949_496 }),
            "{error:?}"
        );
        assert_eq!(node.seen().len(), 2);
    }
}
