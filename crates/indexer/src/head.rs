//! Live canonical-head polling, persistence-depth handling, and ordinary or deep reorg recovery.

use zakura_chain::{
    block,
    parameters::{GENESIS_PREVIOUS_BLOCK_HASH, constants::MAX_BLOCK_REORG_HEIGHT},
};
use ztreamer_jsonrpc::RpcError;

use crate::{
    blocking::BlockingTaskCancelled,
    codec::CompactBlockRecord,
    index::{BlockId, Index, IndexError, IndexState, PERSIST_DEPTH, SEAL_DEPTH},
    ingest::{IngestError, OrderedBuilder},
    parser::{CompactParseError, parse_block},
    pipeline::PipelineConfig,
    types::{
        BlockAtHeightError, FetchAtHeightError, ParsedCompactBlock, RawBlockError, RawIndexBlock,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    #[error("Zakura head request failed: {0}")]
    Node(String),
    /// A request to a remote zakura node failed.
    #[error("remote Zakura head request failed: {0}")]
    Remote(#[from] RpcError),
    #[error("Zakura head block could not be decoded: {0}")]
    Decode(String),
    #[error("Zakura returned height {actual} for requested height {expected}")]
    Height { expected: u32, actual: u32 },
    /// The block at `height` does not link to the block below it. The chain of the node changed
    /// between two reads of the poll. The next poll reads the new chain.
    #[error(
        "Zakura block {height} does not link to the block below it: the chain changed during the reads"
    )]
    Parent { height: u32 },
    #[error("the local anchor at height {height} is not canonical in Zakura")]
    Anchor { height: u32 },
    #[error("Zakura's canonical head changed while it was read")]
    Changed,
    #[error("Zakura returned more than its {MAX_BLOCK_REORG_HEIGHT}-block non-finalized window")]
    Window,
    #[error(transparent)]
    Parse(#[from] CompactParseError),
    /// The blocking pool stopped before a blocking task of the poll ran.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
}

impl From<RawBlockError> for HeadError {
    fn from(error: RawBlockError) -> Self {
        Self::Decode(error.to_string())
    }
}

impl From<BlockAtHeightError> for HeadError {
    fn from(error: BlockAtHeightError) -> Self {
        match error {
            BlockAtHeightError::Rpc(error) => Self::Remote(error),
            BlockAtHeightError::Decode { source, .. } => source.into(),
            BlockAtHeightError::Height { expected, actual } => Self::Height { expected, actual },
        }
    }
}

impl From<FetchAtHeightError> for HeadError {
    fn from(error: FetchAtHeightError) -> Self {
        match error {
            FetchAtHeightError::Block(error) => error.into(),
            FetchAtHeightError::Cancelled(error) => Self::Cancelled(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeadSyncError {
    #[error(transparent)]
    Head(#[from] HeadError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error("canonical reorg has no retained common ancestor")]
    NoCommonAncestor,
    #[error("canonical reorg crosses sealed height {height}")]
    DeepReorg { height: u32 },
}

/// The canonical-head operations Ztreamer needs from Zakura.
///
/// # Implementation guide
///
/// Implement [`block`](Self::block). Implement [`tip`](Self::tip) when the source can tell its
/// tip without a block read. Implement [`block_hash`](Self::block_hash) when the source can read
/// a hash for less than the cost of a block: [`poll_canonical_head`] reads hashes to find the
/// first changed height, and reads blocks from that height only.
#[tonic::async_trait]
pub trait CanonicalBlockSource: Send {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        Ok(None)
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError>;

    /// The hash of the canonical block at `height`, or `None` when the source has no block at
    /// that height.
    ///
    /// The default reads the block with [`block`](Self::block).
    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        Ok(self.block(height).await?.map(|block| block.hash))
    }
}

/// The canonical chain from a start height, as one [`poll_canonical_head`] call read it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalSuffix {
    /// The last known block that is still canonical, or `None` when no known block is
    /// canonical. The poll does not read the blocks through this block.
    pub retained: Option<BlockId>,
    /// The canonical blocks after the retained block, in height order.
    pub blocks: Vec<ParsedCompactBlock>,
}

/// Reads one stable canonical suffix after `previous_hash` and prepares its compact records.
///
/// `known` holds the blocks that the caller has from `start` upward. The poll uses the longest
/// prefix of `known` that is one linked chain from `previous_hash`. It reads hashes with
/// [`CanonicalBlockSource::block_hash`] to find the last known block that is still canonical,
/// and reads blocks after that block only. Each block must link to its predecessor, and the
/// last height must have the same hash after the reads, thus the result is one canonical chain.
pub async fn poll_canonical_head(
    source: &mut impl CanonicalBlockSource,
    start: u32,
    previous_hash: block::Hash,
    known: &[CompactBlockRecord],
) -> Result<CanonicalSuffix, HeadError> {
    if let Some(anchor_height) = start.checked_sub(1)
        && source.block_hash(anchor_height).await? != Some(previous_hash)
    {
        return Err(HeadError::Anchor { height: anchor_height });
    }

    let linked = known
        .iter()
        .zip(start..)
        .scan(previous_hash, |parent, (block, height)| {
            let linked = block.height == height && block.previous_hash == *parent;
            *parent = block.hash;
            linked.then_some(())
        })
        .count();
    let known = &known[..linked];
    // A canonical block makes all blocks below it canonical. Read the last known hash first,
    // because a new block on the same chain is the usual change. Then bisect.
    let mut canonical = known.len();
    if let Some(last) = known.last()
        && source.block_hash(last.height).await? != Some(last.hash)
    {
        let mut changed = known.len() - 1;
        canonical = 0;
        while canonical < changed {
            let middle = canonical + (changed - canonical) / 2;
            if source.block_hash(known[middle].height).await? == Some(known[middle].hash) {
                canonical = middle + 1;
            } else {
                changed = middle;
            }
        }
    }
    let retained =
        canonical.checked_sub(1).map(|index| BlockId::new(known[index].height, known[index].hash));

    let mut raw = Vec::new();
    let mut expected_parent = retained.map_or(previous_hash, |block| block.hash);
    let mut height = retained
        .map_or(Some(start), |block| block.height.checked_add(1))
        .ok_or(HeadError::Window)?;
    while let Some(block) = source.block(height).await? {
        if height - start == MAX_BLOCK_REORG_HEIGHT {
            return Err(HeadError::Window);
        }
        let parent: block::Hash = block
            .bytes
            .get(4..36)
            .and_then(|bytes| bytes.try_into().ok())
            .map(block::Hash)
            .ok_or_else(|| HeadError::Decode("block header is truncated".to_owned()))?;
        if parent != expected_parent {
            return Err(HeadError::Parent { height });
        }
        expected_parent = block.hash;
        raw.push(block);
        height = height.checked_add(1).ok_or(HeadError::Window)?;
    }

    let last = raw.last().map(|block| BlockId::new(block.height.0, block.hash)).or(retained);
    if let Some(last) = last
        && source.block_hash(last.height).await? != Some(last.hash)
    {
        return Err(HeadError::Changed);
    }

    let blocks = raw.iter().map(parse_block).collect::<Result<Vec<_>, _>>()?;
    Ok(CanonicalSuffix { retained, blocks })
}

/// Reconciles the canonical head and atomically replaces an ordinary reorg suffix.
pub async fn sync_head_once(
    index: &Index,
    source: &mut impl CanonicalBlockSource,
    current_head: &[CompactBlockRecord],
    config: PipelineConfig,
) -> Result<(IndexState, Vec<CompactBlockRecord>), HeadSyncError> {
    let mut state = index.state()?;
    if current_head.is_empty() {
        return match sync_extension_once(index, source, state, config).await {
            Err(HeadSyncError::Head(HeadError::Anchor { height })) => {
                Err(HeadSyncError::DeepReorg { height })
            }
            result => result,
        };
    }

    let old_visible_tip =
        current_head.last().map(|block| block.height).expect("the current head is non-empty");
    let old_visible_tip_hash =
        current_head.last().map(|block| block.hash).expect("the current head is non-empty");
    if source.tip().await? == Some(BlockId::new(old_visible_tip, old_visible_tip_hash)) {
        return Ok((state, current_head.to_vec()));
    }
    let anchor_height = old_visible_tip.checked_sub(SEAL_DEPTH);
    let anchor_record = anchor_height
        .map(|height| current_record(index, state, current_head, height))
        .transpose()?
        .flatten();
    let start = anchor_height.map_or(0, |height| height + 1);
    let previous_hash =
        anchor_record.as_ref().map_or(GENESIS_PREVIOUS_BLOCK_HASH, |block| block.hash);
    let known = (start..=old_visible_tip)
        .map(|height| current_record(index, state, current_head, height))
        .map_while(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;
    let suffix = match poll_canonical_head(source, start, previous_hash, &known).await {
        Err(HeadError::Anchor { height }) => return Err(HeadSyncError::DeepReorg { height }),
        result => result?,
    };

    let mut common = suffix
        .retained
        .or_else(|| anchor_record.as_ref().map(|block| BlockId::new(block.height, block.hash)));
    // A chain that changes during the poll can make the poll read blocks that did not change.
    for block in &suffix.blocks {
        match current_record(index, state, current_head, block.height)? {
            Some(old) if old.hash == block.hash => {
                common = Some(BlockId::new(old.height, old.hash));
            }
            _ => break,
        }
    }
    let common = common.ok_or(HeadSyncError::NoCommonAncestor)?;
    let visible_tip = suffix.blocks.last().map_or(common.height, |block| block.height);

    let old_durable_tip = state.durable_tip().ok_or(IndexError::Metadata)?;
    let ancestor_height = common.height.min(old_durable_tip.height);
    let ancestor_record = current_record(index, state, current_head, ancestor_height)?
        .ok_or(IndexError::Coverage { height: ancestor_height })?;
    // The records above the ancestor and through the common block do not change.
    let mut canonical = (ancestor_height + 1..=common.height)
        .map(|height| {
            current_record(index, state, current_head, height)?
                .ok_or(IndexError::Coverage { height })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // With no record above the ancestor, the ancestor is the common block.
    let common_record = canonical.last().unwrap_or(&ancestor_record);
    let base_state = IndexState {
        durable_tip: Some(common),
        sealed_through: state.sealed_through(),
        verified_through: state.verified_through(),
        generation: state.generation(),
        tree_sizes: common_record.end_tree_sizes,
    };
    let mut builder = OrderedBuilder::new(base_state, config.max_pending_bytes)?;
    for block in suffix.blocks.into_iter().filter(|block| block.height > common.height) {
        builder.push(block)?;
    }
    while let Some(batch) = builder.build_batch(Some(visible_tip), None, config.max_batch_bytes)? {
        canonical.extend(batch.records);
    }

    let new_durable_through = visible_tip.checked_sub(PERSIST_DEPTH);
    let durable_tip =
        new_durable_through.map_or(ancestor_height, |height| height.max(ancestor_height));
    let replacement = canonical
        .iter()
        .filter(|block| block.height > ancestor_height && block.height <= durable_tip)
        .cloned()
        .collect::<Vec<_>>();
    if ancestor_height != old_durable_tip.height || !replacement.is_empty() {
        state = index.replace_mutable_suffix(
            state.generation(),
            BlockId::new(ancestor_record.height, ancestor_record.hash),
            replacement,
            visible_tip.checked_sub(SEAL_DEPTH),
        )?;
    }
    let durable_tip = state.durable_tip().expect("a reorg replacement retains its ancestor").height;
    let volatile = canonical.into_iter().filter(|block| block.height > durable_tip).collect();
    Ok((state, volatile))
}

/// Finds the common ancestor and atomically publishes a fully staged deep-reorg generation.
pub async fn recover_deep_reorg(
    index: &Index,
    source: &mut impl CanonicalBlockSource,
    current_head: &[CompactBlockRecord],
    config: PipelineConfig,
) -> Result<(IndexState, Vec<CompactBlockRecord>), HeadSyncError> {
    let state = index.state()?;
    let old_visible_tip = current_head
        .last()
        .map(|block| block.height)
        .or_else(|| state.durable_tip().map(|tip| tip.height))
        .ok_or(HeadSyncError::NoCommonAncestor)?;
    let genesis = source.block(0).await?.ok_or(HeadSyncError::NoCommonAncestor)?;
    if current_record(index, state, current_head, 0)?.is_none_or(|old| old.hash != genesis.hash) {
        return Err(HeadSyncError::NoCommonAncestor);
    }

    let mut matching = 0u32;
    let mut different = old_visible_tip.checked_add(1).ok_or(IngestError::Overflow)?;
    while matching + 1 < different {
        let height = matching + (different - matching) / 2;
        let canonical = source.block(height).await?;
        let old = current_record(index, state, current_head, height)?;
        if canonical.zip(old).is_some_and(|(canonical, old)| canonical.hash == old.hash) {
            matching = height;
        } else {
            different = height;
        }
    }

    let common = current_record(index, state, current_head, matching)?
        .ok_or(HeadSyncError::NoCommonAncestor)?;
    let prepared = poll_canonical_head(source, matching + 1, common.hash, &[]).await?.blocks;
    let visible_tip = prepared.last().map_or(matching, |block| block.height);
    let base_state = IndexState {
        durable_tip: Some(BlockId::new(common.height, common.hash)),
        sealed_through: state.sealed_through(),
        verified_through: state.verified_through(),
        generation: state.generation(),
        tree_sizes: common.end_tree_sizes,
    };
    let mut builder = OrderedBuilder::new(base_state, config.max_pending_bytes)?;
    for block in prepared {
        builder.push(block)?;
    }
    let mut canonical = Vec::new();
    while let Some(batch) = builder.build_batch(Some(visible_tip), None, config.max_batch_bytes)? {
        canonical.extend(batch.records);
    }

    let durable_tip =
        visible_tip.checked_sub(PERSIST_DEPTH).map_or(matching, |height| height.max(matching));
    let replacement =
        canonical.iter().filter(|block| block.height <= durable_tip).cloned().collect();
    let state = index.replace_deep_suffix(
        state.generation(),
        BlockId::new(common.height, common.hash),
        replacement,
        visible_tip.checked_sub(SEAL_DEPTH),
    )?;
    let durable_tip =
        state.durable_tip().expect("deep recovery retains its common ancestor").height;
    Ok((state, canonical.into_iter().filter(|block| block.height > durable_tip).collect()))
}

async fn sync_extension_once(
    index: &Index,
    source: &mut impl CanonicalBlockSource,
    mut state: IndexState,
    config: PipelineConfig,
) -> Result<(IndexState, Vec<CompactBlockRecord>), HeadSyncError> {
    let start = state
        .durable_tip()
        .map_or(Ok(0), |tip| tip.height.checked_add(1).ok_or(IngestError::Overflow))?;
    let previous_hash = state.durable_tip().map_or(GENESIS_PREVIOUS_BLOCK_HASH, |tip| tip.hash);
    let prepared = poll_canonical_head(source, start, previous_hash, &[]).await?.blocks;
    let Some(visible_tip) = prepared.last().map(|block| block.height) else {
        return Ok((state, Vec::new()));
    };
    let durable_through = visible_tip.checked_sub(PERSIST_DEPTH);
    let seal_through = visible_tip.checked_sub(SEAL_DEPTH);
    let mut builder = OrderedBuilder::new(state, config.max_pending_bytes)?;
    for block in prepared {
        builder.push(block)?;
    }

    while let Some(batch) =
        builder.build_batch(durable_through, seal_through, config.max_batch_bytes)?
    {
        state = index.write(batch)?;
    }

    let mut volatile = Vec::new();
    while let Some(batch) = builder.build_batch(Some(visible_tip), None, config.max_batch_bytes)? {
        volatile.extend(batch.records);
    }
    Ok((state, volatile))
}

fn current_record(
    index: &Index,
    state: IndexState,
    current_head: &[CompactBlockRecord],
    height: u32,
) -> Result<Option<CompactBlockRecord>, IndexError> {
    if state.durable_tip().is_some_and(|durable| height <= durable.height) {
        return index.read_block(state.generation(), height).map(Some);
    }
    Ok(current_head
        .binary_search_by_key(&height, |block| block.height)
        .ok()
        .map(|position| current_head[position].clone()))
}

#[cfg(test)]
mod tests;
