use crate::index::BlockId;
use crate::test_utils::{hash, open_index, raw_block};

use super::*;
use zakura_chain::transaction;

struct Source(Vec<RawIndexBlock>);

struct TipOnly(BlockId);

#[tonic::async_trait]
impl CanonicalBlockSource for Source {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        Ok(self.0.last().map(|block| BlockId::new(block.height.0, block.hash)))
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        Ok(self.0.get(height as usize).cloned())
    }
}

/// A source that records the height of each block read.
struct Counted {
    source: Source,
    bodies: Vec<u32>,
}

#[tonic::async_trait]
impl CanonicalBlockSource for Counted {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        self.source.tip().await
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        self.bodies.push(height);
        self.source.block(height).await
    }

    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        self.source.block_hash(height).await
    }
}

/// A source that serves `before` until its first block read, then serves `after`. It models
/// a chain that changes after the hash search and before the block reads.
struct Switching {
    before: Source,
    after: Source,
    switched: bool,
}

impl Switching {
    fn new(before: Source, after: Source) -> Self {
        Self { before, after, switched: false }
    }

    fn current(&mut self) -> &mut Source {
        if self.switched { &mut self.after } else { &mut self.before }
    }
}

#[tonic::async_trait]
impl CanonicalBlockSource for Switching {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        self.current().tip().await
    }

    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        self.switched = true;
        self.after.block(height).await
    }

    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        self.current().block_hash(height).await
    }
}

#[tonic::async_trait]
impl CanonicalBlockSource for TipOnly {
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        Ok(Some(self.0))
    }

    async fn block(&mut self, _height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        panic!("unchanged tips must not trigger block reads")
    }
}

#[test]
fn polls_a_stable_canonical_suffix() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let mut source = Source((0..=3).map(raw_block).collect());
        let blocks = poll_canonical_head(&mut source, 1, hash(0), &[]).await.unwrap().blocks;
        assert_eq!(blocks.iter().map(|block| block.height).collect::<Vec<_>>(), [1, 2, 3]);

        source.0[0].hash = block::Hash([9; 32]);
        assert!(matches!(
            poll_canonical_head(&mut source, 1, hash(0), &[]).await,
            Err(HeadError::Anchor { height: 0 })
        ));
    });
}

#[test]
fn persists_depth_ten_and_returns_the_volatile_head() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=12).map(raw_block).collect());
        let (state, volatile) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 2);
        assert_eq!(
            volatile.iter().map(|block| block.height).collect::<Vec<_>>(),
            (3..=12).collect::<Vec<_>>()
        );
    });
}

#[test]
fn unchanged_tip_skips_suffix_reads() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=20).map(raw_block).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();
        let mut source = TipOnly(BlockId::new(20, hash(20)));

        let (next_state, next_head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(next_state, state);
        assert_eq!(next_head, head);
    });
}

#[test]
fn a_tip_change_reads_the_new_blocks_only() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=150).map(raw_block).collect());
        let (_, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        let mut source =
            Counted { source: Source((0..=152).map(raw_block).collect()), bodies: Vec::new() };
        let (state, head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(source.bodies, [151, 152, 153]);
        assert_eq!(state.durable_tip().unwrap().height, 142);
        assert_eq!(
            head.iter().map(|block| block.height).collect::<Vec<_>>(),
            (143..=152).collect::<Vec<_>>()
        );
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

#[test]
fn a_reorg_in_the_window_reads_the_changed_blocks_and_replaces_the_suffix() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=150).map(raw_block).collect());
        let (_, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();
        let branch = || Source((0..=152).map(|height| raw_branch_block(height, 145)).collect());

        let mut source = Counted { source: branch(), bodies: Vec::new() };
        let (state, head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(source.bodies, (145..=153).collect::<Vec<_>>());
        // An index that reads the branch from genesis has the same records.
        let fresh_dir = tempfile::tempdir().unwrap();
        let fresh = open_index(fresh_dir.path());
        let (fresh_state, fresh_head) =
            sync_head_once(&fresh, &mut branch(), &[], PipelineConfig::default()).await.unwrap();
        assert_eq!(state.durable_tip(), fresh_state.durable_tip());
        assert_eq!(head, fresh_head);
        assert_eq!(head[2].hash, branch_hash(145));
        for height in 140..=142 {
            assert_eq!(
                index
                    .snapshot(state.generation())
                    .unwrap()
                    .block::<CompactBlockRecord>(height)
                    .unwrap(),
                fresh
                    .snapshot(fresh_state.generation())
                    .unwrap()
                    .block::<CompactBlockRecord>(height)
                    .unwrap()
            );
        }
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

#[test]
fn a_retained_block_that_leaves_the_chain_fails_the_parent_check() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=150).map(raw_block).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();
        // The search retains block 150. Then the chain forks at 148.
        let fork = || Source((0..=152).map(|height| raw_branch_block(height, 148)).collect());
        let mut source = Switching::new(Source((0..=151).map(raw_block).collect()), fork());

        let result = sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await;

        assert!(
            matches!(result, Err(HeadSyncError::Head(HeadError::Parent { height: 151 }))),
            "{result:?}"
        );
        assert_eq!(index.state().unwrap(), state);
        let (_, head) =
            sync_head_once(&index, &mut fork(), &head, PipelineConfig::default()).await.unwrap();
        assert_eq!(head.last().unwrap().hash, branch_hash(152));
    });
}

#[test]
fn replaces_only_the_volatile_head_without_rewriting_the_index() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=20).map(raw_block).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        source = Source((0..=20).map(|height| raw_branch_block(height, 15)).collect());
        let (next_state, head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(next_state, state);
        assert_eq!(head.first().unwrap().height, 11);
        assert_eq!(head[4].hash, branch_hash(15));
        assert_eq!(head.last().unwrap().hash, branch_hash(20));
    });
}

#[test]
fn rolls_the_durable_tip_back_to_a_shorter_chain() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=20).map(raw_block).collect());
        let (_, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        source = Source((0..=5).map(raw_block).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 5);
        assert!(head.is_empty());
        assert!(
            index
                .snapshot(state.generation())
                .unwrap()
                .block_by_hash::<CompactBlockRecord>(hash(6))
                .unwrap()
                .is_none()
        );
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

#[test]
fn atomically_replaces_a_durable_reorg() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=120).map(raw_block).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();
        assert_eq!(state.durable_tip().unwrap().height, 110);

        source = Source((0..=121).map(|height| raw_branch_block(height, 21)).collect());
        let (state, head) =
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await.unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 111);
        assert_eq!(
            index
                .snapshot(state.generation())
                .unwrap()
                .block::<CompactBlockRecord>(21)
                .unwrap()
                .hash,
            branch_hash(21)
        );
        assert_eq!(head.first().unwrap().height, 112);
        assert_eq!(head.last().unwrap().height, 121);
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

#[test]
fn deep_reorg_is_reported_then_recovered() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=120).map(raw_block).collect());
        let (_, head) =
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        source = Source((0..=121).map(|height| raw_branch_block(height, 20)).collect());
        assert!(matches!(
            sync_head_once(&index, &mut source, &head, PipelineConfig::default()).await,
            Err(HeadSyncError::DeepReorg { height: 20 })
        ));
        let (state, head) =
            recover_deep_reorg(&index, &mut source, &head, PipelineConfig::default())
                .await
                .unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 111);
        assert_eq!(
            index
                .snapshot(state.generation())
                .unwrap()
                .block::<CompactBlockRecord>(20)
                .unwrap()
                .hash,
            branch_hash(20)
        );
        assert_eq!(head.first().unwrap().height, 112);
        assert_eq!(head.last().unwrap().height, 121);
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

#[test]
fn restart_recovers_a_persisted_noncanonical_suffix() {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let mut source = Source((0..=120).map(raw_block).collect());
        sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();

        let mut source = Source((0..=105).map(|height| raw_branch_block(height, 100)).collect());
        assert!(matches!(
            sync_head_once(&index, &mut source, &[], PipelineConfig::default()).await,
            Err(HeadSyncError::DeepReorg { height: 110 })
        ));

        let (state, head) =
            recover_deep_reorg(&index, &mut source, &[], PipelineConfig::default()).await.unwrap();
        assert_eq!(state.durable_tip().unwrap().height, 99);
        assert_eq!(
            index
                .snapshot(state.generation())
                .unwrap()
                .block::<CompactBlockRecord>(99)
                .unwrap()
                .hash,
            hash(99)
        );
        assert_eq!(head.first().unwrap().height, 100);
        assert_eq!(head.first().unwrap().hash, branch_hash(100));
        assert_eq!(head.last().unwrap().height, 105);
        index.latest().unwrap().verify_continuity().unwrap();
    });
}

fn raw_branch_block(height: u32, fork: u32) -> RawIndexBlock {
    let mut block = raw_block(height);
    if height >= fork {
        block.hash = branch_hash(height);
        block.txids[0] = transaction::Hash(branch_hash(height).0);
        block.bytes[4..36].copy_from_slice(
            &if height == fork { hash(height - 1) } else { branch_hash(height - 1) }.0,
        );
    }
    block
}

fn branch_hash(height: u32) -> block::Hash {
    let mut hash = hash(height);
    hash.0[31] = 1;
    hash
}

#[test]
fn a_fetch_error_keeps_its_cause_in_the_head_error() {
    use crate::blocking::BlockingTaskCancelled;

    let cancelled = HeadError::from(FetchAtHeightError::Cancelled(BlockingTaskCancelled));
    assert!(matches!(cancelled, HeadError::Cancelled(BlockingTaskCancelled)));
    let height = BlockAtHeightError::Height { expected: 3, actual: 4 };
    let height = HeadError::from(FetchAtHeightError::Block(height));
    assert!(matches!(height, HeadError::Height { expected: 3, actual: 4 }));
}
