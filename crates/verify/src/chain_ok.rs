//! The first step of `--serve-unverified`: a replica installs each range after a chain check
//! and serves it.
//!
//! [`ChainOkImport::run`] installs the ranges of a manifest above the sealed tip of the index,
//! in order. Each range passes [`RangeBlob::check_chain_and_end_hash`]: the blob links to the
//! range before it, and the node attests its end hash. The row has the state
//! [`VerifyState::ChainOk`]. The import does not rebuild the note commitment trees.
//!
//! Later, [`Importer::run`](crate::importer::Importer::run) proves these epochs from the blobs
//! in the index, and moves `verified_through` over them.
//!
//! [`VerifyState::ChainOk`]: ztreamer_indexer::index::range_meta::VerifyState::ChainOk

use std::{panic, sync::Arc};

use tokio::sync::watch;
use tracing::info;
use ztreamer_indexer::index::{Index, IndexError};

use crate::{
    attest::Attestor,
    fetch::{FetchError, Fetcher},
    importer::ImportError,
    manifest::Manifest,
    range::{RangeBlob, RangeLink},
};

/// What a [`ChainOkImport`] installed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ChainOkReport {
    /// The ranges installed.
    pub ranges: usize,
    /// The last height of the last range installed, or `None` when the import installed none.
    pub chain_ok_through: Option<u32>,
    /// `true` if the shutdown signal stopped the import before the end of the manifest.
    pub stopped: bool,
}

/// Installs the ranges of one source in an index after the chain check only.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use tokio::sync::watch;
/// use ztreamer_indexer::index::Index;
/// use ztreamer_verify::{
///     attest::Attestor,
///     chain_ok::{ChainOkImport, ChainOkReport},
///     fetch::Fetcher,
///     importer::ImportError,
///     manifest::Manifest,
/// };
///
/// # async fn run(
/// #     fetcher: Fetcher,
/// #     manifest: Manifest,
/// #     attestor: Arc<dyn Attestor>,
/// #     index: Arc<Index>,
/// # ) -> Result<ChainOkReport, ImportError> {
/// let (_stop, shutdown) = watch::channel(false);
/// let import = ChainOkImport::new(fetcher, manifest, attestor, index).run(shutdown);
/// tokio::spawn(import).await.unwrap()
/// # }
/// ```
pub struct ChainOkImport {
    source: Fetcher,
    manifest: Manifest,
    attestor: Arc<dyn Attestor>,
    index: Arc<Index>,
}

impl std::fmt::Debug for ChainOkImport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainOkImport")
            .field("source", &self.source)
            .field("sealed_through", &self.manifest.sealed_through())
            .finish_non_exhaustive()
    }
}

impl ChainOkImport {
    /// An import of the ranges of `manifest` from `source` into `index`. `attestor` gives the
    /// end hash of each range.
    ///
    /// The caller checks the network and the genesis block of `manifest` against the index.
    pub fn new(
        source: Fetcher,
        manifest: Manifest,
        attestor: Arc<dyn Attestor>,
        index: Arc<Index>,
    ) -> Self {
        Self { source, manifest, attestor, index }
    }

    /// Installs the ranges of the manifest above the sealed tip of the index, in ascending
    /// order, as the module docs give.
    ///
    /// It refuses an index that is sealed at or above the manifest. It fetches a blob again
    /// after a wrong digest, until the fetcher drops the source. It checks `shutdown` before
    /// each range, and stops when it is `true`. The future is `Send + 'static`. The caller
    /// spawns it.
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<ChainOkReport, ImportError> {
        let state = self.blocking(|index| index.state()).await?;
        let (sealed, tip) = (state.sealed_through(), self.manifest.sealed_through());
        if let Some(index) = sealed.filter(|&sealed| sealed >= tip) {
            return Err(ImportError::Ahead { index, manifest: tip });
        }
        // During an import, the index has no mutable rows. Thus its tip ends the sealed ranges.
        let mut previous = state.durable_tip().map_or(RangeLink::GENESIS, |tip| RangeLink {
            end_hash: tip.hash,
            end_tree_sizes: state.tree_sizes(),
        });
        let mut report = ChainOkReport::default();
        let above = |start: u32| sealed.is_none_or(|sealed| start > sealed);
        for entry in self.manifest.entries().iter().filter(|entry| above(entry.start_height)) {
            if *shutdown.borrow() {
                report.stopped = true;
                break;
            }
            let blob = loop {
                match self.source.blob(entry).await {
                    Err(FetchError::Digest { .. }) => continue,
                    result => break result?,
                }
            };
            let meta = RangeBlob::new(&blob, entry)
                .check_chain_and_end_hash(&previous, &*self.attestor)
                .await
                .map_err(|error| ImportError::in_epoch(entry.end_height, error))?;
            previous = RangeLink::from(&meta.entry);
            let start = entry.start_height;
            self.blocking(move |index| index.install_sealed_range(start, &blob, &meta)).await?;
            report.ranges += 1;
            report.chain_ok_through = Some(entry.end_height);
        }
        info!(?report, "installed ranges after the chain check");
        Ok(report)
    }

    /// Runs an index call on the blocking thread pool.
    async fn blocking<T: Send + 'static>(
        &self,
        call: impl FnOnce(&Index) -> Result<T, IndexError> + Send + 'static,
    ) -> Result<T, ImportError> {
        let index = Arc::clone(&self.index);
        Ok(tokio::task::spawn_blocking(move || call(&index))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
    use ztreamer_indexer::index::{
        RANGE_SIZE,
        range_meta::{PoolRoots, SubtreeNode, VerifyState},
    };

    use super::*;
    use crate::{
        attest::NodeError,
        fetch::{HttpSource, Source},
        manifest::Pool,
        test_utils::{FileServer, SEED, Snapshot, SourceIndex},
    };

    /// Ranges of empty blocks: the epochs end at 9,999, 19,999 and 24,999.
    const RANGES: u32 = 25;

    /// The end hashes in the rows of the source index. It has no tree roots.
    struct EndHashes(BTreeMap<u32, block::Hash>);

    #[async_trait]
    impl Attestor for EndHashes {
        async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
            self.0.get(&height).copied().ok_or(NodeError::Height { expected: height, actual: 0 })
        }

        async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
            Err(NodeError::Height { expected: height, actual: 0 })
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

    #[tokio::test]
    async fn each_range_gets_a_chain_ok_row_and_no_verified_prefix() {
        let source = SourceIndex::new(RANGES).await;
        let snapshot = Snapshot::export(&source);
        let server = FileServer::start(snapshot.dir.path()).await;
        let dir = tempfile::tempdir().unwrap();
        let target = Arc::new(SourceIndex::open(&dir));
        let key = SigningKey::from_bytes(&SEED).verifying_key();
        let http = HttpSource::new(server.base.clone(), key).unwrap();
        let ends = (0..RANGES).map(|range| {
            let meta = source.index.range_meta(range * RANGE_SIZE).unwrap().unwrap();
            (meta.entry.end_height, meta.entry.end_hash)
        });
        let attestor = Arc::new(EndHashes(ends.collect()));
        let manifest = snapshot.manifest.clone();
        let fetcher = Fetcher::new(Source::Http(Box::new(http)));
        let (_stop, shutdown) = watch::channel(false);

        let import = ChainOkImport::new(fetcher, manifest, attestor, Arc::clone(&target));
        let report = import.run(shutdown).await.unwrap();

        let sealed = snapshot.manifest.sealed_through();
        let expected = ChainOkReport { ranges: 25, chain_ok_through: Some(sealed), stopped: false };
        assert_eq!(report, expected);
        let state = target.state().unwrap();
        assert_eq!((state.sealed_through(), state.verified_through()), (Some(sealed), None));
        assert_eq!(target.range_meta(12_000).unwrap().unwrap().state, VerifyState::ChainOk);
        let blob = target.read_sealed_range_bytes(24_000).unwrap();
        assert_eq!(blob, source.index.read_sealed_range_bytes(24_000).unwrap());
    }
}
