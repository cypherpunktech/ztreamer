//! The install of checked sealed ranges from a snapshot or a peer.

#[cfg(any(test, feature = "test-utils"))]
use {super::tables::range_start, crate::codec::TreeSizes};

use super::{
    BlockId, CheckedRange, Index, IndexError, IndexState,
    range_meta::{RangeMeta, VerifyState},
    tables::{RangeChain, SealedRange},
    writer::IndexWriter,
};
use crate::codec::RANGE_FORMAT_VERSION;

impl Index {
    /// Sets the tip of an empty index to `tip`, with `tree_sizes`, as if sealed ranges cover
    /// `0..=tip.height`. The index holds no range. Thus it does not obey the coverage rule, and
    /// [`Index::open`] refuses it. For benches only: an install of the range after `tip` needs
    /// no ranges below it.
    ///
    /// # Panics
    ///
    /// Panics if the index has a state row or a block row, or if `tip.height` is not the last
    /// height of a range.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_sealed_tip_of_empty_index(
        &self,
        tip: BlockId,
        tree_sizes: TreeSizes,
    ) -> Result<IndexState, IndexError> {
        let mut txn = self.env.write_txn()?;
        assert_eq!(self.tables.metadata.get_state(&txn)?, IndexState::default());
        assert!(!self.tables.has_blocks(&txn)?, "the index holds blocks");
        assert_eq!(range_start(tip.height + 1), tip.height + 1, "the tip is not a range end");
        let state = IndexState {
            durable_tip: Some(tip),
            sealed_through: Some(tip.height),
            verified_through: None,
            generation: 1,
            tree_sizes,
        };
        self.tables.metadata.put_state(&mut txn, state)?;
        txn.commit()?;
        Ok(state)
    }

    /// Installs one packed range from a snapshot, and its metadata, in one write.
    ///
    /// Conditions for an install:
    ///
    /// - The index has no mutable rows.
    /// - `start` is `sealed_through + 1`, or 0 on an empty index. Thus ranges install in height
    ///   order, with no gaps.
    /// - The length and the BLAKE3 digest of the blob are `meta.entry.blob_len` and
    ///   `meta.entry.blob_digest`.
    /// - Each record of the blob decodes and links to the block below it. The first record links
    ///   to the tip, or to the genesis previous hash for range 0.
    /// - The tree sizes do not decrease from the tip to the last record.
    /// - No block hash of the blob is in the index already, and no hash occurs two times.
    /// - `meta.entry` gives the range format version, the heights, the end hash and the end tree
    ///   sizes of the blob.
    /// - `meta.state` is not [`VerifyState::Rejected`].
    ///
    /// The method does the checks on the state first. Then it checks the length and the digest.
    /// Then it decodes the blob. It calculates the digest before it starts the write
    /// transaction, because the digest does not need the index.
    ///
    /// The write adds the blob, its hash entries and `meta`. It moves the tip and
    /// `sealed_through` to the range end. A [`VerifyState::Verified`] range advances
    /// `verified_through` if the old value is `start - 1`, or not set for range 0. Other states
    /// do not change `verified_through`.
    pub fn install_sealed_range(
        &self,
        start: u32,
        blob: &[u8],
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
        let blob_check = meta.entry.check_blob(blob);
        let writer = self.start_install(start)?;
        blob_check.map_err(|mismatch| IndexError::Blob { start, mismatch })?;
        let state = writer.state();
        let chain = SealedRange::new(start, blob)?
            .check_chain(state.install_parent_hash(), state.tree_sizes)?;
        Self::finish_install(writer, start, blob, &chain, meta)
    }

    /// Installs a range that [`CheckedRange::check`] checked, and its metadata, in one write.
    ///
    /// The conditions are the conditions of [`Index::install_sealed_range`]. The
    /// [`CheckedRange`] proves the conditions that do not need the index. Thus this method does
    /// only the checks that need the index, and calculates no digest and decodes no record:
    ///
    /// - The index has no mutable rows, and the range starts at `sealed_through + 1`, or at 0 on
    ///   an empty index.
    /// - `meta.entry.blob_len` and `meta.entry.blob_digest` are the length and the digest that
    ///   the check of the range compared.
    /// - The first record links to the tip, or to the genesis previous hash for range 0, and
    ///   its tree sizes are not smaller than the tree sizes of the tip.
    /// - `meta` describes the range, and no block hash of the range is in the index already.
    ///
    /// The write is the write of [`Index::install_sealed_range`].
    pub fn install_checked_range(
        &self,
        range: &CheckedRange,
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
        let start = range.start();
        let writer = self.start_install(start)?;
        range
            .check_entry_blob(&meta.entry)
            .map_err(|mismatch| IndexError::Blob { start, mismatch })?;
        let (state, chain) = (writer.state(), range.chain());
        if !chain.links_to(state.install_parent_hash(), state.tree_sizes) {
            return Err(IndexError::RangeBlob { height: start });
        }
        Self::finish_install(writer, start, range.blob(), chain, meta)
    }

    /// Starts the write of an install at `start`. Refuses an install that is not at the next
    /// range start, and an index with mutable rows.
    fn start_install(&self, start: u32) -> Result<IndexWriter<'_>, IndexError> {
        let writer = IndexWriter::start_latest(&self.env, self.tables)?;
        let state = writer.state();
        let next = state.next_range_start()?;
        if start != next {
            return Err(IndexError::RangeOrder { start, next });
        }
        if state.has_mutable_rows() {
            return Err(IndexError::MutableRows);
        }
        Ok(writer)
    }

    /// Checks that `meta` describes the range `chain` at `start`, then writes the range, its
    /// hash entries and `meta`, and moves the tip to the range end.
    fn finish_install(
        mut writer: IndexWriter<'_>,
        start: u32,
        blob: &[u8],
        chain: &RangeChain,
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
        let last = &chain.last;
        let entry = &meta.entry;
        if entry.range_version != RANGE_FORMAT_VERSION
            || entry.start_height != start
            || entry.end_height != last.height
            || entry.end_hash != last.hash
            || entry.end_tree_sizes != last.end_tree_sizes
            || meta.state == VerifyState::Rejected
        {
            return Err(IndexError::RangeMeta { start });
        }

        writer.put_new_hashes(&chain.ids)?;
        writer.put_sealed_range(start, blob, meta)?;
        writer.set_tip(BlockId::new(last.height, last.hash), last.end_tree_sizes);
        writer.commit()
    }
}

#[cfg(test)]
mod tests;
