//! The index's five LMDB tables, and the block lookup that spans them.
//!
//! Each table has a newtype that knows its key and value types. The newtypes do not enforce the
//! rules between the tables. [`IndexWriter`](super::writer::IndexWriter) does that. The docs of
//! the `index` module give the full table layout, the rules, and an example.

use heed::{
    Database, Env, RoIter, RoTxn, RwTxn,
    byteorder::BigEndian,
    types::{Bytes, U32},
};
use zakura_chain::block;

use super::{IndexError, IndexState, RANGE_SIZE, metadata::Metadata, range_meta::RangeMetaTable};
use crate::codec::{CodecError, CompactBlockRecord, RangeDecoder, StoredBlock};

type HeightDb = Database<U32<BigEndian>, Bytes>;
type HeightIter<'txn> = RoIter<'txn, U32<BigEndian>, Bytes>;

/// Returns the first height of the range that holds `height`.
pub(crate) const fn range_start(height: u32) -> u32 {
    height - height % RANGE_SIZE
}

/// Handles to the five databases of one LMDB environment.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Tables {
    pub(crate) metadata: Metadata,
    pub(crate) sealed_ranges: SealedRanges,
    pub(crate) mutable_blocks: MutableBlocks,
    pub(crate) hash_to_height: HashIndex,
    pub(crate) range_meta: RangeMetaTable,
}

impl Tables {
    /// Opens the five named databases, and creates the ones that do not exist.
    pub(crate) fn open(env: &Env, txn: &mut RwTxn<'_>) -> Result<Self, IndexError> {
        Ok(Self {
            metadata: Metadata::new(env.create_database(txn, Some("metadata"))?),
            sealed_ranges: SealedRanges(env.create_database(txn, Some("sealed_ranges"))?),
            mutable_blocks: MutableBlocks(env.create_database(txn, Some("mutable_blocks"))?),
            hash_to_height: HashIndex(env.create_database(txn, Some("hash_to_height"))?),
            range_meta: RangeMetaTable::new(env.create_database(txn, Some("range_meta"))?),
        })
    }

    /// Reads the block at `height` from the table that holds it under `state`.
    pub(crate) fn read<T: StoredBlock>(
        &self,
        txn: &RoTxn<'_>,
        state: IndexState,
        height: u32,
    ) -> Result<T, IndexError> {
        if state.durable_tip.is_none_or(|tip| height > tip.height) {
            return Err(IndexError::Coverage { height });
        }
        if state.sealed_through.is_some_and(|sealed| height <= sealed) {
            let range =
                self.sealed_ranges.find(txn, height)?.ok_or(IndexError::Coverage { height })?;
            return range.read(height).map_err(Into::into);
        }
        T::decode(self.mutable_blocks.get(txn, height)?.ok_or(IndexError::Coverage { height })?)
            .map_err(Into::into)
    }

    /// Returns whether any of the three block tables holds a row.
    pub(crate) fn has_blocks(&self, txn: &RoTxn<'_>) -> Result<bool, IndexError> {
        Ok(!self.sealed_ranges.is_empty(txn)?
            || !self.mutable_blocks.is_empty(txn)?
            || !self.hash_to_height.is_empty(txn)?)
    }
}

/// Packed ranges of [`RANGE_SIZE`] blocks.
///
/// - Key: the first height of the range, `u32` big-endian. It is a multiple of [`RANGE_SIZE`].
/// - Value: the records of the range, packed by `codec::write_range`, which gives the byte
///   layout.
///
/// The ranges cover the heights from 0 to `sealed_through`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SealedRanges(HeightDb);

impl SealedRanges {
    /// Finds the range that holds `height`, or `None` if no range starts at
    /// [`range_start`]`(height)`.
    pub(crate) fn find<'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        height: u32,
    ) -> Result<Option<SealedRange<'txn>>, IndexError> {
        let start = range_start(height);
        let Some(bytes) = self.get(txn, start)? else {
            return Ok(None);
        };
        Ok(Some(SealedRange { start, decoder: RangeDecoder::new(bytes)? }))
    }

    pub(crate) fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        start: u32,
    ) -> Result<Option<&'txn [u8]>, IndexError> {
        Ok(self.0.get(txn, &start)?)
    }

    pub(crate) fn iter<'txn>(&self, txn: &'txn RoTxn<'_>) -> Result<HeightIter<'txn>, IndexError> {
        Ok(self.0.iter(txn)?)
    }

    pub(crate) fn len(&self, txn: &RoTxn<'_>) -> Result<u64, IndexError> {
        Ok(self.0.len(txn)?)
    }

    pub(crate) fn is_empty(&self, txn: &RoTxn<'_>) -> Result<bool, IndexError> {
        Ok(self.0.is_empty(txn)?)
    }

    pub(crate) fn put(
        &self,
        txn: &mut RwTxn<'_>,
        start: u32,
        packed: &[u8],
    ) -> Result<(), IndexError> {
        Ok(self.0.put(txn, &start, packed)?)
    }

    pub(crate) fn delete(&self, txn: &mut RwTxn<'_>, start: u32) -> Result<bool, IndexError> {
        Ok(self.0.delete(txn, &start)?)
    }
}

/// One packed range, found by a height inside it.
pub(crate) struct SealedRange<'txn> {
    start: u32,
    decoder: RangeDecoder<'txn>,
}

impl SealedRange<'_> {
    pub(crate) const fn start(&self) -> u32 {
        self.start
    }

    pub(crate) const fn end(&self) -> u32 {
        self.start.saturating_add(RANGE_SIZE - 1)
    }

    /// Decodes the record at the absolute `height`.
    pub(crate) fn read<T: StoredBlock>(&self, height: u32) -> Result<T, CodecError> {
        let index = height.checked_sub(self.start).ok_or(CodecError::RangeIndex)?;
        self.decoder.record_as(index as usize)
    }
}

/// One encoded block for each height above the sealed ranges.
///
/// - Key: the height, `u32` big-endian.
/// - Value: one [`CompactBlockRecord::encode`] output: `version (u8) | height | hash |
///   previous hash | length | protobuf CompactBlock`.
///
/// The rows cover the heights from `sealed_through + 1` to the durable tip. A seal copies the
/// row bytes into a range without a new encode.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MutableBlocks(HeightDb);

impl MutableBlocks {
    pub(crate) fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'_>,
        height: u32,
    ) -> Result<Option<&'txn [u8]>, IndexError> {
        Ok(self.0.get(txn, &height)?)
    }

    pub(crate) fn iter<'txn>(&self, txn: &'txn RoTxn<'_>) -> Result<HeightIter<'txn>, IndexError> {
        Ok(self.0.iter(txn)?)
    }

    pub(crate) fn len(&self, txn: &RoTxn<'_>) -> Result<u64, IndexError> {
        Ok(self.0.len(txn)?)
    }

    pub(crate) fn is_empty(&self, txn: &RoTxn<'_>) -> Result<bool, IndexError> {
        Ok(self.0.is_empty(txn)?)
    }

    pub(crate) fn put(
        &self,
        txn: &mut RwTxn<'_>,
        height: u32,
        bytes: &[u8],
    ) -> Result<(), IndexError> {
        Ok(self.0.put(txn, &height, bytes)?)
    }

    pub(crate) fn delete(&self, txn: &mut RwTxn<'_>, height: u32) -> Result<bool, IndexError> {
        Ok(self.0.delete(txn, &height)?)
    }

    /// Deletes the row at each height. A height without a row is skipped.
    pub(crate) fn delete_range(
        &self,
        txn: &mut RwTxn<'_>,
        heights: impl IntoIterator<Item = u32>,
    ) -> Result<(), IndexError> {
        heights.into_iter().try_for_each(|height| self.delete(txn, height).map(drop))
    }

    /// Reads the rows of `heights` for a seal of the range at `start`. A row holds the same
    /// record bytes that a range holds, so this copies the bytes and does not encode them
    /// again. It decodes and checks each row once, with the same rules as
    /// [`CompactBlockRecord::encode`], so that a damaged row does not go into a sealed range.
    pub(crate) fn read_checked(
        &self,
        txn: &RoTxn<'_>,
        heights: std::ops::Range<u32>,
        start: u32,
    ) -> Result<Vec<Vec<u8>>, IndexError> {
        heights
            .map(|height| {
                let bytes = self.get(txn, height)?.ok_or(IndexError::IncompleteRange { start })?;
                CompactBlockRecord::decode(bytes)?.check()?;
                Ok(bytes.to_vec())
            })
            .collect()
    }
}

/// The height of each canonical block hash.
///
/// - Key: the block hash, 32 bytes.
/// - Value: the height, `u32` big-endian.
///
/// It has one entry for each block from 0 to the durable tip, sealed or mutable.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HashIndex(Database<Bytes, U32<BigEndian>>);

impl HashIndex {
    pub(crate) fn get(
        &self,
        txn: &RoTxn<'_>,
        hash: &block::Hash,
    ) -> Result<Option<u32>, IndexError> {
        Ok(self.0.get(txn, hash.0.as_slice())?)
    }

    pub(crate) fn len(&self, txn: &RoTxn<'_>) -> Result<u64, IndexError> {
        Ok(self.0.len(txn)?)
    }

    pub(crate) fn is_empty(&self, txn: &RoTxn<'_>) -> Result<bool, IndexError> {
        Ok(self.0.is_empty(txn)?)
    }

    pub(crate) fn put(
        &self,
        txn: &mut RwTxn<'_>,
        hash: &block::Hash,
        height: u32,
    ) -> Result<(), IndexError> {
        Ok(self.0.put(txn, hash.0.as_slice(), &height)?)
    }

    pub(crate) fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        hash: &block::Hash,
    ) -> Result<bool, IndexError> {
        Ok(self.0.delete(txn, hash.0.as_slice())?)
    }

    /// Checks that the hash of `record` maps to its height.
    pub(crate) fn check(
        &self,
        txn: &RoTxn<'_>,
        record: &CompactBlockRecord,
    ) -> Result<(), IndexError> {
        if self.get(txn, &record.hash)? != Some(record.height) {
            return Err(IndexError::HashIndex { height: record.height });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{codec::encode_range, index::Index, test_utils::record};

    #[test]
    fn range_start_rounds_down_to_the_range() {
        assert_eq!(range_start(0), 0);
        assert_eq!(range_start(999), 0);
        assert_eq!(range_start(1_000), 1_000);
        assert_eq!(range_start(1_999), 1_000);
        assert_eq!(range_start(u32::MAX), 4_294_967_000);
    }

    #[test]
    fn sealed_range_reads_records_by_absolute_height() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let records: Vec<_> = (1_000..2_000).map(|height| record(height, Vec::new())).collect();
        let mut txn = index.env.write_txn().unwrap();
        let sealed_ranges = index.tables.sealed_ranges;
        sealed_ranges.put(&mut txn, 1_000, &encode_range(&records).unwrap()).unwrap();

        let range = sealed_ranges.find(&txn, 1_537).unwrap().unwrap();
        assert_eq!((range.start(), range.end()), (1_000, 1_999));
        for height in [1_000, 1_537, 1_999] {
            assert_eq!(range.read::<CompactBlockRecord>(height).unwrap().height, height);
        }
        for height in [999, 2_000] {
            assert!(matches!(
                range.read::<CompactBlockRecord>(height),
                Err(CodecError::RangeIndex)
            ));
        }
        assert!(sealed_ranges.find(&txn, 2_000).unwrap().is_none());
    }
}
