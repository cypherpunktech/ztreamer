//! The write batches of the index: [`WriteBatch`], the records that one write appends, and its
//! encoded form, which the writer takes.

use zakura_chain::block;
use ztreamer_chain::codec::{CodecError, CompactBlockRecord, TreeSizes};

/// An ordered batch produced by
/// [`OrderedBuilder::build_batch`](crate::ingest::OrderedBuilder::build_batch) for appending to
/// the [`Index`](super::Index).
#[derive(Debug)]
pub struct WriteBatch {
    /// Index generation the batch was built against.
    pub(crate) base_generation: u64,
    /// Blocks below and equaling this height are sealed,
    /// the rest stored in `mutable_blocks`
    pub(crate) seal_through: Option<u32>,
    pub(crate) records: Vec<CompactBlockRecord>,
}

impl WriteBatch {
    /// Encodes all records on a maximum of `threads` scoped threads. The result keeps the
    /// encoded bytes and the identity of each block, but not the decoded transactions.
    pub(crate) fn encode(self, threads: usize) -> Result<EncodedBatch, CodecError> {
        let end_tree_sizes =
            self.records.last().expect("builders never emit empty batches").end_tree_sizes;
        let encode = |records: &[CompactBlockRecord]| {
            records
                .iter()
                .map(|record| {
                    Ok(EncodedBlock {
                        height: record.height,
                        hash: record.hash,
                        previous_hash: record.previous_hash,
                        bytes: record.encode()?,
                    })
                })
                .collect::<Result<Vec<_>, CodecError>>()
        };
        let blocks = if threads <= 1 {
            encode(&self.records)?
        } else {
            // A batch is never empty (see `end_tree_sizes` above), so `chunk` is at least 1.
            let chunk = self.records.len().div_ceil(threads);
            let chunks = std::thread::scope(|scope| {
                let handles: Vec<_> = self
                    .records
                    .chunks(chunk)
                    .map(|records| scope.spawn(move || encode(records)))
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("record encoding does not panic"))
                    .collect::<Result<Vec<_>, _>>()
            })?;
            chunks.into_iter().flatten().collect()
        };
        Ok(EncodedBatch {
            base_generation: self.base_generation,
            seal_through: self.seal_through,
            blocks,
            end_tree_sizes,
        })
    }
}

/// A [`WriteBatch`] with encoded records. It does not keep the decoded transactions, so it
/// uses less memory while it waits for the writer.
pub(crate) struct EncodedBatch {
    pub(crate) base_generation: u64,
    pub(crate) seal_through: Option<u32>,
    pub(crate) blocks: Vec<EncodedBlock>,
    /// The tree sizes at the end of the last block.
    pub(crate) end_tree_sizes: TreeSizes,
}

/// One encoded block record, with the identity that the writer needs.
pub(crate) struct EncodedBlock {
    pub(crate) height: u32,
    pub(crate) hash: block::Hash,
    pub(crate) previous_hash: block::Hash,
    /// The [`CompactBlockRecord::encode`] output.
    pub(crate) bytes: Vec<u8>,
}
