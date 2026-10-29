//! [`RangeCursor`]: the chunked read of one block-range stream.

use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;
use zakura_chain::block;
use ztreamer_indexer::{
    codec::StoredBlock,
    index::{Index, IndexError},
};

use super::{ServingSnapshot, index_status, reorganized};

/// The number of blocks that one chunk of a range stream reads.
const CHUNK_BLOCKS: u32 = 64;

/// The records of one range stream, read in chunks of [`CHUNK_BLOCKS`] blocks.
///
/// A chunk is all durable or all volatile. A durable chunk reads the latest generation of the
/// index, in one LMDB transaction that ends before the cursor gives the first record of the
/// chunk. A volatile chunk reads the volatile head of the serving snapshot. The index can commit
/// between two chunks. Thus each chunk must link to the last record of the chunk before it, else
/// the cursor gives a reorg error.
///
/// After an error the cursor gives no more records. The cursor holds the range stream permit
/// until it is dropped.
pub(crate) struct RangeCursor<T> {
    index: Arc<Index>,
    snapshot: ServingSnapshot,
    end: u32,
    ascending: bool,
    /// The durable tip height of the serving snapshot.
    durable_tip: Option<u32>,
    /// The first height of the next chunk, or `None` after the last chunk or an error.
    next: Option<u32>,
    /// (hash, previous_hash) of the last record of the last chunk.
    last: Option<(block::Hash, block::Hash)>,
    /// The records of the current chunk that the cursor did not give yet.
    records: std::vec::IntoIter<T>,
    _permit: OwnedSemaphorePermit,
}

impl<T: StoredBlock> RangeCursor<T> {
    /// A cursor over the heights from `start` to `end`, both included. `end` below `start` reads
    /// in descending order. The caller checked the range against `snapshot`.
    pub(crate) fn new(
        index: Arc<Index>,
        snapshot: ServingSnapshot,
        start: u32,
        end: u32,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            index,
            end,
            ascending: start <= end,
            durable_tip: snapshot.durable_tip.map(|tip| tip.height),
            snapshot,
            next: Some(start),
            last: None,
            records: Vec::new().into_iter(),
            _permit: permit,
        }
    }

    /// The last height of the chunk that starts at `height`, and whether the chunk is durable. A
    /// chunk ends at the durable tip or at the first height above it, so that it does not hold
    /// durable and volatile blocks together.
    fn chunk_bounds(&self, height: u32) -> (u32, bool) {
        let mut chunk_end = if self.ascending {
            height.saturating_add(CHUNK_BLOCKS - 1).min(self.end)
        } else {
            height.saturating_sub(CHUNK_BLOCKS - 1).max(self.end)
        };
        let durable = self.durable_tip.is_some_and(|tip| height <= tip);
        if let Some(tip) = self.durable_tip {
            if self.ascending && durable {
                chunk_end = chunk_end.min(tip);
            } else if !self.ascending && !durable {
                chunk_end = chunk_end.max(tip.saturating_add(1));
            }
        }
        (chunk_end, durable)
    }

    /// Reads the durable chunk from `height` to `chunk_end` from the latest index generation.
    fn read_durable(&self, height: u32, chunk_end: u32) -> Result<Vec<T>, Status> {
        let mut chunk = Vec::with_capacity(CHUNK_BLOCKS as usize);
        self.index
            .latest()
            .and_then(|index_snapshot| {
                index_snapshot.read_blocks(height, chunk_end, |record| {
                    chunk.push(record);
                    true
                })
            })
            .map_err(|error| match error {
                // Validation saw this height; if the index no longer has it, the chain shrank.
                IndexError::Coverage { height } => reorganized(height),
                other => index_status(other),
            })?;
        Ok(chunk)
    }

    /// Reads the volatile chunk from `height` to `chunk_end` from the serving snapshot.
    fn read_volatile(&self, height: u32, chunk_end: u32) -> Result<Vec<T>, Status> {
        (0..=height.abs_diff(chunk_end))
            .map(|offset| {
                let height = if self.ascending { height + offset } else { height - offset };
                self.snapshot
                    .volatile(height)
                    .map(T::from_record)
                    .ok_or_else(|| index_status(IndexError::Coverage { height }))
            })
            .collect()
    }

    /// Reads the next chunk, checks that it links to the last chunk, and moves the cursor past
    /// it. Returns `None` after the last chunk or an error.
    fn next_chunk(&mut self) -> Option<Result<Vec<T>, Status>> {
        let height = self.next?;
        let (chunk_end, durable) = self.chunk_bounds(height);
        let read = if durable {
            self.read_durable(height, chunk_end)
        } else {
            self.read_volatile(height, chunk_end)
        };
        let chunk = read.and_then(|chunk| self.check_link(chunk));
        match &chunk {
            Ok(records) => {
                self.last = records.last().map(|record| (record.hash(), record.previous_hash()));
                self.next = (chunk_end != self.end).then_some(if self.ascending {
                    chunk_end + 1
                } else {
                    chunk_end - 1
                });
            }
            Err(_) => self.next = None,
        }
        Some(chunk)
    }

    /// Refuses a chunk whose first record does not link to the last record of the chunk before.
    fn check_link(&self, chunk: Vec<T>) -> Result<Vec<T>, Status> {
        let (Some((last_hash, last_previous)), Some(first)) = (self.last, chunk.first()) else {
            return Ok(chunk);
        };
        let linked = if self.ascending {
            first.previous_hash() == last_hash
        } else {
            last_previous == first.hash()
        };
        if linked { Ok(chunk) } else { Err(reorganized(first.height())) }
    }
}

impl<T: StoredBlock> Iterator for RangeCursor<T> {
    type Item = Result<T, Status>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(record) = self.records.next() {
                return Some(Ok(record));
            }
            match self.next_chunk()? {
                Ok(chunk) => self.records = chunk.into_iter(),
                Err(status) => return Some(Err(status)),
            }
        }
    }
}
