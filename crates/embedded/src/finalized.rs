//! The embedded [`HistoricalSource`]: Zakura's finalized database. It reads the transactions of
//! a segment with one range iterator, and each header and transaction hash by its key.

use std::time::Instant;

use zakura_chain::{
    block::{self, Height},
    serialization::{CompactSizeMessage, ZcashSerialize},
};
use zakura_state::{TransactionLocation, ZakuraDb};
use ztreamer_chain::parser::parse_stored_block;
use ztreamer_sync::pipeline::{HistoricalSource, PipelineError, SegmentSender, WorkerStats};

/// Zakura's finalized database as a [`HistoricalSource`]. [`NodeClient::finalized_db`] gives it.
///
/// [`NodeClient::finalized_db`]: crate::NodeClient::finalized_db
#[derive(Clone, Debug)]
pub struct FinalizedDb(pub(crate) ZakuraDb);

impl HistoricalSource for FinalizedDb {
    fn tip(&self) -> Result<Option<(Height, block::Hash)>, PipelineError> {
        Ok(ZakuraDb::tip(&self.0))
    }

    fn block_hash(&self, height: u32) -> Result<Option<block::Hash>, PipelineError> {
        Ok(self.0.block_header(Height(height).into()).map(|header| header.hash()))
    }

    fn prune_height(&self) -> Result<u32, PipelineError> {
        Ok(ZakuraDb::prune_height(&self.0).unwrap_or(Height::MIN).0)
    }

    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        max_source_bytes: usize,
        parsed_tx: &mut SegmentSender<'_>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError> {
        let transaction_range = TransactionLocation::min_for_height(Height(start))
            ..=TransactionLocation::max_for_height(Height(end));
        let mut transactions =
            self.0.raw_transactions_by_location_range(transaction_range).peekable();

        for raw_height in start..=end {
            let height = Height(raw_height);
            let started = Instant::now();
            let header = self.0.block_header(height.into());
            stats.header_read += started.elapsed();
            let header =
                header.ok_or(PipelineError::SourceGap { expected: raw_height, actual: None })?;

            let mut block_transactions = Vec::new();
            let mut txids = Vec::new();
            loop {
                let started = Instant::now();
                let transaction = transactions.next_if(|(location, _)| location.height == height);
                stats.transaction_read += started.elapsed();
                let Some((transaction_location, transaction)) = transaction else {
                    break;
                };
                block_transactions.push(transaction);

                let started = Instant::now();
                let txid = self.0.transaction_hash(transaction_location);
                stats.txid_read += started.elapsed();
                let Some(txid) = txid else {
                    return Err(PipelineError::SourceChanged);
                };
                txids.push(txid);
            }
            if block_transactions.is_empty() {
                return Err(PipelineError::MissingBody { height: raw_height });
            }
            // A hash after the last transaction means that the block changed during the read.
            let next = TransactionLocation::from_usize(height, block_transactions.len());
            if self.0.transaction_hash(next).is_some() {
                return Err(PipelineError::SourceChanged);
            }

            let hash = header.hash();
            if height.0 == end
                && expected_end_hash.is_some_and(|expected_hash| hash != expected_hash)
            {
                return Err(PipelineError::SourceChanged);
            }
            let tx_count = CompactSizeMessage::try_from(block_transactions.len())
                .expect("stored block transaction count is valid");
            let size = header
                .zcash_serialized_size()
                .checked_add(tx_count.zcash_serialized_size())
                .and_then(|size| {
                    block_transactions.iter().try_fold(size, |size, transaction| {
                        size.checked_add(transaction.raw_bytes().len())
                    })
                })
                .ok_or(PipelineError::SourceSize { height: raw_height })?;
            let block_bytes = size
                .checked_add(
                    txids
                        .len()
                        .checked_mul(32)
                        .ok_or(PipelineError::SourceSize { height: raw_height })?,
                )
                .ok_or(PipelineError::SourceSize { height: raw_height })?;
            if block_bytes > max_source_bytes {
                return Err(PipelineError::BlockExceedsByteLimit {
                    height: raw_height,
                    required: block_bytes,
                    limit: max_source_bytes,
                });
            }
            stats.read = stats.header_read + stats.transaction_read + stats.txid_read;
            stats.ranges = 1;
            stats.blocks += 1;
            stats.transactions += block_transactions.len() as u64;
            stats.bytes += size as u64;

            let started = Instant::now();
            let parsed = parse_stored_block(
                raw_height,
                hash,
                header.previous_block_hash,
                u32::try_from(header.time.timestamp()).expect("stored block time is a u32"),
                block_transactions
                    .iter()
                    .zip(txids)
                    .map(|(transaction, txid)| (transaction.raw_bytes().as_slice(), txid)),
            );
            stats.parse += started.elapsed();
            let parsed = parsed?;

            let started = Instant::now();
            let sent = parsed_tx.send(parsed);
            stats.send_wait += started.elapsed();
            sent?;
        }

        Ok(())
    }
}
