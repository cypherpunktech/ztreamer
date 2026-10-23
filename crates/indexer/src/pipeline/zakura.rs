//! The production [`HistoricalSource`]: Zakura's finalized database, read with its native
//! range iterators.

use std::time::Instant;

use zakura_chain::{
    block::{self, Height},
    serialization::{CompactSizeMessage, ZcashSerialize},
};
use zakura_state::{TransactionLocation, ZakuraDb};

use super::{HistoricalSource, PipelineError, WorkerStats, flow_control::SegmentSender};
use crate::parser::parse_stored_block;

impl HistoricalSource for ZakuraDb {
    fn tip(&self) -> Option<(Height, block::Hash)> {
        ZakuraDb::tip(self)
    }

    fn block_hash(&self, height: u32) -> Option<block::Hash> {
        self.block_header(Height(height).into()).map(|header| header.hash())
    }

    fn prune_height(&self) -> u32 {
        ZakuraDb::prune_height(self).unwrap_or(Height::MIN).0
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
        let mut headers = self.block_headers_by_height_range(Height(start)..=Height(end));
        let transaction_range = TransactionLocation::min_for_height(Height(start))
            ..=TransactionLocation::max_for_height(Height(end));
        let mut transactions =
            self.raw_transactions_by_location_range(transaction_range.clone()).peekable();
        let mut transaction_hashes =
            self.transaction_hashes_by_location_range(transaction_range).peekable();

        for raw_height in start..=end {
            let height = Height(raw_height);
            let started = Instant::now();
            let header = headers.next();
            stats.header_read += started.elapsed();
            let (actual_height, header) =
                header.ok_or(PipelineError::SourceGap { expected: raw_height, actual: None })?;
            if actual_height != height {
                return Err(PipelineError::SourceGap {
                    expected: raw_height,
                    actual: Some(actual_height.0),
                });
            }

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
                let txid = transaction_hashes.next();
                stats.txid_read += started.elapsed();
                let Some((location, txid)) = txid else {
                    return Err(PipelineError::SourceChanged);
                };
                if location != transaction_location {
                    return Err(PipelineError::SourceChanged);
                }
                txids.push(txid);
            }
            if block_transactions.is_empty() {
                return Err(PipelineError::MissingBody { height: raw_height });
            }
            if transaction_hashes.peek().is_some_and(|(location, _)| location.height == height) {
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
