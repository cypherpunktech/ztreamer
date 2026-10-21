//! Versioned codecs for individual compact blocks and random-access 1,000-block ranges.

use prost::{Message, bytes::Bytes};
use serde::{Deserialize, Serialize};
use ztreamer_protocol::{EncodedCompactBlock, compact_bytes, proto};

use crate::{Digest, index::RANGE_SIZE, parser::CompactTransaction};

const BLOCK_FORMAT_VERSION: u8 = 1;
const RANGE_FORMAT_VERSION: u8 = 1;
pub(crate) const MAX_RECORD_BYTES: usize = 2_000_000;

const PROTOBUF_ENVELOPE_BYTES: usize = 1 + 2 * size_of::<u32>() + 2 * size_of::<Digest>();

/// Owned record views: no LMDB transaction or mapped slice escapes a read call.
pub trait StoredBlock: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
    fn from_record(record: &CompactBlockRecord) -> Self;
    fn height(&self) -> u32;
    fn hash(&self) -> Digest;
    fn previous_hash(&self) -> Digest;
}

/// Metadata for continuity checks plus a response ready for Prost/Tonic to copy.
pub struct EncodedBlockRecord {
    pub height: u32,
    pub hash: Digest,
    pub previous_hash: Digest,
    pub block: EncodedCompactBlock,
}

/// A decoded protobuf view for requests that need pool/nullifier projection.
pub struct ProtobufBlockRecord(pub proto::CompactBlock);

impl StoredBlock for ProtobufBlockRecord {
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let (height, hash, previous_hash, payload) = protobuf_envelope(bytes)?;
        let block = proto::CompactBlock::decode(payload).map_err(|_| CodecError::InvalidRecord)?;
        if block.height != u64::from(height)
            || block.hash != hash
            || block.prev_hash != previous_hash
            || block.chain_metadata.is_none()
        {
            return Err(CodecError::InvalidRecord);
        }
        Ok(Self(block))
    }

    fn from_record(record: &CompactBlockRecord) -> Self {
        Self(record.to_proto())
    }
    fn height(&self) -> u32 {
        self.0.height as u32
    }
    fn hash(&self) -> Digest {
        self.0.hash.as_slice().try_into().expect("validated hash")
    }
    fn previous_hash(&self) -> Digest {
        self.0.prev_hash.as_slice().try_into().expect("validated previous hash")
    }
}

impl StoredBlock for EncodedBlockRecord {
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let (height, hash, previous_hash, payload) = protobuf_envelope(bytes)?;
        Ok(Self {
            height,
            hash,
            previous_hash,
            block: EncodedCompactBlock::from_encoded(Bytes::copy_from_slice(payload)),
        })
    }

    fn from_record(record: &CompactBlockRecord) -> Self {
        let mut block = record.to_proto();
        block.vtx.retain(|tx| {
            !tx.spends.is_empty()
                || !tx.outputs.is_empty()
                || !tx.actions.is_empty()
                || !tx.ironwood_actions.is_empty()
        });
        Self {
            height: record.height,
            hash: record.hash,
            previous_hash: record.previous_hash,
            block: block.into(),
        }
    }

    fn height(&self) -> u32 {
        self.height
    }
    fn hash(&self) -> Digest {
        self.hash
    }
    fn previous_hash(&self) -> Digest {
        self.previous_hash
    }
}

impl StoredBlock for CompactBlockRecord {
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Self::decode(bytes)
    }
    fn from_record(record: &Self) -> Self {
        record.clone()
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn hash(&self) -> Digest {
        self.hash
    }
    fn previous_hash(&self) -> Digest {
        self.previous_hash
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// The final index record, which gets stored in LMDB.
pub struct CompactBlockRecord {
    pub height: u32,
    pub hash: Digest,
    pub previous_hash: Digest,
    pub time: u32,
    pub transactions: Vec<CompactTransaction>,
    pub end_tree_sizes: TreeSizes,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Cumulative tree sizes per pool, by number of note commitments.
pub struct TreeSizes {
    pub sapling: u32,
    pub orchard: u32,
    pub ironwood: u32,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CodecError {
    #[error("encoded value is truncated")]
    Truncated,
    #[error("unsupported {kind} format version {version}")]
    Version { kind: &'static str, version: u8 },
    #[error("encoded count, length, or offset is invalid")]
    Length,
    #[error("encoded value has trailing bytes")]
    TrailingBytes,
    #[error("encoded block record is invalid")]
    InvalidRecord,
    #[error("range must contain exactly {RANGE_SIZE} contiguous aligned blocks")]
    InvalidRange,
    #[error("range record index is outside 0..{RANGE_SIZE}")]
    RangeIndex,
}

impl CompactBlockRecord {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut bytes = Vec::new();
        self.encode_into(&mut bytes)?;
        Ok(bytes)
    }

    /// Checks the rules that a stored record must obey and that the protobuf decode does not
    /// check: each compact transaction must contain shielded data, as emitted by the parser.
    pub(crate) fn check(&self) -> Result<(), CodecError> {
        if self.transactions.iter().any(|tx| {
            tx.sapling_spends.is_empty()
                && tx.sapling_outputs.is_empty()
                && tx.orchard_actions.is_empty()
                && tx.ironwood_actions.is_empty()
        }) {
            return Err(CodecError::InvalidRecord);
        }
        Ok(())
    }

    fn encode_into(&self, bytes: &mut Vec<u8>) -> Result<(), CodecError> {
        self.check()?;
        let block = self.to_proto();
        let len = block.encoded_len();
        if len.checked_add(PROTOBUF_ENVELOPE_BYTES).is_none_or(|len| len > MAX_RECORD_BYTES) {
            return Err(CodecError::Length);
        }
        bytes.reserve(PROTOBUF_ENVELOPE_BYTES + len);
        bytes.push(BLOCK_FORMAT_VERSION);
        put_u32(bytes, self.height);
        bytes.extend_from_slice(&self.hash);
        bytes.extend_from_slice(&self.previous_hash);
        put_u32(bytes, len as u32);
        block.encode(bytes).map_err(|_| CodecError::Length)
    }

    pub(crate) fn encoded_size_bound(
        transactions: &[CompactTransaction],
    ) -> Result<usize, CodecError> {
        // Conservative protobuf sizes, including tags/lengths and maximum varints.
        // u128 keeps count arithmetic safe before enforcing the record limit.
        let size = 192
            + transactions
                .iter()
                .map(|tx| {
                    64 + 36 * tx.sapling_spends.len() as u128
                        + 128 * tx.sapling_outputs.len() as u128
                        + 160 * tx.orchard_actions.len() as u128
                        + 160 * tx.ironwood_actions.len() as u128
                })
                .sum::<u128>();
        usize::try_from(size)
            .ok()
            .filter(|size| *size <= MAX_RECORD_BYTES)
            .ok_or(CodecError::Length)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let (height, hash, previous_hash, payload) = protobuf_envelope(bytes)?;
        // Bytes fields share this owned buffer while decoding.
        let block = compact_bytes::CompactBlock::decode(Bytes::copy_from_slice(payload))
            .map_err(|_| CodecError::InvalidRecord)?;
        let record = Self::from_proto(block)?;
        if record.height != height || record.hash != hash || record.previous_hash != previous_hash {
            return Err(CodecError::InvalidRecord);
        }
        Ok(record)
    }
}

fn protobuf_envelope(bytes: &[u8]) -> Result<(u32, Digest, Digest, &[u8]), CodecError> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(CodecError::Length);
    }
    let mut reader = Reader::new(bytes);
    if reader.u8()? != BLOCK_FORMAT_VERSION {
        return Err(CodecError::InvalidRecord);
    }
    let height = reader.u32()?;
    let hash = reader.array()?;
    let previous_hash = reader.array()?;
    let len = reader.len()?;
    let payload = reader.take(len)?;
    reader.finish().map_err(|_| CodecError::InvalidRecord)?;
    Ok((height, hash, previous_hash, payload))
}

pub fn encode_range(records: &[CompactBlockRecord]) -> Result<Vec<u8>, CodecError> {
    // Allocate only the fixed envelope. Do not read every record to calculate the size.
    write_range(records, 0)
}

/// Puts encoded block records (as stored in `mutable_blocks`) into a range. It does not decode
/// the records. It reads only the envelope of each record to check the height sequence.
pub fn encode_range_from_bytes(parts: &[&[u8]]) -> Result<Vec<u8>, CodecError> {
    let body_len = parts.iter().try_fold(0usize, |len, part| {
        len.checked_add(size_of::<u32>() + part.len()).ok_or(CodecError::Length)
    })?;
    write_range(parts, body_len)
}

/// A record that [`write_range`] can put into a range.
trait RangeRecord {
    /// The height, the hash, and the previous hash of the record.
    fn id(&self) -> Result<(u32, Digest, Digest), CodecError>;
    /// Writes the record encoding at the end of `bytes`.
    fn write_to(&self, bytes: &mut Vec<u8>) -> Result<(), CodecError>;
}

impl RangeRecord for CompactBlockRecord {
    fn id(&self) -> Result<(u32, Digest, Digest), CodecError> {
        Ok((self.height, self.hash, self.previous_hash))
    }

    fn write_to(&self, bytes: &mut Vec<u8>) -> Result<(), CodecError> {
        self.encode_into(bytes)
    }
}

/// The bytes of a `mutable_blocks` row. They are already the record encoding.
impl RangeRecord for &[u8] {
    fn id(&self) -> Result<(u32, Digest, Digest), CodecError> {
        protobuf_envelope(self)
            .map(|(height, hash, previous_hash, _)| (height, hash, previous_hash))
    }

    fn write_to(&self, bytes: &mut Vec<u8>) -> Result<(), CodecError> {
        bytes.extend_from_slice(self);
        Ok(())
    }
}

/// Checks that `records` are one aligned range with a correct hash chain. Then it writes the
/// envelope, the offset table, and each record with its length, in one pass.
///
/// An aligned range holds exactly [`RANGE_SIZE`] blocks, and its first height is a multiple of
/// [`RANGE_SIZE`]. With `RANGE_SIZE = 1000`, the range that starts at 2,000 holds heights
/// 2,000 to 2,999 and no other heights. Each block must be the child of the block before it:
///
/// ```text
/// records[0]    height 2000  previous_hash = P (the parent of the range, outside it)
/// records[1]    height 2001  previous_hash = hash of records[0]
/// ...
/// records[999]  height 2999  previous_hash = hash of records[998], hash = H
/// ```
///
/// The range then stores P and H in its envelope. A reader can check that two ranges connect
/// without decoding a record. The checks below are in this order: the count, the start
/// height, then for each next record the height + 1 and the link to the previous hash.
///
/// The output bytes (all integers are big-endian `u32`):
///
/// ```text
/// version (u8) | first height | last height | P (32 bytes) | H (32 bytes)
/// offsets: RANGE_SIZE + 1 values. Each is a position in the body, from the body start.
///          offsets[i] is where record i starts. offsets[RANGE_SIZE] is the body length.
/// body:    for each record: length | record bytes
/// ```
///
/// The function writes an offset and a length before it knows them, so it writes zeros first
/// and sets the values after it writes each record. `body_len` is only the capacity hint for
/// the body. It can be zero.
fn write_range<R: RangeRecord>(records: &[R], body_len: usize) -> Result<Vec<u8>, CodecError> {
    if records.len() != RANGE_SIZE as usize {
        return Err(CodecError::InvalidRange);
    }
    let (first_height, mut last_hash, first_previous_hash) = records[0].id()?;
    // The range must start at a range boundary: 0, 1000, 2000, ...
    if !first_height.is_multiple_of(RANGE_SIZE) {
        return Err(CodecError::InvalidRange);
    }
    let mut last_height = first_height;
    // Each record must have the next height and point to the hash of the record before it.
    for record in &records[1..] {
        let (height, hash, previous_hash) = record.id()?;
        if last_height.checked_add(1) != Some(height) || previous_hash != last_hash {
            return Err(CodecError::InvalidRange);
        }
        (last_height, last_hash) = (height, hash);
    }

    // offset table size is (number of records + 1) * 4 bytes
    let offsets_len = records
        .len()
        .checked_add(1)
        .and_then(|len| len.checked_mul(size_of::<u32>()))
        .ok_or(CodecError::Length)?;
    let capacity = 1usize
        .checked_add(2 * size_of::<u32>() + 2 * size_of::<Digest>())
        .and_then(|len| len.checked_add(offsets_len))
        .and_then(|len| len.checked_add(body_len))
        .ok_or(CodecError::Length)?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(RANGE_FORMAT_VERSION);
    put_u32(&mut bytes, first_height);
    put_u32(&mut bytes, last_height);
    bytes.extend_from_slice(&first_previous_hash);
    bytes.extend_from_slice(&last_hash);
    // reserve offset table
    let offsets_start = bytes.len();
    bytes.resize(offsets_start + offsets_len, 0);
    let body_start = bytes.len();

    // serialize directly into final buffer
    for (index, record) in records.iter().enumerate() {
        let offset = bytes.len() - body_start;
        set_u32(&mut bytes, offsets_start + index * size_of::<u32>(), offset)?;
        let length_offset = bytes.len();
        put_u32(&mut bytes, 0);
        let record_start = bytes.len();
        record.write_to(&mut bytes)?;
        // after serializing, backpatch the length field
        let record_len = bytes.len() - record_start;
        set_u32(&mut bytes, length_offset, record_len)?;
    }
    // write the 1001st offset to mark end of the body
    let final_offset = bytes.len() - body_start;
    set_u32(&mut bytes, offsets_start + records.len() * size_of::<u32>(), final_offset)?;
    Ok(bytes)
}

pub fn decode_range_record(bytes: &[u8], index: usize) -> Result<CompactBlockRecord, CodecError> {
    RangeDecoder::new(bytes)?.record(index)
}

/// Parses a range envelope once, then decodes selected records without allocating an offset table.
pub(crate) struct RangeDecoder<'a> {
    start: u32,
    first_previous_hash: Digest,
    terminal_hash: Digest,
    offsets: &'a [u8],
    body: &'a [u8],
}

impl<'a> RangeDecoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != RANGE_FORMAT_VERSION {
            return Err(CodecError::Version { kind: "range", version });
        }
        let start = reader.u32()?;
        let end = reader.u32()?;
        let first_previous_hash = reader.array::<32>()?;
        let terminal_hash = reader.array::<32>()?;
        if !start.is_multiple_of(RANGE_SIZE) || start.checked_add(RANGE_SIZE - 1) != Some(end) {
            return Err(CodecError::InvalidRange);
        }

        let offsets = reader.take((RANGE_SIZE as usize + 1) * size_of::<u32>())?;
        let body = reader.take(reader.remaining())?;
        let decoder = Self { start, first_previous_hash, terminal_hash, offsets, body };
        if decoder.offset(0) != 0
            || decoder.offset(RANGE_SIZE as usize) != body.len()
            || (1..=RANGE_SIZE as usize)
                .any(|index| decoder.offset(index - 1) > decoder.offset(index))
        {
            return Err(CodecError::Length);
        }
        Ok(decoder)
    }

    pub(crate) fn record(&self, index: usize) -> Result<CompactBlockRecord, CodecError> {
        self.record_as(index)
    }

    pub(crate) fn record_as<T: StoredBlock>(&self, index: usize) -> Result<T, CodecError> {
        if index >= RANGE_SIZE as usize {
            return Err(CodecError::RangeIndex);
        }
        let envelope =
            self.body.get(self.offset(index)..self.offset(index + 1)).ok_or(CodecError::Length)?;
        let mut envelope = Reader::new(envelope);
        let record_len = envelope.len()?;
        let record = T::decode(envelope.take(record_len)?)?;
        envelope.finish()?;
        if self.start.checked_add(index as u32) != Some(record.height())
            || (index == 0 && record.previous_hash() != self.first_previous_hash)
            || (index + 1 == RANGE_SIZE as usize && record.hash() != self.terminal_hash)
        {
            return Err(CodecError::InvalidRange);
        }
        Ok(record)
    }

    fn offset(&self, index: usize) -> usize {
        let start = index * size_of::<u32>();
        u32::from_be_bytes(
            self.offsets[start..start + size_of::<u32>()]
                .try_into()
                .expect("offset table width was checked"),
        ) as usize
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn set_u32(bytes: &mut [u8], offset: usize, value: usize) -> Result<(), CodecError> {
    bytes[offset..offset + size_of::<u32>()]
        .copy_from_slice(&u32::try_from(value).map_err(|_| CodecError::Length)?.to_be_bytes());
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Length)?;
        let value = self.bytes.get(self.offset..end).ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("length was checked"))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn len(&mut self) -> Result<usize, CodecError> {
        Ok(self.u32()? as usize)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining() == 0 { Ok(()) } else { Err(CodecError::TrailingBytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{CompactSaplingOutput, CompactShieldedAction};

    #[test]
    fn rejects_transactions_without_shielded_data() {
        let record = CompactBlockRecord {
            height: 0,
            hash: [0; 32],
            previous_hash: [0; 32],
            time: 0,
            end_tree_sizes: TreeSizes::default(),
            transactions: vec![CompactTransaction {
                index: 0,
                txid: [0; 32],
                sapling_spends: vec![],
                sapling_outputs: vec![],
                orchard_actions: vec![],
                ironwood_actions: vec![],
            }],
        };
        assert_eq!(record.encode(), Err(CodecError::InvalidRecord));
    }

    #[test]
    fn size_bound_covers_header_extremes_and_large_single_pool_transactions() {
        for count in [0, 1, 127, 128, 1024] {
            for pool in 0..4 {
                let tx = CompactTransaction {
                    index: u64::MAX,
                    txid: [0; 32],
                    sapling_spends: if pool == 0 { vec![[0; 32]; count] } else { vec![] },
                    sapling_outputs: if pool == 1 {
                        vec![
                            CompactSaplingOutput {
                                cmu: [0; 32],
                                ephemeral_key: [0; 32],
                                ciphertext: [0; 52],
                            };
                            count
                        ]
                    } else {
                        vec![]
                    },
                    orchard_actions: if pool == 2 { vec![action(0); count] } else { vec![] },
                    ironwood_actions: if pool == 3 { vec![action(0); count] } else { vec![] },
                };
                let record = CompactBlockRecord {
                    height: u32::MAX,
                    hash: [0; 32],
                    previous_hash: [0; 32],
                    time: u32::MAX,
                    transactions: if count == 0 { vec![] } else { vec![tx] },
                    end_tree_sizes: TreeSizes {
                        sapling: u32::MAX,
                        orchard: u32::MAX,
                        ironwood: u32::MAX,
                    },
                };
                let bytes = record.encode().unwrap();
                assert!(
                    bytes.len()
                        <= CompactBlockRecord::encoded_size_bound(&record.transactions).unwrap()
                );
                assert_eq!(CompactBlockRecord::decode(&bytes).unwrap(), record);
            }
        }
    }

    #[test]
    fn block_and_range_round_trip() {
        let transaction = CompactTransaction {
            index: 2,
            txid: [3; 32],
            sapling_spends: vec![[4; 32]],
            sapling_outputs: vec![CompactSaplingOutput {
                cmu: [5; 32],
                ephemeral_key: [6; 32],
                ciphertext: [7; 52],
            }],
            orchard_actions: vec![action(8)],
            ironwood_actions: vec![action(9)],
        };
        let first = CompactBlockRecord {
            height: 0,
            hash: [1; 32],
            previous_hash: [0; 32],
            time: 10,
            transactions: vec![transaction],
            end_tree_sizes: TreeSizes { sapling: 1, orchard: 1, ironwood: 1 },
        };
        let encoded_first = first.encode().unwrap();
        assert_eq!(
            EncodedBlockRecord::decode(&encoded_first).unwrap().block.encode_to_vec(),
            first.to_proto().encode_to_vec()
        );
        for len in 0..encoded_first.len() {
            assert!(CompactBlockRecord::decode(&encoded_first[..len]).is_err());
            assert!(EncodedBlockRecord::decode(&encoded_first[..len]).is_err());
        }
        let mut mismatched = encoded_first.clone();
        mismatched[1] ^= 1;
        assert_eq!(CompactBlockRecord::decode(&mismatched), Err(CodecError::InvalidRecord));
        assert!(
            encoded_first.len()
                <= CompactBlockRecord::encoded_size_bound(&first.transactions).unwrap()
        );
        assert_eq!(CompactBlockRecord::decode(&encoded_first).unwrap(), first);
        assert_eq!(
            CompactBlockRecord::decode(&[encoded_first, vec![0]].concat()),
            Err(CodecError::InvalidRecord)
        );
        assert_eq!(
            CompactBlockRecord::decode(&vec![0; MAX_RECORD_BYTES + 1]),
            Err(CodecError::Length)
        );
        assert_eq!(CompactBlockRecord::decode(&[0xff; 81]), Err(CodecError::InvalidRecord));

        let mut records = vec![first];
        for height in 1..RANGE_SIZE {
            let previous_hash = records.last().unwrap().hash;
            records.push(CompactBlockRecord {
                height,
                hash: hash(height),
                previous_hash,
                time: height,
                transactions: Vec::new(),
                end_tree_sizes: TreeSizes::default(),
            });
        }
        let encoded = encode_range(&records).unwrap();
        let range = RangeDecoder::new(&encoded).unwrap();
        for (index, expected) in records.iter().enumerate() {
            let encoded = range.record_as::<EncodedBlockRecord>(index).unwrap();
            assert_eq!(encoded.height, expected.height);
            assert_eq!(encoded.hash, expected.hash);
            assert_eq!(encoded.previous_hash, expected.previous_hash);
            let actual = encoded.block.into_decoded().unwrap();
            let expected = expected.to_proto();
            assert_eq!(actual, expected);
        }
        assert_eq!(decode_range_record(&encoded, 0).unwrap(), records[0]);
        assert_eq!(decode_range_record(&encoded, 537).unwrap(), records[537]);
        assert_eq!(decode_range_record(&encoded, 999).unwrap(), records[999]);

        // Stored record bytes give the same range.
        let stored: Vec<_> = records.iter().map(|r| r.encode().unwrap()).collect();
        let mut parts: Vec<_> = stored.iter().map(Vec::as_slice).collect();
        assert_eq!(encode_range_from_bytes(&parts).unwrap(), encoded);

        // Heights out of order.
        parts.swap(3, 4);
        assert_eq!(encode_range_from_bytes(&parts), Err(CodecError::InvalidRange));
        records.swap(3, 4);
        assert_eq!(encode_range(&records), Err(CodecError::InvalidRange));
        records.swap(3, 4);

        // Heights in order, but block 500 does not point to the hash of block 499.
        records[500].previous_hash = [0xee; 32];
        assert_eq!(encode_range(&records), Err(CodecError::InvalidRange));
        let stored: Vec<_> = records.iter().map(|r| r.encode().unwrap()).collect();
        let parts: Vec<_> = stored.iter().map(Vec::as_slice).collect();
        assert_eq!(encode_range_from_bytes(&parts), Err(CodecError::InvalidRange));
    }

    fn action(byte: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [byte; 32],
            commitment: [byte; 32],
            ephemeral_key: [byte; 32],
            ciphertext: [byte; 52],
        }
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
