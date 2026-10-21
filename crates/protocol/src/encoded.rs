//! An ordinary Prost message whose common encoding path is a contiguous copy.

use prost::{
    DecodeError, Message,
    bytes::{Buf, BufMut, Bytes},
    encoding::{DecodeContext, WireType},
};

use crate::proto;

#[derive(Clone, Debug, PartialEq)]
pub struct EncodedCompactBlock(Repr);

#[derive(Clone, Debug, PartialEq)]
enum Repr {
    Encoded(Bytes),
    Decoded(proto::CompactBlock),
}

impl EncodedCompactBlock {
    /// Wraps a trusted, complete protobuf payload, without a length prefix.
    /// Callers must obtain it from an encoder or a validated index record.
    pub const fn from_encoded(bytes: Bytes) -> Self {
        Self(Repr::Encoded(bytes))
    }

    pub fn into_decoded(self) -> Result<proto::CompactBlock, DecodeError> {
        match self.0 {
            Repr::Encoded(bytes) => proto::CompactBlock::decode(bytes),
            Repr::Decoded(block) => Ok(block),
        }
    }
}

impl From<proto::CompactBlock> for EncodedCompactBlock {
    fn from(block: proto::CompactBlock) -> Self {
        Self::from_encoded(block.encode_to_vec().into())
    }
}

impl Default for EncodedCompactBlock {
    fn default() -> Self {
        Self(Repr::Decoded(proto::CompactBlock::default()))
    }
}

impl Message for EncodedCompactBlock {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match &self.0 {
            Repr::Encoded(bytes) => buf.put_slice(bytes),
            Repr::Decoded(block) => block.encode_raw(buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match &self.0 {
            Repr::Encoded(bytes) => bytes.len(),
            Repr::Decoded(block) => block.encoded_len(),
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        if let Repr::Encoded(bytes) = &self.0 {
            self.0 = Repr::Decoded(proto::CompactBlock::decode(bytes.clone())?);
        }
        let Repr::Decoded(block) = &mut self.0 else { unreachable!() };
        block.merge_field(tag, wire_type, buf, ctx)
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_prost_encoding_decoding_and_merging() {
        let block = proto::CompactBlock { height: 123, hash: vec![4; 32], ..Default::default() };
        let bytes = block.encode_to_vec();
        let mut encoded = EncodedCompactBlock::from(block.clone());
        assert_eq!(encoded.encoded_len(), bytes.len());
        assert_eq!(encoded.encode_to_vec(), bytes);
        assert_eq!(
            EncodedCompactBlock::decode(bytes.as_slice()).unwrap().into_decoded().unwrap(),
            block
        );
        let delta = proto::CompactBlock { time: 99, ..Default::default() };
        encoded.merge(delta.encode_to_vec().as_slice()).unwrap();
        assert_eq!(encoded.clone().into_decoded().unwrap().time, 99);
        encoded.clear();
        assert_eq!(encoded.encoded_len(), 0);
    }
}
