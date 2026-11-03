//! [`WireFields`]: the fields of an encoded protobuf message, read without a decode.

use prost::encoding::{decode_varint, encode_varint};
use ztreamer_chain::codec::CodecError;

/// The fields of an encoded protobuf message, in their encoded order. Groups are not supported.
///
/// The iterator gives an error for a truncated or malformed field, and then stops.
pub(crate) struct WireFields<'a>(&'a [u8]);

impl<'a> WireFields<'a> {
    pub(crate) const fn new(message: &'a [u8]) -> Self {
        Self(message)
    }

    fn read(&mut self) -> Result<WireField<'a>, CodecError> {
        let message = self.0;
        let mut rest = message;
        let key = decode_varint(&mut rest).map_err(|_| CodecError::InvalidRecord)?;
        let (value, skip) = match key & 7 {
            0 => {
                decode_varint(&mut rest).map_err(|_| CodecError::InvalidRecord)?;
                (&[][..], 0)
            }
            1 => (&[][..], 8),
            2 => {
                let len = decode_varint(&mut rest).map_err(|_| CodecError::InvalidRecord)?;
                let len = usize::try_from(len).map_err(|_| CodecError::Length)?;
                (rest.get(..len).ok_or(CodecError::Truncated)?, len)
            }
            5 => (&[][..], 4),
            _ => return Err(CodecError::InvalidRecord),
        };
        rest = rest.get(skip..).ok_or(CodecError::Truncated)?;
        self.0 = rest;
        Ok(WireField { number: key >> 3, bytes: &message[..message.len() - rest.len()], value })
    }
}

impl<'a> Iterator for WireFields<'a> {
    type Item = Result<WireField<'a>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let field = self.read();
        if field.is_err() {
            self.0 = &[];
        }
        Some(field)
    }
}

/// One field of an encoded protobuf message.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WireField<'a> {
    /// The field number.
    pub(crate) number: u64,
    /// The whole field: its key and its value.
    pub(crate) bytes: &'a [u8],
    /// The value of a length-delimited field. Empty for each other wire type.
    pub(crate) value: &'a [u8],
}

impl WireField<'_> {
    /// Writes a length-delimited field with `number` and `value` at the end of `out`.
    pub(crate) fn write_len(number: u64, value: &[u8], out: &mut Vec<u8>) {
        encode_varint(number << 3 | 2, out);
        encode_varint(value.len() as u64, out);
        out.extend_from_slice(value);
    }
}
