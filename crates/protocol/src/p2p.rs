//! Message numbers, status encoding, and bounded frame reassembly for the custom p2p service.

pub const SERVICE_ID: &str = "zakura.ztreamer.v1";
pub const CAPABILITY: u64 = 1 << 17;
pub const STREAM_KIND: u16 = 65;
pub const STREAM_VERSION: u16 = 1;
pub const FRAME_FLAG_MORE: u16 = 1;
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Each request is one protobuf message. Streaming methods emit one response
/// message per item followed by `StreamEnd`; failures emit `ErrorResponse`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Message {
    GetLatestBlockRequest = 0,
    GetBlockRequest = 1,
    GetBlockNullifiersRequest = 2,
    GetBlockRangeRequest = 3,
    GetBlockRangeNullifiersRequest = 4,
    GetTransactionRequest = 5,
    GetTreeStateRequest = 6,
    GetLatestTreeStateRequest = 7,
    GetSubtreeRootsRequest = 8,
    GetLightdInfoRequest = 9,
    PingRequest = 10,
    GetLatestBlockResponse = 11,
    GetBlockResponse = 12,
    GetBlockNullifiersResponse = 13,
    GetBlockRangeResponse = 14,
    GetBlockRangeNullifiersResponse = 15,
    GetTransactionResponse = 16,
    GetTreeStateResponse = 17,
    GetLatestTreeStateResponse = 18,
    GetSubtreeRootsResponse = 19,
    GetLightdInfoResponse = 20,
    PingResponse = 21,
    StreamEnd = 22,
    ErrorResponse = 23,
}

impl Message {
    pub const fn response(self) -> Option<Self> {
        match self {
            Self::GetLatestBlockRequest => Some(Self::GetLatestBlockResponse),
            Self::GetBlockRequest => Some(Self::GetBlockResponse),
            Self::GetBlockNullifiersRequest => Some(Self::GetBlockNullifiersResponse),
            Self::GetBlockRangeRequest => Some(Self::GetBlockRangeResponse),
            Self::GetBlockRangeNullifiersRequest => Some(Self::GetBlockRangeNullifiersResponse),
            Self::GetTransactionRequest => Some(Self::GetTransactionResponse),
            Self::GetTreeStateRequest => Some(Self::GetTreeStateResponse),
            Self::GetLatestTreeStateRequest => Some(Self::GetLatestTreeStateResponse),
            Self::GetSubtreeRootsRequest => Some(Self::GetSubtreeRootsResponse),
            Self::GetLightdInfoRequest => Some(Self::GetLightdInfoResponse),
            Self::PingRequest => Some(Self::PingResponse),
            _ => None,
        }
    }
}

impl From<Message> for u16 {
    fn from(message: Message) -> Self {
        message as u16
    }
}

impl TryFrom<u16> for Message {
    type Error = String;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        const MESSAGES: [Message; 24] = [
            Message::GetLatestBlockRequest,
            Message::GetBlockRequest,
            Message::GetBlockNullifiersRequest,
            Message::GetBlockRangeRequest,
            Message::GetBlockRangeNullifiersRequest,
            Message::GetTransactionRequest,
            Message::GetTreeStateRequest,
            Message::GetLatestTreeStateRequest,
            Message::GetSubtreeRootsRequest,
            Message::GetLightdInfoRequest,
            Message::PingRequest,
            Message::GetLatestBlockResponse,
            Message::GetBlockResponse,
            Message::GetBlockNullifiersResponse,
            Message::GetBlockRangeResponse,
            Message::GetBlockRangeNullifiersResponse,
            Message::GetTransactionResponse,
            Message::GetTreeStateResponse,
            Message::GetLatestTreeStateResponse,
            Message::GetSubtreeRootsResponse,
            Message::GetLightdInfoResponse,
            Message::PingResponse,
            Message::StreamEnd,
            Message::ErrorResponse,
        ];
        MESSAGES.get(value as usize).copied().ok_or_else(|| format!("invalid message type {value}"))
    }
}

/// Protobuf payload used by [`Message::ErrorResponse`].
#[derive(Clone, PartialEq, prost::Message)]
pub struct P2pStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

#[derive(Default)]
pub struct MessageDecoder(Option<(Message, Vec<u8>)>);

impl MessageDecoder {
    pub fn push(
        &mut self,
        message_type: u16,
        flags: u16,
        bytes: Vec<u8>,
    ) -> Result<Option<(Message, Vec<u8>)>, String> {
        if flags > FRAME_FLAG_MORE {
            return Err(format!("invalid frame flags {flags}"));
        }
        let message = Message::try_from(message_type)?;
        if self.0.as_ref().is_some_and(|(current, _)| *current != message) {
            return Err("message type changed before the final frame".into());
        }
        let current_len = self.0.as_ref().map_or(0, |(_, bytes)| bytes.len());
        if bytes.len() > MAX_MESSAGE_BYTES.saturating_sub(current_len) {
            return Err(format!("message exceeds {MAX_MESSAGE_BYTES} bytes"));
        }
        let (_, payload) = self.0.get_or_insert((message, Vec::new()));
        payload.extend(bytes);
        Ok((flags == 0).then(|| self.0.take().expect("message is present")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_numbers_and_reassembly() {
        for number in 0..=23 {
            let message = Message::try_from(number).unwrap();
            assert_eq!(u16::from(message), number);
        }
        assert_eq!(Message::GetBlockRangeRequest.response(), Some(Message::GetBlockRangeResponse));

        let mut decoder = MessageDecoder::default();
        assert!(
            decoder
                .push(Message::GetBlockRequest.into(), FRAME_FLAG_MORE, vec![1, 2])
                .unwrap()
                .is_none()
        );
        assert_eq!(
            decoder.push(Message::GetBlockRequest.into(), 0, vec![3]).unwrap(),
            Some((Message::GetBlockRequest, vec![1, 2, 3]))
        );
        assert!(decoder.push(Message::GetBlockRequest.into(), 2, Vec::new()).is_err());
    }
}
