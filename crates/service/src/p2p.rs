//! Zakura custom-service transport for [`crate::CompactService`].

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
};

use prost::Message as _;
use tokio_stream::StreamExt as _;
use tonic::Status;
use zakura_network::zakura::{
    CustomService, Frame, FramedSend, LOCAL_MAX_CONTROL_FRAME_BYTES, Peer, Service, Stream,
    ZakuraConnId, ZakuraPeerId, ZakuraServiceId,
};
use ztreamer_protocol::p2p::MessageDecoder;
pub use ztreamer_protocol::p2p::{
    CAPABILITY, FRAME_FLAG_MORE, MAX_MESSAGE_BYTES, Message, P2pStatus, SERVICE_ID, STREAM_KIND,
    STREAM_VERSION,
};

use crate::CompactService;
use ztreamer_protocol::proto;

const FRAME_PAYLOAD_BYTES: usize = LOCAL_MAX_CONTROL_FRAME_BYTES as usize - 8;
const STREAMS: [Stream; 1] = [Stream {
    kind: STREAM_KIND,
    version: STREAM_VERSION,
    frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
    capability: CAPABILITY,
    ..Stream::PERSISTENT
}];

#[derive(Clone)]
pub struct P2pCompactService {
    compact: Arc<OnceLock<CompactService>>,
    active_peers: Arc<Mutex<HashSet<(ZakuraPeerId, ZakuraConnId)>>>,
}

impl std::fmt::Debug for P2pCompactService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("P2pCompactService")
            .field("ready", &self.compact.get().is_some())
            .field("active_peers", &self.active_peers)
            .finish()
    }
}

impl P2pCompactService {
    pub fn new(compact: CompactService) -> Self {
        let service = Self::pending();
        service.compact.set(compact).unwrap_or_else(|_| unreachable!("new OnceLock is empty"));
        service
    }

    /// Creates a frontend that can be registered before embedded Zakura starts.
    pub fn pending() -> Self {
        Self { compact: Arc::default(), active_peers: Arc::default() }
    }

    /// Attaches the backend after Zakura exposes its `ReadStateService`.
    pub fn install(&self, compact: CompactService) -> Result<(), &'static str> {
        self.compact.set(compact).map_err(|_| "compact service is already installed")
    }

    pub fn registration(self: &Arc<Self>) -> CustomService {
        CustomService {
            service: self.clone(),
            provides: vec![ZakuraServiceId::new(SERVICE_ID).expect("static service id is valid")],
            seeks: Vec::new(),
        }
    }

    async fn dispatch(
        &self,
        message: Message,
        payload: &[u8],
        send: &FramedSend,
    ) -> Result<(), ()> {
        let Some(compact) = self.compact.get() else {
            return send_status(send, Status::unavailable("compact service is starting")).await;
        };
        macro_rules! unary {
            ($request:ty, $name:ident => $result:expr, $response:expr) => {{
                let $name = decode::<$request>(payload)?;
                match $result {
                    Ok(response) => send_message(send, $response, &response).await,
                    Err(status) => send_status(send, status).await,
                }
            }};
        }
        macro_rules! stream {
            ($request:ty, $name:ident => $result:expr, $response:expr) => {{
                let $name = decode::<$request>(payload)?;
                let mut responses = match $result {
                    Ok(response) => response,
                    Err(status) => return send_status(send, status).await,
                };
                while let Some(response) = responses.next().await {
                    match response {
                        Ok(response) => send_message(send, $response, &response).await?,
                        Err(status) => return send_status(send, status).await,
                    }
                }
                send_bytes(send, Message::StreamEnd, Vec::new()).await
            }};
        }

        match message {
            Message::GetLatestBlockRequest => unary!(
                proto::ChainSpec,
                _request => compact.latest_block(),
                Message::GetLatestBlockResponse
            ),
            Message::GetBlockRequest => unary!(
                proto::BlockId,
                request => compact.encoded_block(request, false).await,
                Message::GetBlockResponse
            ),
            Message::GetBlockNullifiersRequest => unary!(
                proto::BlockId,
                request => compact.encoded_block(request, true).await,
                Message::GetBlockNullifiersResponse
            ),
            Message::GetBlockRangeRequest => stream!(
                proto::BlockRange,
                request => compact.encoded_range(request, false).await,
                Message::GetBlockRangeResponse
            ),
            Message::GetBlockRangeNullifiersRequest => stream!(
                proto::BlockRange,
                request => compact.encoded_range(request, true).await,
                Message::GetBlockRangeNullifiersResponse
            ),
            Message::GetTransactionRequest => unary!(
                proto::TxFilter,
                request => compact.transaction(request).await,
                Message::GetTransactionResponse
            ),
            Message::GetTreeStateRequest => unary!(
                proto::BlockId,
                request => compact.tree_state(request).await,
                Message::GetTreeStateResponse
            ),
            Message::GetLatestTreeStateRequest => unary!(
                proto::Empty,
                _request => compact.latest_tree_state().await,
                Message::GetLatestTreeStateResponse
            ),
            Message::GetSubtreeRootsRequest => stream!(
                proto::GetSubtreeRootsArg,
                request => compact.subtree_roots(request).await,
                Message::GetSubtreeRootsResponse
            ),
            Message::GetLightdInfoRequest => unary!(
                proto::Empty,
                _request => Ok::<_, Status>(compact.lightd_info()),
                Message::GetLightdInfoResponse
            ),
            Message::PingRequest => unary!(
                proto::Duration,
                _request => compact.ping(),
                Message::PingResponse
            ),
            _ => Err(()),
        }
    }

    fn remove(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        if let Ok(mut peers) = self.active_peers.lock() {
            peers.remove(&(peer.clone(), conn_id));
        }
    }
}

impl Service for P2pCompactService {
    fn name(&self) -> &'static str {
        "ztreamer"
    }

    fn streams(&self) -> &[Stream] {
        &STREAMS
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.active_peers.lock().is_ok_and(|peers| peers.contains(&(peer.clone(), conn_id)))
    }

    fn add_peer(&self, mut peer: Peer) {
        let peer_id = peer.id.clone();
        let conn_id = peer.conn_id;
        let connection_cancel = peer.cancel_token();
        let service_cancel = peer.service_cancel_token();
        let Some((mut recv, send)) = peer.take_stream(STREAM_KIND) else {
            return;
        };
        if let Ok(mut peers) = self.active_peers.lock() {
            peers.insert((peer_id.clone(), conn_id));
        }

        let service = self.clone();
        tokio::spawn(async move {
            let mut decoder = MessageDecoder::default();
            loop {
                let frame = tokio::select! {
                    _ = service_cancel.cancelled() => break,
                    frame = recv.recv() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };
                let request = match decoder.push(frame.message_type, frame.flags, frame.payload) {
                    Ok(Some(request)) => request,
                    Ok(None) => continue,
                    Err(_) => {
                        connection_cancel.cancel();
                        break;
                    }
                };
                if request.0.response().is_none()
                    || service.dispatch(request.0, &request.1, &send).await.is_err()
                {
                    connection_cancel.cancel();
                    break;
                }
            }
            service.remove(&peer_id, conn_id);
        });
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.remove(peer, conn_id);
    }
}

/// Wraps the p2p frontend for `zakurad::node::run_with_services_ready`.
pub fn custom_service(compact: CompactService) -> CustomService {
    Arc::new(P2pCompactService::new(compact)).registration()
}

fn decode<T: prost::Message + Default>(payload: &[u8]) -> Result<T, ()> {
    T::decode(payload).map_err(|_| ())
}

async fn send_message(
    send: &FramedSend,
    message: Message,
    value: &impl prost::Message,
) -> Result<(), ()> {
    send_bytes(send, message, value.encode_to_vec()).await
}

async fn send_status(send: &FramedSend, status: Status) -> Result<(), ()> {
    send_message(
        send,
        Message::ErrorResponse,
        &P2pStatus { code: status.code() as i32, message: status.message().to_owned() },
    )
    .await
}

async fn send_bytes(send: &FramedSend, message: Message, payload: Vec<u8>) -> Result<(), ()> {
    let (message, payload) = if payload.len() > MAX_MESSAGE_BYTES {
        (
            Message::ErrorResponse,
            P2pStatus {
                code: tonic::Code::ResourceExhausted as i32,
                message: format!("response exceeds {MAX_MESSAGE_BYTES} bytes"),
            }
            .encode_to_vec(),
        )
    } else {
        (message, payload)
    };
    let mut chunks = payload.chunks(FRAME_PAYLOAD_BYTES).peekable();
    if chunks.peek().is_none() {
        return send
            .send(Frame { message_type: message.into(), flags: 0, payload })
            .await
            .map_err(|_| ());
    }
    while let Some(chunk) = chunks.next() {
        send.send(Frame {
            message_type: message.into(),
            flags: if chunks.peek().is_some() { FRAME_FLAG_MORE } else { 0 },
            payload: chunk.to_vec(),
        })
        .await
        .map_err(|_| ())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{block, parameters::Network};
    use zakura_network::zakura::framed_channel;
    use zakura_state::Config;
    use ztreamer_indexer::{
        codec::{CompactBlockRecord, TreeSizes},
        index::{Index, IndexState},
    };

    #[tokio::test]
    async fn forwards_streaming_compact_ranges() {
        let directory = tempfile::tempdir().unwrap();
        let index = Arc::new(
            Index::open(directory.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([9; 32]))
                .unwrap(),
        );
        let (_state, read_state, _tip, _change) =
            zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                .await
                .expect("ephemeral state initializes");
        let compact = CompactService::new(index, IndexState::default(), "main", read_state);
        compact
            .publish_head(
                IndexState::default(),
                vec![
                    record(0, block::Hash([1; 32]), block::Hash([0; 32])),
                    record(1, block::Hash([2; 32]), block::Hash([1; 32])),
                ],
            )
            .unwrap();
        let service = Arc::new(P2pCompactService::pending());
        let registration = service.registration();
        assert_eq!(registration.provides[0].as_str(), SERVICE_ID);
        assert!(service.install(compact).is_ok());
        let (send, mut recv) = framed_channel(3);
        let request = proto::BlockRange {
            start: Some(proto::BlockId { height: 0, hash: Vec::new() }),
            end: Some(proto::BlockId { height: 1, hash: Vec::new() }),
            pool_types: Vec::new(),
        };

        service
            .dispatch(Message::GetBlockRangeRequest, &request.encode_to_vec(), &send)
            .await
            .unwrap();

        for height in 0..=1 {
            let frame = recv.recv().await.unwrap();
            assert_eq!(frame.message_type, u16::from(Message::GetBlockRangeResponse));
            assert_eq!(frame.flags, 0);
            assert_eq!(
                proto::CompactBlock::decode(frame.payload.as_slice()).unwrap().height,
                height
            );
        }
        let end = recv.recv().await.unwrap();
        assert_eq!(end.message_type, u16::from(Message::StreamEnd));
        assert!(end.payload.is_empty());
    }

    fn record(height: u32, hash: block::Hash, previous_hash: block::Hash) -> CompactBlockRecord {
        CompactBlockRecord {
            height,
            hash,
            previous_hash,
            time: height,
            transactions: Vec::new(),
            end_tree_sizes: TreeSizes::default(),
        }
    }
}
