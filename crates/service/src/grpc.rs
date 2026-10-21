//! gRPC adapter for [`crate::CompactService`].

use std::collections::HashSet;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use ztreamer_protocol::{
    EncodedCompactBlock, proto, wire::compact_tx_streamer_server::CompactTxStreamer,
};

use crate::{CompactService, service::RpcStream};

// Bound the per-request deduplication set.
const MAX_BALANCE_STREAM_ADDRESSES: usize = 10_000;

#[tonic::async_trait]
impl CompactTxStreamer for CompactService {
    type GetBlockRangeStream = RpcStream<EncodedCompactBlock>;
    type GetBlockRangeNullifiersStream = RpcStream<EncodedCompactBlock>;
    type GetTaddressTxidsStream = RpcStream<proto::RawTransaction>;
    type GetTaddressTransactionsStream = RpcStream<proto::RawTransaction>;
    type GetMempoolTxStream = RpcStream<proto::CompactTx>;
    type GetMempoolStreamStream = RpcStream<proto::RawTransaction>;
    type GetSubtreeRootsStream = RpcStream<proto::SubtreeRoot>;
    type GetAddressUtxosStreamStream = RpcStream<proto::GetAddressUtxosReply>;

    async fn get_latest_block(
        &self,
        _request: Request<proto::ChainSpec>,
    ) -> Result<Response<proto::BlockId>, Status> {
        Ok(Response::new(self.latest_block()?))
    }

    async fn get_block(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<EncodedCompactBlock>, Status> {
        Ok(Response::new(self.encoded_block(request.into_inner(), false).await?))
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<EncodedCompactBlock>, Status> {
        Ok(Response::new(self.encoded_block(request.into_inner(), true).await?))
    }

    async fn get_block_range(
        &self,
        request: Request<proto::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        Ok(Response::new(self.encoded_range(request.into_inner(), false).await?))
    }

    async fn get_block_range_nullifiers(
        &self,
        request: Request<proto::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        Ok(Response::new(self.encoded_range(request.into_inner(), true).await?))
    }

    async fn get_transaction(
        &self,
        request: Request<proto::TxFilter>,
    ) -> Result<Response<proto::RawTransaction>, Status> {
        Ok(Response::new(self.transaction(request.into_inner()).await?))
    }

    async fn get_tree_state(
        &self,
        request: Request<proto::BlockId>,
    ) -> Result<Response<proto::TreeState>, Status> {
        Ok(Response::new(self.tree_state(request.into_inner()).await?))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::TreeState>, Status> {
        Ok(Response::new(self.latest_tree_state().await?))
    }

    async fn get_subtree_roots(
        &self,
        request: Request<proto::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        Ok(Response::new(self.subtree_roots(request.into_inner()).await?))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::LightdInfo>, Status> {
        Ok(Response::new(self.lightd_info()))
    }

    async fn ping(
        &self,
        _request: Request<proto::Duration>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        Ok(Response::new(self.ping()?))
    }

    async fn send_transaction(
        &self,
        request: Request<proto::RawTransaction>,
    ) -> Result<Response<proto::SendResponse>, Status> {
        Ok(Response::new(self.send_transaction(request.into_inner()).await?))
    }

    async fn get_taddress_txids(
        &self,
        request: Request<proto::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        Ok(Response::new(self.taddress_transactions(request.into_inner()).await?))
    }

    async fn get_taddress_transactions(
        &self,
        request: Request<proto::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        Ok(Response::new(self.taddress_transactions(request.into_inner()).await?))
    }

    async fn get_taddress_balance(
        &self,
        request: Request<proto::AddressList>,
    ) -> Result<Response<proto::Balance>, Status> {
        Ok(Response::new(self.taddress_balance(request.into_inner().addresses).await?))
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<proto::Address>>,
    ) -> Result<Response<proto::Balance>, Status> {
        let mut request = request.into_inner();
        let mut seen = HashSet::new();
        let mut value_zat = 0i64;
        while let Some(address) = request.next().await {
            let address = address?.address;
            let parsed = self.parse_address(&address)?;
            if seen.contains(&parsed) {
                continue;
            }
            if seen.len() >= MAX_BALANCE_STREAM_ADDRESSES {
                return Err(Status::resource_exhausted(format!(
                    "too many distinct addresses (limit {MAX_BALANCE_STREAM_ADDRESSES})"
                )));
            }
            seen.insert(parsed);
            let balance = self.taddress_balance_parsed(HashSet::from([parsed])).await?.value_zat;
            value_zat = value_zat
                .checked_add(balance)
                .ok_or_else(|| Status::out_of_range("transparent balance exceeds i64"))?;
        }
        Ok(Response::new(proto::Balance { value_zat }))
    }

    async fn get_mempool_tx(
        &self,
        request: Request<proto::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        Ok(Response::new(self.mempool_tx(request.into_inner()).await?))
    }

    async fn get_mempool_stream(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        Ok(Response::new(self.mempool_stream().await?))
    }

    async fn get_address_utxos(
        &self,
        request: Request<proto::GetAddressUtxosArg>,
    ) -> Result<Response<proto::GetAddressUtxosReplyList>, Status> {
        Ok(Response::new(proto::GetAddressUtxosReplyList {
            address_utxos: self.address_utxos(request.into_inner()).await?,
        }))
    }

    async fn get_address_utxos_stream(
        &self,
        request: Request<proto::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let utxos = self.address_utxos(request.into_inner()).await?;
        Ok(Response::new(Box::pin(tokio_stream::iter(utxos.into_iter().map(Ok)))))
    }
}
