//! Transparent address requests: transaction lookups, balances, and UTXOs.

use std::collections::HashSet;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tower::ServiceExt;
use zakura_chain::{block, transparent};
use zakura_state::{ReadRequest, ReadResponse};
use ztreamer_protocol::proto;

use super::{CompactService, RpcStream, source_status};

const MAX_UTXO_ADDRESSES: usize = 1_000;
impl CompactService {
    pub(crate) async fn taddress_transactions(
        &self,
        request: proto::TransparentAddressBlockFilter,
    ) -> Result<RpcStream<proto::RawTransaction>, Status> {
        let address = self.parse_address(&request.address)?;
        let range =
            request.range.ok_or_else(|| Status::invalid_argument("block range is required"))?;
        let tip = self.zakura_tip().await?;
        let endpoint = |endpoint: Option<proto::BlockId>| -> Result<Option<u32>, Status> {
            endpoint
                .map(|block| {
                    u32::try_from(block.height)
                        .map(|height| height.min(tip))
                        .map_err(|_| Status::invalid_argument("block height exceeds u32"))
                })
                .transpose()
        };
        let mut start = endpoint(range.start)?.unwrap_or(0);
        let mut end = endpoint(range.end)?.unwrap_or(tip);
        if end == 0 {
            end = tip;
        }
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::TransactionIdsByAddresses {
                addresses: HashSet::from([address]),
                height_range: block::Height(start)..=block::Height(end),
            })
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressesTransactionIds(transactions) = response else {
            return Err(Status::internal("unexpected address transaction response"));
        };

        let service = self.clone();
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            for hash in transactions.into_values() {
                match service.mined_transaction(hash).await {
                    Ok(Some(transaction)) => {
                        if sender.send(Ok(transaction)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = sender
                            .send(Err(Status::unavailable(
                                "Zakura address index referenced a missing transaction",
                            )))
                            .await;
                        return;
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    pub(crate) async fn taddress_balance(
        &self,
        addresses: Vec<String>,
    ) -> Result<proto::Balance, Status> {
        self.taddress_balance_parsed(self.parse_addresses(addresses)?).await
    }

    pub(crate) async fn taddress_balance_parsed(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<proto::Balance, Status> {
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::AddressBalance(addresses))
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressBalance { balance, .. } = response else {
            return Err(Status::internal("unexpected address balance response"));
        };
        Ok(proto::Balance {
            value_zat: i64::try_from(u64::from(balance))
                .map_err(|_| Status::out_of_range("transparent balance exceeds i64"))?,
        })
    }

    pub(crate) async fn address_utxos(
        &self,
        request: proto::GetAddressUtxosArg,
    ) -> Result<Vec<proto::GetAddressUtxosReply>, Status> {
        if request.addresses.len() > MAX_UTXO_ADDRESSES {
            return Err(Status::invalid_argument(format!(
                "too many addresses: {} exceeds {MAX_UTXO_ADDRESSES}",
                request.addresses.len()
            )));
        }
        let addresses = self.parse_addresses(request.addresses)?;
        let response = self
            .zakura
            .clone()
            .oneshot(ReadRequest::UtxosByAddresses(addresses))
            .await
            .map_err(source_status)?;
        let ReadResponse::AddressUtxos(utxos) = response else {
            return Err(Status::internal("unexpected address UTXO response"));
        };
        let limit = usize::try_from(request.max_entries).unwrap_or(usize::MAX);
        utxos
            .utxos()
            .filter(|(_, _, location, _)| u64::from(location.height().0) >= request.start_height)
            .take(if limit == 0 { usize::MAX } else { limit })
            .map(|(address, txid, location, output)| {
                Ok(proto::GetAddressUtxosReply {
                    address: address.to_string(),
                    txid: txid.0.to_vec(),
                    index: i32::try_from(location.output_index().index()).map_err(|_| {
                        Status::out_of_range("transparent output index exceeds i32")
                    })?,
                    script: output.lock_script.as_raw_bytes().to_vec(),
                    value_zat: i64::try_from(u64::from(output.value)).map_err(|_| {
                        Status::out_of_range("transparent output value exceeds i64")
                    })?,
                    height: u64::from(location.height().0),
                })
            })
            .collect()
    }

    fn parse_addresses(
        &self,
        addresses: impl IntoIterator<Item = String>,
    ) -> Result<HashSet<transparent::Address>, Status> {
        addresses.into_iter().map(|address| self.parse_address(&address)).collect()
    }

    pub(crate) fn parse_address(&self, address: &str) -> Result<transparent::Address, Status> {
        let address: transparent::Address = address
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid address: {error}")))?;
        Ok(address)
    }

    async fn zakura_tip(&self) -> Result<u32, Status> {
        let response =
            self.zakura.clone().oneshot(ReadRequest::Tip).await.map_err(source_status)?;
        match response {
            ReadResponse::Tip(Some((height, _))) => Ok(height.0),
            ReadResponse::Tip(None) => Err(Status::unavailable("Zakura chain is empty")),
            _ => Err(Status::internal("unexpected tip response")),
        }
    }
}
