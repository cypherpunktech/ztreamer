//! Typed calls for the zakura methods that `ztreamerd` uses with a remote node.
//!
//! The request and response types come from `zakura_rpc::client`. Thus this client and the node
//! use the same code to encode and decode each value.

use std::time::Duration;

use http::Uri;
use zakura_rpc::{
    client::{
        BlockObject, GetAddressBalanceRequest, GetAddressBalanceResponse, GetAddressTxIdsRequest,
        GetAddressUtxosResponse, GetBlockHashResponse, GetBlockchainInfoResponse,
        GetRawMempoolResponse, GetRawTransactionResponse, GetSubtreesByIndexResponse,
        GetTreestateResponse, HexData, SendRawTransactionResponse,
        zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex, transaction},
    },
    methods::GetAddressUtxosRequest,
};
use zakura_state::HashOrHeight;

use crate::{RpcClient, RpcConfig, RpcError};

/// Timeouts for the methods that can be slower than the client default.
///
/// [`ZakuraClient::config`] applies them. The other methods use the default timeout of
/// [`RpcConfig`].
pub const DEFAULT_METHOD_TIMEOUTS: [(&str, Duration); 7] = [
    ("getblock", Duration::from_secs(30)),
    ("getrawtransaction", Duration::from_secs(30)),
    ("z_gettreestate", Duration::from_secs(30)),
    ("z_getsubtreesbyindex", Duration::from_secs(30)),
    ("getaddressbalance", Duration::from_secs(30)),
    ("getaddresstxids", Duration::from_secs(60)),
    ("getaddressutxos", Duration::from_secs(60)),
];

/// Typed calls to one zakura node, on an [`RpcClient`].
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
///
/// # async fn demo() -> Result<(), ztreamer_remote::RpcError> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap());
/// let node = ZakuraClient::new(RpcClient::new(config)?);
/// let info = node.get_blockchain_info().await?;
/// # let _ = info;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ZakuraClient {
    rpc: RpcClient,
}

impl ZakuraClient {
    /// A configuration for `url` with [`DEFAULT_METHOD_TIMEOUTS`]. A later
    /// [`RpcConfig::with_method_timeout`] call for the same method replaces the default.
    pub fn config(url: Uri) -> RpcConfig {
        DEFAULT_METHOD_TIMEOUTS
            .into_iter()
            .fold(RpcConfig::new(url), |config, (method, timeout)| {
                config.with_method_timeout(method, timeout)
            })
    }

    /// Wraps a client.
    pub const fn new(rpc: RpcClient) -> Self {
        Self { rpc }
    }

    /// The client below the typed calls.
    pub const fn rpc(&self) -> &RpcClient {
        &self.rpc
    }

    /// `getblockchaininfo`: the chain, the tip and the value pools.
    pub async fn get_blockchain_info(&self) -> Result<GetBlockchainInfoResponse, RpcError> {
        self.rpc.call("getblockchaininfo", ()).await
    }

    /// `getbestblockhash`: the hash of the tip.
    pub async fn get_best_block_hash(&self) -> Result<GetBlockHashResponse, RpcError> {
        self.rpc.call("getbestblockhash", ()).await
    }

    /// `getblockhash`: the hash of the best-chain block at `height`.
    pub async fn get_block_hash(
        &self,
        height: block::Height,
    ) -> Result<GetBlockHashResponse, RpcError> {
        self.rpc.call("getblockhash", (height.0,)).await
    }

    /// `getblock` with verbosity 0: the serialized block.
    pub async fn get_block_raw(&self, block: HashOrHeight) -> Result<HexData, RpcError> {
        self.rpc.call("getblock", (block.to_string(), 0)).await
    }

    /// `getblock` with verbosity 1: the block header fields, the tree roots and the
    /// transaction ids.
    pub async fn get_block(&self, block: HashOrHeight) -> Result<BlockObject, RpcError> {
        self.rpc.call("getblock", (block.to_string(), 1)).await
    }

    /// `z_gettreestate`: the note commitment tree states after `block`.
    pub async fn z_get_treestate(
        &self,
        block: HashOrHeight,
    ) -> Result<GetTreestateResponse, RpcError> {
        self.rpc.call("z_gettreestate", (block.to_string(),)).await
    }

    /// `z_getsubtreesbyindex`: the completed subtree roots of `pool` from `start`.
    ///
    /// `pool` is `sapling`, `orchard` or `ironwood`. With no `limit`, the node returns all
    /// subtrees from `start`.
    pub async fn z_get_subtrees_by_index(
        &self,
        pool: &str,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<GetSubtreesByIndexResponse, RpcError> {
        match limit {
            Some(limit) => self.rpc.call("z_getsubtreesbyindex", (pool, start, limit)).await,
            None => self.rpc.call("z_getsubtreesbyindex", (pool, start)).await,
        }
    }

    /// `getrawmempool`: the ids of the mempool transactions.
    pub async fn get_raw_mempool(&self) -> Result<GetRawMempoolResponse, RpcError> {
        self.rpc.call("getrawmempool", (false,)).await
    }

    /// `getrawtransaction`: a mempool or best-chain transaction.
    ///
    /// With `verbose` false, the node returns the serialized transaction. With `verbose` true,
    /// the node also returns the decoded fields and the block height.
    pub async fn get_raw_transaction(
        &self,
        txid: transaction::Hash,
        verbose: bool,
    ) -> Result<GetRawTransactionResponse, RpcError> {
        self.rpc.call("getrawtransaction", (txid.to_string(), u8::from(verbose))).await
    }

    /// `sendrawtransaction`: sends a serialized transaction to the mempool of the node.
    pub async fn send_raw_transaction(
        &self,
        transaction: &[u8],
    ) -> Result<SendRawTransactionResponse, RpcError> {
        self.rpc.call("sendrawtransaction", (HexData(transaction.to_vec()),)).await
    }

    /// `getaddresstxids`: the ids of the transactions that touch the addresses.
    pub async fn get_address_tx_ids(
        &self,
        request: &GetAddressTxIdsRequest,
    ) -> Result<Vec<String>, RpcError> {
        self.rpc.call("getaddresstxids", (request,)).await
    }

    /// `getaddressbalance`: the transparent balance of the addresses.
    pub async fn get_address_balance(
        &self,
        request: &GetAddressBalanceRequest,
    ) -> Result<GetAddressBalanceResponse, RpcError> {
        self.rpc.call("getaddressbalance", (request,)).await
    }

    /// `getaddressutxos`: the unspent transparent outputs of the addresses.
    pub async fn get_address_utxos(
        &self,
        request: &GetAddressUtxosRequest,
    ) -> Result<GetAddressUtxosResponse, RpcError> {
        self.rpc.call("getaddressutxos", (request,)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_applies_default_timeouts() {
        let rpc =
            RpcClient::new(ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap())).unwrap();
        for (method, timeout) in DEFAULT_METHOD_TIMEOUTS {
            assert_eq!(rpc.timeout_for(method), timeout, "{method}");
        }
        assert_eq!(rpc.timeout_for("getbestblockhash"), Duration::from_secs(15));
    }

    #[test]
    fn explicit_method_timeout_replaces_the_default() {
        let config = ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap())
            .with_method_timeout("getblock", Duration::from_secs(5));
        let rpc = RpcClient::new(config).unwrap();
        assert_eq!(rpc.timeout_for("getblock"), Duration::from_secs(5));
    }
}
