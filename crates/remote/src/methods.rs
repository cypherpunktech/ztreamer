//! Typed calls for the zakura methods that `ztreamerd` uses with a remote node.
//!
//! The request and response types come from `zakura_rpc::client`. Thus this client and the node
//! use the same code to encode and decode each value.

use std::{fmt, time::Duration};

use http::Uri;
use serde::{Serialize, de::DeserializeOwned};
use zakura_rpc::{
    client::{
        GetAddressBalanceRequest, GetAddressBalanceResponse, GetAddressTxIdsRequest,
        GetAddressUtxosResponse, GetBlockHashResponse, GetBlockchainInfoResponse,
        GetRawMempoolResponse, GetRawTransactionResponse, GetSubtreesByIndexResponse,
        GetTreestateResponse, HexData, SendRawTransactionResponse,
        zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex, transaction},
    },
    methods::GetAddressUtxosRequest,
};
use zakura_state::HashOrHeight;

use crate::{RpcClient, RpcConfig, RpcError, client::Retries};

/// A zakura JSON-RPC method that [`ZakuraClient`] calls.
///
/// [`Self::name`] is the name on the wire. [`ZakuraClient`], its default timeouts and the mock
/// chain in `test_utils` use this type, thus each name has one spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    /// `getblockchaininfo`.
    GetBlockchainInfo,
    /// `getblockcount`.
    GetBlockCount,
    /// `getblockhash`.
    GetBlockHash,
    /// `getblock`.
    GetBlock,
    /// `z_gettreestate`.
    ZGetTreestate,
    /// `z_getsubtreesbyindex`.
    ZGetSubtreesByIndex,
    /// `getrawmempool`.
    GetRawMempool,
    /// `getrawtransaction`.
    GetRawTransaction,
    /// `sendrawtransaction`.
    SendRawTransaction,
    /// `getaddresstxids`.
    GetAddressTxIds,
    /// `getaddressbalance`.
    GetAddressBalance,
    /// `getaddressutxos`.
    GetAddressUtxos,
}

impl Method {
    /// The name of the method on the wire.
    pub const fn name(self) -> &'static str {
        match self {
            Self::GetBlockchainInfo => "getblockchaininfo",
            Self::GetBlockCount => "getblockcount",
            Self::GetBlockHash => "getblockhash",
            Self::GetBlock => "getblock",
            Self::ZGetTreestate => "z_gettreestate",
            Self::ZGetSubtreesByIndex => "z_getsubtreesbyindex",
            Self::GetRawMempool => "getrawmempool",
            Self::GetRawTransaction => "getrawtransaction",
            Self::SendRawTransaction => "sendrawtransaction",
            Self::GetAddressTxIds => "getaddresstxids",
            Self::GetAddressBalance => "getaddressbalance",
            Self::GetAddressUtxos => "getaddressutxos",
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Timeouts for the methods that can be slower than the client default.
///
/// [`ZakuraClient::config`] applies them. The other methods use the default timeout of
/// [`RpcConfig`].
const DEFAULT_METHOD_TIMEOUTS: [(Method, Duration); 7] = [
    (Method::GetBlock, Duration::from_secs(30)),
    (Method::GetRawTransaction, Duration::from_secs(30)),
    (Method::ZGetTreestate, Duration::from_secs(30)),
    (Method::ZGetSubtreesByIndex, Duration::from_secs(30)),
    (Method::GetAddressBalance, Duration::from_secs(30)),
    (Method::GetAddressTxIds, Duration::from_secs(60)),
    (Method::GetAddressUtxos, Duration::from_secs(60)),
];

/// The lowest height whose block body a node keeps, from its `getblockchaininfo` answer.
///
/// # Implementation guide
///
/// Only [`GetBlockchainInfoResponse`] implements it. A node that prunes keeps the bodies from
/// its `pruneheight`. A node that does not prune keeps all bodies, from genesis.
pub trait FirstBlockWithBody {
    /// The lowest height whose block body the node keeps.
    fn first_block_with_body(&self) -> block::Height;
}

impl FirstBlockWithBody for GetBlockchainInfoResponse {
    fn first_block_with_body(&self) -> block::Height {
        self.prune_height().unwrap_or(block::Height::MIN)
    }
}

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
    /// A configuration for `url` with a longer timeout for each slow method: 60 s for the
    /// address lists, 30 s for blocks, transactions, tree states, subtrees and balances. A later
    /// [`RpcConfig::with_method_timeout`] call for the same method replaces the default.
    pub fn config(url: Uri) -> RpcConfig {
        DEFAULT_METHOD_TIMEOUTS
            .into_iter()
            .fold(RpcConfig::new(url), |config, (method, timeout)| {
                config.with_method_timeout(method.name(), timeout)
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

    async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        params: P,
    ) -> Result<R, RpcError> {
        self.rpc.call(method.name(), params).await
    }

    /// `getblockchaininfo`: the chain, the tip and the value pools.
    pub async fn get_blockchain_info(&self) -> Result<GetBlockchainInfoResponse, RpcError> {
        self.call(Method::GetBlockchainInfo, ()).await
    }

    /// `getblockcount`: the height of the tip. This is the cheapest call that gives the tip.
    pub async fn get_block_count(&self) -> Result<block::Height, RpcError> {
        self.call(Method::GetBlockCount, ()).await.map(block::Height)
    }

    /// `getblockhash`: the hash of the best-chain block at `height`.
    pub async fn get_block_hash(
        &self,
        height: block::Height,
    ) -> Result<GetBlockHashResponse, RpcError> {
        self.call(Method::GetBlockHash, (height.0,)).await
    }

    /// `getblock` with verbosity 0: the serialized block.
    pub async fn get_block_raw(&self, block: HashOrHeight) -> Result<HexData, RpcError> {
        self.call(Method::GetBlock, (block.to_string(), 0)).await
    }

    /// `z_gettreestate`: the note commitment tree states after `block`.
    pub async fn z_get_treestate(
        &self,
        block: HashOrHeight,
    ) -> Result<GetTreestateResponse, RpcError> {
        self.call(Method::ZGetTreestate, (block.to_string(),)).await
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
            Some(limit) => self.call(Method::ZGetSubtreesByIndex, (pool, start, limit)).await,
            None => self.call(Method::ZGetSubtreesByIndex, (pool, start)).await,
        }
    }

    /// `getrawmempool`: the ids of the mempool transactions.
    pub async fn get_raw_mempool(&self) -> Result<GetRawMempoolResponse, RpcError> {
        self.call(Method::GetRawMempool, (false,)).await
    }

    /// `getrawtransaction` with verbose 1: a mempool or best-chain transaction, its decoded
    /// fields and, for a mined transaction, its block height.
    pub async fn get_raw_transaction(
        &self,
        txid: transaction::Hash,
    ) -> Result<GetRawTransactionResponse, RpcError> {
        self.call(Method::GetRawTransaction, (txid.to_string(), 1)).await
    }

    /// `sendrawtransaction`: sends a serialized transaction to the mempool of the node.
    ///
    /// This call retries only when the request did not reach the node
    /// ([`RpcError::is_unsent`]). After a timeout, the node can have the transaction already,
    /// and a second attempt then gets the error "transaction already exists in mempool".
    pub async fn send_raw_transaction(
        &self,
        transaction: &[u8],
    ) -> Result<SendRawTransactionResponse, RpcError> {
        let params = (HexData(transaction.to_vec()),);
        self.rpc.call_with(Method::SendRawTransaction.name(), params, Retries::Unsent).await
    }

    /// `getaddresstxids`: the ids of the transactions that touch the addresses.
    pub async fn get_address_tx_ids(
        &self,
        request: &GetAddressTxIdsRequest,
    ) -> Result<Vec<String>, RpcError> {
        self.call(Method::GetAddressTxIds, (request,)).await
    }

    /// `getaddressbalance`: the transparent balance of the addresses.
    pub async fn get_address_balance(
        &self,
        request: &GetAddressBalanceRequest,
    ) -> Result<GetAddressBalanceResponse, RpcError> {
        self.call(Method::GetAddressBalance, (request,)).await
    }

    /// `getaddressutxos`: the unspent transparent outputs of the addresses.
    pub async fn get_address_utxos(
        &self,
        request: &GetAddressUtxosRequest,
    ) -> Result<GetAddressUtxosResponse, RpcError> {
        self.call(Method::GetAddressUtxos, (request,)).await
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
            assert_eq!(rpc.timeout_for(method.name()), timeout, "{method}");
        }
        assert_eq!(rpc.timeout_for("getbestblockhash"), Duration::from_secs(15));
    }

    #[test]
    fn first_block_with_body_is_the_prune_height_or_genesis() {
        let mut info: serde_json::Value =
            serde_json::from_str(include_str!("../tests/vectors/getblockchaininfo.json")).unwrap();
        let first = |info: &serde_json::Value| {
            serde_json::from_value::<GetBlockchainInfoResponse>(info.clone())
                .unwrap()
                .first_block_with_body()
        };
        assert_eq!(first(&info), block::Height::MIN);
        info["pruned"] = true.into();
        info["pruneheight"] = 40.into();
        assert_eq!(first(&info), block::Height(40));
    }

    #[test]
    fn method_names_round_trip() {
        for method in Method::ALL {
            assert_eq!(Method::from_name(method.name()), Some(method));
            assert_eq!(method.to_string(), method.name());
        }
        assert_eq!(Method::GetBlockchainInfo.name(), "getblockchaininfo");
        assert_eq!(Method::ZGetSubtreesByIndex.name(), "z_getsubtreesbyindex");
        assert_eq!(Method::from_name("getpeerinfo"), None);
    }

    #[test]
    fn explicit_method_timeout_replaces_the_default() {
        let config = ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap())
            .with_method_timeout("getblock", Duration::from_secs(5));
        let rpc = RpcClient::new(config).unwrap();
        assert_eq!(rpc.timeout_for("getblock"), Duration::from_secs(5));
    }
}
