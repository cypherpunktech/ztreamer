//! Errors a remote call can produce, and which of them are worth retrying.

use std::{fmt, time::Duration};

use http::StatusCode;
use serde::{Deserialize, Serialize};

/// An error code from the node: a JSON-RPC 2.0 code or a zcashd legacy code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RpcCode(pub i64);

impl RpcCode {
    /// JSON-RPC 2.0 "method not found".
    ///
    /// A zakura listener with cookie auth disabled on Mainnet or Testnet removes every method
    /// that is not on its unauthenticated allow list, so this is also what a restricted listener
    /// answers for a method that needs credentials.
    pub const METHOD_NOT_FOUND: Self = Self(-32601);

    /// zcashd legacy code: a miscellaneous error.
    ///
    /// This is the generic error code of zakura. zakura also sends it for each error that it
    /// raises as JSON-RPC 2.0 "invalid params" (-32602): params that do not decode, and
    /// `getblockhash` for a height above the tip. Its `rpc_call_compatibility` middleware
    /// changes that code to this code and keeps the message. Thus this code alone does not
    /// identify the error.
    pub const MISC: Self = Self(-1);

    /// zcashd legacy code: a parameter is not valid.
    ///
    /// zakura answers `getblock` with this code for a block that it does not have: an unknown
    /// hash, or a height above its tip. `getblockhash` answers this code for a height at or below
    /// the tip when the node has no hash for it. `z_gettreestate` answers it when it cannot read
    /// the block, which on a pruned node is also a block below the prune height.
    pub const INVALID_PARAMETER: Self = Self(-8);

    /// zcashd legacy code: the node is still warming up.
    pub const IN_WARMUP: Self = Self(-28);

    /// zcashd legacy code: an address or a key is not valid or is unknown.
    ///
    /// zakura answers `getrawtransaction` with this code for a transaction that is not in its
    /// mempool or best chain, and for a txid that is not valid hex. `getblockheader` and `getblock`
    /// with verbosity 1 or 2 answer it for an unknown block hash. The address methods
    /// (`getaddressbalance`, `getaddresstxids`, `getaddressutxos`) answer it for an address that
    /// is not valid.
    pub const INVALID_ADDRESS_OR_KEY: Self = Self(-5);
}

impl fmt::Display for RpcCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The error object the node returns inside a JSON-RPC response.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RpcErrorObject {
    /// JSON-RPC or zcashd legacy error code.
    pub code: RpcCode,
    /// Human readable message.
    pub message: String,
}

/// Why a remote call failed.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// The client could not be built from its configuration.
    #[error("invalid remote RPC configuration: {0}")]
    Config(String),

    /// The request never produced an HTTP response: DNS, connect, TLS, or a read error.
    #[error("{method}: request failed: {source}")]
    Transport {
        /// Method that was being called.
        method: String,
        /// Underlying client error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The node accepted the connection and closed it without answering.
    ///
    /// zakura does this when cookie auth is on and the credentials are wrong, so the hint names
    /// that case.
    #[error("{method}: node closed the connection without a response ({hint})")]
    ConnectionClosed {
        /// Method that was being called.
        method: String,
        /// Most likely cause.
        hint: &'static str,
    },

    /// No response within the method's timeout.
    #[error("{method}: timed out after {timeout:?}")]
    Timeout {
        /// Method that was being called.
        method: String,
        /// Timeout that elapsed.
        timeout: Duration,
    },

    /// HTTP 401 or 403.
    #[error("{method}: node rejected the credentials (HTTP {status})")]
    Unauthorized {
        /// Method that was being called.
        method: String,
        /// HTTP status code.
        status: StatusCode,
    },

    /// Any other non-2xx HTTP status.
    #[error("{method}: node returned HTTP {status}: {body}")]
    Http {
        /// Method that was being called.
        method: String,
        /// HTTP status code.
        status: StatusCode,
        /// Start of the response body.
        body: String,
    },

    /// The node answered with a JSON-RPC error object.
    #[error("{method}: node returned error {code}: {message}")]
    Rpc {
        /// Method that was being called.
        method: String,
        /// Error code from the node.
        code: RpcCode,
        /// Message from the node.
        message: String,
    },

    /// The response was not a JSON-RPC envelope for this request.
    #[error("{method}: malformed response: {detail}")]
    Protocol {
        /// Method that was being called.
        method: String,
        /// What was wrong.
        detail: String,
    },

    /// The `result` field did not match the expected type.
    #[error("{method}: could not decode result: {source}")]
    Decode {
        /// Method that was being called.
        method: String,
        /// Serde error.
        #[source]
        source: serde_json::Error,
    },
}

impl RpcError {
    /// The message of zakura for a `getblockhash` height above its tip.
    pub(crate) const ABOVE_TIP_MESSAGE: &str = "Provided index is greater than the current tip";

    /// Error code from the node, when the node answered with an error object.
    pub const fn rpc_code(&self) -> Option<RpcCode> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// True when the node does not serve this method, including on a restricted listener.
    pub fn is_method_not_found(&self) -> bool {
        self.rpc_code() == Some(RpcCode::METHOD_NOT_FOUND)
    }

    /// True when the node has no block at the requested height or with the requested hash.
    ///
    /// zakura answers `getblockhash` for a height above its tip with [`RpcCode::MISC`] and the
    /// message "Provided index is greater than the current tip". It answers all other requests
    /// for a missing block, `getblock` included, with [`RpcCode::INVALID_PARAMETER`]. A
    /// [`RpcCode::MISC`] error with a different message is not a missing block.
    pub fn is_block_not_found(&self) -> bool {
        self.rpc_code() == Some(RpcCode::INVALID_PARAMETER) || self.is_height_above_tip()
    }

    /// True when the node has no transaction with the requested id, in its mempool or in its
    /// best chain. zakura answers `getrawtransaction` for a missing transaction with
    /// [`RpcCode::INVALID_ADDRESS_OR_KEY`].
    pub fn is_transaction_not_found(&self) -> bool {
        self.rpc_code() == Some(RpcCode::INVALID_ADDRESS_OR_KEY)
    }

    /// True when the node refused a height above its tip.
    ///
    /// zakura raises this error as JSON-RPC 2.0 "invalid params", and its middleware sends it
    /// with [`RpcCode::MISC`] on the wire. Only the message makes it different from the other
    /// [`RpcCode::MISC`] errors.
    fn is_height_above_tip(&self) -> bool {
        matches!(
            self,
            Self::Rpc { code: RpcCode::MISC, message, .. } if message == Self::ABOVE_TIP_MESSAGE
        )
    }

    /// True when the request did not reach the node: the connection, the DNS lookup or the TLS
    /// handshake failed. Thus a second attempt cannot run the call twice on the node.
    pub fn is_unsent(&self) -> bool {
        let Self::Transport { source, .. } = self else {
            return false;
        };
        source
            .downcast_ref::<hyper_util::client::legacy::Error>()
            .is_some_and(hyper_util::client::legacy::Error::is_connect)
    }

    /// True when the same request may succeed if sent again.
    ///
    /// Every call except `sendrawtransaction` is idempotent on the node. `sendrawtransaction`
    /// retries only a failure where the request did not reach the node ([`Self::is_unsent`]).
    /// No call retries an answer that will not change: a node error other than warm-up, a bad
    /// request, a decode failure.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { .. } | Self::ConnectionClosed { .. } | Self::Timeout { .. } => true,
            Self::Http { status, .. } => matches!(
                *status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT
            ),
            Self::Rpc { code, .. } => *code == RpcCode::IN_WARMUP,
            Self::Config(_)
            | Self::Unauthorized { .. }
            | Self::Protocol { .. }
            | Self::Decode { .. } => false,
        }
    }

    pub(crate) fn from_object(method: &str, object: RpcErrorObject) -> Self {
        Self::Rpc { method: method.to_owned(), code: object.code, message: object.message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: StatusCode) -> RpcError {
        RpcError::Http { method: "m".into(), status, body: String::new() }
    }

    fn rpc(code: RpcCode) -> RpcError {
        rpc_with_message(code, "")
    }

    fn rpc_with_message(code: RpcCode, message: &str) -> RpcError {
        RpcError::from_object("m", RpcErrorObject { code, message: message.into() })
    }

    #[test]
    fn retry_classification() {
        assert!(http(StatusCode::SERVICE_UNAVAILABLE).is_retryable());
        assert!(http(StatusCode::TOO_MANY_REQUESTS).is_retryable());
        assert!(!http(StatusCode::INTERNAL_SERVER_ERROR).is_retryable());
        assert!(!http(StatusCode::BAD_REQUEST).is_retryable());
        assert!(rpc(RpcCode::IN_WARMUP).is_retryable());
        assert!(!rpc(RpcCode(-8)).is_retryable());
        assert!(!rpc(RpcCode::METHOD_NOT_FOUND).is_retryable());
        assert!(rpc(RpcCode::METHOD_NOT_FOUND).is_method_not_found());
        assert!(rpc(RpcCode::INVALID_PARAMETER).is_block_not_found());
        assert!(rpc_with_message(RpcCode::MISC, RpcError::ABOVE_TIP_MESSAGE).is_block_not_found());
        assert!(!rpc_with_message(RpcCode::MISC, "Invalid params").is_block_not_found());
        assert!(!rpc(RpcCode::MISC).is_block_not_found());
        assert!(
            !rpc_with_message(RpcCode(-32602), RpcError::ABOVE_TIP_MESSAGE).is_block_not_found()
        );
        assert!(!rpc(RpcCode::IN_WARMUP).is_block_not_found());
        assert!(!http(StatusCode::NOT_FOUND).is_block_not_found());
        assert!(rpc(RpcCode::INVALID_ADDRESS_OR_KEY).is_transaction_not_found());
        assert!(!rpc(RpcCode::INVALID_PARAMETER).is_transaction_not_found());
        assert!(!rpc(RpcCode::INVALID_ADDRESS_OR_KEY).is_block_not_found());
        assert!(
            RpcError::Timeout { method: "m".into(), timeout: Duration::from_secs(1) }
                .is_retryable()
        );
        let unauthorized =
            RpcError::Unauthorized { method: "m".into(), status: StatusCode::UNAUTHORIZED };
        assert!(!unauthorized.is_retryable());
        assert!(!RpcError::Config("x".into()).is_retryable());
    }

    #[test]
    fn codes_parse_as_plain_integers() {
        assert_eq!(
            serde_json::to_value(RpcCode::INVALID_PARAMETER).unwrap(),
            serde_json::json!(-8)
        );
        assert_eq!(
            serde_json::to_value(RpcCode::INVALID_ADDRESS_OR_KEY).unwrap(),
            serde_json::json!(-5)
        );
        let object: RpcErrorObject =
            serde_json::from_str(r#"{"code":-32601,"message":"Method not found"}"#).unwrap();
        assert_eq!(object.code, RpcCode::METHOD_NOT_FOUND);
        assert_eq!(serde_json::to_value(RpcCode::IN_WARMUP).unwrap(), serde_json::json!(-28));
        assert_eq!(RpcCode(-8).to_string(), "-8");
    }
}
