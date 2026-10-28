//! JSON-RPC 2.0 request and response bodies.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RpcError, RpcErrorObject};

#[derive(Serialize)]
pub(crate) struct Request<'a> {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: &'a str,
    pub params: Value,
}

impl<'a> Request<'a> {
    pub(crate) const fn new(id: u64, method: &'a str, params: Value) -> Self {
        Self { jsonrpc: "2.0", id, method, params }
    }
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcErrorObject>,
    #[serde(default)]
    id: Option<Value>,
}

/// Parses a response body and returns the `result` value for `expected_id`.
pub(crate) fn parse_result(method: &str, expected_id: u64, body: &[u8]) -> Result<Value, RpcError> {
    let response: Response = serde_json::from_slice(body).map_err(|error| RpcError::Protocol {
        method: method.to_owned(),
        detail: format!("not a JSON-RPC response: {error}"),
    })?;
    if let Some(error) = response.error {
        return Err(RpcError::from_object(method, error));
    }
    match response.id {
        Some(Value::Number(id)) if id.as_u64() == Some(expected_id) => {}
        other => {
            return Err(RpcError::Protocol {
                method: method.to_owned(),
                detail: format!("response id {other:?} does not match request id {expected_id}"),
            });
        }
    }
    Ok(response.result.unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::RpcCode;

    #[test]
    fn request_is_strict_2_0() {
        let request = Request::new(7, "getblock", json!(["5", 0]));
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"jsonrpc": "2.0", "id": 7, "method": "getblock", "params": ["5", 0]})
        );
    }

    #[test]
    fn result_and_error_shapes() {
        let ok = br#"{"jsonrpc":"2.0","result":{"a":1},"id":3}"#;
        assert_eq!(parse_result("m", 3, ok).unwrap(), json!({"a": 1}));

        let null = br#"{"jsonrpc":"2.0","result":null,"id":3}"#;
        assert_eq!(parse_result("m", 3, null).unwrap(), Value::Null);

        let bitcoind_style = br#"{"result":null,"error":{"code":-8,"message":"bad"},"id":3}"#;
        match parse_result("m", 3, bitcoind_style).unwrap_err() {
            RpcError::Rpc { code, message, .. } => {
                assert_eq!(code, RpcCode(-8));
                assert_eq!(message, "bad");
            }
            other => panic!("unexpected {other:?}"),
        }

        let wrong_id = br#"{"jsonrpc":"2.0","result":1,"id":4}"#;
        assert!(matches!(parse_result("m", 3, wrong_id), Err(RpcError::Protocol { .. })));

        let garbage = b"<html>";
        assert!(matches!(parse_result("m", 3, garbage), Err(RpcError::Protocol { .. })));
    }
}
