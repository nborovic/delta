//! JSON-RPC wire types shared by the engine and the front ends.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;

/// Standard JSON-RPC codes.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;

/// Errors the proxy creates itself (-33090 to -33099). JSON-RPC reserves
/// -32768 to -32000, and nodes and providers put their own server errors
/// there (Agave -32001.., Surfpool -32091, ...), so delta stays outside it
/// to never be confused with an upstream's error.
pub const UNKNOWN_ROUTE: i64 = -33090;
pub const UNAUTHORIZED: i64 = -33091;
pub const RATE_LIMITED: i64 = -33092;
pub const QUORUM_NOT_REACHED: i64 = -33094;
pub const UPSTREAM_FAILED: i64 = -33095;
pub const SUBSCRIPTION_LIMIT: i64 = -33096;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }
}

/// What an upstream (or the proxy) answered for one request: either the raw
/// `result` JSON, passed through without re-parsing, or an error object.
#[derive(Clone, Debug)]
pub enum Answer {
    Result(Bytes),
    Error(RpcError),
}

impl Answer {
    pub fn error(code: i64, message: impl Into<String>) -> Self {
        Answer::Error(RpcError::new(code, message))
    }

    /// Serializes a full response object with the client's `id`.
    pub fn to_response(&self, id: &RawValue) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(b"{\"jsonrpc\":\"2.0\",");
        match self {
            Answer::Result(raw) => {
                out.extend_from_slice(b"\"result\":");
                out.extend_from_slice(raw);
            }
            Answer::Error(err) => {
                out.extend_from_slice(b"\"error\":");
                serde_json::to_writer(&mut out, err).expect("error serializes");
            }
        }
        out.extend_from_slice(b",\"id\":");
        out.extend_from_slice(id.get().as_bytes());
        out.push(b'}');
        out
    }
}

/// The JSON `null` id used when a request carried none or could not be parsed.
pub fn null_id() -> Box<RawValue> {
    RawValue::from_string("null".to_owned()).expect("valid json")
}

/// Builds the body sent upstream. The id is fixed so that responses do not
/// depend on the client's id and can be cached and compared.
pub fn upstream_body(method: &str, params: Option<&Value>) -> Bytes {
    #[derive(Serialize)]
    struct Req<'a> {
        jsonrpc: &'static str,
        id: u8,
        method: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        params: Option<&'a Value>,
    }
    let body = serde_json::to_vec(&Req { jsonrpc: "2.0", id: 1, method, params })
        .expect("request serializes");
    Bytes::from(body)
}

/// Parses an upstream response body into an [`Answer`]. `"result": null` is
/// a real answer; `None` means the body was not a JSON-RPC response at all,
/// including a JSON object with neither `result` nor `error`.
pub fn parse_upstream(body: &[u8]) -> Option<Answer> {
    let fields: std::collections::BTreeMap<String, &RawValue> = serde_json::from_slice(body).ok()?;
    if let Some(err) = fields.get("error").filter(|e| e.get().trim() != "null") {
        return serde_json::from_str::<RpcError>(err.get()).ok().map(Answer::Error);
    }
    let result = fields.get("result")?;
    Some(Answer::Result(Bytes::copy_from_slice(result.get().as_bytes())))
}

/// Serializes a JSON value with object keys sorted, independent of whether
/// serde_json's `preserve_order` feature is enabled somewhere in the build.
pub fn canonical_json(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, k).expect("string serializes");
                out.push(b':');
                canonical_json(&map[*k], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical_json(v, out);
            }
            out.push(b']');
        }
        other => serde_json::to_writer(&mut *out, other).expect("scalar serializes"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_result_error_and_null() {
        match parse_upstream(br#"{"jsonrpc":"2.0","id":1,"result":{"a":1}}"#).unwrap() {
            Answer::Result(b) => assert_eq!(&b[..], br#"{"a":1}"#),
            _ => panic!(),
        }
        match parse_upstream(br#"{"jsonrpc":"2.0","id":1,"result":null}"#).unwrap() {
            Answer::Result(b) => assert_eq!(&b[..], b"null"),
            _ => panic!(),
        }
        match parse_upstream(br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"behind"}}"#)
            .unwrap()
        {
            Answer::Error(e) => assert_eq!(e.code, -32005),
            _ => panic!(),
        }
        assert!(parse_upstream(br#"{"jsonrpc":"2.0","id":1}"#).is_none(), "neither result nor error");
        assert!(parse_upstream(b"{}").is_none());
        assert!(parse_upstream(br#"[{"result":1}]"#).is_none());
        assert!(parse_upstream(b"<html>").is_none());
    }

    #[test]
    fn response_keeps_client_id() {
        let id = RawValue::from_string("\"abc\"".into()).unwrap();
        let out = Answer::Result(Bytes::from_static(b"42")).to_response(&id);
        assert_eq!(out, br#"{"jsonrpc":"2.0","result":42,"id":"abc"}"#);
    }

    #[test]
    fn canonical_sorts_keys() {
        let v: Value = serde_json::from_str(r#"{"b":1,"a":{"d":2,"c":[{"z":1,"y":2}]}}"#).unwrap();
        let mut out = Vec::new();
        canonical_json(&v, &mut out);
        assert_eq!(out, br#"{"a":{"c":[{"y":2,"z":1}],"d":2},"b":1}"#);
    }
}
