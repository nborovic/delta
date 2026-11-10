//! Chain knowledge that lives in code rather than config. The route model is
//! chain-agnostic; these defaults make it safe for Solana.

use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::HashMap;

use crate::jsonrpc::canonical_json;

/// Methods that change chain state, as far as the name tells: Agave's own
/// and providers' `send*` methods (Jito's `sendBundle`, ...). Only used to
/// warn: a write follows the configured policies like any other method.
pub fn is_write(method: &str) -> bool {
    method == "requestAirdrop" || method.starts_with("send")
}

/// Writes checked against `"*"` and prefix keys, where no name is spelled out.
pub const KNOWN_WRITES: [&str; 3] = ["sendTransaction", "requestAirdrop", "sendBundle"];

/// JSON-RPC errors that make a call count as failed (another node can likely
/// answer), rather than being a final answer.
pub fn is_retryable_rpc_error(code: i64) -> bool {
    matches!(code, NODE_UNHEALTHY | -32004 | -32016)
}

/// Agave returns this when the node itself is unhealthy or behind. It is the
/// only JSON-RPC error that counts against health.
pub const NODE_UNHEALTHY: i64 = -32005;

pub fn is_subscribe(method: &str) -> bool {
    method.ends_with("Subscribe")
}

pub fn is_unsubscribe(method: &str) -> bool {
    method.ends_with("Unsubscribe")
}

/// Subscription methods (subscribe or unsubscribe) take no policies.
pub fn is_subscription_method(method: &str) -> bool {
    is_subscribe(method) || is_unsubscribe(method)
}

/// `accountUnsubscribe` -> `accountSubscribe`.
pub fn subscribe_for_unsubscribe(method: &str) -> Option<String> {
    method.strip_suffix("Unsubscribe").map(|base| format!("{base}Subscribe"))
}

/// `accountSubscribe` -> `accountNotification`.
pub fn notification_for(method: &str) -> String {
    let base = method.strip_suffix("Subscribe").unwrap_or(method);
    format!("{base}Notification")
}

/// Subscriptions whose notifications carry the latest state (only the newest
/// one matters), as opposed to event streams where every message counts.
/// `programSubscribe` is an event stream: each notification is a different
/// account, so keeping only the newest would lose the others.
pub fn is_state_subscription(method: &str) -> bool {
    matches!(method, "accountSubscribe" | "slotSubscribe" | "rootSubscribe" | "signatureSubscribe")
}

/// The commitment a request asked for, if any (the config object is the last
/// positional parameter).
pub fn commitment(params: Option<&Value>) -> Option<&str> {
    let cfg = match params? {
        Value::Array(items) => items.last()?.as_object()?,
        Value::Object(map) => map,
        _ => return None,
    };
    cfg.get("commitment")?.as_str()
}

/// Canonical form of the params for cache keys and subscription
/// deduplication: object keys sorted (by the serializer) and a trailing empty
/// config object dropped (`[x, {}]` and `[x]` are the same request). No
/// default values are filled in, because a wrong guess would make two
/// different requests share an entry. Base58 strings are left untouched
/// (case-sensitive).
pub fn canonical_params(params: Option<&Value>) -> Vec<u8> {
    let canonical = match params {
        None => Value::Array(Vec::new()),
        Some(Value::Null) => Value::Array(Vec::new()),
        Some(Value::Array(items)) => {
            let mut items = items.clone();
            if matches!(items.last(), Some(Value::Object(cfg)) if cfg.is_empty()) {
                items.pop();
            }
            Value::Array(items)
        }
        Some(other) => other.clone(),
    };
    let mut out = Vec::with_capacity(64);
    canonical_json(&canonical, &mut out);
    out
}

pub fn params_hash(params: Option<&Value>) -> blake3::Hash {
    blake3::hash(&canonical_params(params))
}

/// `null`, empty arrays/objects, and `{context, value: null | []}`.
pub fn is_empty_result(raw: &[u8]) -> bool {
    let trimmed = raw.trim_ascii();
    if matches!(trimmed, b"null" | b"[]" | b"{}") {
        return true;
    }
    if trimmed.first() != Some(&b'{') {
        return false;
    }
    let Ok(map) = serde_json::from_slice::<HashMap<&str, &RawValue>>(trimmed) else {
        return false;
    };
    match (map.get("context"), map.get("value")) {
        (Some(_), Some(value)) => matches!(value.get().trim(), "null" | "[]"),
        _ => false,
    }
}

/// Fields that legitimately differ between nodes and are ignored when quorum
/// compares answers. A code default, the same for every method for now.
const QUORUM_IGNORE: &[&[&str]] = &[&["context", "slot"], &["context", "apiVersion"]];

/// Hash of a result after removing the fields quorum ignores.
pub fn quorum_hash(_method: &str, raw: &[u8]) -> blake3::Hash {
    let Ok(mut value) = serde_json::from_slice::<Value>(raw) else {
        return blake3::hash(raw);
    };
    for path in QUORUM_IGNORE {
        remove_path(&mut value, path);
    }
    let mut out = Vec::with_capacity(raw.len());
    canonical_json(&value, &mut out);
    blake3::hash(&out)
}

fn remove_path(value: &mut Value, path: &[&str]) {
    match path {
        [] => {}
        [last] => {
            if let Value::Object(map) = value {
                map.remove(*last);
            }
        }
        [first, rest @ ..] => {
            if let Some(inner) = value.get_mut(*first) {
                remove_path(inner, rest);
            }
        }
    }
}

/// The slot a notification refers to, used to keep subscription ordering.
pub fn notification_slot(result: &Value) -> Option<u64> {
    if let Some(slot) = result.as_u64() {
        // rootSubscribe: the notification is the slot itself.
        return Some(slot);
    }
    result
        .get("context")
        .and_then(|c| c.get("slot"))
        .and_then(Value::as_u64)
        .or_else(|| result.get("slot").and_then(Value::as_u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_sorts_keys_and_fills_nothing() {
        let a = canonical_params(Some(&json!(["abc"])));
        assert_eq!(a, canonical_params(Some(&json!(["abc", {}]))));
        // A missing commitment or encoding is not assumed to be the default.
        assert_ne!(a, canonical_params(Some(&json!(["abc", {"commitment": "finalized"}]))));
        assert_ne!(
            canonical_params(Some(&json!(["k"]))),
            canonical_params(Some(&json!(["k", {"encoding": "binary"}])))
        );
        // Key order does not matter.
        assert_eq!(
            canonical_params(Some(&json!(["k", {"commitment": "confirmed", "encoding": "base64"}]))),
            canonical_params(Some(&json!(["k", {"encoding": "base64", "commitment": "confirmed"}])))
        );
        // Base58 case is preserved.
        assert_ne!(a, canonical_params(Some(&json!(["ABC"]))));
        assert_eq!(canonical_params(None), canonical_params(Some(&json!([]))));
    }

    #[test]
    fn empty_results() {
        assert!(is_empty_result(b"null"));
        assert!(is_empty_result(b" [] "));
        assert!(is_empty_result(br#"{"context":{"slot":1},"value":null}"#));
        assert!(!is_empty_result(br#"{"context":{"slot":1},"value":{"lamports":1}}"#));
        assert!(!is_empty_result(b"42"));
    }

    #[test]
    fn notification_slots() {
        assert_eq!(notification_slot(&json!(1234)), Some(1234), "rootSubscribe sends the slot itself");
        assert_eq!(notification_slot(&json!({"context": {"slot": 7}, "value": 1})), Some(7));
        assert_eq!(notification_slot(&json!({"slot": 9, "parent": 8, "root": 1})), Some(9));
        assert_eq!(notification_slot(&json!({"value": 1})), None);
    }

    #[test]
    fn quorum_ignores_slot() {
        let a = quorum_hash("getBalance", br#"{"context":{"slot":10,"apiVersion":"2.0"},"value":5}"#);
        let b = quorum_hash("getBalance", br#"{"value":5,"context":{"slot":11,"apiVersion":"2.1"}}"#);
        let c = quorum_hash("getBalance", br#"{"context":{"slot":10},"value":6}"#);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
