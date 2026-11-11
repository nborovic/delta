//! JSON-RPC handling, routing and method matching.

use std::time::Duration;

use serde_json::{json, Value};

use crate::harness::*;

const ALL: &str = r#"
[routes.r]
upstreams = ["*"]
[routes.r.methods]
"*" = {}
"#;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn responses_are_surfpools() {
    let env = env(&[0], ALL).await;
    let key = pubkey();
    let sig = env.chain(0).airdrop(&key, 2_000_000_000).await;
    env.chain(0).wait_finalized(&sig).await;
    let processed = json!({"commitment": "processed", "encoding": "base64"});
    for (method, params) in [
        ("getGenesisHash", json!(null)),
        ("getBalance", json!([key, {"commitment": "processed"}])),
        ("getAccountInfo", json!([SYSTEM_PROGRAM, processed])),
        ("getMultipleAccounts", json!([[SYSTEM_PROGRAM, key], processed])),
        ("getMinimumBalanceForRentExemption", json!([165])),
        ("getSignaturesForAddress", json!([key])),
        ("getTransaction", json!([sig, {"commitment": "confirmed", "maxSupportedTransactionVersion": 0}])),
    ] {
        let via_proxy = env.call("r", method, params.clone()).await;
        let direct = env.chain(0).call(method, params).await;
        let strip = |v: &Value| {
            let mut r = v["result"].clone();
            if let Some(ctx) = r.get_mut("context") {
                *ctx = Value::Null;
            }
            r
        };
        assert_eq!(strip(&via_proxy), strip(&direct), "{method}");
        assert_eq!(via_proxy["jsonrpc"], "2.0");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn ids_are_echoed() {
    let env = env(&[0], ALL).await;
    for id in [json!(7), json!("abc"), json!(9007199254740993u64), json!(null), json!(-1)] {
        let (status, v) = env.client.json("/rpc/r", &json!({"jsonrpc": "2.0", "id": id, "method": "getLatestBlockhash"}), &[]).await;
        assert_eq!(status, 200);
        assert_eq!(v["id"], id);
        assert!(v["result"]["value"]["blockhash"].is_string(), "{v}");
    }
    // The upstream always sees the proxy's own id.
    assert!(env.http[0].bodies().iter().all(|b| b["id"] == 1));
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn batches_keep_order_and_ids() {
    let env = env(
        &[0],
        r#"
[routes.r]
upstreams = ["*"]
[routes.r.methods]
getLatestBlockhash = {}
getEpochInfo = {}
getBalance = {}
"#,
    )
    .await;
    let batch = json!([
        {"jsonrpc": "2.0", "id": "a", "method": "getLatestBlockhash"},
        {"jsonrpc": "2.0", "id": 2, "method": "getSlot"},
        5,
        {"jsonrpc": "2.0", "id": 4, "method": "getBalance", "params": []},
        {"jsonrpc": "2.0", "id": 5},
        {"jsonrpc": "2.0", "id": 6, "method": "getEpochInfo"},
    ]);
    let (status, v) = env.client.json("/rpc/r", &batch, &[]).await;
    assert_eq!(status, 200);
    let items = v.as_array().unwrap();
    assert_eq!(items.len(), 6);
    assert_eq!(items[0]["id"], "a");
    assert!(items[0]["result"]["value"]["blockhash"].is_string());
    assert_eq!((items[1]["id"].clone(), code(&items[1])), (json!(2), -32601), "not accepted by the route");
    assert_eq!((items[2]["id"].clone(), code(&items[2])), (Value::Null, -32600));
    assert_eq!((items[3]["id"].clone(), code(&items[3])), (json!(4), -32602), "Surfpool's own error");
    assert_eq!((items[4]["id"].clone(), code(&items[4])), (json!(5), -32600), "no method");
    assert_eq!(items[5]["id"], 6);
    assert!(items[5]["result"]["absoluteSlot"].is_u64());
    let sent = env.http[0].bodies().pop().unwrap();
    let methods: Vec<&str> = sent.as_array().unwrap().iter().map(|e| e["method"].as_str().unwrap()).collect();
    assert_eq!(methods, ["getLatestBlockhash", "getBalance", "getEpochInfo"], "only valid, accepted elements are forwarded");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn notifications_run_but_get_no_response() {
    // As on a Solana node: a request without an `id` member is a
    // notification, executed but never answered.
    let env = env(&[0], ALL).await;
    let note = |m: &str| json!({"jsonrpc": "2.0", "method": m});

    let (status, text) = env.client.raw("/rpc/r", note("getLatestBlockhash").to_string(), &[]).await;
    assert_eq!((status, text.as_str()), (200, ""));
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 1, "still sent upstream");

    let (_, v) = env.client.json("/rpc/r", &json!([note("getSlot"), {"jsonrpc": "2.0", "id": 7, "method": "getSlot"}]), &[]).await;
    assert_eq!(v.as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v[0]["id"], 7);
    let sent = env.http[0].bodies().pop().unwrap();
    assert!(sent[0].get("id").is_none() && sent[1]["id"].is_u64(), "forwarded as a notification and a request: {sent}");

    let (status, text) = env.client.raw("/rpc/r", json!([note("getSlot"), note("getEpochInfo")]).to_string(), &[]).await;
    assert_eq!((status, text.as_str()), (200, ""), "a batch of notifications");
    let (status, text) = env.client.raw("/rpc/r", note("noSuchMethod").to_string(), &[]).await;
    assert_eq!((status, text.as_str()), (200, ""), "not even an error");

    let (_, v) = env.client.json("/rpc/r", &json!({"jsonrpc": "2.0", "id": null, "method": "getSlot"}), &[]).await;
    assert!(v["result"].is_u64() && v.get("id") == Some(&Value::Null), "an explicit null id is answered: {v}");
    let (_, v) = env.client.json("/rpc/r", &json!({"jsonrpc": "2.0"}), &[]).await;
    assert_eq!(code(&v), -32600, "no method is no notification");
}

/// The default load balancing prefers u1; getBalance's own prefers u0 and
/// adds quorum.
const MIXED: &str = r#"
[routes.r]
upstreams = ["*"]
loadBalancing = { type = "priority", order = ["u1", "u0"] }
[routes.r.methods]
"*" = {}
getSlot = false
getLatestBlockhash = { cache = { ttl = 60000 } }
getBalance = { loadBalancing = { type = "priority", order = ["u0", "u1"] }, mode = { type = "quorum", threshold = 2, upstreams = 2 }, cache = { ttl = 60000 } }
"#;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_batch_is_forwarded_whole_to_one_upstream_minus_refused_methods() {
    let env = env(&[0, 0], MIXED).await;
    let key = pubkey();
    let batch = json!([
        {"jsonrpc": "2.0", "id": "a", "method": "getLatestBlockhash"},
        {"jsonrpc": "2.0", "id": 2, "method": "getSlot"},
        {"jsonrpc": "2.0", "id": 3, "method": "getBalance", "params": [key]},
        {"jsonrpc": "2.0", "method": "getEpochInfo"}
    ]);
    let (_, v) = env.client.json("/rpc/r", &batch, &[]).await;
    let v = v.as_array().unwrap();
    assert_eq!(v.len(), 3, "the notification gets no entry: {v:?}");
    assert_eq!((&v[0]["id"], &v[1]["id"], &v[2]["id"]), (&json!("a"), &json!(2), &json!(3)));
    assert!(v[0]["result"]["value"]["blockhash"].is_string());
    assert_eq!(code(&v[1]), -32601, "the allow-list still applies");
    assert_eq!(v[2]["result"]["value"], 0);

    // One call, to the route's default pick (u1), with getSlot left out;
    // getBalance's own load balancing and quorum do not apply.
    assert_eq!((env.http[0].total(), env.http[1].calls("batch")), (0, 1));
    let sent = env.http[1].bodies().pop().unwrap();
    let methods: Vec<&str> = sent.as_array().unwrap().iter().map(|e| e["method"].as_str().unwrap()).collect();
    assert_eq!(methods, ["getLatestBlockhash", "getBalance", "getEpochInfo"]);

    // Nor does the cache: the same batch goes upstream again.
    env.client.json("/rpc/r", &batch, &[]).await;
    assert_eq!(env.http[1].calls("batch"), 2);
    // A single request still gets its policies: quorum asks both.
    env.client.call("r", "getBalance", json!([key])).await;
    assert_eq!(env.calls("getBalance"), vec![1, 1]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_batch_splits_by_the_upstreams_its_methods_use() {
    let env = env(
        &[0, 0],
        r#"
[routes.r]
upstreams = ["u0"]
[routes.r.methods]
"*" = {}
getBalance = { upstreams = ["u1"] }
"#,
    )
    .await;
    let key = pubkey();
    let batch = json!([
        {"jsonrpc": "2.0", "id": 1, "method": "getSlot"},
        {"jsonrpc": "2.0", "id": 2, "method": "getBalance", "params": [key]},
        {"jsonrpc": "2.0", "id": 3, "method": "getEpochInfo"},
        {"jsonrpc": "2.0", "id": 4, "method": "getBalance", "params": [key]}
    ]);
    let (_, v) = env.client.json("/rpc/r", &batch, &[]).await;
    let v = v.as_array().unwrap();
    let ids: Vec<&Value> = v.iter().map(|e| &e["id"]).collect();
    assert_eq!(ids, [&json!(1), &json!(2), &json!(3), &json!(4)], "answers in request order");
    assert!(v[0]["result"].is_u64() && v[2]["result"]["absoluteSlot"].is_u64());
    assert_eq!((&v[1]["result"]["value"], &v[3]["result"]["value"]), (&json!(0), &json!(0)));

    // One batch per upstream list: getBalance only ever reaches u1.
    assert_eq!((env.http[0].calls("batch"), env.http[1].calls("batch")), (1, 1));
    let methods = |i: usize| -> Vec<String> {
        let sent = env.http[i].bodies().pop().unwrap();
        sent.as_array().unwrap().iter().map(|e| e["method"].as_str().unwrap().to_owned()).collect()
    };
    assert_eq!(methods(0), ["getSlot", "getEpochInfo"]);
    assert_eq!(methods(1), ["getBalance", "getBalance"]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_failed_batch_fails_every_element() {
    let env = env(&[0, 0], MIXED).await;
    env.http[1].set(Fault::Status(503));
    let batch = json!([
        {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"},
        {"jsonrpc": "2.0", "id": 2, "method": "getEpochInfo"}
    ]);
    let (_, v) = env.client.json("/rpc/r", &batch, &[]).await;
    for e in v.as_array().unwrap() {
        assert_eq!(code(e), -33095, "{e}");
        assert!(message(e).contains("HTTP 503"), "{e}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn batch_and_body_limits() {
    let env = env(&[0], ALL).await;
    let one = |i: usize| json!({"jsonrpc": "2.0", "id": i, "method": "getLatestBlockhash"});

    let (_, v) = env.client.json("/rpc/r", &json!([]), &[]).await;
    assert_eq!(code(&v), -32600);

    // No limit on the number of elements, as on a Solana node: only the
    // body size is limited.
    let big: Vec<Value> = (0..500).map(one).collect();
    let (_, v) = env.client.json("/rpc/r", &Value::Array(big), &[]).await;
    assert_eq!(v.as_array().unwrap().len(), 500);
    assert!(v.as_array().unwrap().iter().all(|r| r["result"]["value"]["blockhash"].is_string()));
    assert_eq!(env.http[0].calls("batch"), 1, "one upstream call");

    let (_, text) = env.client.raw("/rpc/r", "{\"jsonrpc\":", &[]).await;
    assert_eq!(code(&serde_json::from_str(&text).unwrap()), -32700);

    let huge = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":["{}"]}}"#, "x".repeat(11 * 1024 * 1024));
    let (status, _) = env.client.raw("/rpc/r", huge, &[]).await;
    assert_eq!(status, 413);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn paths_and_http_methods() {
    let env = env(&[0], ALL).await;
    assert_eq!(env.client.get("/health").await, (200, "ok".into()));
    assert_eq!(env.client.get("/rpc/r").await.0, 405);
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}).to_string();
    assert_eq!(env.client.raw("/nope", body.clone(), &[]).await.0, 404);
    assert_eq!(env.client.raw("/rpc/", body.clone(), &[]).await.0, 404);
    assert_eq!(env.client.raw("/rpc/r/x", body.clone(), &[]).await.0, 404);

    let (status, text) = env.client.raw("/rpc/r/", body.clone(), &[]).await;
    assert_eq!(status, 200, "a trailing slash is the same route");
    assert!(text.contains("blockhash"), "{text}");

    let (status, text) = env.client.raw("/rpc/missing", body, &[]).await;
    assert_eq!(status, 200);
    assert_eq!(code(&serde_json::from_str(&text).unwrap()), -33090);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn method_keys_most_specific_wins() {
    let env = env(
        &[0],
        r#"
[routes.r]
upstreams = ["*"]
[routes.r.methods]
"*"          = false
"get*"       = {}
"getBlock*"  = false
getBlockHeight = {}
getBalance   = { cache = { ttl = 60000 } }
getSlot      = false
"#,
    )
    .await;
    let key = pubkey();
    // exact key: accepted, cached (its own TTL)
    for _ in 0..2 {
        result(&env.call("r", "getBalance", json!([key])).await);
    }
    assert_eq!(env.http[0].calls("getBalance"), 1);
    // prefix key: accepted, not cached
    for _ in 0..2 {
        result(&env.call("r", "getEpochInfo", json!(null)).await);
    }
    assert_eq!(env.http[0].calls("getEpochInfo"), 2);
    // exact `false` beats the prefix
    assert_eq!(code(&env.call("r", "getSlot", json!(null)).await), -32601);
    // the longer prefix beats the shorter one, an exact key beats both
    assert_eq!(code(&env.call("r", "getBlockTime", json!([1])).await), -32601);
    result(&env.call("r", "getBlockHeight", json!(null)).await);
    // "*" = false
    assert_eq!(code(&env.call("r", "requestAirdrop", json!([key, 1])).await), -32601);
    assert_eq!(env.http[0].calls("getSlot") + env.http[0].calls("getBlockTime") + env.http[0].calls("requestAirdrop"), 0);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn subscriptions_need_a_websocket() {
    let env = env(&[0], ALL).await;
    let v = env.call("r", "slotSubscribe", json!(null)).await;
    assert_eq!(code(&v), -32601);
    assert!(message(&v).contains("WebSocket"), "{v}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_unknown_subscribe_over_http_is_a_call() {
    let env = env(&[0], ALL).await;
    env.call("r", "fooSubscribe", json!(null)).await;
    assert_eq!(env.http[0].calls("fooSubscribe"), 1, "forwarded like any method");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn upstream_errors_pass_through_unretried() {
    let env = env(
        &[0],
        r#"
[routes.r]
upstreams = ["*"]
retry = { attempts = 3, delay = 0 }
[routes.r.methods]
"*" = {}
"#,
    )
    .await;
    let via_proxy = env.call("r", "getBalance", json!([])).await;
    let direct = env.chain(0).call("getBalance", json!([])).await;
    assert_eq!(via_proxy["error"], direct["error"]);
    assert_eq!(env.http[0].calls("getBalance"), 1, "a final JSON-RPC error is not retried");

    let v = env.call("r", "notAMethod", json!(null)).await;
    assert_eq!(code(&v), -32601, "Surfpool's own method-not-found");
    assert_eq!(env.http[0].calls("notAMethod"), 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn browsers_can_call_it_cors() {
    let env = env(&[0], ALL).await;
    let browser = [("origin", "https://app.example.com")];
    let header = |h: &reqwest::header::HeaderMap, k: &str| h.get(k).map(|v| v.to_str().unwrap().to_owned());

    // The preflight a browser sends before a POST with web3.js's headers.
    let unkeyed = Client { base: env.client.base.clone(), key: None };
    let (status, h) = unkeyed
        .headers("OPTIONS", "/rpc/r", "", &[
            browser[0],
            ("access-control-request-method", "POST"),
            ("access-control-request-headers", "content-type, solana-client"),
        ])
        .await;
    assert_eq!(status, 204, "a preflight carries no key and needs none");
    assert_eq!(header(&h, "access-control-allow-origin").as_deref(), Some("*"));
    assert!(header(&h, "access-control-allow-methods").unwrap().contains("POST"));
    assert_eq!(header(&h, "access-control-allow-headers").as_deref(), Some("content-type, solana-client"));

    // The real request, and an error, both readable by the page.
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}).to_string();
    let (status, h) = env.client.headers("POST", "/rpc/r", &body, &[browser[0], ("content-type", "application/json")]).await;
    assert_eq!((status, header(&h, "access-control-allow-origin").as_deref()), (200, Some("*")));
    let (status, h) = unkeyed.headers("POST", "/rpc/r", &body, &[browser[0], ("content-type", "application/json")]).await;
    assert_eq!((status, header(&h, "access-control-allow-origin").as_deref()), (401, Some("*")), "the page can read why");
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 1, "a preflight never reaches an upstream");
}

/// Sends a POST whose body never arrives in full; returns the open stream.
async fn stalled_post(env: &Env) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut tcp = tokio::net::TcpStream::connect(env.addr).await.unwrap();
    let head = format!(
        "POST /rpc/r?api-key={HARNESS_KEY} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{{\"jsonrpc\""
    );
    tcp.write_all(head.as_bytes()).await.unwrap();
    tcp
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_request_body_that_never_finishes_times_out() {
    use tokio::io::AsyncReadExt;
    let mut limits = fast_limits();
    limits.client_read_timeout = ms(400);
    let env = env_with(&[0], ALL, limits).await;
    let mut tcp = stalled_post(&env).await;
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(ms(3000), tcp.read(&mut buf)).await.expect("answered in time").unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(text.starts_with("HTTP/1.1 408"), "{text}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn shutdown_finishes_despite_a_stalled_client_and_closes_websockets() {
    let mut limits = fast_limits();
    limits.client_read_timeout = Duration::from_secs(60); // the stalled body would hold shutdown for a minute
    limits.shutdown_grace = ms(500);
    let mut env = env_with(&[0], ALL, limits).await;
    let _stalled = stalled_post(&env).await;
    let mut ws = env.ws("r").await;
    ws.subscribe("slotSubscribe", json!(null)).await;

    let took = env.stop(Duration::from_secs(5)).await.expect("shutdown finished");
    assert!(took < Duration::from_secs(2), "within the grace period: {took:?}");
    let reason = ws.closed(ms(2000)).await.expect("the client WebSocket was closed");
    assert!(reason.contains("shutting down"), "{reason}");
}

