//! Access logs: one line per request, refusal and subscription, with what
//! happened to it, and never a secret or a request's params.

use serde_json::json;

use crate::harness::*;

const ROUTES: &str = r#"
[routes.fast]
upstreams = ["*"]
mode = { type = "hedge", upstreams = 2 }
cache = { ttl = 60000 }
[routes.fast.methods]
"*" = {}

[routes.safe]
upstreams = ["*"]
mode = { type = "quorum", threshold = 2, upstreams = 2 }
[routes.safe.methods]
getBalance = {}
"#;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn every_request_gets_one_line_with_what_happened() {
    let (logs, _guard) = Logs::capture();
    let env = env(&[0, 0], ROUTES).await;
    let key = pubkey();

    env.call("fast", "getBalance", json!([key])).await;
    let line = logs.matching(&["delta::access", "method=getBalance"]);
    assert_eq!(line.len(), 1, "{line:?}");
    for part in ["via=http", "key=harness", "ip=127.0.0.1", "route=fast", "outcome=ok", "cache=miss", "calls=2"] {
        assert!(line[0].contains(part), "missing {part}: {}", line[0]);
    }
    assert!(line[0].contains("upstream=u0") || line[0].contains("upstream=u1"), "{}", line[0]);
    // Each upstream call has its own debug line.
    assert!(eventually(ms(2000), || async { logs.matching(&["delta::upstream", "method=getBalance"]).len() == 2 }).await);

    logs.clear();
    env.call("fast", "getBalance", json!([key])).await;
    let line = logs.matching(&["delta::access", "method=getBalance"]);
    assert!(line[0].contains("cache=hit") && line[0].contains("calls=0"), "{}", line[0]);

    // Errors carry their code and message; a refused quorum names no upstream.
    env.http[1].on_method("getBalance", rewrite(|v| v["result"]["value"] = json!(42)));
    env.call("safe", "getBalance", json!([pubkey()])).await;
    let line = logs.matching(&["route=safe"]);
    assert!(line[0].contains("outcome=error") && line[0].contains("code=-33094") && line[0].contains("upstream=-"), "{}", line[0]);

    // Batches: one line per element; requests that never reach a route too.
    logs.clear();
    env.client
        .json("/rpc/fast", &json!([{"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}, {"jsonrpc": "2.0", "id": 2}]), &[])
        .await;
    env.call("nope", "getLatestBlockhash", json!(null)).await;
    let access = logs.matching(&["delta::access"]);
    assert_eq!(access.len(), 3, "{access:?}");
    assert!(access.iter().any(|l| l.contains("method=-") && l.contains("code=-32600")));
    assert!(access.iter().any(|l| l.contains("route=nope") && l.contains("code=-33090")));
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn refusals_are_logged_without_the_key() {
    let (logs, _guard) = Logs::capture();
    let env = env(&[0, 0], ROUTES).await;
    let wrong = "00000000-0000-4000-8000-00000000dead";
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}).to_string();
    env.client.raw(&format!("/rpc/fast?api-key={wrong}"), body.clone(), &[]).await;
    env.client.raw("/rpc/fast/x", body, &[]).await;
    let refused = logs.matching(&["outcome=refused"]);
    assert_eq!(refused.len(), 2, "{refused:?}");
    assert!(refused[0].contains("status=401") && refused[0].contains("key=-") && refused[0].contains("path=/rpc/fast"), "{}", refused[0]);
    assert!(refused[1].contains("status=404") && refused[1].contains("key=harness"), "{}", refused[1]);
    let all = logs.lines().join("\n");
    assert!(!all.contains(wrong) && !all.contains(HARNESS_KEY), "no key value is ever logged");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn params_and_upstream_urls_are_never_logged() {
    let (logs, _guard) = Logs::capture();
    let env = env(&[0, 0], ROUTES).await;
    let secret_param = pubkey();
    env.call("fast", "getBalance", json!([secret_param])).await;
    env.http[0].set(Fault::Status(500));
    env.http[1].set(Fault::Drop);
    env.call("fast", "getAccountInfo", json!([secret_param])).await;
    let all = logs.lines().join("\n");
    assert!(all.contains("method=getAccountInfo"), "{all}");
    assert!(!all.contains(&secret_param), "params stay out of the logs");
    for shim in &env.http {
        assert!(!all.contains(&shim.url()) && !all.contains(&shim.addr.to_string()), "upstream URLs stay out of the logs");
    }
    assert!(all.contains("HTTP 500"), "the failure reason is logged: {all}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn websockets_log_the_connection_and_each_message() {
    let (logs, _guard) = Logs::capture();
    let env = env(&[0, 0], ROUTES).await;
    let mut ws = env.ws("fast").await;
    let id = ws.subscribe("slotSubscribe", json!(null)).await;
    ws.call("getLatestBlockhash", json!(null)).await;
    ws.call("slotUnsubscribe", json!([id])).await;
    ws.close().await;
    assert!(eventually(ms(2000), || async { !logs.matching(&["websocket closed"]).is_empty() }).await);

    let opened = logs.matching(&["websocket opened"]);
    assert_eq!(opened.len(), 1);
    let via = opened[0].split_whitespace().find(|p| p.starts_with("via=ws/")).expect("via=ws/<n>").to_owned();
    for method in ["slotSubscribe", "getLatestBlockhash", "slotUnsubscribe"] {
        let line = logs.matching(&[&via, &format!("method={method}")]);
        assert!(line.len() == 1 && line[0].contains("outcome=ok"), "{method}: {line:?}");
    }
    let closed = logs.matching(&["websocket closed"]);
    assert!(closed[0].contains(&via) && closed[0].contains("messages=3"), "{}", closed[0]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn upstream_error_messages_are_never_logged() {
    // An upstream error message can echo the request's parameters.
    let (logs, _guard) = Logs::capture();
    let env = env(&[0, 0], ROUTES).await;
    for shim in &env.http {
        shim.set(Fault::RpcError(-32602, "Invalid param: PRIVATE_PARAM_MARKER"));
    }
    let v = env.call("fast", "getBalance", json!([pubkey()])).await;
    assert!(message(&v).contains("PRIVATE_PARAM_MARKER"), "the client still gets it: {v}");
    let line = logs.matching(&["delta::access", "method=getBalance"]);
    assert!(line[0].contains("code=-32602") && line[0].contains("error=-"), "{}", line[0]);
    assert!(!logs.lines().join("\n").contains("PRIVATE_PARAM_MARKER"));

    // delta's own messages are still logged.
    env.call("nope", "getBalance", json!([pubkey()])).await;
    assert!(!logs.matching(&["code=-33090", "error=unknown route"]).is_empty());
}

