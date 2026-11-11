//! WebSockets: normal requests over a socket, and subscriptions (dedup,
//! reference counting, upstream drops, gap recovery).

use serde_json::{json, Value};

use crate::harness::*;

const STREAM: &str = r#"
[routes.s]
upstreams = ["*"]
[routes.s.methods]
"*" = {}
getBalance = { cache = { ttl = 60000 } }

[routes.only0]
upstreams = ["u0"]
[routes.only0.methods]
"*" = {}
"#;

fn slots(notes: &[Value]) -> Vec<u64> {
    notes.iter().map(|n| n["params"]["result"]["slot"].as_u64().unwrap()).collect()
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn notifications_over_a_websocket_get_no_response() {
    // As on a Solana node: nothing comes back, not even a subscribe's id,
    // but the subscription is made and its notifications flow.
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    ws.send_raw(&json!({"jsonrpc": "2.0", "method": "getLatestBlockhash"}).to_string()).await;
    ws.send_raw(&json!({"jsonrpc": "2.0", "method": "slotSubscribe"}).to_string()).await;
    let n = ws.notification(ms(5000)).await.expect("slotNotification");
    assert_eq!(n["method"], "slotNotification");
    assert!(result(&ws.call("getSlot", Value::Null).await).is_u64());
    ws.notifications_for(ms(300)).await;
    let responses: Vec<&Value> = ws.received.iter().filter(|m| m.get("id").is_some()).collect();
    assert_eq!(responses.len(), 1, "only the getSlot call is answered: {responses:?}");
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_websocket_batch_keeps_subscriptions_and_forwards_the_rest() {
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    ws.send_raw(&json!([
        {"jsonrpc": "2.0", "id": 1, "method": "slotSubscribe"},
        {"jsonrpc": "2.0", "id": 2, "method": "getLatestBlockhash"},
        {"jsonrpc": "2.0", "id": 3, "method": "getEpochInfo"}
    ]).to_string())
    .await;
    let v = ws.next_response(ms(5000)).await.expect("batch response");
    let v = v.as_array().unwrap();
    assert_eq!(v.len(), 3);
    assert!(v[0]["result"].is_u64(), "delta's own subscription id: {v:?}");
    assert!(v[1]["result"]["value"]["blockhash"].is_string());
    assert!(v[2]["result"]["absoluteSlot"].is_u64());
    assert_eq!(env.http[0].calls("batch"), 1, "the two requests went up as one batch");
    assert_eq!(env.ws[0].count("slotSubscribe"), 1);
    assert_eq!(ws.notification(ms(5000)).await.expect("slotNotification")["method"], "slotNotification");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn requests_over_a_websocket_use_the_same_pipeline() {
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    let key = pubkey();
    for _ in 0..3 {
        assert!(result(&ws.call("getBalance", json!([key])).await)["value"].is_u64());
    }
    assert_eq!(env.http[0].calls("getBalance"), 1, "cached like over HTTP");
    assert_eq!(code(&ws.call("getBalance", json!([])).await), -32602);

    ws.send_raw(&json!([
        {"jsonrpc": "2.0", "id": "x", "method": "getLatestBlockhash"},
        {"jsonrpc": "2.0", "id": "y", "method": "getEpochInfo"}
    ]).to_string())
    .await;
    let batch = ws.next_response(ms(5000)).await.unwrap();
    assert_eq!(batch[0]["id"], "x");
    assert_eq!(batch[1]["id"], "y");

    ws.send_raw("not json").await;
    assert_eq!(code(&ws.next_response(ms(5000)).await.unwrap()), -32700);
    ws.send_raw(&json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}).to_string()).await;
    let v = ws.next_response(ms(5000)).await.unwrap();
    assert!(v["result"]["value"]["blockhash"].is_string(), "the connection survives bad input: {v}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn unknown_routes_and_rejected_methods_over_a_websocket() {
    let env = env(
        &[0],
        r#"
[routes.s]
upstreams = ["*"]
[routes.s.methods]
slotSubscribe = {}
getLatestBlockhash = {}
"#,
    )
    .await;
    let mut ws = Ws::connect(&env.ws_url("nope")).await;
    assert_eq!(code(&ws.call("getLatestBlockhash", json!(null)).await), -33090);
    assert_eq!(code(&ws.call("slotSubscribe", json!(null)).await), -33090);

    let mut ws = env.ws("s").await;
    assert_eq!(code(&ws.call("rootSubscribe", json!(null)).await), -32601);
    assert_eq!(code(&ws.call("slotUnsubscribe", json!([1])).await), -32601, "unsubscribe must be accepted too");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_unknown_subscribe_is_forwarded_as_a_call() {
    // Only known subscriptions are delta's own; a name ending in Subscribe
    // is not enough.
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    for method in ["fooSubscribe", "fooUnsubscribe"] {
        ws.call(method, json!([1])).await;
        assert_eq!(env.http[0].calls(method), 1, "{method} went to the upstream over HTTP");
    }
    assert!(env.ws[0].sent_methods().is_empty(), "nothing went over an upstream WebSocket");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn slot_subscription_is_deduplicated_across_upstreams_and_clients() {
    let env = env(&[0, 0], STREAM).await;
    let mut a = env.ws("s").await;
    let mut b = env.ws("s").await;
    let mut c = env.ws("s").await;
    let ia = a.subscribe("slotSubscribe", json!(null)).await;
    let ib = b.subscribe("slotSubscribe", json!(null)).await;
    let ic = c.subscribe("slotSubscribe", json!([])).await;
    assert!(ia != ib && ib != ic, "each client gets its own id");
    assert_eq!(env.ws[0].count("slotSubscribe"), 1, "one upstream subscription per upstream");
    assert_eq!(env.ws[1].count("slotSubscribe"), 1);

    for (ws, id) in [(&mut a, ia), (&mut b, ib), (&mut c, ic)] {
        let notes = ws.notifications_for(ms(1500)).await;
        assert!(notes.len() >= 5, "{}", notes.len());
        assert!(notes.iter().all(|n| n["method"] == "slotNotification" && n["params"]["subscription"] == id));
        let s = slots(&notes);
        assert!(s.windows(2).all(|w| w[0] < w[1]), "each slot once, in order (two upstreams send each): {s:?}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_last_unsubscribe_closes_the_upstream_subscription() {
    let env = env(&[0], STREAM).await;
    let mut a = env.ws("s").await;
    let mut b = env.ws("s").await;
    let ia = a.subscribe("slotSubscribe", json!(null)).await;
    let ib = b.subscribe("slotSubscribe", json!(null)).await;

    assert_eq!(result(&a.call("slotUnsubscribe", json!([ia])).await), true);
    tokio::time::sleep(ms(300)).await;
    assert_eq!(env.ws[0].count("slotUnsubscribe"), 0, "b still holds it");
    assert!(!b.notifications_for(ms(500)).await.is_empty());
    a.notifications_for(ms(200)).await;
    assert!(a.notifications_for(ms(500)).await.is_empty(), "a no longer receives");

    // Closing the connection detaches its subscriptions too.
    let _ = ib;
    b.close().await;
    assert!(eventually(ms(3000), || async { env.ws[0].count("slotUnsubscribe") == 1 }).await);
    assert!(eventually(ms(3000), || async { env.ws[0].live() == 0 }).await, "nothing left: the upstream socket is closed");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn unsubscribe_checks_owner_and_kind() {
    let env = env(&[0], STREAM).await;
    let mut a = env.ws("s").await;
    let mut b = env.ws("s").await;
    let id = a.subscribe("slotSubscribe", json!(null)).await;
    assert_eq!(code(&b.call("slotUnsubscribe", json!([id])).await), -32602, "another connection's id");
    assert_eq!(code(&a.call("rootUnsubscribe", json!([id])).await), -32602, "wrong kind");
    assert_eq!(code(&a.call("slotUnsubscribe", json!([id + 1000])).await), -32602, "unknown id");
    assert_eq!(code(&a.call("slotUnsubscribe", json!(null)).await), -32602, "no id");
    assert_eq!(result(&a.call("slotUnsubscribe", json!([id])).await), true);
    assert_eq!(code(&a.call("slotUnsubscribe", json!([id])).await), -32602, "twice");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn equivalent_params_share_one_upstream_subscription() {
    let env = env(&[0], STREAM).await;
    let key = pubkey();
    let mut a = env.ws("s").await;
    let mut b = env.ws("s").await;
    a.subscribe("accountSubscribe", json!([key, {"encoding": "base64", "commitment": "confirmed"}])).await;
    b.subscribe("accountSubscribe", json!([key, {"commitment": "confirmed", "encoding": "base64"}])).await;
    assert_eq!(env.ws[0].count("accountSubscribe"), 1, "key order does not matter");
    b.subscribe("accountSubscribe", json!([key, {"commitment": "confirmed", "encoding": "jsonParsed"}])).await;
    assert_eq!(env.ws[0].count("accountSubscribe"), 2, "different params, different subscription");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn account_notifications_reach_every_subscriber() {
    let env = env(&[0, 0], STREAM).await;
    let key = pubkey();
    let mut a = env.ws("s").await;
    let mut b = env.ws("s").await;
    let params = json!([key, {"encoding": "base64", "commitment": "confirmed"}]);
    let ia = a.subscribe("accountSubscribe", params.clone()).await;
    let ib = b.subscribe("accountSubscribe", params).await;
    result(&env.call("s", "requestAirdrop", json!([key, 1_000_000_000])).await);
    for (ws, id) in [(&mut a, ia), (&mut b, ib)] {
        let n = ws.notification(ms(10_000)).await.expect("account notification");
        assert_eq!(n["method"], "accountNotification");
        assert_eq!(n["params"]["subscription"], id);
        assert_eq!(n["params"]["result"]["value"]["lamports"], 1_000_000_000);
        assert!(ws.notifications_for(ms(800)).await.is_empty(), "the second upstream's copy is dropped");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn signature_subscription_ends_after_its_notification() {
    let env = env(&[0], STREAM).await;
    let key = pubkey();
    let sig = result(&env.call("s", "requestAirdrop", json!([key, 1_000_000_000])).await).as_str().unwrap().to_owned();
    let mut ws = env.ws("s").await;
    let id = ws.subscribe("signatureSubscribe", json!([sig, {"commitment": "confirmed"}])).await;
    let n = ws.notification(ms(15_000)).await.expect("signature notification");
    assert_eq!(n["method"], "signatureNotification");
    assert_eq!(n["params"]["subscription"], id);
    assert!(n["params"]["result"]["value"]["err"].is_null());
    assert_eq!(code(&ws.call("signatureUnsubscribe", json!([id])).await), -32602, "already finished");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn upstream_subscribe_errors_are_returned() {
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    let v = ws.call("accountSubscribe", json!(["not-a-pubkey"])).await;
    assert!(v.get("error").is_some(), "{v}");
    // The failed subscription left nothing behind.
    let id = ws.subscribe("slotSubscribe", json!(null)).await;
    assert!(ws.notification(ms(3000)).await.is_some_and(|n| n["params"]["subscription"] == id));
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_subscription_survives_one_upstream_dropping() {
    let env = env(&[0, 0], STREAM).await;
    let mut ws = env.ws("s").await;
    ws.subscribe("slotSubscribe", json!(null)).await;
    ws.notifications_for(ms(500)).await;
    env.ws[0].refuse(true);
    env.ws[0].cut();
    let s = slots(&ws.notifications_for(ms(1500)).await);
    assert!(s.len() >= 8, "u1 keeps it going: {s:?}");
    assert!(s.windows(2).all(|w| w[1] == w[0] + 1), "no gap: {s:?}");

    // u0 comes back: the proxy reconnects and resubscribes on its own.
    env.ws[0].refuse(false);
    assert!(eventually(ms(10_000), || async { env.ws[0].count("slotSubscribe") >= 2 }).await, "resubscribed");
    let s = slots(&ws.notifications_for(ms(1000)).await);
    assert!(s.windows(2).all(|w| w[0] < w[1]), "still once per slot: {s:?}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn state_subscriptions_recover_the_current_value_after_a_gap() {
    let env = env(&[0], STREAM).await;
    let key = pubkey();
    let mut ws = env.ws("only0").await;
    let id = ws
        .subscribe("accountSubscribe", json!([key, {"encoding": "base64", "commitment": "confirmed"}]))
        .await;
    // Every upstream of the subscription goes away; the account changes
    // meanwhile; the upstream comes back.
    env.ws[0].refuse(true);
    env.ws[0].cut();
    tokio::time::sleep(ms(300)).await;
    let sig = env.chain(0).airdrop(&key, 3_000_000_000).await;
    env.chain(0).wait_landed(&sig).await;
    tokio::time::sleep(ms(500)).await;
    env.ws[0].refuse(false);
    let n = ws.notification(ms(15_000)).await.expect("the current value after reconnecting");
    assert_eq!(n["params"]["subscription"], id);
    assert_eq!(n["params"]["result"]["value"]["lamports"], 3_000_000_000u64);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn event_streams_close_the_client_after_a_gap() {
    // programSubscribe is an event stream too: each notification is a
    // different account, so nothing can be rebuilt after a gap.
    for (method, params) in [("logsSubscribe", json!(["all"])), ("programSubscribe", json!(["11111111111111111111111111111111"]))] {
        let env = env(&[0], STREAM).await;
        let mut ws = env.ws("only0").await;
        ws.subscribe(method, params).await;
        env.ws[0].refuse(true);
        env.ws[0].cut();
        tokio::time::sleep(ms(300)).await;
        env.ws[0].refuse(false);
        let reason = ws.closed(ms(15_000)).await.expect("closed so the client knows it missed events");
        assert!(reason.contains(method) && reason.contains("gap"), "{reason}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_event_stream_is_closed_only_once_every_upstream_was_down() {
    let env = env(&[0, 0], STREAM).await;
    let mut ws = env.ws("s").await;
    ws.subscribe("logsSubscribe", json!(["all"])).await;
    // The subscribe answers once the first upstream confirms; cutting u0
    // before u1 has confirmed would leave a moment with neither active,
    // which is a real gap.
    assert!(eventually(ms(3000), || async { env.ws[1].sub_id("logsSubscribe").is_some() }).await);
    tokio::time::sleep(ms(200)).await;
    env.ws[0].refuse(true);
    env.ws[0].cut();
    let early = ws.closed(ms(2500)).await;
    assert!(early.is_none(), "u1 still carries the stream: no gap; closed with {early:?}");
    assert!(result(&ws.call("getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());

    env.ws[1].refuse(true);
    env.ws[1].cut();
    tokio::time::sleep(ms(300)).await;
    env.ws[0].refuse(false);
    let reason = ws.closed(ms(15_000)).await.expect("both were down: events may be lost");
    assert!(reason.contains("gap"), "{reason}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn subscribing_with_every_upstream_down_fails() {
    let env = env(&[0, 0], STREAM).await;
    env.ws[0].refuse(true);
    env.ws[1].refuse(true);
    let mut ws = env.ws("s").await;
    let v = ws.call("slotSubscribe", json!(null)).await;
    assert_eq!(code(&v), -33095, "{v}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn stale_and_duplicate_notifications_are_dropped() {
    let env = env(&[0, 0], STREAM).await;
    let key = pubkey();
    let mut ws = env.ws("s").await;
    let id = ws.subscribe("accountSubscribe", json!([key, {"encoding": "base64"}])).await;
    let up0 = env.ws[0].sub_id("accountSubscribe").unwrap();
    let up1 = env.ws[1].sub_id("accountSubscribe").unwrap();
    let note = |sub: u64, slot: u64, lamports: u64| {
        json!({"jsonrpc": "2.0", "method": "accountNotification", "params": {"subscription": sub, "result": {
            "context": {"slot": slot},
            "value": {"lamports": lamports, "data": ["", "base64"], "owner": "11111111111111111111111111111111", "executable": false, "rentEpoch": 0, "space": 0}
        }}})
        .to_string()
    };
    env.ws[0].inject(&note(up0, 1_000_000, 5));
    let n = ws.notification(ms(3000)).await.unwrap();
    assert_eq!((n["params"]["subscription"].clone(), n["params"]["result"]["value"]["lamports"].clone()), (json!(id), json!(5)));
    env.ws[1].inject(&note(up1, 1_000_000, 5));
    env.ws[1].inject(&note(up1, 999_999, 4));
    assert!(ws.notifications_for(ms(500)).await.is_empty(), "a duplicate and an older slot");
    env.ws[1].inject(&note(up1, 1_000_001, 6));
    let n = ws.notification(ms(3000)).await.unwrap();
    assert_eq!(n["params"]["result"]["value"]["lamports"], 6);
    env.ws[1].inject(&note(up1 + 999, 1_000_002, 7));
    assert!(ws.notifications_for(ms(500)).await.is_empty(), "an unknown upstream subscription id");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn routes_with_different_upstreams_share_and_release_subscriptions_correctly() {
    let env = env(&[0, 0], STREAM).await;
    let mut wide = env.ws("s").await; // u0 + u1
    let mut narrow = env.ws("only0").await; // u0
    wide.subscribe("slotSubscribe", json!(null)).await;
    narrow.subscribe("slotSubscribe", json!(null)).await;
    assert_eq!((env.ws[0].count("slotSubscribe"), env.ws[1].count("slotSubscribe")), (1, 1), "one per upstream, shared");

    // Data from u1 reaches only the route that lists u1.
    let up1 = env.ws[1].sub_id("slotSubscribe").unwrap();
    let far = 1_000_000_000u64;
    env.ws[1].inject(&json!({"jsonrpc": "2.0", "method": "slotNotification",
        "params": {"subscription": up1, "result": {"slot": far, "parent": far - 1, "root": far - 32}}}).to_string());
    let seen = |notes: Vec<Value>| notes.iter().any(|n| n["params"]["result"]["slot"] == far);
    assert!(seen(wide.notifications_for(ms(600)).await));
    assert!(!seen(narrow.notifications_for(ms(600)).await), "u1 is not an upstream of only0");

    // The wide route leaving releases u1 but keeps u0 for the narrow one.
    wide.close().await;
    assert!(eventually(ms(3000), || async { env.ws[1].count("slotUnsubscribe") == 1 }).await);
    assert_eq!(env.ws[0].count("slotUnsubscribe"), 0);
    assert!(narrow.notifications_for(ms(800)).await.len() >= 3, "narrow keeps receiving");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_client_leaving_before_the_upstream_confirms_leaves_nothing_behind() {
    let env = env(&[0], STREAM).await;
    let key = pubkey();
    let params = json!([key, {"encoding": "base64", "commitment": "confirmed"}]);
    env.ws[0].hold(true);
    let mut early = env.ws("only0").await;
    early.send_raw(&json!({"jsonrpc": "2.0", "id": 1, "method": "accountSubscribe", "params": params}).to_string()).await;
    assert!(eventually(ms(3000), || async { env.ws[0].count("accountSubscribe") == 1 }).await);
    early.close().await;
    tokio::time::sleep(ms(300)).await;
    env.ws[0].hold(false);
    assert!(
        eventually(ms(3000), || async { env.ws[0].count("accountUnsubscribe") == 1 }).await,
        "the late confirmation is unsubscribed: {:?}",
        env.ws[0].sent_methods()
    );

    // The same subscription works for the next client.
    let mut ws = env.ws("only0").await;
    let id = ws.subscribe("accountSubscribe", params).await;
    result(&env.call("only0", "requestAirdrop", json!([key, 1_000_000_000])).await);
    let n = ws.notification(ms(10_000)).await.expect("notification");
    assert_eq!(n["params"]["subscription"], id);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_signature_that_lands_during_an_outage_is_still_reported_once() {
    let env = env(&[0], STREAM).await;
    let sig = env.chain(0).airdrop(&pubkey(), 1_000_000_000).await;
    let mut ws = env.ws("only0").await;
    let id = ws.subscribe("signatureSubscribe", json!([sig, {"commitment": "finalized"}])).await;
    env.ws[0].refuse(true);
    env.ws[0].cut();
    env.chain(0).wait_finalized(&sig).await;
    env.ws[0].refuse(false);
    let n = ws.notification(ms(15_000)).await.expect("recovered after reconnecting");
    assert_eq!(n["params"]["subscription"], id);
    assert!(n["params"]["result"]["value"]["err"].is_null());
    assert!(ws.notifications_for(ms(1500)).await.is_empty(), "exactly once");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_failed_recovery_read_closes_the_client() {
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("only0").await;
    ws.subscribe("accountSubscribe", json!([pubkey(), {"encoding": "base64"}])).await;
    env.ws[0].refuse(true);
    env.ws[0].cut();
    env.http[0].on_method("getAccountInfo", Fault::Status(500));
    tokio::time::sleep(ms(300)).await;
    env.ws[0].refuse(false);
    let reason = ws.closed(ms(15_000)).await.expect("closed rather than silently missing changes");
    assert!(reason.contains("accountSubscribe gap"), "{reason}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_upstream_gets_more_connections_at_its_subscription_limit() {
    let env = env_upstreams(&[0], "", "maxSubscriptionsPerConnection = 2, maxConnections = 2", STREAM, fast_limits()).await;
    let mut ws = env.ws("s").await;
    let keys: Vec<String> = (0..5).map(|_| pubkey()).collect();
    let account = |k: &str| json!([k, {"encoding": "base64", "commitment": "confirmed"}]);
    let mut ids = Vec::new();
    for k in &keys[..4] {
        ids.push(ws.subscribe("accountSubscribe", account(k)).await);
    }
    ws.subscribe("accountSubscribe", account(&keys[0])).await; // the same subscription again: shared, no room used
    assert_eq!(env.ws[0].accepted(), 2, "two subscriptions per connection: a second connection");

    let v = ws.call("accountSubscribe", account(&keys[4])).await;
    assert_eq!(code(&v), -33096, "both connections full and maxConnections reached: {v}");

    // A subscription on the second connection works.
    result(&env.call("s", "requestAirdrop", json!([keys[3], ONE])).await);
    let n = ws.notification(ms(10_000)).await.expect("notification from the second connection");
    assert_eq!(n["params"]["subscription"], ids[3]);

    // Freeing one (not keys[0]: it is still held by the second subscribe)
    // makes room again.
    assert_eq!(result(&ws.call("accountUnsubscribe", json!([ids[1]])).await), true);
    ws.notifications_for(ms(300)).await;
    let v = ws.call("accountSubscribe", account(&keys[4])).await;
    assert!(v["result"].is_u64(), "room on a connection again: {v}");
    assert_eq!(env.ws[0].accepted(), 2, "no third connection");
}

const ONE: u64 = 1_000_000_000;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_subscribe_response_arrives_before_its_first_notification() {
    // The upstream confirms and notifies back to back, as Agave does for a
    // signature that is already confirmed. web3.js drops notifications for
    // ids it has not been told yet, so the response must come first.
    let env = env(&[0], STREAM).await;
    let key = pubkey();
    env.ws[0].hold(true);
    let mut ws = env.ws("only0").await;
    ws.send_raw(&json!({"jsonrpc": "2.0", "id": 7, "method": "accountSubscribe", "params": [key, {"encoding": "base64"}]}).to_string())
        .await;
    assert!(eventually(ms(3000), || async { env.ws[0].sub_id("accountSubscribe").is_some() }).await);
    let up = env.ws[0].sub_id("accountSubscribe").unwrap();
    env.ws[0].inject(&json!({"jsonrpc": "2.0", "method": "accountNotification", "params": {"subscription": up, "result": {
        "context": {"slot": 1_000_000},
        "value": {"lamports": 5, "data": ["", "base64"], "owner": SYSTEM_PROGRAM, "executable": false, "rentEpoch": 0, "space": 0}
    }}}).to_string());
    env.ws[0].hold(false);

    let response = ws.response_for(&json!(7), ms(5000)).await.expect("response");
    let id = result(&response).as_u64().unwrap();
    let note = ws.notification(ms(5000)).await;
    assert!(note.is_some(), "no notification; received: {:?}", ws.received);
    assert_eq!(note.unwrap()["params"]["subscription"], id);
    let first = &ws.received[0];
    assert_eq!(first["id"], 7, "the response came first: {:?}", ws.received);
}

fn logs_note(sub: u64, slot: u64, sig: &str) -> String {
    json!({"jsonrpc": "2.0", "method": "logsNotification", "params": {"subscription": sub, "result": {
        "context": {"slot": slot}, "value": {"signature": sig, "err": null, "logs": []}
    }}})
    .to_string()
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn event_streams_keep_late_events_and_drop_duplicates() {
    let env = env(&[0, 0], STREAM).await;
    let mut ws = env.ws("s").await;
    ws.subscribe("logsSubscribe", json!(["all"])).await;
    let (a, b) = (env.ws[0].sub_id("logsSubscribe").unwrap(), env.ws[1].sub_id("logsSubscribe").unwrap());
    let base = 5_000_000;
    let sigs = |n: usize| -> Vec<String> { (0..n).map(|_| fake_signature()).collect() };

    // u0 is ahead; u1 then sends the same event and one u0 never had.
    let (seen, late) = (fake_signature(), fake_signature());
    env.ws[0].inject(&logs_note(a, base + 1, &seen));
    tokio::time::sleep(ms(100)).await;
    env.ws[1].inject(&logs_note(b, base + 1, &seen));
    env.ws[1].inject(&logs_note(b, base, &late));
    let got: Vec<String> = ws.notifications_for(ms(600)).await.iter().map(|n| n["params"]["result"]["value"]["signature"].as_str().unwrap().to_owned()).collect();
    assert_eq!(got, [seen.clone(), late], "the duplicate is dropped, the late event is not");

    // A busy slot does not let duplicates through.
    let busy = sigs(300);
    for s in &busy {
        env.ws[0].inject(&logs_note(a, base + 2, s));
    }
    assert_eq!(ws.notifications_for(ms(1500)).await.len(), 300);
    env.ws[1].inject(&logs_note(b, base + 2, &busy[0]));
    assert!(ws.notifications_for(ms(500)).await.is_empty(), "300 events later, still a duplicate");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_websocket_has_a_bounded_number_of_messages_in_flight() {
    let mut limits = fast_limits();
    limits.ws_in_flight = 2;
    let env = env_with(&[0], STREAM, limits).await;
    env.http[0].on_method("getLatestBlockhash", Fault::Delay(ms(300)));
    let mut ws = env.ws("only0").await; // no cache: identical requests are not merged
    for i in 0..6 {
        ws.send_raw(&json!({"jsonrpc": "2.0", "id": i, "method": "getLatestBlockhash"}).to_string()).await;
    }
    for i in 0..6 {
        let v = ws.response_for(&json!(i), ms(10_000)).await.expect("every message is answered");
        assert!(v["result"]["value"]["blockhash"].is_string(), "{v}");
    }
    assert_eq!(env.http[0].peak(), 2, "never more than 2 of this client's messages in flight");
}


/// The keep-alive web3.js and @solana/kit send every 5 s.
const KEEPALIVE: &str = r#"{"jsonrpc":"2.0","method":"ping"}"#;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn keepalive_pings_are_answered_by_delta() {
    let key = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d";
    let env = env(&[0], &format!("{STREAM}\n[apiKeys.k]\nkey = \"{key}\"\nroutes = [\"*\"]\ntotalRps = 2\n")).await;
    let mut ws = Ws::connect(&format!("{}?api-key={key}", env.ws_url("s"))).await;
    for _ in 0..10 {
        ws.send_raw(KEEPALIVE).await;
    }
    assert!(result(&ws.call("getSlot", Value::Null).await).is_u64(), "pings are not charged: the call still fits totalRps = 2");
    ws.notifications_for(ms(200)).await;
    let responses = ws.received.iter().filter(|m| m.get("id").is_some()).count();
    assert_eq!(responses, 1, "nothing answers a ping: {:?}", ws.received);
    assert_eq!(env.http[0].calls("ping"), 0, "never sent upstream, though the route accepts \"*\"");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_ping_with_an_id_is_a_normal_request() {
    let env = env(&[0], STREAM).await;
    let mut ws = env.ws("s").await;
    let v = ws.call("ping", Value::Null).await;
    assert!(v.get("result").is_some() || v.get("error").is_some(), "answered: {v}");
    assert_eq!(env.http[0].calls("ping"), 1, "forwarded like any method");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_idle_connection_is_closed() {
    let mut limits = fast_limits();
    limits.ws_idle_timeout = ms(800);
    let env = env_with(&[0], STREAM, limits).await;
    let mut ws = env.ws("only0").await;
    let reason = ws.closed(ms(3000)).await.expect("closed after 800 ms without traffic");
    assert!(reason.contains("idle") && reason.contains("ping"), "{reason}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn keepalives_and_notifications_keep_a_connection_open() {
    let mut limits = fast_limits();
    limits.ws_idle_timeout = ms(800);
    let env = env_with(&[0], STREAM, limits).await;

    let mut pinging = env.ws("only0").await;
    for _ in 0..5 {
        pinging.send_raw(KEEPALIVE).await;
        tokio::time::sleep(ms(300)).await;
    }
    assert!(result(&pinging.call("getSlot", Value::Null).await).is_u64(), "open after 1.5 s of pings");

    let mut subscribed = env.ws("only0").await;
    subscribed.subscribe("slotSubscribe", json!([])).await;
    assert!(subscribed.closed(ms(2000)).await.is_none(), "slot notifications keep it busy");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_idle_close_releases_only_that_clients_share_of_a_subscription() {
    let mut limits = fast_limits();
    limits.ws_idle_timeout = ms(800);
    let env = env_with(&[0], STREAM, limits).await;
    let key = pubkey();
    let params = json!([key, {"encoding": "base64", "commitment": "confirmed"}]);
    let mut quiet = env.ws("only0").await;
    let mut alive = env.ws("only0").await;
    quiet.subscribe("accountSubscribe", params.clone()).await;
    let id = alive.subscribe("accountSubscribe", params).await;
    assert_eq!(env.ws[0].count("accountSubscribe"), 1, "one upstream subscription, shared");

    // `alive` keeps pinging; `quiet` sends nothing and its account never changes.
    let reason = loop {
        alive.send_raw(KEEPALIVE).await;
        if let Some(reason) = quiet.closed(ms(300)).await {
            break reason;
        }
    };
    assert!(reason.contains("idle"), "{reason}");
    for _ in 0..3 {
        alive.send_raw(KEEPALIVE).await;
        tokio::time::sleep(ms(200)).await;
    }
    assert_eq!(env.ws[0].count("accountUnsubscribe"), 0, "the other client still holds it");

    result(&env.call("only0", "requestAirdrop", json!([key, 1_000_000_000])).await);
    let n = alive.notification(ms(10_000)).await.expect("still notified");
    assert_eq!(n["params"]["subscription"], id);

    // Once the last holder goes idle too, the upstream subscription goes.
    assert!(alive.closed(ms(3000)).await.is_some());
    assert!(eventually(ms(3000), || async { env.ws[0].count("accountUnsubscribe") == 1 }).await);
}

