//! Retry policy: which failures are retried, delays, merging, deadlines, and
//! retry around every mode.

use std::time::Instant;

use serde_json::{json, Value};

use crate::harness::*;

fn retry_route(attempts: u32, delay: u64) -> String {
    format!(
        r#"
[routes.r]
upstreams = ["*"]
retry = {{ attempts = {attempts}, delay = {delay} }}
loadBalancing = {{ type = "round-robin" }}
[routes.r.methods]
"*" = {{}}
"#
    )
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn retries_until_success() {
    let env = env(&[0], &retry_route(3, 0)).await;
    env.http[0].once([Fault::Status(500), Fault::Status(502)]);
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 3);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn gives_up_after_the_attempts() {
    let env = env(&[0], &retry_route(2, 0)).await;
    env.http[0].set(Fault::Status(503));
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -33095);
    assert!(message(&v).contains("u0: HTTP 503"), "{v}");
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 3, "1 call + 2 retries");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn waits_the_delay_between_attempts() {
    let env = env(&[0], &retry_route(2, 250)).await;
    env.http[0].set(Fault::Status(500));
    let t = Instant::now();
    env.call("r", "getLatestBlockhash", json!(null)).await;
    assert!(t.elapsed() >= ms(500), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn every_transport_failure_is_retried() {
    let env = env(&[0], &retry_route(5, 0)).await;
    env.http[0].once([Fault::Drop, Fault::Garbage, Fault::Status(500), Fault::TooMany(None), Fault::Status(401)]);
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 6);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn per_call_timeout_is_retried() {
    let mut limits = fast_limits();
    limits.per_call_timeout = ms(300);
    let env = env_with(&[0], &retry_route(1, 0), limits).await;
    env.http[0].once([Fault::Hang]);
    let t = Instant::now();
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert!(t.elapsed() >= ms(300) && t.elapsed() < ms(2000), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn node_errors_are_retried_and_the_last_one_returned() {
    let env = env(&[0], &retry_route(2, 0)).await;
    env.http[0].set(Fault::RpcError(-32005, "Node is behind by 42 slots"));
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -32005, "the upstream's error, not -33095");
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 3);

    for c in [-32004, -32016] {
        env.http[0].heal();
        env.http[0].once([Fault::RpcError(c, "retryable")]);
        assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string(), "{c}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_retry_can_go_to_another_upstream() {
    let env = env(&[0, 0], &retry_route(1, 0)).await;
    env.http[0].set(Fault::Status(500));
    for _ in 0..8 {
        assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    }
    assert_eq!(env.http[1].calls("getLatestBlockhash"), 8);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn method_overrides_merge_and_false_turns_retry_off() {
    let env = env(
        &[0],
        r#"
[routes.r]
upstreams = ["*"]
retry = { attempts = 1, delay = 0 }
[routes.r.methods]
getLatestBlockhash  = {}
getEpochInfo = { retry = { attempts = 3 } }
getSlot    = { retry = false }
"#,
    )
    .await;
    env.http[0].set(Fault::Status(500));
    for m in ["getLatestBlockhash", "getEpochInfo", "getSlot"] {
        assert_eq!(code(&env.call("r", m, json!(null)).await), -33095);
    }
    assert_eq!(env.calls("getLatestBlockhash"), vec![2], "the route's retry");
    assert_eq!(env.calls("getEpochInfo"), vec![4], "attempts overridden, delay inherited");
    assert_eq!(env.calls("getSlot"), vec![1], "retry = false");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_request_deadline_stops_retries() {
    let mut limits = fast_limits();
    limits.request_deadline = ms(1000);
    let env = env_with(&[0], &retry_route(50, 200), limits).await;
    env.http[0].set(Fault::Status(500));
    let t = Instant::now();
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -33095);
    assert!(t.elapsed() < ms(1500), "{:?}", t.elapsed());
    assert!(env.http[0].calls("getLatestBlockhash") <= 7);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_deadline_cuts_a_hanging_call() {
    let mut limits = fast_limits();
    limits.request_deadline = ms(800);
    let env = env_with(&[0], &retry_route(3, 0), limits).await;
    env.http[0].set(Fault::Hang);
    let t = Instant::now();
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -33095);
    assert!(message(&v).contains("deadline"), "{v}");
    assert!(t.elapsed() < ms(1300), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn writes_are_retried() {
    let env = env(&[0], &retry_route(2, 0)).await;
    env.http[0].once([Fault::Status(503)]);
    let key = pubkey();
    let sig = env.call("r", "requestAirdrop", json!([key, 1_000_000_000])).await;
    let sig = result(&sig).as_str().unwrap().to_owned();
    env.chain(0).wait_landed(&sig).await;
    assert_eq!(env.http[0].calls("requestAirdrop"), 2);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_reply_with_neither_result_nor_error_is_retried_not_null() {
    let env = env(&[0, 0], &retry_route(1, 0)).await;
    let empty = || rewrite(|v| *v = json!({"jsonrpc": "2.0", "id": 1}));
    env.http[0].set(empty());
    for _ in 0..4 {
        assert!(result(&env.call("r", "getBalance", json!([pubkey()])).await)["value"].is_u64());
    }
    assert!(env.http[1].calls("getBalance") >= 2, "u1 answered: {:?}", env.calls("getBalance"));
}


fn mode_route(mode: &str, attempts: u32) -> String {
    format!(
        r#"
[routes.r]
upstreams = ["*"]
mode = {mode}
retry = {{ attempts = {attempts}, delay = 0 }}
loadBalancing = {{ type = "round-robin" }}
[routes.r.methods]
"*" = {{}}
"#
    )
}

const HEDGE_2: &str = r#"{ type = "hedge", fanout = 2 }"#;
const QUORUM_2_OF_2: &str = r#"{ type = "quorum", threshold = 2, fanout = 2 }"#;

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_hedge_is_raced_again_when_every_upstream_failed() {
    let env = env(&[0, 0], &mode_route(HEDGE_2, 1)).await;
    env.http[0].once([Fault::Status(500)]);
    env.http[1].once([Fault::Status(503)]);
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert_eq!(env.calls("getLatestBlockhash"), vec![2, 2], "two races of two");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_hedge_with_one_success_is_not_retried() {
    let env = env(&[0, 0], &mode_route(HEDGE_2, 3)).await;
    env.http[0].set(Fault::Status(500));
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert_eq!(env.calls("getLatestBlockhash"), vec![1, 1]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_hedge_that_failed_every_attempt_returns_the_last_node_error() {
    let env = env(&[0, 0], &mode_route(HEDGE_2, 2)).await;
    env.http[0].set(Fault::RpcError(-32005, "Node is behind by 42 slots"));
    env.http[1].set(Fault::Status(500));
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -32005, "{v}");
    assert_eq!(env.calls("getLatestBlockhash"), vec![3, 3]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_final_node_error_is_never_retried_in_any_mode() {
    for mode in [r#"{ type = "single" }"#, HEDGE_2, QUORUM_2_OF_2] {
        let env = env(&[0, 0], &mode_route(mode, 3)).await;
        // Surfpool's own -32602 for missing params: every node would repeat it.
        let v = env.call("r", "getBalance", json!([])).await;
        assert_eq!(code(&v), -32602, "{mode}: {v}");
        assert!(env.total_calls("getBalance") <= 2, "{mode}: {:?}", env.calls("getBalance"));
    }
}

fn bump_value(v: &mut Value) {
    let n = v["result"]["value"].as_u64().unwrap_or(0);
    v["result"]["value"] = json!(n + 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_quorum_not_reached_is_asked_again() {
    let env = env(&[0, 0], &mode_route(QUORUM_2_OF_2, 1)).await;
    env.http[1].once([rewrite(bump_value)]);
    let v = env.call("r", "getBalance", json!([pubkey()])).await;
    assert_eq!(result(&v)["value"], json!(0), "the second attempt agreed: {v}");
    assert_eq!(env.calls("getBalance"), vec![2, 2]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_quorum_that_never_agrees_is_refused_after_the_attempts() {
    let env = env(&[0, 0], &mode_route(QUORUM_2_OF_2, 2)).await;
    env.http[1].on_method("getBalance", rewrite(bump_value));
    let v = env.call("r", "getBalance", json!([pubkey()])).await;
    assert_eq!(code(&v), -33094, "{v}");
    assert_eq!(env.calls("getBalance"), vec![3, 3]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_method_with_its_own_mode_keeps_the_routes_retry() {
    let env = env(
        &[0, 0],
        r#"
[routes.r]
upstreams = ["*"]
mode = { type = "quorum", threshold = 2, fanout = 2 }
retry = { attempts = 1, delay = 0 }
loadBalancing = { type = "round-robin" }
[routes.r.methods]
getLatestBlockhash = { mode = { type = "hedge", fanout = 2 } }
"#,
    )
    .await;
    env.http[0].once([Fault::Status(500)]);
    env.http[1].once([Fault::Status(500)]);
    assert!(result(&env.call("r", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert_eq!(env.calls("getLatestBlockhash"), vec![2, 2]);
}
