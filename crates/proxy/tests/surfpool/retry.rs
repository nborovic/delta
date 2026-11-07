//! Retry policy: which failures are retried, delays, merging, deadlines.

use std::time::Instant;

use serde_json::json;

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

