//! Upstream health: what counts as a failure, trips, backoff, probes, and
//! the per-method level.

use std::time::Instant;

use serde_json::json;

use crate::harness::*;

/// Round-robin over two upstreams on one chain.
const RR: &str = r#"
[routes.r]
upstreams = ["*"]
loadBalancing = { type = "round-robin" }
[routes.r.methods]
"*" = {}
"#;

/// Sends `n` requests and returns how many reached each upstream.
async fn send(env: &Env, method: &str, n: usize) -> Vec<usize> {
    env.reset_counts();
    for _ in 0..n {
        env.call("r", method, json!(null)).await;
    }
    env.calls(method)
}

/// Sends round-robin requests until `failing` has failed five times.
async fn trip(env: &Env, failing: usize) {
    env.reset_counts();
    for _ in 0..30 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
        if env.http[failing].total() >= 5 {
            break;
        }
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn five_consecutive_failures_trip_an_upstream() {
    let env = env(&[0, 0], RR).await;
    for (i, fault) in [Fault::Status(500), Fault::Drop, Fault::Garbage, Fault::RpcError(-32005, "behind")].into_iter().enumerate() {
        let env = &env;
        env.http[0].set(fault);
        trip(env, 0).await;
        assert_eq!(send(env, "getLatestBlockhash", 10).await, vec![0, 10], "fault #{i}");
        env.http[0].heal();
        // Wait out the backoff so the next fault starts from healthy.
        assert!(eventually(ms(5000), || async { send(env, "getLatestBlockhash", 2).await[0] >= 1 }).await);
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn four_failures_do_not_trip() {
    let env = env(&[0, 0], RR).await;
    env.http[0].once(vec![Fault::Status(500); 4]);
    let s = send(&env, "getLatestBlockhash", 20).await;
    assert_eq!(s, vec![10, 10]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_failure_ratio_trips_too() {
    let env = env(&[0, 0], RR).await;
    // Alternate failure / success on u0: never 5 in a row, but 50% of the
    // window. The ratio is checked on a failure once the window holds 20
    // requests: u0's 21st call (a failure) trips it.
    env.http[0].once((0..30).map(|i| if i % 2 == 0 { Fault::Status(500) } else { Fault::Pass }));
    for _ in 0..60 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
        if env.http[0].total() >= 21 {
            break;
        }
    }
    assert_eq!(send(&env, "getLatestBlockhash", 10).await, vec![0, 10]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn final_and_retryable_errors_never_count() {
    let env = env(&[0, 0], RR).await;
    for fault in [Fault::RpcError(-32602, "bad"), Fault::RpcError(-32004, "skipped"), Fault::RpcError(-32016, "min ctx"), Fault::Status(400)] {
        env.http[0].set(fault);
        let s = send(&env, "getLatestBlockhash", 20).await;
        assert_eq!(s, vec![10, 10]);
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn too_many_requests_trips_overloaded_at_once_and_ignores_retry_after() {
    let env = env(&[0, 0], RR).await;
    env.http[0].once([Fault::TooMany(Some(30))]);
    let t = Instant::now();
    let s = send(&env, "getLatestBlockhash", 10).await;
    assert_eq!(s, vec![1, 9], "one 429 is enough");
    // The overloaded backoff (300 ms here), not Retry-After: 30.
    assert!(eventually(ms(3000), || async { send(&env, "getLatestBlockhash", 2).await[0] >= 1 }).await);
    assert!(t.elapsed() >= ms(250), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_tripped_upstream_gets_one_probe_after_the_backoff() {
    let env = env(&[0, 0], RR).await;
    env.http[0].set(Fault::Status(500));
    trip(&env, 0).await;
    let tripped = Instant::now();
    assert_eq!(send(&env, "getLatestBlockhash", 6).await, vec![0, 6]);

    // Still failing: probes keep failing and the backoff doubles.
    assert!(eventually(ms(3000), || async { send(&env, "getLatestBlockhash", 1).await[0] == 1 }).await);
    let first_probe = tripped.elapsed();
    assert!(first_probe >= ms(450), "backoff 500 ms: {first_probe:?}");
    assert_eq!(send(&env, "getLatestBlockhash", 6).await, vec![0, 6], "the failed probe re-tripped it");

    env.http[0].heal();
    assert!(eventually(ms(5000), || async { send(&env, "getLatestBlockhash", 1).await[0] == 1 }).await);
    assert_eq!(send(&env, "getLatestBlockhash", 10).await, vec![5, 5], "the successful probe made it healthy");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn method_level_health() {
    let env = env(&[0, 0], RR).await;
    // Providers often refuse single methods per plan: 401/403 only mark the
    // (upstream, method) pair.
    env.http[0].on_method("getProgramAccounts", Fault::Status(403));
    for _ in 0..12 {
        env.call("r", "getProgramAccounts", program_accounts()).await;
    }
    env.reset_counts();
    for _ in 0..10 {
        assert!(result(&env.call("r", "getProgramAccounts", program_accounts()).await).is_array());
    }
    assert_eq!(env.calls("getProgramAccounts"), vec![0, 10], "u0 avoided for getProgramAccounts");
    assert_eq!(send(&env, "getLatestBlockhash", 10).await, vec![5, 5], "but not for anything else");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn an_upstream_trip_affects_every_method_and_route() {
    let env = env(
        &[0, 0],
        &format!(
            "{RR}{}",
            r#"
[routes.other]
upstreams = ["*"]
loadBalancing = { type = "round-robin" }
[routes.other.methods]
"*" = {}
"#
        ),
    )
    .await;
    env.http[0].set(Fault::Status(500));
    trip(&env, 0).await;
    env.reset_counts();
    for _ in 0..10 {
        env.call("other", "getEpochInfo", json!(null)).await;
    }
    assert_eq!(env.calls("getEpochInfo"), vec![0, 10]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn with_everything_down_the_proxy_still_tries() {
    let env = env(&[0, 0], RR).await;
    env.http[0].set(Fault::Status(500));
    env.http[1].set(Fault::Status(500));
    for _ in 0..12 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
    }
    env.http[0].heal();
    env.http[1].heal();
    // Both are unhealthy: rather than failing outright the selector falls
    // back to them, so requests succeed as soon as the nodes recover.
    let v = env.call("r", "getLatestBlockhash", json!(null)).await;
    assert!(result(&v)["value"]["blockhash"].is_string());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn calls_cut_by_the_request_deadline_do_not_count() {
    let mut limits = fast_limits();
    limits.request_deadline = ms(300);
    limits.per_call_timeout = ms(2000);
    let env = env_with(&[0, 0], RR, limits).await;
    env.http[0].set(Fault::Delay(ms(600)));
    for _ in 0..12 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
    }
    env.http[0].heal();
    assert_eq!(send(&env, "getLatestBlockhash", 10).await, vec![5, 5], "u0 is still healthy: the proxy's budget ran out, not u0");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn per_call_timeouts_count() {
    let mut limits = fast_limits();
    limits.per_call_timeout = ms(200);
    let env = env_with(&[0, 0], RR, limits).await;
    env.http[0].set(Fault::Hang);
    trip(&env, 0).await;
    env.http[0].heal();
    assert_eq!(send(&env, "getLatestBlockhash", 6).await, vec![0, 6]);
}

/// Every account the System Program owns, without their data: a cheap but
/// real getProgramAccounts.
fn program_accounts() -> serde_json::Value {
    json!([SYSTEM_PROGRAM, {"encoding": "base64", "dataSlice": {"offset": 0, "length": 0}}])
}

