//! Hedge policy: race N upstreams, first success wins.

use std::time::Instant;

use serde_json::json;

use crate::harness::*;

fn hedge_route(n: usize, extra: &str) -> String {
    format!(
        r#"
[routes.h]
upstreams = ["*"]
hedge = {{ upstreams = {n} }}
loadBalancing = {{ type = "round-robin" }}
{extra}
[routes.h.methods]
"*" = {{}}
"#
    )
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_fastest_answer_wins_and_the_slow_call_still_completes() {
    let env = env(&[0, 0], &hedge_route(2, "")).await;
    env.http[0].set(Fault::Delay(ms(1500)));
    let t = Instant::now();
    assert!(result(&env.call("h", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());
    assert!(t.elapsed() < ms(1000), "{:?}", t.elapsed());
    assert_eq!(env.calls("getLatestBlockhash"), vec![1, 1]);
    // The losing call is not aborted: it is fully sent and answered.
    assert!(eventually(ms(3000), || async { env.http[0].completed() == 1 }).await);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn errors_do_not_end_the_race() {
    let env = env(&[0, 0], &hedge_route(2, "")).await;
    env.http[0].set(Fault::Status(500));
    env.http[1].set(Fault::Delay(ms(300)));
    assert!(result(&env.call("h", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string());

    env.http[0].set(Fault::RpcError(-32602, "final"));
    assert!(result(&env.call("h", "getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string(), "a final error loses to a later success");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn when_nobody_succeeds() {
    let env = env(&[0, 0], &hedge_route(2, "")).await;
    let v = env.call("h", "getBalance", json!([])).await;
    assert_eq!(code(&v), -32602, "a final error from the upstreams is returned");

    env.http[0].set(Fault::Status(500));
    env.http[1].set(Fault::Drop);
    let v = env.call("h", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -33095);

    env.http[1].set(Fault::RpcError(-32005, "behind"));
    assert_eq!(code(&env.call("h", "getLatestBlockhash", json!(null)).await), -32005, "the node error, not -33095");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn hedges_to_exactly_n() {
    for n in [1, 2, 3] {
        let env = env(&[0, 0, 0], &hedge_route(n, "")).await;
        for _ in 0..6 {
            env.call("h", "getLatestBlockhash", json!(null)).await;
        }
        assert!(eventually(ms(2000), || async { env.total_calls("getLatestBlockhash") == 6 * n }).await, "n = {n}: {:?}", env.calls("getLatestBlockhash"));
        assert!(env.calls("getLatestBlockhash").iter().all(|&c| c == 2 * n), "round-robin spreads it: {:?}", env.calls("getLatestBlockhash"));
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn writes_are_sent_to_every_hedged_upstream() {
    // Two separate chains: the airdrop must land on both.
    let env = env(&[0, 1], &hedge_route(2, "")).await;
    let key = pubkey();
    result(&env.call("h", "requestAirdrop", json!([key, 1_000_000_000])).await);
    assert!(eventually(ms(3000), || async { env.total_calls("requestAirdrop") == 2 }).await);
    for c in 0..2 {
        let chain = env.chain(c);
        assert!(eventually(ms(10_000), || async { chain.balance(&key).await == 1_000_000_000 }).await, "chain {c}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn hedged_calls_go_through_the_cache() {
    let env = env(&[0, 0], &hedge_route(2, "cache = { ttl = 60000 }")).await;
    let key = pubkey();
    env.call("h", "getBalance", json!([key])).await;
    assert!(eventually(ms(2000), || async { env.total_calls("getBalance") == 2 }).await);
    for _ in 0..3 {
        env.call("h", "getBalance", json!([key])).await;
    }
    assert_eq!(env.calls("getBalance"), vec![1, 1], "both upstreams' entries are hits");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_deadline_ends_the_race() {
    let mut limits = fast_limits();
    limits.request_deadline = ms(700);
    let env = env_with(&[0, 0], &hedge_route(2, ""), limits).await;
    env.http[0].set(Fault::Hang);
    env.http[1].set(Fault::Hang);
    let t = Instant::now();
    let v = env.call("h", "getLatestBlockhash", json!(null)).await;
    assert_eq!(code(&v), -33095);
    assert!(t.elapsed() < ms(1200), "{:?}", t.elapsed());
}
