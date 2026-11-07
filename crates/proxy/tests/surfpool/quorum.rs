//! Quorum policy: N upstreams, return once `threshold` agree.

use std::time::Instant;

use serde_json::{json, Value};

use crate::harness::*;

fn quorum_route(threshold: usize, n: usize) -> String {
    format!(
        r#"
[routes.q]
upstreams = ["*"]
quorum = {{ threshold = {threshold}, upstreams = {n} }}
loadBalancing = {{ type = "round-robin" }}
[routes.q.methods]
"*" = {{}}
"#
    )
}

fn bump_value(v: &mut Value) {
    let n = v["result"]["value"].as_u64().unwrap_or(0);
    v["result"]["value"] = json!(n + 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn agreement() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    let key = pubkey();
    env.chain(0).airdrop(&key, 1_000_000_000).await;
    let v = env.call("q", "getBalance", json!([key, {"commitment": "processed"}])).await;
    assert!(result(&v)["value"].is_u64());
    assert_eq!(env.calls("getBalance"), vec![1, 1]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn disagreement_is_refused() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    env.http[1].on_method("getBalance", rewrite(bump_value));
    let v = env.call("q", "getBalance", json!([pubkey()])).await;
    assert_eq!(code(&v), -33094, "never falls back to one of the answers");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn context_slot_and_api_version_are_ignored() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    env.http[1].on_method(
        "getBalance",
        rewrite(|v| {
            v["result"]["context"]["slot"] = json!(v["result"]["context"]["slot"].as_u64().unwrap() + 7);
            v["result"]["context"]["apiVersion"] = json!("9.9.9");
        }),
    );
    result(&env.call("q", "getBalance", json!([pubkey()])).await);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn two_of_three() {
    let env = env(&[0, 0, 0], &quorum_route(2, 3)).await;
    let key = pubkey();
    env.http[2].on_method("getBalance", rewrite(bump_value));
    result(&env.call("q", "getBalance", json!([key])).await);

    env.http[2].set(Fault::Status(500));
    result(&env.call("q", "getBalance", json!([key])).await);

    env.http[1].on_method("getBalance", rewrite(|v| v["result"]["value"] = json!(12345)));
    env.http[2].heal();
    env.http[2].on_method("getBalance", rewrite(bump_value));
    assert_eq!(code(&env.call("q", "getBalance", json!([key])).await), -33094, "three different answers");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn returns_as_soon_as_the_threshold_is_met() {
    let env = env(&[0, 0, 0], &quorum_route(2, 3)).await;
    env.http[2].set(Fault::Delay(ms(2000)));
    let t = Instant::now();
    result(&env.call("q", "getBalance", json!([pubkey()])).await);
    assert!(t.elapsed() < ms(1000), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn fails_as_soon_as_the_threshold_is_out_of_reach() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    env.http[0].set(Fault::Status(500));
    env.http[1].set(Fault::Delay(ms(2000)));
    let t = Instant::now();
    assert_eq!(code(&env.call("q", "getBalance", json!([pubkey()])).await), -33094);
    assert!(t.elapsed() < ms(1000), "{:?}", t.elapsed());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn identical_errors_are_an_agreement() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    assert_eq!(code(&env.call("q", "getBalance", json!([])).await), -32602);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn real_chains_diverge_then_agree() {
    let env = env(&[0, 1], &quorum_route(2, 2)).await;
    let key = pubkey();
    let params = json!([key, {"commitment": "processed"}]);
    let sig = env.chain(0).airdrop(&key, 1_000_000_000).await;
    env.chain(0).wait_landed(&sig).await;
    assert_eq!(code(&env.call("q", "getBalance", params.clone()).await), -33094, "only chain 0 has the lamports");

    let sig = env.chain(1).airdrop(&key, 1_000_000_000).await;
    env.chain(1).wait_landed(&sig).await;
    let v = env.call("q", "getBalance", params).await;
    assert_eq!(result(&v)["value"], 1_000_000_000);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn writes_follow_the_configured_quorum() {
    // Allowed, with a warning at startup: every member gets the write.
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    result(&env.call("q", "requestAirdrop", json!([pubkey(), 1_000_000_000])).await);
    assert_eq!(env.total_calls("requestAirdrop"), 2, "sent to every quorum member");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn too_few_healthy_upstreams_fails_without_calling() {
    let env = env(
        &[0, 0],
        &format!(
            "{}{}",
            quorum_route(2, 2),
            r#"
[routes.only0]
upstreams = ["u0"]
[routes.only0.methods]
"*" = {}
"#
        ),
    )
    .await;
    // Health is shared by every route: trip u0 through another route.
    env.http[0].set(Fault::Status(500));
    for _ in 0..5 {
        env.call("only0", "getLatestBlockhash", json!(null)).await;
    }
    env.reset_counts();
    assert_eq!(code(&env.call("q", "getBalance", json!([pubkey()])).await), -33094);
    assert_eq!(env.total_calls("getBalance"), 0);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn empty_replies_cannot_agree() {
    let env = env(&[0, 0], &quorum_route(2, 2)).await;
    for shim in &env.http {
        shim.set(rewrite(|v| *v = json!({"jsonrpc": "2.0", "id": 1})));
    }
    assert_eq!(code(&env.call("q", "getBalance", json!([pubkey()])).await), -33094);
}

