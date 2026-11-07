//! Load balancing strategies and which upstreams a route uses.

use futures::future::join_all;
use serde_json::json;

use crate::harness::*;

fn lb_route(lb: &str) -> String {
    format!(
        r#"
[routes.r]
upstreams = ["*"]
loadBalancing = {lb}
[routes.r.methods]
"*" = {{}}
"#
    )
}

async fn spread(env: &Env, n: usize) -> Vec<usize> {
    env.reset_counts();
    for _ in 0..n {
        result(&env.call("r", "getLatestBlockhash", json!(null)).await);
    }
    env.calls("getLatestBlockhash")
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn round_robin() {
    let env = env(&[0, 0, 0], &lb_route(r#"{ type = "round-robin" }"#)).await;
    assert_eq!(spread(&env, 30).await, vec![10, 10, 10]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn weighted_with_globs() {
    // u0 = 3 (exact beats the glob), every other `u*` = 1.
    let env = env(&[0, 0, 0], &lb_route(r#"{ type = "weighted", weights = { "u*" = 1, u0 = 3 } }"#)).await;
    let s = spread(&env, 500).await;
    assert!((220..=380).contains(&s[0]), "{s:?}");
    assert!((50..=150).contains(&s[1]) && (50..=150).contains(&s[2]), "{s:?}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn priority_fails_over_and_back() {
    let env = env(&[0, 0], &lb_route(r#"{ type = "priority", order = ["u0", "u1"] }"#)).await;
    assert_eq!(spread(&env, 10).await, vec![10, 0]);

    env.http[0].set(Fault::Status(500));
    env.reset_counts();
    for _ in 0..10 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
    }
    let s = env.calls("getLatestBlockhash");
    assert_eq!(s[0], 5, "five failures trip u0: {s:?}");
    assert_eq!(s[1], 5, "then everything goes to u1: {s:?}");

    // After the backoff one probe goes to u0; a success makes it healthy.
    env.http[0].heal();
    assert!(
        eventually(ms(3000), || async {
            env.call("r", "getLatestBlockhash", json!(null)).await;
            env.reset_counts();
            env.call("r", "getLatestBlockhash", json!(null)).await;
            env.calls("getLatestBlockhash") == vec![1, 0]
        })
        .await,
        "back on u0"
    );
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn priority_ranks_rotate_within_a_glob() {
    let env = env(&[0, 0, 0], &lb_route(r#"{ type = "priority", order = ["u2", "u*"] }"#)).await;
    assert_eq!(spread(&env, 6).await, vec![0, 0, 6], "u2 first");
    env.http[2].set(Fault::Status(500));
    for _ in 0..5 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
    }
    let s = spread(&env, 10).await;
    assert_eq!(s, vec![5, 5, 0], "u2 tripped: u0 and u1 share the second rank");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn round_robin_is_the_default() {
    let env = env(
        &[0, 0, 0],
        r#"
[routes.r]
upstreams = ["*"]
[routes.r.methods]
"*" = {}
"#,
    )
    .await;
    assert_eq!(spread(&env, 30).await, vec![10, 10, 10]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn method_overrides_replace_load_balancing_whole() {
    let env = env(
        &[0, 0],
        r#"
[routes.r]
upstreams = ["*"]
loadBalancing = { type = "priority", order = ["u0"] }
[routes.r.methods]
getLatestBlockhash  = {}
getEpochInfo = { loadBalancing = { type = "round-robin" } }
"#,
    )
    .await;
    for _ in 0..10 {
        env.call("r", "getLatestBlockhash", json!(null)).await;
        env.call("r", "getEpochInfo", json!(null)).await;
    }
    assert_eq!(env.calls("getLatestBlockhash"), vec![10, 0]);
    assert_eq!(env.calls("getEpochInfo"), vec![5, 5]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_route_only_uses_its_upstreams() {
    let env = env(
        &[0, 0, 0],
        r#"
[routes.one]
upstreams = ["u1"]
loadBalancing = { type = "round-robin" }
[routes.one.methods]
"*" = {}

[routes.glob]
upstreams = ["u*"]
loadBalancing = { type = "round-robin" }
[routes.glob.methods]
"*" = {}
"#,
    )
    .await;
    for _ in 0..6 {
        env.call("one", "getLatestBlockhash", json!(null)).await;
    }
    assert_eq!(env.calls("getLatestBlockhash"), vec![0, 6, 0]);
    env.reset_counts();
    for _ in 0..6 {
        env.call("glob", "getLatestBlockhash", json!(null)).await;
    }
    assert_eq!(env.calls("getLatestBlockhash"), vec![2, 2, 2]);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn upstreams_at_their_inflight_limit_overflow_to_others() {
    let mut limits = fast_limits();
    limits.inflight_per_upstream = 1;
    let env = env_with(&[0, 0], &lb_route(r#"{ type = "priority", order = ["u0", "u1"] }"#), limits).await;
    env.http[0].set(Fault::Delay(ms(500)));
    env.http[1].set(Fault::Delay(ms(500)));
    let answers = join_all((0..4).map(|_| env.call("r", "getLatestBlockhash", json!(null)))).await;
    assert!(answers.iter().all(|a| a["result"]["value"]["blockhash"].is_string()));
    let s = env.calls("getLatestBlockhash");
    assert!(s[1] >= 1, "u1 takes the overflow: {s:?}");
    assert_eq!(s[0] + s[1], 4);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_method_can_use_its_own_upstreams() {
    let env = env(
        &[0, 0],
        r#"
[routes.r]
upstreams = ["u0"]
[routes.r.methods]
getSlot = {}
getBalance = { upstreams = ["u1"] }
slotSubscribe = { upstreams = ["u1"] }
"#,
    )
    .await;
    for _ in 0..3 {
        result(&env.call("r", "getSlot", serde_json::Value::Null).await);
        result(&env.call("r", "getBalance", serde_json::json!([pubkey()])).await);
    }
    assert_eq!(env.calls("getSlot"), vec![3, 0], "the route's upstream");
    assert_eq!(env.calls("getBalance"), vec![0, 3], "the method's own, outside the route's list");

    let mut ws = env.ws("r").await;
    ws.subscribe("slotSubscribe", serde_json::Value::Null).await;
    assert_eq!((env.ws[0].count("slotSubscribe"), env.ws[1].count("slotSubscribe")), (0, 1));
    assert!(ws.notification(ms(5000)).await.is_some(), "notifications from u1");
}
