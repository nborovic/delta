//! Cache policy: TTL, namespaces, keys, and the code rules on what is never
//! stored.

use std::time::Instant;

use futures::future::join_all;
use serde_json::{json, Value};

use crate::harness::*;

fn cached_route(ttl: u64) -> String {
    format!(
        r#"
[routes.c]
upstreams = ["*"]
cache = {{ ttl = {ttl} }}
loadBalancing = {{ type = "round-robin" }}
[routes.c.methods]
"*" = {{}}
"#
    )
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn hit_then_expiry() {
    let env = env(&[0], &cached_route(400)).await;
    let key = pubkey();
    let a = env.call("c", "getBalance", json!([key])).await;
    let b = env.call("c", "getBalance", json!([key])).await;
    assert_eq!(a, b, "a hit returns the stored response");
    assert_eq!(env.http[0].calls("getBalance"), 1);
    tokio::time::sleep(ms(600)).await;
    env.call("c", "getBalance", json!([key])).await;
    assert_eq!(env.http[0].calls("getBalance"), 2, "expired after the TTL");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn no_policy_no_cache_and_ttl_zero() {
    let env = env(
        &[0],
        r#"
[routes.plain]
upstreams = ["*"]
[routes.plain.methods]
"*" = {}

[routes.c]
upstreams = ["*"]
cache = { ttl = 60000 }
[routes.c.methods]
getBalance   = {}
getEpochInfo   = { cache = { ttl = 0 } }
"#,
    )
    .await;
    let key = pubkey();
    for _ in 0..2 {
        env.call("plain", "getBalance", json!([key])).await;
    }
    assert_eq!(env.http[0].calls("getBalance"), 2, "no cache policy");
    for _ in 0..2 {
        env.call("c", "getBalance", json!([key])).await;
        env.call("c", "getEpochInfo", json!(null)).await;
    }
    assert_eq!(env.http[0].calls("getBalance"), 3, "inherited from the route");
    assert_eq!(env.http[0].calls("getEpochInfo"), 2, "ttl = 0 turns it off");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn namespaces_share_entries() {
    let env = env(
        &[0],
        r#"
[routes.a]
upstreams = ["*"]
cache = { namespace = "shared", ttl = 60000 }
[routes.a.methods]
getBalance = {}

[routes.b]
upstreams = ["*"]
cache = { namespace = "shared", ttl = 60000 }
[routes.b.methods]
getBalance = {}

[routes.own]
upstreams = ["*"]
cache = { ttl = 60000 }
[routes.own.methods]
getBalance = {}
"#,
    )
    .await;
    let key = pubkey();
    env.call("a", "getBalance", json!([key])).await;
    env.call("b", "getBalance", json!([key])).await;
    assert_eq!(env.http[0].calls("getBalance"), 1, "same namespace: one entry");
    env.call("own", "getBalance", json!([key])).await;
    assert_eq!(env.http[0].calls("getBalance"), 2, "default namespace is the route name");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn entries_are_per_upstream() {
    let env = env(&[0, 0], &cached_route(60000)).await;
    env.http[1].on_method("getBalance", rewrite(|v| v["result"]["value"] = json!(7)));
    let key = pubkey();
    let mut values = Vec::new();
    for _ in 0..6 {
        values.push(result(&env.call("c", "getBalance", json!([key])).await)["value"].as_u64().unwrap());
    }
    assert_eq!(values, [0, 7, 0, 7, 0, 7], "each upstream's own answer, never the other's");
    assert_eq!(env.calls("getBalance"), vec![1, 1], "round-robin: one miss per upstream, then hits");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn keys_are_canonical_but_never_assume_defaults() {
    let env = env(&[0], &cached_route(60000)).await;
    let key = pubkey();
    let calls = || env.http[0].calls("getBalance");

    env.call("c", "getBalance", json!([key])).await;
    env.call("c", "getBalance", json!([key, {}])).await;
    assert_eq!(calls(), 1, "a trailing {{}} is the same request");

    env.call("c", "getBalance", json!([key, {"commitment": "confirmed", "minContextSlot": 0}])).await;
    env.call("c", "getBalance", json!([key, {"minContextSlot": 0, "commitment": "confirmed"}])).await;
    assert_eq!(calls(), 2, "key order does not matter");

    env.call("c", "getBalance", json!([key, {"commitment": "finalized"}])).await;
    assert_eq!(calls(), 3, "an explicit default is a different entry");

    env.call("c", "getBalance", json!([key.to_lowercase()])).await;
    assert_eq!(calls(), 4, "params are case-sensitive");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn null_empty_and_errors_are_never_stored() {
    let env = env(&[0], &cached_route(60000)).await;
    let key = pubkey();
    for _ in 0..2 {
        assert!(result(&env.call("c", "getAccountInfo", json!([key])).await)["value"].is_null());
        assert_eq!(result(&env.call("c", "getSignaturesForAddress", json!([key])).await), &json!([]));
        assert_eq!(code(&env.call("c", "getBalance", json!([])).await), -32602);
    }
    assert_eq!(env.http[0].calls("getAccountInfo"), 2, "value: null");
    assert_eq!(env.http[0].calls("getSignaturesForAddress"), 2, "[]");
    assert_eq!(env.http[0].calls("getBalance"), 2, "errors");

    // A zero balance is a value, not an empty result.
    for _ in 0..2 {
        env.call("c", "getBalance", json!([key])).await;
    }
    assert_eq!(env.http[0].calls("getBalance"), 3);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn writes_follow_the_configured_cache() {
    // Allowed, with a warning at startup: a repeat is answered from the
    // cache and never sent.
    let env = env(&[0], &cached_route(60000)).await;
    let key = pubkey();
    let first = env.call("c", "requestAirdrop", json!([key, 1_000_000_000])).await;
    let second = env.call("c", "requestAirdrop", json!([key, 1_000_000_000])).await;
    assert_eq!(first, second);
    assert_eq!(env.http[0].calls("requestAirdrop"), 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn finality_rules() {
    // The checks before finalization use their own route (own namespace):
    // whatever they store must not hide calls counted later.
    let env = env(&[0], &format!("{}{}", cached_route(60000), cached_route(60000).replace("routes.c", "routes.pre"))).await;
    let chain = env.chain(0);
    let key = pubkey();
    let sig = chain.airdrop(&key, 1_000_000_000).await;
    let calls = |m: &str| env.http[0].calls(m);

    // Not landed yet / not finalized yet: nothing is stored.
    chain.wait_landed(&sig).await;
    let status = env.call("pre", "getSignatureStatuses", json!([[sig]])).await;
    if result(&status)["value"][0]["confirmationStatus"] != "finalized" {
        env.call("pre", "getSignatureStatuses", json!([[sig]])).await;
        assert_eq!(calls("getSignatureStatuses"), 2, "a confirmed status is not stored");
    }

    chain.wait_finalized(&sig).await;
    env.reset_counts();
    for _ in 0..2 {
        env.call("c", "getSignatureStatuses", json!([[sig]])).await;
    }
    assert_eq!(calls("getSignatureStatuses"), 1, "every status finalized: stored");
    for _ in 0..2 {
        env.call("c", "getSignatureStatuses", json!([[sig, fake_signature()]])).await;
    }
    assert_eq!(calls("getSignatureStatuses"), 3, "a null entry: not stored");

    for _ in 0..2 {
        result(&env.call("c", "getTransaction", json!([sig])).await);
        result(&env.call("c", "getTransaction", json!([sig, {"commitment": "confirmed"}])).await);
    }
    assert_eq!(calls("getTransaction"), 4, "no explicit finalized: never stored");
    for _ in 0..2 {
        result(&env.call("c", "getTransaction", json!([sig, {"commitment": "finalized"}])).await);
    }
    assert_eq!(calls("getTransaction"), 5, "explicit finalized: stored");

    let slot = result(&env.call("c", "getTransaction", json!([sig, {"commitment": "finalized"}])).await)["slot"]
        .as_u64()
        .unwrap();
    for _ in 0..2 {
        env.call("c", "getBlock", json!([slot, {"transactionDetails": "none", "rewards": false}])).await;
        env.call("c", "getBlockTime", json!([slot])).await;
    }
    assert_eq!(calls("getBlock"), 2);
    assert_eq!(calls("getBlockTime"), 2, "getBlockTime is never stored");
    for _ in 0..2 {
        result(&env.call("c", "getBlock", json!([slot, {"commitment": "finalized", "transactionDetails": "none", "rewards": false}])).await);
        result(&env.call("c", "getBlocks", json!([slot, slot + 2, {"commitment": "finalized"}])).await);
    }
    assert_eq!(calls("getBlock"), 3);
    assert_eq!(calls("getBlocks"), 1);

    // Finalized null (not landed) is still never stored: it can change.
    let missing = fake_signature();
    for _ in 0..2 {
        env.call("c", "getTransaction", json!([missing, {"commitment": "finalized"}])).await;
    }
    assert_eq!(calls("getTransaction"), 7);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn max_entry_mb() {
    let big = |v: &mut Value| {
        v["result"]["value"] = json!({
            "data": ["A".repeat(2 * 1024 * 1024), "base64"],
            "executable": false, "lamports": 1, "owner": "11111111111111111111111111111111", "rentEpoch": 0, "space": 0
        });
    };
    for (entry_mb, expected_calls) in [(1, 2), (4, 1)] {
        let toml = format!("[stores.moka]\nmaxSizeMb = 64\nmaxEntryMb = {entry_mb}\n{}", cached_route(60000));
        let env = env(&[0], &toml).await;
        env.http[0].on_method("getAccountInfo", rewrite(big));
        let key = pubkey();
        for _ in 0..2 {
            let v = env.call("c", "getAccountInfo", json!([key])).await;
            assert_eq!(result(&v)["value"]["data"][0].as_str().unwrap().len(), 2 * 1024 * 1024);
        }
        assert_eq!(env.http[0].calls("getAccountInfo"), expected_calls, "maxEntryMb = {entry_mb}");
    }
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn concurrent_identical_requests_make_one_call() {
    let env = env(&[0], &cached_route(60000)).await;
    env.http[0].on_method("getBalance", Fault::Delay(ms(300)));
    let key = pubkey();
    let t = Instant::now();
    let answers = join_all((0..20).map(|_| env.call("c", "getBalance", json!([key])))).await;
    assert!(t.elapsed() < ms(2000));
    assert!(answers.windows(2).all(|w| w[0] == w[1]));
    assert_eq!(env.http[0].calls("getBalance"), 1);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn concurrent_identical_errors_are_shared_but_not_stored() {
    let env = env(&[0], &cached_route(60000)).await;
    env.http[0].on_method("getBalance", Fault::Delay(ms(300)));
    let answers = join_all((0..10).map(|_| env.call("c", "getBalance", json!([])))).await;
    assert!(answers.iter().all(|a| code(a) == -32602));
    assert_eq!(env.http[0].calls("getBalance"), 1, "in-flight callers share the one call");
    env.call("c", "getBalance", json!([])).await;
    assert_eq!(env.http[0].calls("getBalance"), 2, "but the error is not stored");
}
