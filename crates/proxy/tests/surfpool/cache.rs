//! Cache policy: one entry per request, TTL per reading route, namespaces,
//! keys, and the code rules on what is never stored.

use std::time::Instant;

use futures::future::join_all;
use serde_json::{json, Value};

use crate::harness::*;

/// Every method these tests cache, each named with the same TTL; anything
/// else is accepted uncached.
const CACHED: &[&str] = &[
    "getBalance", "getAccountInfo", "getSignaturesForAddress", "getSignatureStatuses",
    "getTransaction", "getBlock", "getBlockTime", "getBlocks", "getBlocksWithLimit",
];

fn cached_route(ttl: u64) -> String {
    let methods: String = CACHED.iter().map(|m| format!("{m} = {{ cache = {{ ttl = {ttl} }} }}\n")).collect();
    format!(
        r#"
[routes.c]
upstreams = ["*"]
loadBalancing = {{ type = "round-robin" }}
[routes.c.methods]
"*" = {{}}
{methods}"#
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
async fn only_methods_named_with_a_cache_are_cached() {
    let env = env(
        &[0],
        r#"
[routes.plain]
upstreams = ["*"]
[routes.plain.methods]
"*" = {}

[routes.c]
upstreams = ["*"]
[routes.c.methods]
"*"        = {}
getBalance = { cache = { ttl = 60000 } }
"#,
    )
    .await;
    let key = pubkey();
    for _ in 0..2 {
        env.call("plain", "getBalance", json!([key])).await;
    }
    assert_eq!(env.http[0].calls("getBalance"), 2, "no cache named");
    for _ in 0..2 {
        env.call("c", "getBalance", json!([key])).await;
        env.call("c", "getEpochInfo", json!(null)).await;
    }
    assert_eq!(env.http[0].calls("getBalance"), 3, "named with a cache");
    assert_eq!(env.http[0].calls("getEpochInfo"), 2, "accepted through \"*\", never cached");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn routes_share_entries_unless_they_set_a_namespace() {
    let env = env(
        &[0],
        r#"
[routes.a]
upstreams = ["*"]
[routes.a.methods]
getBalance = { cache = { ttl = 60000 } }

[routes.b]
upstreams = ["*"]
[routes.b.methods]
getBalance = { cache = { ttl = 60000 } }

[routes.own]
upstreams = ["*"]
[routes.own.methods]
getBalance = { cache = { namespace = "own", ttl = 60000 } }
"#,
    )
    .await;
    let key = pubkey();
    env.call("a", "getBalance", json!([key])).await;
    env.call("b", "getBalance", json!([key])).await;
    assert_eq!(env.http[0].calls("getBalance"), 1, "the default namespace is shared");
    env.call("own", "getBalance", json!([key])).await;
    assert_eq!(env.http[0].calls("getBalance"), 2, "a namespace of its own");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn each_route_judges_an_entry_by_its_own_ttl() {
    let env = env(
        &[0],
        r#"
[routes.fast]
upstreams = ["*"]
[routes.fast.methods]
getBalance = { cache = { ttl = 200 } }

[routes.slow]
upstreams = ["*"]
[routes.slow.methods]
getBalance = { cache = { ttl = 60000 } }
"#,
    )
    .await;
    let key = pubkey();
    let calls = || env.http[0].calls("getBalance");
    env.call("slow", "getBalance", json!([key])).await;
    env.call("fast", "getBalance", json!([key])).await;
    assert_eq!(calls(), 1, "young enough for both");
    tokio::time::sleep(ms(400)).await;
    env.call("slow", "getBalance", json!([key])).await;
    assert_eq!(calls(), 1, "400 ms old: still fresh for the slow route");
    env.call("fast", "getBalance", json!([key])).await;
    assert_eq!(calls(), 2, "too old for the fast route: fetched again");
    env.call("slow", "getBalance", json!([key])).await;
    assert_eq!(calls(), 2, "and the fresh answer serves the slow route too");
    tokio::time::sleep(ms(400)).await;
    env.call("slow", "getBalance", json!([key])).await;
    assert_eq!(calls(), 2, "stored by the fast route, still kept for the slow one");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn one_entry_per_request_whatever_upstream_answered() {
    let env = env(&[0, 0], &cached_route(60000)).await;
    env.http[1].on_method("getBalance", rewrite(|v| v["result"]["value"] = json!(7)));
    let key = pubkey();
    let mut values = Vec::new();
    for _ in 0..4 {
        values.push(result(&env.call("c", "getBalance", json!([key])).await)["value"].as_u64().unwrap());
    }
    assert_eq!(values, [0, 0, 0, 0], "the first answer, whichever upstream round-robin picks next");
    assert_eq!(env.total_calls("getBalance"), 1, "one miss in all, not one per upstream");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_quorum_stores_the_agreed_answer_apart_from_single_answers() {
    let env = env(
        &[0, 0],
        r#"
[routes.q]
upstreams = ["*"]
mode = { type = "quorum", threshold = 2, fanout = 2 }
[routes.q.methods]
getBalance = { cache = { ttl = 60000 } }

[routes.s]
upstreams = ["*"]
[routes.s.methods]
getBalance = { cache = { ttl = 60000 } }
"#,
    )
    .await;
    let key = pubkey();
    for _ in 0..3 {
        assert!(result(&env.call("q", "getBalance", json!([key])).await)["value"].is_u64());
    }
    assert_eq!(env.calls("getBalance"), vec![1, 1], "agreed once, then answered from the cache");
    env.call("s", "getBalance", json!([key])).await;
    assert_eq!(env.total_calls("getBalance"), 3, "a single upstream's answer is a different entry");
    env.call("q", "getBalance", json!([key])).await;
    assert_eq!(env.total_calls("getBalance"), 3, "and never serves as the quorum's");
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
async fn history_and_ranges_are_cached_like_any_answer() {
    let env = env(&[0], &cached_route(60000)).await;
    let chain = env.chain(0);
    let key = pubkey();
    let sig = chain.airdrop(&key, 1_000_000_000).await;
    chain.wait_finalized(&sig).await;
    env.reset_counts();
    let calls = |m: &str| env.http[0].calls(m);

    // Statuses are cached like any answer (a startup warning says so), even
    // one that is not final or holds a null entry.
    let unknown = fake_signature();
    for _ in 0..2 {
        env.call("c", "getSignatureStatuses", json!([[sig, unknown]])).await;
    }
    assert_eq!(calls("getSignatureStatuses"), 1);

    // History is final whatever the commitment (Alpenglow): each request
    // shape is fetched once.
    for _ in 0..2 {
        result(&env.call("c", "getTransaction", json!([sig])).await);
        result(&env.call("c", "getTransaction", json!([sig, {"commitment": "confirmed"}])).await);
        result(&env.call("c", "getTransaction", json!([sig, {"commitment": "finalized"}])).await);
    }
    assert_eq!(calls("getTransaction"), 3, "no commitment, confirmed and finalized: one entry each");

    let slot = result(&env.call("c", "getTransaction", json!([sig, {"commitment": "finalized"}])).await)["slot"]
        .as_u64()
        .unwrap();
    for _ in 0..2 {
        result(&env.call("c", "getBlock", json!([slot, {"transactionDetails": "none", "rewards": false}])).await);
        env.call("c", "getBlockTime", json!([slot])).await;
    }
    assert_eq!(calls("getBlock"), 1);
    assert_eq!(calls("getBlockTime"), 1);

    // Block ranges too, even one reaching the tip (the TTL bounds it). Real
    // slots are taken from the node itself (straight, not through delta).
    let known = direct(&chain.rpc, "getBlocksWithLimit", json!([slot, 3])).await;
    let known: Vec<u64> = serde_json::from_value(known["result"].clone()).unwrap();
    let (start, end) = (known[0], known[2]);
    for _ in 0..2 {
        result(&env.call("c", "getBlocks", json!([start, end])).await);
        result(&env.call("c", "getBlocks", json!([start, end + 100_000])).await);
        result(&env.call("c", "getBlocks", json!([start])).await);
        result(&env.call("c", "getBlocksWithLimit", json!([start, 2])).await);
        result(&env.call("c", "getBlocksWithLimit", json!([start, 400_000])).await);
    }
    assert_eq!(calls("getBlocks"), 3, "each range once, whether it reaches its end, the tip, or has no end");
    assert_eq!(calls("getBlocksWithLimit"), 2, "each limit once, full or cut at the tip");

    // Null (not landed) is still never stored: it can change.
    let missing = fake_signature();
    for _ in 0..2 {
        env.call("c", "getTransaction", json!([missing, {"commitment": "finalized"}])).await;
    }
    assert_eq!(calls("getTransaction"), 5);
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
