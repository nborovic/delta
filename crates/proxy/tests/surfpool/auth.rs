//! API keys: key check, routes per key, rate limits, allowed IPs and domains, over
//! HTTP and WebSocket.

use serde_json::{json, Value};

use crate::harness::*;

const K1: &str = "3f8a2c1e-7b4d-4e9a-b6c2-1d5e8f0a9b37";
const K2: &str = "6a1f0c2e-8a3b-4c5d-9e7f-0123456789ab";
const K3: &str = "7b2e1d3f-9b4c-4d6e-8f01-123456789abc";
const K4: &str = "8c3f2e40-ac5d-4e7f-9012-23456789abcd";
const K5: &str = "9d4a3f51-bd6e-4f80-8123-3456789abcde";
const K6: &str = "ae5b4062-ce7f-4091-9234-456789abcdef";

/// The proxy's peer in these tests is 127.0.0.1: trusting it makes
/// `X-Forwarded-For` count, as behind a real load balancer.
const BEHIND_LB: &str = "trustedProxies = [\"127.0.0.1\"]";

fn config() -> String {
    format!(
        r#"
[routes.fast]
upstreams = ["*"]
[routes.fast.methods]
"*" = {{}}

[routes.stream]
upstreams = ["*"]
[routes.stream.methods]
"*" = {{}}

[routes.admin]
upstreams = ["*"]
[routes.admin.methods]
"*" = {{}}

[apiKeys.backend]
key = "{K1}"
routes = ["fast", "str*"]
totalRps = 5

[apiKeys.office]
key = "{K2}"
routes = ["*"]
allowedIps = ["10.0.0.0/8", "192.168.1.7"]

[apiKeys.second]
key = "{K4}"
routes = ["*"]
totalRps = 5

[apiKeys.public]
key = "{K6}"
routes = ["fast"]
rpmPerIp = 2
totalRps = 3

[apiKeys.watcher]
key = "{K5}"
routes = ["stream"]
maxSubscriptions = 2

[apiKeys.web]
key = "{K3}"
routes = ["fast"]
allowedDomains = ["app.example.com", "*.example.org"]
"#
    )
}

fn req() -> Value {
    json!({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"})
}

async fn post(env: &Env, path: &str, headers: &[(&str, &str)]) -> (u16, Value) {
    env.client.json(path, &req(), headers).await
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_key_is_required_and_checked() {
    let env = env(&[0], &config()).await;
    for path in ["/rpc/fast".to_owned(), "/rpc/fast?api-key=nope".into(), "/rpc/fast?api-key=00000000-0000-0000-0000-000000000000".into()] {
        let (status, v) = post(&env, &path, &[]).await;
        assert_eq!((status, code(&v)), (401, -33091), "{path}");
    }
    let (status, v) = post(&env, &format!("/rpc/fast?api-key={K1}"), &[]).await;
    assert_eq!(status, 200);
    assert!(result(&v)["value"]["blockhash"].is_string(), "{v}");
    // Upper case is the same UUID.
    assert_eq!(post(&env, &format!("/rpc/fast?api-key={}", K1.to_uppercase()), &[]).await.0, 200);
    // /health also needs a key once keys are configured.
    assert_eq!(env.client.get("/health").await.0, 401);
    assert_eq!(env.client.get(&format!("/health?api-key={K1}")).await.0, 200);
    assert_eq!(env.http[0].calls("getLatestBlockhash"), 2, "refused requests never reach an upstream");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn routes_per_key() {
    let env = env_top(&[0], BEHIND_LB, &config()).await;
    assert_eq!(post(&env, &format!("/rpc/fast?api-key={K1}"), &[]).await.0, 200);
    assert_eq!(post(&env, &format!("/rpc/stream?api-key={K1}"), &[]).await.0, 200, "glob str*");
    assert_eq!(post(&env, &format!("/rpc/admin?api-key={K1}"), &[]).await.0, 401);
    assert_eq!(post(&env, &format!("/rpc/admin?api-key={K2}"), &[("x-forwarded-for", "10.1.2.3")]).await.0, 200, "*");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn rps_counts_batch_elements_and_refills() {
    let env = env(&[0], &config()).await;
    let path = format!("/rpc/fast?api-key={K1}");
    let batch = json!([req(), req(), req()]);
    assert_eq!(env.client.json(&path, &batch, &[]).await.0, 200);
    assert_eq!(post(&env, &path, &[]).await.0, 200);
    assert_eq!(post(&env, &path, &[]).await.0, 200);
    let (status, v) = post(&env, &path, &[]).await;
    assert_eq!((status, code(&v)), (429, -33092), "3 + 1 + 1 = 5 used");
    tokio::time::sleep(ms(450)).await;
    assert_eq!(post(&env, &path, &[]).await.0, 200, "5 rps refills one token per 200 ms");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn one_budget_per_key_across_routes_and_transports() {
    let env = env(&[0], &config()).await;
    for _ in 0..5 {
        assert_eq!(post(&env, &format!("/rpc/fast?api-key={K1}"), &[]).await.0, 200);
    }
    assert_eq!(post(&env, &format!("/rpc/stream?api-key={K1}"), &[]).await.0, 429, "another route, same budget");
    let mut ws = Ws::connect(&format!("{}?api-key={K1}", env.ws_url("stream"))).await;
    assert_eq!(code(&ws.call("getLatestBlockhash", json!(null)).await), -33092, "a WebSocket, same budget");
    assert_eq!(post(&env, &format!("/rpc/fast?api-key={K4}"), &[]).await.0, 200, "another limited key has its own");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn rpm_per_ip_limits_each_client_and_total_rps_all_of_them() {
    let env = env_top(&[0], BEHIND_LB, &config()).await;
    let path = format!("/rpc/fast?api-key={K6}");
    let from = |ip: &'static str| [("x-forwarded-for", ip)];
    assert_eq!(post(&env, &path, &from("1.1.1.1")).await.0, 200);
    assert_eq!(post(&env, &path, &from("1.1.1.1")).await.0, 200);
    let (status, v) = post(&env, &path, &from("1.1.1.1")).await;
    assert_eq!((status, code(&v)), (429, -33092), "2 per minute from 1.1.1.1");
    assert_eq!(post(&env, &path, &from("2.2.2.2")).await.0, 200, "another IP has its own; 3 of the key's 3 used");
    assert_eq!(post(&env, &path, &from("3.3.3.3")).await.0, 429, "but the key's total is spent");

    // Over a WebSocket, each message counts against the IP it connected from.
    let ws_path = format!("{}?api-key={K6}", env.ws_url("fast"));
    tokio::time::sleep(ms(1100)).await; // the key's total refilled: only the per-IP limit refuses below
    let mut spent = Ws::connect_with(&ws_path, &from("1.1.1.1")).await;
    assert_eq!(code(&spent.call("getLatestBlockhash", json!(null)).await), -33092, "1.1.1.1 is still spent");
    let mut ws = Ws::connect_with(&ws_path, &from("4.4.4.4")).await;
    assert!(ws.call("getLatestBlockhash", json!(null)).await.get("result").is_some());
    assert!(ws.call("getLatestBlockhash", json!(null)).await.get("result").is_some());
    assert_eq!(code(&ws.call("getLatestBlockhash", json!(null)).await), -33092);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_batch_larger_than_the_bucket_is_not_refused_forever() {
    let env = env(&[0], &config()).await;
    let path = format!("/rpc/fast?api-key={K1}");
    let batch = Value::Array((0..8).map(|_| req()).collect());
    let (status, v) = env.client.json(&path, &batch, &[]).await;
    assert_eq!(status, 200, "a full bucket takes it: {v}");
    assert_eq!(post(&env, &path, &[]).await.0, 429, "then it is in debt");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn allowed_ips_use_the_forwarded_for_of_a_trusted_proxy() {
    let env = env_top(&[0], BEHIND_LB, &config()).await;
    let path = format!("/rpc/fast?api-key={K2}");
    assert_eq!(post(&env, &path, &[]).await.0, 401, "127.0.0.1 is not allowed");
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "10.9.9.9")]).await.0, 200);
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "192.168.1.7")]).await.0, 200);
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "192.168.1.8")]).await.0, 401);
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "10.0.0.1, 8.8.8.8")]).await.0, 401, "the client cannot spoof the first entry");
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "8.8.8.8, 10.0.0.1")]).await.0, 200);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn without_trusted_proxies_forwarded_for_is_ignored() {
    // A client talking to delta directly could write any address there.
    let env = env(&[0], &config()).await;
    let path = format!("/rpc/fast?api-key={K2}");
    assert_eq!(post(&env, &path, &[("x-forwarded-for", "10.9.9.9")]).await.0, 401);
    let env = env_top(&[0], "trustedProxies = [\"10.0.0.0/8\"]", &config()).await;
    assert_eq!(post(&env, &format!("/rpc/fast?api-key={K2}"), &[("x-forwarded-for", "10.9.9.9")]).await.0, 401, "127.0.0.1 is not a trusted proxy");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn allowed_domains_check_the_origin() {
    let env = env(&[0], &config()).await;
    let path = format!("/rpc/fast?api-key={K3}");
    assert_eq!(post(&env, &path, &[]).await.0, 200, "no Origin: not a browser");
    assert_eq!(post(&env, &path, &[("origin", "https://app.example.com")]).await.0, 200);
    assert_eq!(post(&env, &path, &[("origin", "https://APP.example.com:8443")]).await.0, 200);
    assert_eq!(post(&env, &path, &[("origin", "https://evil.com")]).await.0, 401);
    assert_eq!(post(&env, &path, &[("origin", "https://x.app.example.com")]).await.0, 401, "exact domain");
    assert_eq!(post(&env, &path, &[("origin", "https://a.b.example.org")]).await.0, 200, "*.example.org");
    assert_eq!(post(&env, &path, &[("origin", "https://example.org")]).await.0, 401, "*. needs a subdomain");
    assert_eq!(post(&env, &path, &[("origin", "null")]).await.0, 401);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn websockets_check_the_key_at_the_upgrade_and_rps_per_message() {
    let env = env(&[0], &config()).await;
    assert!(Ws::try_connect(&env.ws_url("stream")).await.is_err(), "no key");
    assert!(Ws::try_connect(&format!("{}?api-key={K1}", env.ws_url("admin"))).await.is_err(), "route not allowed");

    let mut ws = Ws::connect(&format!("{}?api-key={K1}", env.ws_url("stream"))).await;
    let mut limited = 0;
    for _ in 0..8 {
        let v = ws.call("getLatestBlockhash", json!(null)).await;
        if v.get("error").is_some() {
            assert_eq!(code(&v), -33092);
            assert!(v["id"].is_u64(), "the error answers that message: {v}");
            limited += 1;
        }
    }
    assert!(limited >= 2, "{limited}");
    let v = ws.call("getLatestBlockhash", json!(null)).await;
    assert!(v.get("error").is_some());
    tokio::time::sleep(ms(450)).await;
    assert!(result(&ws.call("getLatestBlockhash", json!(null)).await)["value"]["blockhash"].is_string(), "the connection stays open");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_keys_subscription_limit_spans_its_connections() {
    let env = env(&[0], &config()).await;
    let url = format!("{}?api-key={K5}", env.ws_url("stream"));
    let (mut a, mut b) = (Ws::connect(&url).await, Ws::connect(&url).await);
    let first = a.subscribe("slotSubscribe", json!(null)).await;
    b.subscribe("accountSubscribe", json!([pubkey()])).await;
    let v = b.call("accountSubscribe", json!([pubkey()])).await;
    assert_eq!(code(&v), -33096, "two held across both connections: {v}");
    assert!(message(&v).contains("API key"), "{v}");

    assert_eq!(result(&a.call("slotUnsubscribe", json!([first])).await), true);
    b.subscribe("accountSubscribe", json!([pubkey()])).await;
    b.close().await;
    tokio::time::sleep(ms(300)).await;
    a.subscribe("slotSubscribe", json!(null)).await;
    a.subscribe("accountSubscribe", json!([pubkey()])).await;
}

