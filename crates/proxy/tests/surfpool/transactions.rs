//! Real signed transactions: System Program transfers built and signed in
//! the test, sent, simulated and watched through the proxy.

use serde_json::{json, Value};

use crate::harness::*;

const ONE_SOL: u64 = 1_000_000_000;

/// A route over every upstream; `policies` are its own lines, e.g.
/// `mode = { type = "hedge", upstreams = 2 }`.
fn route(name: &str, policies: &str) -> String {
    format!("[routes.{name}]\nupstreams = [\"*\"]\n{policies}\n[routes.{name}.methods]\n\"*\" = {{}}\n")
}

fn base64() -> Value {
    json!({"encoding": "base64"})
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn simulate_send_and_read_back_a_transfer() {
    let env = env(&[0], &route("r", "")).await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 5_000_000, &chain.latest_blockhash().await);

    let sim = env.call("r", "simulateTransaction", json!([tx, base64()])).await;
    let sim = &result(&sim)["value"];
    assert!(sim["err"].is_null(), "{sim}");
    assert!(sim["logs"].to_string().contains("Program 11111111111111111111111111111111 success"), "{sim}");
    assert_eq!(chain.balance(&to).await, 0, "a simulation changes nothing");

    let sent = env.call("r", "sendTransaction", json!([tx, base64()])).await;
    assert_eq!(result(&sent), &json!(sig));
    chain.wait_finalized(&sig).await;

    let read = env.call("r", "getTransaction", json!([sig, {"commitment": "finalized", "maxSupportedTransactionVersion": 0}])).await;
    let read = result(&read);
    assert_eq!(read["transaction"]["signatures"][0], json!(sig));
    assert!(read["meta"]["err"].is_null(), "{read}");
    let balance = env.call("r", "getBalance", json!([to, {"commitment": "finalized"}])).await;
    assert_eq!(result(&balance)["value"], 5_000_000);
    let history = env.call("r", "getSignaturesForAddress", json!([to])).await;
    assert_eq!(result(&history)[0]["signature"], json!(sig));
    let status = env.call("r", "getSignatureStatuses", json!([[sig]])).await;
    assert_eq!(result(&status)["value"][0]["confirmationStatus"], "finalized");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_hedged_send_reaches_both_nodes_and_lands_once() {
    // Two upstreams on one chain: the chain sees the same transaction twice.
    let env = env(&[0, 0], &route("h", "mode = { type = \"hedge\", upstreams = 2 }")).await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 7_000_000, &chain.latest_blockhash().await);
    let sent = env.call("h", "sendTransaction", json!([tx, base64()])).await;
    assert_eq!(result(&sent), &json!(sig), "the success wins over the duplicate's 'already processed'");
    assert!(eventually(ms(3000), || async { env.calls("sendTransaction") == vec![1, 1] }).await);
    chain.wait_landed(&sig).await;
    assert_eq!(chain.balance(&to).await, 7_000_000, "once, not twice");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_hedged_send_succeeds_when_one_node_rejects_it() {
    // Separate chains: the sender has funds on chain 0 only, so chain 1
    // rejects the transaction while chain 0 accepts it.
    let env = env(&[0, 1], &route("h", "mode = { type = \"hedge\", upstreams = 2 }")).await;
    let wallet = env.chain(0).funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 3_000_000, &env.chain(0).latest_blockhash().await);
    let sent = env.call("h", "sendTransaction", json!([tx, base64()])).await;
    assert_eq!(result(&sent), &json!(sig));
    assert!(eventually(ms(3000), || async { env.calls("sendTransaction") == vec![1, 1] }).await);
    env.chain(0).wait_landed(&sig).await;
    assert_eq!(env.chain(0).balance(&to).await, 3_000_000);
    assert_eq!(env.chain(1).balance(&to).await, 0);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_retried_send_after_a_lost_reply_lands_once() {
    // The node received and processed the transaction, but its reply was
    // lost; the retry sends the same bytes again.
    let env = env(&[0], &route("r", "retry = { attempts = 1, delay = 0 }")).await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 9_000_000, &chain.latest_blockhash().await);
    env.http[0].once([Fault::Lost]);
    let sent = env.call("r", "sendTransaction", json!([tx, base64()])).await;
    assert_eq!(env.http[0].calls("sendTransaction"), 2, "retried");
    // The retry is refused as a duplicate: the client gets that error even
    // though its transfer landed.
    assert!(sent["result"] == json!(sig) || message(&sent).contains("already been processed"), "{sent}");
    chain.wait_landed(&sig).await;
    assert_eq!(chain.balance(&to).await, 9_000_000, "the same signed bytes cannot be applied twice");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_send_is_exempted_from_route_policies_with_its_own_entry() {
    // The route verifies reads by quorum; the send turns it off, as the
    // startup warning advises. Nothing is cached unless named.
    let env = env(
        &[0, 0],
        "[routes.q]\nupstreams = [\"*\"]\nmode = { type = \"quorum\", threshold = 2, upstreams = 2 }\n\
         [routes.q.methods]\n\"*\" = {}\nsendTransaction = { mode = { type = \"single\" } }\n",
    )
    .await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 1_000_000, &chain.latest_blockhash().await);
    assert_eq!(result(&env.call("q", "sendTransaction", json!([tx, base64()])).await), &json!(sig));
    assert_eq!(env.total_calls("sendTransaction"), 1, "sent once, not to every quorum member");
    env.call("q", "sendTransaction", json!([tx, base64()])).await;
    assert_eq!(env.total_calls("sendTransaction"), 2, "never cached: not named with a cache");
    chain.wait_landed(&sig).await;
    assert_eq!(chain.balance(&to).await, 1_000_000);

    // A simulation is a read: both members run it and must agree.
    let (tx2, _) = wallet.transfer(&to, 1_000, &chain.latest_blockhash().await);
    let sim = env.call("q", "simulateTransaction", json!([tx2, base64()])).await;
    assert!(result(&sim)["value"]["err"].is_null(), "{sim}");
    assert_eq!(env.total_calls("simulateTransaction"), 2);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_transfer_seen_through_one_websocket() {
    // Subscribe to the recipient and the signature, send over the same
    // socket, and get both notifications.
    let env = env(&[0], &route("s", "")).await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let (tx, sig) = wallet.transfer(&to, 4_000_000, &chain.latest_blockhash().await);
    let mut ws = env.ws("s").await;
    let account = ws.subscribe("accountSubscribe", json!([to, {"encoding": "base64", "commitment": "confirmed"}])).await;
    let signature = ws.subscribe("signatureSubscribe", json!([sig, {"commitment": "confirmed"}])).await;
    assert_eq!(result(&ws.call("sendTransaction", json!([tx, base64()])).await), &json!(sig));

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let n = ws.notification(ms(15_000)).await.expect("both notifications");
        seen.push((n["params"]["subscription"].as_u64().unwrap(), n));
    }
    let of = |id: u64| seen.iter().find(|(s, _)| *s == id).map(|(_, n)| n).unwrap();
    assert_eq!(of(account)["params"]["result"]["value"]["lamports"], 4_000_000);
    assert!(of(signature)["params"]["result"]["value"]["err"].is_null());
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn a_transfers_logs_arrive_once_from_two_nodes() {
    // Two upstreams on one chain both send the same logsNotification.
    let env = env(&[0, 0], &route("s", "")).await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let to = pubkey();
    let mut ws = env.ws("s").await;
    let id = ws.subscribe("logsSubscribe", json!([{"mentions": [to]}, {"commitment": "confirmed"}])).await;
    assert_eq!((env.ws[0].count("logsSubscribe"), env.ws[1].count("logsSubscribe")), (1, 1));
    let (tx, sig) = wallet.transfer(&to, 2_000_000, &chain.latest_blockhash().await);
    result(&chain.call("sendTransaction", json!([tx, base64()])).await);

    let n = ws.notification(ms(15_000)).await.expect("logsNotification");
    assert_eq!(n["method"], "logsNotification");
    assert_eq!(n["params"]["subscription"], id);
    assert_eq!(n["params"]["result"]["value"]["signature"], json!(sig));
    assert!(n["params"]["result"]["value"]["logs"].to_string().contains("success"));
    assert!(ws.notifications_for(ms(1000)).await.is_empty(), "the second node's copy is dropped");
}
