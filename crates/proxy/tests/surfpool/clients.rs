//! The official Rust clients (`solana-client`, `solana-pubsub-client`)
//! pointed at the proxy instead of a node: everything they send and expect
//! has to work unchanged.

use std::time::Duration;

use futures::StreamExt;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcSignatureSubscribeConfig};
use solana_client::rpc_response::RpcSignatureResult;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;

use crate::harness::*;

const ONE_SOL: u64 = 1_000_000_000;

/// Two upstreams on one chain, every method accepted.
async fn two_upstreams() -> Env {
    env(&[0, 0], "[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\n\"*\" = {}\n").await
}

fn rpc(env: &Env, route: &str) -> RpcClient {
    RpcClient::new_with_commitment(env.rpc_url(route), CommitmentConfig::confirmed())
}

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), f).await.unwrap_or_else(|_| panic!("timed out: {what}"))
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn rpc_client_reads_through_the_proxy() {
    let env = two_upstreams().await;
    let client = rpc(&env, "r");
    let system = solana_system_interface::program::ID;

    assert!(client.get_slot().await.unwrap() > 0);
    client.get_latest_blockhash().await.unwrap();
    assert!(client.get_account(&system).await.unwrap().executable);
    let accounts = client.get_multiple_accounts(&[system, Pubkey::new_unique()]).await.unwrap();
    assert!(accounts[0].as_ref().is_some_and(|a| a.executable));
    assert!(accounts[1].is_none());
    assert_eq!(client.get_balance(&Pubkey::new_unique()).await.unwrap(), 0);
    assert!(env.calls("getSlot").iter().sum::<usize>() >= 1);

    // The proxy's own errors reach the client as JSON-RPC errors.
    let err = rpc(&env, "nope").get_slot().await.unwrap_err();
    assert!(format!("{err:?}").contains("-33090"), "{err:?}");
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn rpc_client_airdrops_sends_and_confirms_a_transfer() {
    let env = two_upstreams().await;
    let client = rpc(&env, "r");
    let wallet = Wallet::new();
    let payer = wallet.keypair().pubkey();

    let airdrop = client.request_airdrop(&payer, 2 * ONE_SOL).await.unwrap();
    within("airdrop confirmed", async {
        while !client.confirm_transaction(&airdrop).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert_eq!(client.get_balance(&payer).await.unwrap(), 2 * ONE_SOL);

    // What a wallet does: fetch a blockhash, sign, send, wait for
    // confirmation through getSignatureStatuses.
    let to = Pubkey::new_unique();
    let tx = wallet.transfer_tx(&to, 5_000_000, client.get_latest_blockhash().await.unwrap());
    let sig = within("send and confirm", client.send_and_confirm_transaction(&tx)).await.unwrap();
    assert_eq!(sig, tx.signatures[0]);
    assert_eq!(client.get_balance(&to).await.unwrap(), 5_000_000);
    assert_eq!(env.chain(0).balance(&to.to_string()).await, 5_000_000);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn pubsub_client_slot_and_account_subscriptions() {
    let env = two_upstreams().await;
    let pubsub = PubsubClient::new(&env.ws_url("r")).await.unwrap();

    // Both upstreams send every slot; the client sees each one once, in order.
    let (mut slots, unsubscribe) = pubsub.slot_subscribe().await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 5 {
        seen.push(within("slot notification", slots.next()).await.unwrap().slot);
    }
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
    unsubscribe().await;

    let to = Pubkey::new_unique();
    let config = RpcAccountInfoConfig { commitment: Some(CommitmentConfig::confirmed()), ..Default::default() };
    let (mut account, _unsubscribe) = pubsub.account_subscribe(&to, Some(config)).await.unwrap();
    env.chain(0).airdrop(&to.to_string(), 3_000_000).await;
    let update = within("account notification", account.next()).await.unwrap();
    assert_eq!(update.value.lamports, 3_000_000);
}

/// The WebSocket URL web3.js and the Solana CLI derive from an RPC URL
/// with a port: same host, path and query, the port + 1.
fn guessed_ws_url(rpc_url: &str) -> String {
    let mut url = url::Url::parse(rpc_url).unwrap();
    url.set_scheme("ws").unwrap();
    let port = url.port().unwrap();
    url.set_port(Some(port + 1)).unwrap();
    url.to_string()
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn the_websocket_is_where_solana_clients_guess_it() {
    let env = two_upstreams().await;
    let rpc_url = env.rpc_url("r");
    let pubsub = PubsubClient::new(&guessed_ws_url(&rpc_url)).await.unwrap();
    let (mut slots, _unsubscribe) = pubsub.slot_subscribe().await.unwrap();
    within("slot notification on port + 1", slots.next()).await.unwrap();
    // Port + 1 serves HTTP too.
    let next = rpc_url.replace(&env.addr.port().to_string(), &(env.addr.port() + 1).to_string());
    assert!(RpcClient::new(next).get_slot().await.unwrap() > 0);
}

#[tokio::test]
#[ignore = "needs surfpool"]
async fn pubsub_client_confirms_a_signature() {
    let env = two_upstreams().await;
    let chain = env.chain(0);
    let wallet = chain.funded_wallet(2 * ONE_SOL).await;
    let tx = wallet.transfer_tx(&Pubkey::new_unique(), 1_000_000, chain.latest_blockhash().await.parse().unwrap());

    // How the CLI and most SDKs wait for a transaction: signatureSubscribe,
    // then send.
    let pubsub = PubsubClient::new(&env.ws_url("r")).await.unwrap();
    let config = RpcSignatureSubscribeConfig { commitment: Some(CommitmentConfig::confirmed()), enable_received_notification: None };
    let (mut statuses, _unsubscribe) = pubsub.signature_subscribe(&tx.signatures[0], Some(config)).await.unwrap();
    let sent = rpc(&env, "r").send_transaction(&tx).await.unwrap();
    assert_eq!(sent, tx.signatures[0]);

    let status = within("signature notification", statuses.next()).await.unwrap();
    match status.value {
        RpcSignatureResult::ProcessedSignature(r) => assert!(r.err.is_none(), "{:?}", r.err),
        other => panic!("{other:?}"),
    }
}
