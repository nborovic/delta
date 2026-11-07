# delta

**A self-hosted JSON-RPC reverse proxy for Solana.**

delta puts several RPC providers and nodes behind one endpoint and gives every URL route its own rules: which methods it accepts, how requests are spread across upstreams, what is cached, and whether calls are retried, raced or checked by quorum. Upstream health and the cache are shared by every route, so a misbehaving node is avoided everywhere at once.

## Features

- **Per-route policies:** a method allow-list plus cache, retry, hedge, quorum and load balancing (round-robin, random, weighted, priority), with per-method overrides.
- **Hedging and quorum:** race several upstreams and take the first success, or answer only when enough of them agree.
- **Upstream health** per upstream and per method, with backoff and a single probe before an upstream is used again.
- **A Solana-aware cache:** data that can still change is stored only once it is finalized; errors and empty results never are.
- **Deduplicated subscriptions:** opened on several upstreams, each notification forwarded once, one upstream subscription shared by many clients.
- **API keys** with per-key routes, rate limits, allowed IPs and origins, and subscription limits.
- **Configured in TOML or Rust:** `delta scaffold` turns a TOML config into a Rust project.

## Quick start

The demo runs on your machine against three offline [Surfpool](https://surfpool.run) nodes. You need Rust and `surfpool`.

```sh
surfpool start --offline --no-studio --no-tui -y -p 8899 -w 8900   # terminal 1: surf-a
surfpool start --offline --no-studio --no-tui -y -p 8999 -w 9000   # terminal 2: surf-b
surfpool start --offline --no-studio --no-tui -y -p 9099 -w 9100   # terminal 3: surf-c
cargo run --release --bin delta -- config.surfpool.toml            # terminal 4
```

```sh
KEY=11111111-2222-4333-8444-555555555555
solana balance 11111111111111111111111111111111 -u "http://127.0.0.1:8080/rpc/failover?api-key=$KEY"
```

Any Solana client works the same way: give it a route's URL, with the key in the query string, in place of a node's URL. Like a Solana node, delta also answers on the port + 1 (8081 here), where web3.js and the Solana CLI look for the WebSocket; both ports serve HTTP and WebSocket. [`config.surfpool.toml`](config.surfpool.toml) has one route per idea:

| Route | What it shows |
| --- | --- |
| `/rpc/hedge` | every method raced on surf-a and surf-b |
| `/rpc/safe` | quorum: two of three nodes must agree |
| `/rpc/cache` | `getLatestBlockhash` only, cached 5 s, weighted 3:2:1 |
| `/rpc/failover` | everything on surf-a while it is healthy, then surf-b |
| `/rpc/stream` | subscriptions on all three nodes, deduplicated |

`scripts/smoke.sh` checks the whole demo end to end with curl and the Solana CLI.

## Configuration

`${VAR}` in any string is read from the environment. The whole file is checked at startup, and every problem is reported at once.

```toml
listen = "0.0.0.0:8080"
trustedProxies = ["10.0.0.0/8"]       # optional: front proxies whose X-Forwarded-For is believed

[apiKeys.backend]                     # at least one key is required
key = "${BACKEND_KEY}"                # a UUID
routes = ["*"]
rps = 500

[upstreams]
helius = { http = "https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY}", maxSubscriptionsPerConnection = 1000 }
own-1  = { http = "http://10.0.0.11:8899", ws = "ws://10.0.0.11:8900" }

[routes.fast]                         # served at /rpc/fast
upstreams = ["*"]                     # the route's upstreams and policies apply to every method
hedge = { upstreams = 2 }
cache = { ttl = 200 }

[routes.fast.methods]
getAccountInfo     = {}
getLatestBlockhash = { cache = { ttl = 0 } }
getBlock           = { upstreams = ["own-1"], hedge = false }
sendTransaction    = false
```

[`config.example.toml`](config.example.toml) is a larger example.

- **Methods:** keys are an exact name, a prefix glob (`"get*"`) or `"*"`; the most specific wins. `{}` accepts with the route's upstreams and policies, a table overrides them, `false` rejects. A method's `upstreams` replaces the route's and may name any upstream.
- **Policies**, on a route or a method: `cache = { ttl, namespace }`, `retry = { attempts, delay }`, `hedge = { upstreams }`, `quorum = { threshold, upstreams }`, `loadBalancing = { type, ... }`. Only one of retry, hedge and quorum per method; `false` turns an inherited one off. Writes (`sendTransaction`, `requestAirdrop`, `sendBundle`, other `send*`) follow them too; a cache or quorum on one loads with a warning.
- **Batches** are forwarded as batches: elements whose methods use the same upstreams go together to one of them, picked by the route's `loadBalancing` for the route's upstreams and round-robin otherwise. Besides a method's upstreams, only the allow-list applies: refused elements are answered by delta and never sent.
- **Upstreams:** a URL, or `{ http, ws, maxSubscriptionsPerConnection, maxConnections }`.
- **API keys:** `key`, `routes`, `rps`, `allowedIps`, `allowedDomains`, `maxSubscriptions`. Every request sends `?api-key=<uuid>`.
- **Stores:** `[stores.moka]` with `maxSizeMb` and `maxEntryMb` sets the cache size.

## Errors

Errors created by delta use codes outside JSON-RPC's reserved range, so they never clash with node or provider errors, which are passed through unchanged.

| Code | HTTP | Meaning |
| --- | --- | --- |
| `-33090` | 200 | unknown route |
| `-33091` | 401 | missing or invalid API key, or a disallowed route, IP or origin |
| `-33092` | 429 | rate limited |
| `-33094` | 200 | quorum not reached |
| `-33095` | 200 | no upstream could answer |
| `-33096` | 200 | subscription limit reached |

## Using delta from Rust

The `delta` crate is a library; the binary is a thin wrapper. The builder takes the same structure as the TOML and runs the same checks:

```rust
use delta::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Proxy::builder()
        .listen("0.0.0.0:8080")
        .upstream("helius", Upstream::url(format!("https://mainnet.helius-rpc.com/?api-key={}", env("HELIUS_KEY")?)))
        .api_key("backend", ApiKey::new(env("BACKEND_KEY")?).routes(["*"]))
        .route("fast", Route::new(["helius"]).method("getAccountInfo", Policies::new()))
        .build()?
        .run()
        .await
}
```

`delta scaffold config.toml my-proxy` generates such a project from a TOML file; [`example/`](example) is one, made from the demo config.

## Development

```sh
cargo test --workspace                              # unit tests
cargo test -p delta --test surfpool -- --ignored    # integration tests against Surfpool, with fault injection
cargo test -p delta --test scaffold -- --ignored    # compiles a scaffolded project
```

delta is in early development. Not yet supported: metrics, latency-based load balancing, config reload without a restart, several networks in one proxy, and a shared cache for several instances.

## License

[MIT](LICENSE)
