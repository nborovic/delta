//! Test harness: Surfpool simnets as real upstreams, with a fault injector in
//! front of each one, and the proxy on an ephemeral port.
//!
//! ```text
//! client -> proxy -> Shim (HTTP faults)  -> surfpool RPC
//!                 -> WsShim (cut/inject)  -> surfpool WebSocket
//! ```

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use delta::{HealthLimits, Limits, Proxy};
use futures::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{json, Value};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// ---------------------------------------------------------------- surfpool

static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);

/// One offline Surfpool simnet (100 ms slots; finalized trails by ~30 slots).
pub struct Surfpool {
    child: Child,
    dir: PathBuf,
    pub rpc: String,
    pub ws: String,
}

impl Surfpool {
    pub async fn start() -> Surfpool {
        let bin = std::env::var("SURFPOOL").unwrap_or_else(|_| "surfpool".into());
        for _attempt in 0..5 {
            let (rpc_port, ws_port) = (free_port(), free_port());
            let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
                .join("surfpool")
                .join(format!("{}-{}", std::process::id(), NEXT_DIR.fetch_add(1, Ordering::Relaxed)));
            std::fs::create_dir_all(&dir).unwrap();
            let log = std::fs::File::create(dir.join("surfpool.log")).unwrap();
            let child = Command::new(&bin)
                .args(["start", "--no-tui", "--no-deploy", "-y", "--offline", "--no-studio", "--ci", "-t", "100"])
                .args(["-p", &rpc_port.to_string(), "-w", &ws_port.to_string()])
                .current_dir(&dir)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap_or_else(|e| panic!("cannot run `{bin}` ({e}); install surfpool or set SURFPOOL=/path/to/surfpool"));
            let mut sp = Surfpool {
                child,
                dir,
                rpc: format!("http://127.0.0.1:{rpc_port}"),
                ws: format!("ws://127.0.0.1:{ws_port}"),
            };
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if sp.child.try_wait().ok().flatten().is_some() {
                    break; // exited (port taken?): try again with new ports
                }
                if let Ok(v) = try_direct(&sp.rpc, "getSlot", Value::Null).await {
                    if v.get("result").is_some() && TcpStream::connect(("127.0.0.1", ws_port)).await.is_ok() {
                        return sp;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = sp.child.kill();
        }
        panic!("surfpool did not start");
    }

    pub async fn call(&self, method: &str, params: Value) -> Value {
        direct(&self.rpc, method, params).await
    }

    pub async fn slot(&self, commitment: &str) -> u64 {
        result(&self.call("getSlot", json!([{"commitment": commitment}])).await).as_u64().unwrap()
    }

    /// Airdrops straight on this chain (not through the proxy); returns the
    /// signature.
    pub async fn airdrop(&self, key: &str, lamports: u64) -> String {
        let v = self.call("requestAirdrop", json!([key, lamports])).await;
        result(&v).as_str().unwrap().to_owned()
    }

    /// Waits until `sig` reaches `finalized` on this chain.
    pub async fn wait_finalized(&self, sig: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let v = self.call("getSignatureStatuses", json!([[sig]])).await;
            if v.pointer("/result/value/0/confirmationStatus").and_then(Value::as_str) == Some("finalized") {
                return;
            }
            assert!(Instant::now() < deadline, "{sig} not finalized in time: {v}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Waits until `sig` is visible at all (processed or later).
    pub async fn wait_landed(&self, sig: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let v = self.call("getSignatureStatuses", json!([[sig]])).await;
            if !v.pointer("/result/value/0").is_none_or(Value::is_null) {
                return;
            }
            assert!(Instant::now() < deadline, "{sig} did not land: {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn balance(&self, key: &str) -> u64 {
        result(&self.call("getBalance", json!([key])).await)["value"].as_u64().unwrap()
    }
}

impl Drop for Surfpool {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn http() -> &'static reqwest::Client {
    static C: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    C.get_or_init(reqwest::Client::new)
}

async fn try_direct(url: &str, method: &str, params: Value) -> Result<Value, reqwest::Error> {
    let mut body = json!({"jsonrpc": "2.0", "id": 1, "method": method});
    if !params.is_null() {
        body["params"] = params;
    }
    http().post(url).json(&body).send().await?.json().await
}

/// A JSON-RPC call straight to a URL.
pub async fn direct(url: &str, method: &str, params: Value) -> Value {
    try_direct(url, method, params).await.unwrap()
}

// ---------------------------------------------------------------- HTTP shim

/// What the shim does with one upstream request.
#[derive(Clone)]
pub enum Fault {
    /// Forward to Surfpool.
    Pass,
    /// Wait, then forward.
    Delay(Duration),
    /// Answer with this HTTP status and an empty body.
    Status(u16),
    /// HTTP 429, with `Retry-After: <secs>` when given.
    TooMany(Option<u64>),
    /// HTTP 200 with this JSON-RPC error.
    RpcError(i64, &'static str),
    /// HTTP 200 with a body that is not JSON-RPC.
    Garbage,
    /// Never answer.
    Hang,
    /// Close the connection without answering.
    Drop,
    /// Forward to Surfpool (which acts on it), then close the connection
    /// without passing the answer on: a reply lost on the way back.
    Lost,
    /// Forward, then edit Surfpool's response before returning it.
    Rewrite(Arc<dyn Fn(&mut Value) + Send + Sync>),
}

pub fn rewrite(f: impl Fn(&mut Value) + Send + Sync + 'static) -> Fault {
    Fault::Rewrite(Arc::new(f))
}

#[derive(Default)]
struct ShimState {
    /// Requests being handled right now, and the most seen at once.
    active: usize,
    peak: usize,
    default: Option<Fault>,
    once: VecDeque<Fault>,
    by_method: HashMap<String, Fault>,
    calls: HashMap<String, usize>,
    total: usize,
    bodies: Vec<Value>,
}

/// HTTP fault injector in front of one Surfpool RPC endpoint. Counts every
/// request by method.
pub struct Shim {
    pub addr: SocketAddr,
    state: Arc<Mutex<ShimState>>,
    completed: Arc<AtomicUsize>,
}

impl Shim {
    pub async fn start(target: String) -> Shim {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ShimState::default()));
        let completed = Arc::new(AtomicUsize::new(0));
        let (st, done) = (state.clone(), completed.clone());
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                let (st, done, target) = (st.clone(), done.clone(), target.clone());
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req| shim_handle(st.clone(), done.clone(), target.clone(), req));
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Shim { addr, state, completed }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Applies to every request from now on (until changed).
    pub fn set(&self, f: Fault) {
        self.state.lock().unwrap().default = Some(f);
    }

    /// Applies to the next requests, one each, before anything else.
    pub fn once(&self, faults: impl IntoIterator<Item = Fault>) {
        self.state.lock().unwrap().once.extend(faults);
    }

    /// Applies to every request for this method (after `once`).
    pub fn on_method(&self, method: &str, f: Fault) {
        self.state.lock().unwrap().by_method.insert(method.to_owned(), f);
    }

    /// Back to forwarding everything.
    pub fn heal(&self) {
        let mut st = self.state.lock().unwrap();
        st.default = None;
        st.once.clear();
        st.by_method.clear();
    }

    pub fn calls(&self, method: &str) -> usize {
        self.state.lock().unwrap().calls.get(method).copied().unwrap_or(0)
    }

    pub fn total(&self) -> usize {
        self.state.lock().unwrap().total
    }

    /// Requests forwarded to Surfpool whose answer came back.
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }

    /// The most requests this shim was handling at the same time.
    pub fn peak(&self) -> usize {
        self.state.lock().unwrap().peak
    }

    /// Request bodies received, oldest first.
    pub fn bodies(&self) -> Vec<Value> {
        self.state.lock().unwrap().bodies.clone()
    }

    pub fn reset_counts(&self) {
        let mut st = self.state.lock().unwrap();
        st.calls.clear();
        st.total = 0;
        st.bodies.clear();
        self.completed.store(0, Ordering::SeqCst);
    }
}

type ShimResp = Response<Full<Bytes>>;

async fn shim_handle(
    st: Arc<Mutex<ShimState>>,
    completed: Arc<AtomicUsize>,
    target: String,
    req: Request<hyper::body::Incoming>,
) -> Result<ShimResp, std::io::Error> {
    let body = req.into_body().collect().await.map_err(std::io::Error::other)?.to_bytes();
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    // A batch is counted once, as "batch".
    let method = if parsed.is_array() { "batch".to_owned() } else { parsed.get("method").and_then(Value::as_str).unwrap_or("").to_owned() };
    struct Active(Arc<Mutex<ShimState>>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.lock().unwrap().active -= 1;
        }
    }
    let _active = {
        let mut s = st.lock().unwrap();
        s.active += 1;
        s.peak = s.peak.max(s.active);
        Active(st.clone())
    };
    let fault = {
        let mut st = st.lock().unwrap();
        *st.calls.entry(method.clone()).or_default() += 1;
        st.total += 1;
        st.bodies.push(parsed);
        st.once
            .pop_front()
            .or_else(|| st.by_method.get(&method).cloned())
            .or_else(|| st.default.clone())
            .unwrap_or(Fault::Pass)
    };
    let reply = |status: u16, body: Vec<u8>| {
        let mut r = Response::new(Full::new(Bytes::from(body)));
        *r.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
        r.headers_mut().insert("content-type", "application/json".parse().unwrap());
        r
    };
    let forward = |body: Bytes| async move {
        let resp = http().post(&target).header("content-type", "application/json").body(body).send().await;
        match resp {
            Ok(r) => {
                let status = r.status().as_u16();
                let bytes = r.bytes().await.unwrap_or_default();
                Ok((status, bytes.to_vec()))
            }
            Err(e) => Err(std::io::Error::other(e)),
        }
    };
    match fault {
        Fault::Pass => {
            let (s, b) = forward(body).await?;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(reply(s, b))
        }
        Fault::Delay(d) => {
            tokio::time::sleep(d).await;
            let (s, b) = forward(body).await?;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(reply(s, b))
        }
        Fault::Rewrite(f) => {
            let (s, b) = forward(body).await?;
            let mut v: Value = serde_json::from_slice(&b).unwrap();
            f(&mut v);
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(reply(s, serde_json::to_vec(&v).unwrap()))
        }
        Fault::Status(code) => Ok(reply(code, Vec::new())),
        Fault::TooMany(retry_after) => {
            let mut r = reply(429, Vec::new());
            if let Some(s) = retry_after {
                r.headers_mut().insert("retry-after", s.to_string().parse().unwrap());
            }
            Ok(r)
        }
        Fault::RpcError(code, msg) => {
            Ok(reply(200, json!({"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": msg}}).to_string().into_bytes()))
        }
        Fault::Garbage => Ok(reply(200, b"<html>bad gateway</html>".to_vec())),
        Fault::Hang => {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(reply(504, Vec::new()))
        }
        Fault::Drop => Err(std::io::Error::other("dropped by the test")),
        Fault::Lost => {
            forward(body).await?;
            completed.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::other("reply lost by the test"))
        }
    }
}

// ---------------------------------------------------------------- WebSocket shim

enum Ctl {
    Inject(String),
    /// Injected while holding: queued behind the held messages.
    Queue(String),
    Release,
    Cut,
}

#[derive(Default)]
struct WsShimState {
    refuse: bool,
    /// Hold Surfpool's messages to the proxy until released.
    hold: bool,
    live: Vec<mpsc::UnboundedSender<Ctl>>,
    accepted: usize,
    /// Messages the proxy sent upstream.
    sent: Vec<Value>,
    /// (connection, request id) -> method, until the upstream answers.
    pending: HashMap<(usize, u64), String>,
    /// Subscriptions Surfpool confirmed: (method, upstream subscription id).
    confirmed: Vec<(String, u64)>,
}

/// WebSocket man-in-the-middle in front of one Surfpool WebSocket endpoint.
pub struct WsShim {
    pub addr: SocketAddr,
    state: Arc<Mutex<WsShimState>>,
}

impl WsShim {
    pub async fn start(target: String) -> WsShim {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(WsShimState::default()));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                let n = {
                    let mut s = st.lock().unwrap();
                    if s.refuse {
                        drop(stream);
                        continue;
                    }
                    s.accepted += 1;
                    s.accepted
                };
                tokio::spawn(ws_pipe(st.clone(), target.clone(), stream, n));
            }
        });
        WsShim { addr, state }
    }

    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Drops every open connection abruptly (no close frame).
    pub fn cut(&self) {
        for tx in self.state.lock().unwrap().live.drain(..) {
            let _ = tx.send(Ctl::Cut);
        }
    }

    /// While on, Surfpool's messages (answers, notifications) are held back;
    /// turning it off delivers them in order.
    pub fn hold(&self, hold: bool) {
        let mut st = self.state.lock().unwrap();
        st.hold = hold;
        if !hold {
            for tx in &st.live {
                let _ = tx.send(Ctl::Release);
            }
        }
    }

    /// New connections are dropped right after the TCP accept.
    pub fn refuse(&self, refuse: bool) {
        self.state.lock().unwrap().refuse = refuse;
    }

    /// Sends a raw message to the proxy on every open connection, as if
    /// Surfpool had sent it.
    pub fn inject(&self, text: &str) {
        let st = self.state.lock().unwrap();
        for tx in &st.live {
            let msg = if st.hold { Ctl::Queue(text.to_owned()) } else { Ctl::Inject(text.to_owned()) };
            let _ = tx.send(msg);
        }
    }

    /// Methods the proxy sent upstream, in order.
    pub fn sent_methods(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .sent
            .iter()
            .filter_map(|v| v.get("method").and_then(Value::as_str).map(str::to_owned))
            .collect()
    }

    pub fn count(&self, method: &str) -> usize {
        self.sent_methods().iter().filter(|m| *m == method).count()
    }

    pub fn sent(&self) -> Vec<Value> {
        self.state.lock().unwrap().sent.clone()
    }

    /// The upstream subscription id Surfpool gave the latest `method`.
    pub fn sub_id(&self, method: &str) -> Option<u64> {
        self.state.lock().unwrap().confirmed.iter().rev().find(|(m, _)| m == method).map(|(_, id)| *id)
    }

    pub fn live(&self) -> usize {
        self.state.lock().unwrap().live.iter().filter(|t| !t.is_closed()).count()
    }

    pub fn accepted(&self) -> usize {
        self.state.lock().unwrap().accepted
    }
}

async fn ws_pipe(st: Arc<Mutex<WsShimState>>, target: String, stream: TcpStream, conn: usize) {
    let Ok(down) = tokio_tungstenite::accept_async(stream).await else { return };
    let Ok((up, _)) = tokio_tungstenite::connect_async(target.as_str()).await else { return };
    let (tx, mut ctl) = mpsc::unbounded_channel();
    st.lock().unwrap().live.push(tx);
    let (mut down_tx, mut down_rx) = down.split();
    let (mut up_tx, mut up_rx) = up.split();
    let mut held = Vec::new();
    loop {
        tokio::select! {
            m = down_rx.next() => match m {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                        let mut s = st.lock().unwrap();
                        if let (Some(id), Some(method)) = (v.get("id").and_then(Value::as_u64), v.get("method").and_then(Value::as_str)) {
                            s.pending.insert((conn, id), method.to_owned());
                        }
                        s.sent.push(v);
                    }
                    if up_tx.send(Message::Text(t)).await.is_err() { break; }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => { let _ = up_tx.close().await; break; }
                Some(Ok(_)) => {}
            },
            m = up_rx.next() => match m {
                Some(Ok(Message::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                        if let (Some(id), Some(sid)) = (v.get("id").and_then(Value::as_u64), v.get("result").and_then(Value::as_u64)) {
                            let mut s = st.lock().unwrap();
                            if let Some(method) = s.pending.remove(&(conn, id)) {
                                s.confirmed.push((method, sid));
                            }
                        }
                    }
                    if st.lock().unwrap().hold {
                        held.push(t);
                    } else if down_tx.send(Message::Text(t)).await.is_err() { break; }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => { let _ = down_tx.close().await; break; }
                Some(Ok(_)) => {}
            },
            c = ctl.recv() => match c {
                Some(Ctl::Inject(t)) => { if down_tx.send(Message::Text(t.into())).await.is_err() { break; } }
                Some(Ctl::Queue(t)) => held.push(t.into()),
                Some(Ctl::Release) => {
                    let mut failed = false;
                    for t in held.drain(..) {
                        if down_tx.send(Message::Text(t)).await.is_err() { failed = true; break; }
                    }
                    if failed { break; }
                }
                Some(Ctl::Cut) | None => break,
            },
        }
    }
}

// ---------------------------------------------------------------- proxy

/// Health backoffs and timeouts short enough for tests; no jitter.
pub fn fast_limits() -> Limits {
    Limits {
        per_call_timeout: Duration::from_secs(2),
        request_deadline: Duration::from_secs(5),
        health: HealthLimits {
            overloaded_base: Duration::from_millis(300),
            overloaded_cap: Duration::from_secs(3),
            unhealthy_base: Duration::from_millis(500),
            unhealthy_cap: Duration::from_secs(3),
            jitter: 0.0,
            probe_timeout: Duration::from_secs(3),
            ..HealthLimits::default()
        },
        ..Limits::default()
    }
}

/// Surfpool chains, one HTTP and one WebSocket shim per upstream (`u0`,
/// `u1`, ...), and the proxy.
pub struct Env {
    pub chains: Vec<Surfpool>,
    pub http: Vec<Shim>,
    pub ws: Vec<WsShim>,
    pub addr: SocketAddr,
    pub client: Client,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    served: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

/// `upstreams[i]` is the chain upstream `u<i>` forwards to: `&[0, 0]` is two
/// upstreams on one chain (same data), `&[0, 1]` two separate chains.
pub async fn env(upstreams: &[usize], toml: &str) -> Env {
    env_with(upstreams, toml, fast_limits()).await
}

pub async fn env_with(upstreams: &[usize], toml: &str, limits: Limits) -> Env {
    env_full(upstreams, "", toml, limits).await
}

/// `top` goes before `[upstreams]` (top-level keys such as `trustedProxies`).
pub async fn env_top(upstreams: &[usize], top: &str, toml: &str) -> Env {
    env_full(upstreams, top, toml, fast_limits()).await
}

pub async fn env_full(upstreams: &[usize], top: &str, toml: &str, limits: Limits) -> Env {
    env_upstreams(upstreams, top, "", toml, limits).await
}

/// `extra` is added to every upstream's table, e.g.
/// `maxSubscriptionsPerConnection = 2`.
pub async fn env_upstreams(upstreams: &[usize], top: &str, extra: &str, toml: &str, limits: Limits) -> Env {
    let n_chains = upstreams.iter().copied().max().map_or(0, |m| m + 1);
    let mut chains = Vec::new();
    for _ in 0..n_chains {
        chains.push(Surfpool::start().await);
    }
    let mut http = Vec::new();
    let mut ws = Vec::new();
    let mut text = format!("listen = \"127.0.0.1:0\"\n{top}\n\n[upstreams]\n");
    for (i, &c) in upstreams.iter().enumerate() {
        let h = Shim::start(chains[c].rpc.clone()).await;
        let w = WsShim::start(chains[c].ws.clone()).await;
        let extra = if extra.is_empty() { String::new() } else { format!(", {extra}") };
        text.push_str(&format!("u{i} = {{ http = \"{}\", ws = \"{}\"{extra} }}\n", h.url(), w.url()));
        http.push(h);
        ws.push(w);
    }
    text.push('\n');
    text.push_str(toml);
    // The proxy refuses to start without an API key. Tests that are not
    // about keys get this one, and the clients send it on every request.
    let key = (!toml.contains("[apiKeys.")).then(|| {
        text.push_str(&format!("\n[apiKeys.harness]\nkey = \"{HARNESS_KEY}\"\nroutes = [\"*\"]\n"));
        HARNESS_KEY.to_owned()
    });
    let config = delta::config::parse_toml(&text, &|_| None).unwrap_or_else(|e| panic!("{e}\n---\n{text}"));
    let proxy = Proxy::from_config_with_limits(&config, limits).unwrap_or_else(|e| panic!("{e}\n---\n{text}"));
    // Like `run()`: the same service on a port and the port after it.
    let (listener, next) = loop {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        if let Ok(n) = TcpListener::bind(("127.0.0.1", p.wrapping_add(1))).await {
            break (l, n);
        }
    };
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let served = tokio::spawn(proxy.serve_all(vec![listener, next], async move {
        let _ = stopped.await;
    }));
    Env { chains, http, ws, addr, client: Client { base: format!("http://{addr}"), key }, stop: Some(stop), served: Some(served) }
}

impl Env {
    /// Starts the proxy's shutdown; returns how long it took to finish, or
    /// None when it did not finish within `limit`.
    pub async fn stop(&mut self, limit: Duration) -> Option<Duration> {
        let started = Instant::now();
        let _ = self.stop.take().expect("stopped once").send(());
        let served = self.served.take().expect("stopped once");
        tokio::time::timeout(limit, served).await.ok().map(|_| started.elapsed())
    }

    pub fn chain(&self, i: usize) -> &Surfpool {
        &self.chains[i]
    }

    /// One JSON-RPC request to `/rpc/<route>`; returns the whole response.
    pub async fn call(&self, route: &str, method: &str, params: Value) -> Value {
        self.client.call(route, method, params).await
    }

    /// `http://.../rpc/<route>?api-key=...`, for real Solana clients.
    pub fn rpc_url(&self, route: &str) -> String {
        with_key(format!("http://{}/rpc/{route}", self.addr), self.client.key.as_deref())
    }

    pub fn ws_url(&self, route: &str) -> String {
        with_key(format!("ws://{}/rpc/{route}", self.addr), self.client.key.as_deref())
    }

    pub async fn ws(&self, route: &str) -> Ws {
        Ws::connect(&self.ws_url(route)).await
    }

    /// Calls per upstream for `method`, in upstream order.
    pub fn calls(&self, method: &str) -> Vec<usize> {
        self.http.iter().map(|s| s.calls(method)).collect()
    }

    pub fn total_calls(&self, method: &str) -> usize {
        self.calls(method).iter().sum()
    }

    pub fn reset_counts(&self) {
        for s in &self.http {
            s.reset_counts();
        }
    }
}

// ---------------------------------------------------------------- clients

/// The key the harness adds when a test configures none of its own.
pub const HARNESS_KEY: &str = "5f0e3c2a-1b4d-4e6f-8a9b-0c1d2e3f4a5b";

pub struct Client {
    pub base: String,
    /// Sent as `?api-key=` unless the path already names a key.
    pub key: Option<String>,
}

fn with_key(url: String, key: Option<&str>) -> String {
    match key {
        Some(k) if !url.contains("api-key=") => {
            let sep = if url.contains('?') { '&' } else { '?' };
            format!("{url}{sep}api-key={k}")
        }
        _ => url,
    }
}

impl Client {
    pub async fn call(&self, route: &str, method: &str, params: Value) -> Value {
        let mut body = json!({"jsonrpc": "2.0", "id": 1, "method": method});
        if !params.is_null() {
            body["params"] = params;
        }
        let (status, v) = self.json(&format!("/rpc/{route}"), &body, &[]).await;
        assert!(status == 200 || status == 429, "HTTP {status}: {v}");
        v
    }

    /// POST any body; returns the status and the raw response body.
    pub async fn raw(&self, path: &str, body: impl Into<reqwest::Body>, headers: &[(&str, &str)]) -> (u16, String) {
        let url = with_key(format!("{}{path}", self.base), self.key.as_deref());
        let mut req = http().post(url).header("content-type", "application/json").body(body);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.unwrap();
        (resp.status().as_u16(), resp.text().await.unwrap())
    }

    pub async fn json(&self, path: &str, body: &Value, headers: &[(&str, &str)]) -> (u16, Value) {
        let (status, text) = self.raw(path, body.to_string(), headers).await;
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Any request; returns the status and the response headers.
    pub async fn headers(&self, method: &str, path: &str, body: &str, headers: &[(&str, &str)]) -> (u16, reqwest::header::HeaderMap) {
        let url = with_key(format!("{}{path}", self.base), self.key.as_deref());
        let mut req = http().request(method.parse().unwrap(), url).body(body.to_owned());
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = req.send().await.unwrap();
        (resp.status().as_u16(), resp.headers().clone())
    }

    pub async fn get(&self, path: &str) -> (u16, String) {
        let resp = http().get(with_key(format!("{}{path}", self.base), self.key.as_deref())).send().await.unwrap();
        (resp.status().as_u16(), resp.text().await.unwrap())
    }
}

/// The `result` of a response; panics on an error response.
#[track_caller]
pub fn result(v: &Value) -> &Value {
    match v.get("result") {
        Some(r) => r,
        None => panic!("expected a result, got {v}"),
    }
}

/// The error code of a response; panics on a result.
#[track_caller]
pub fn code(v: &Value) -> i64 {
    match v.pointer("/error/code").and_then(Value::as_i64) {
        Some(c) => c,
        None => panic!("expected an error, got {v}"),
    }
}

#[track_caller]
pub fn message(v: &Value) -> String {
    v.pointer("/error/message").and_then(Value::as_str).unwrap_or_default().to_owned()
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A client WebSocket: responses are matched by id, notifications buffered.
pub struct Ws {
    stream: WsStream,
    notes: VecDeque<Value>,
    responses: VecDeque<Value>,
    next_id: u64,
    pub close_reason: Option<String>,
    /// Every text message received, in arrival order.
    pub received: Vec<Value>,
}

impl Ws {
    pub async fn connect(url: &str) -> Ws {
        Self::try_connect(url).await.unwrap_or_else(|e| panic!("websocket connect failed: {e}"))
    }

    pub async fn try_connect(url: &str) -> Result<Ws, tokio_tungstenite::tungstenite::Error> {
        let (stream, _) = tokio_tungstenite::connect_async(url).await?;
        Ok(Ws { stream, notes: VecDeque::new(), responses: VecDeque::new(), next_id: 1000, close_reason: None, received: Vec::new() })
    }

    pub async fn send_raw(&mut self, text: &str) {
        self.stream.send(Message::Text(text.to_owned().into())).await.unwrap();
    }

    /// Reads one message into the buffers; false when the connection ended.
    async fn pump(&mut self, until: Instant) -> bool {
        let left = until.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, self.stream.next()).await {
            Err(_) => true,
            Ok(None) | Ok(Some(Err(_))) => false,
            Ok(Some(Ok(Message::Close(frame)))) => {
                self.close_reason = Some(frame.map(|f| f.reason.to_string()).unwrap_or_default());
                false
            }
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                self.received.push(v.clone());
                if v.get("method").is_some() && v.get("id").is_none() {
                    self.notes.push_back(v);
                } else {
                    self.responses.push_back(v);
                }
                true
            }
            Ok(Some(Ok(_))) => true,
        }
    }

    /// Sends one request and waits for its response.
    pub async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let mut req = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if !params.is_null() {
            req["params"] = params;
        }
        self.send_raw(&req.to_string()).await;
        self.response_for(&json!(id), Duration::from_secs(15)).await.unwrap_or_else(|| panic!("no response to {req}"))
    }

    pub async fn response_for(&mut self, id: &Value, timeout: Duration) -> Option<Value> {
        let until = Instant::now() + timeout;
        loop {
            if let Some(pos) = self.responses.iter().position(|r| r.get("id") == Some(id)) {
                return self.responses.remove(pos);
            }
            if Instant::now() >= until || !self.pump(until).await {
                return None;
            }
        }
    }

    /// The next response of any kind (batches come back as one array).
    pub async fn next_response(&mut self, timeout: Duration) -> Option<Value> {
        let until = Instant::now() + timeout;
        loop {
            if let Some(r) = self.responses.pop_front() {
                return Some(r);
            }
            if Instant::now() >= until || !self.pump(until).await {
                return None;
            }
        }
    }

    pub async fn subscribe(&mut self, method: &str, params: Value) -> u64 {
        let v = self.call(method, params).await;
        result(&v).as_u64().unwrap_or_else(|| panic!("subscribe failed: {v}"))
    }

    pub async fn notification(&mut self, timeout: Duration) -> Option<Value> {
        let until = Instant::now() + timeout;
        loop {
            if let Some(n) = self.notes.pop_front() {
                return Some(n);
            }
            if Instant::now() >= until || !self.pump(until).await {
                return None;
            }
        }
    }

    /// Collects notifications for `d`.
    pub async fn notifications_for(&mut self, d: Duration) -> Vec<Value> {
        let until = Instant::now() + d;
        while Instant::now() < until && self.pump(until).await {}
        self.notes.drain(..).collect()
    }

    /// Waits for the server to close the connection; returns the reason.
    pub async fn closed(&mut self, timeout: Duration) -> Option<String> {
        let until = Instant::now() + timeout;
        while Instant::now() < until {
            if !self.pump(until).await {
                return Some(self.close_reason.clone().unwrap_or_default());
            }
        }
        None
    }

    pub async fn close(mut self) {
        let _ = self.stream.close(None).await;
    }
}

// ---------------------------------------------------------------- misc

pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// A random account address.
pub fn pubkey() -> String {
    bs58::encode(rand::random::<[u8; 32]>()).into_string()
}

/// A well-formed signature no chain has seen.
pub fn fake_signature() -> String {
    let mut b = [0u8; 64];
    b[..32].copy_from_slice(&rand::random::<[u8; 32]>());
    b[32..].copy_from_slice(&rand::random::<[u8; 32]>());
    bs58::encode(b).into_string()
}

/// Polls `f` until it returns true.
pub async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let until = Instant::now() + timeout;
    loop {
        if f().await {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ---------------------------------------------------------------- logs

/// Log lines written on this thread while the guard lives. `#[tokio::test]`
/// runs everything (proxy tasks included) on the test's own thread, so this
/// sees every line the proxy writes for this test and nothing from others.
///
/// One global subscriber, installed once, writes each line to the capture of
/// the thread it is written on. (A per-test thread-local subscriber can miss
/// lines while other tests run in parallel: tracing caches per call site
/// whether anyone is interested.)
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

thread_local! {
    static CAPTURE: std::cell::RefCell<Option<Arc<Mutex<Vec<u8>>>>> = const { std::cell::RefCell::new(None) };
}

/// Stops capturing on this thread when dropped.
pub struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CAPTURE.with(|c| *c.borrow_mut() = None);
    }
}

impl Logs {
    pub fn capture() -> (Logs, CaptureGuard) {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter("delta::access=debug,delta::upstream=debug")
                .with_ansi(false)
                .with_writer(|| ThreadWriter)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        let logs = Logs::default();
        CAPTURE.with(|c| *c.borrow_mut() = Some(logs.0.clone()));
        (logs, CaptureGuard)
    }

    pub fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap()).lines().map(str::to_owned).collect()
    }

    /// Lines containing every one of `parts`.
    pub fn matching(&self, parts: &[&str]) -> Vec<String> {
        self.lines().into_iter().filter(|l| parts.iter().all(|p| l.contains(p))).collect()
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

/// Appends to this thread's capture, if any; drops the line otherwise.
struct ThreadWriter;

impl std::io::Write for ThreadWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURE.with(|c| {
            if let Some(sink) = c.borrow().as_ref() {
                sink.lock().unwrap().extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------- transactions

/// A keypair that can sign System Program transfers.
pub struct Wallet {
    key: Keypair,
}

impl Wallet {
    pub fn new() -> Wallet {
        Wallet { key: Keypair::new() }
    }

    pub fn pubkey(&self) -> String {
        self.key.pubkey().to_string()
    }

    pub fn keypair(&self) -> &Keypair {
        &self.key
    }

    /// A signed legacy transaction moving `lamports` to `to`.
    pub fn transfer_tx(&self, to: &Pubkey, lamports: u64, blockhash: Hash) -> Transaction {
        let ix = solana_system_interface::instruction::transfer(&self.key.pubkey(), to, lamports);
        Transaction::new_signed_with_payer(&[ix], Some(&self.key.pubkey()), &[&self.key], blockhash)
    }

    /// The same, base64-encoded for `sendTransaction`/`simulateTransaction`
    /// with `{"encoding": "base64"}`. Returns (transaction, signature).
    pub fn transfer(&self, to: &str, lamports: u64, blockhash: &str) -> (String, String) {
        use base64::Engine as _;
        let tx = self.transfer_tx(&to.parse().unwrap(), lamports, blockhash.parse().unwrap());
        let bytes = bincode::serialize(&tx).unwrap();
        (base64::engine::general_purpose::STANDARD.encode(bytes), tx.signatures[0].to_string())
    }
}

impl Surfpool {
    pub async fn latest_blockhash(&self) -> String {
        let v = self.call("getLatestBlockhash", Value::Null).await;
        result(&v)["value"]["blockhash"].as_str().unwrap().to_owned()
    }

    /// A wallet holding `lamports` on this chain.
    pub async fn funded_wallet(&self, lamports: u64) -> Wallet {
        let w = Wallet::new();
        let sig = self.airdrop(&w.pubkey(), lamports).await;
        self.wait_landed(&sig).await;
        w
    }
}

