//! delta: a self-hosted JSON-RPC reverse proxy. Several upstreams behind one
//! endpoint, a different ruleset per URL route (`/rpc/<route>`).
//!
//! Library-first: everything goes through [`Proxy::builder()`] or
//! [`Proxy::from_toml`]. The `delta` binary reads a TOML file at startup.
//!
//! ```no_run
//! use delta::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     Proxy::builder()
//!         .listen("0.0.0.0:8080")
//!         .moka(Moka::max_size_mb(2048))
//!         .upstream("helius", Upstream::url(format!("https://mainnet.helius-rpc.com/?api-key={}", env("HELIUS_KEY")?)))
//!         .api_key("backend", ApiKey::new(env("BACKEND_KEY")?).total_rps(500).routes(["*"]))
//!         .route("fast", Route::new(["helius"])
//!             .load_balancing(LoadBalancing::RoundRobin).mode(Mode::hedge(1))
//!             .method("getAccountInfo", Policies::new().cache(Cache::ttl(200))))
//!         .build()?
//!         .run()
//!         .await
//! }
//! ```

pub mod app;
pub mod auth;
pub mod config;
pub mod glob;
pub mod resolve;
pub mod scaffold;
pub mod server;
pub mod subs;
pub mod ws;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use delta_engine::{CacheBackend, HttpTransport, MokaCache, Shared};
use tokio::net::TcpListener;

#[doc(hidden)]
pub use delta_engine::{HealthLimits, Limits};

pub use config::{
    env, ApiKey, Cache, Config, ConfigError, LoadBalancing, Mode, Moka, Policies, Retry, Route, Store, Upstream,
};

use app::App;
use auth::Auth;
use subs::Subscriptions;

/// A checked, ready-to-run proxy.
pub struct Proxy {
    app: Arc<App>,
    listen: SocketAddr,
}

/// Builds the same structure as the TOML file; [`ProxyBuilder::build`] runs
/// every config check.
#[derive(Default)]
pub struct ProxyBuilder {
    config: Config,
}

impl ProxyBuilder {
    pub fn listen(mut self, addr: impl Into<String>) -> Self {
        self.config.listen = addr.into();
        self
    }
    /// Front proxies whose `X-Forwarded-For` is believed (IPs or CIDR ranges).
    pub fn trusted_proxies<I: IntoIterator<Item = S>, S: Into<String>>(mut self, proxies: I) -> Self {
        self.config.trusted_proxies = proxies.into_iter().map(Into::into).collect();
        self
    }
    pub fn moka(mut self, moka: Moka) -> Self {
        self.config.stores.moka = Some(moka);
        self
    }
    pub fn upstream(mut self, name: impl Into<String>, upstream: Upstream) -> Self {
        self.config.upstreams.insert(name.into(), upstream);
        self
    }
    pub fn api_key(mut self, name: impl Into<String>, key: ApiKey) -> Self {
        self.config.api_keys.insert(name.into(), key);
        self
    }
    pub fn route(mut self, name: impl Into<String>, route: Route) -> Self {
        self.config.routes.insert(name.into(), route);
        self
    }
    /// The config built so far (the same structure the TOML parses into).
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn build(self) -> Result<Proxy, ConfigError> {
        Proxy::from_config(&self.config)
    }
}

impl Proxy {
    pub fn builder() -> ProxyBuilder {
        ProxyBuilder::default()
    }

    /// Reads a TOML config; `${VAR}` is replaced from the environment.
    pub fn from_toml(path: impl AsRef<Path>) -> Result<Proxy, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::one(format!("cannot read {}: {e}", path.display())))?;
        Self::from_toml_str(&text)
    }

    pub fn from_toml_str(text: &str) -> Result<Proxy, ConfigError> {
        let config = config::parse_toml(text, &|k| std::env::var(k).ok())?;
        Self::from_config(&config)
    }

    pub fn from_config(config: &Config) -> Result<Proxy, ConfigError> {
        Self::from_config_with_limits(config, Limits::default())
    }

    /// [`Proxy::from_config`] with the code defaults (timeouts, health
    /// backoff, batch and body limits) replaced. These are deliberately not
    /// config fields; this exists for tests.
    #[doc(hidden)]
    pub fn from_config_with_limits(config: &Config, mut limits: Limits) -> Result<Proxy, ConfigError> {
        let resolved = resolve::resolve(config)?;
        for w in &resolved.warnings {
            tracing::warn!("config: {w}");
        }
        let cache_bytes = resolved.cache_bytes.unwrap_or(limits.default_cache_mb * 1024 * 1024);
        let max_entry = resolved.max_entry_bytes.unwrap_or(limits.max_cached_bytes as u64).min(cache_bytes);
        limits.max_cached_bytes = usize::try_from(max_entry).unwrap_or(usize::MAX);
        let cache: Arc<dyn CacheBackend> = Arc::new(MokaCache::new(cache_bytes));
        let shared = Shared::new(resolved.upstreams, cache, Arc::new(HttpTransport::new()), limits);
        let app = Arc::new(App {
            subs: Subscriptions::new(shared.clone()),
            shared,
            routes: resolved.routes,
            auth: Auth::new(resolved.api_keys, resolved.trusted_proxies),
            shutdown: tokio::sync::watch::channel(false).0,
        });
        Ok(Proxy { app, listen: resolved.listen })
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen
    }

    /// Binds `listen` and `listen` + 1 and serves until Ctrl-C or SIGTERM.
    ///
    /// Both ports serve HTTP and WebSocket alike. A Solana node has its
    /// WebSocket on the RPC port + 1, and web3.js and the Solana CLI look
    /// for it there when a URL has a port, so they work without being told.
    pub async fn run(self) -> anyhow::Result<()> {
        let addrs = Self::ports(self.listen)?;
        let mut listeners = Vec::new();
        for addr in addrs {
            let listener = TcpListener::bind(addr).await.map_err(|e| anyhow::anyhow!("cannot listen on {addr}: {e}"))?;
            listeners.push(listener);
        }
        tracing::info!("listening on {} and {} (HTTP and WebSocket on both)", addrs[0], addrs[1]);
        self.serve_all(listeners, shutdown_signal()).await?;
        Ok(())
    }

    /// `listen` and `listen` + 1.
    fn ports(listen: SocketAddr) -> anyhow::Result<[SocketAddr; 2]> {
        match listen.port() {
            0 => anyhow::bail!("listen: {listen} needs a fixed port, because delta also listens on port + 1"),
            u16::MAX => anyhow::bail!("listen: port {} leaves no room for port + 1", u16::MAX),
            p => Ok([listen, SocketAddr::new(listen.ip(), p + 1)]),
        }
    }

    /// Serves on an already-bound listener until `shutdown` resolves.
    pub async fn serve(self, listener: TcpListener, shutdown: impl std::future::Future<Output = ()>) -> std::io::Result<()> {
        self.serve_all(vec![listener], shutdown).await
    }

    /// Serves on several already-bound listeners until `shutdown` resolves.
    pub async fn serve_all(self, listeners: Vec<TcpListener>, shutdown: impl std::future::Future<Output = ()>) -> std::io::Result<()> {
        server::serve(self.app, listeners, shutdown).await
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A free port whose neighbour is free too.
    fn free_pair() -> u16 {
        loop {
            let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let p = a.local_addr().unwrap().port();
            if p < u16::MAX && std::net::TcpListener::bind(("127.0.0.1", p + 1)).is_ok() {
                return p;
            }
        }
    }

    #[tokio::test]
    async fn run_serves_on_listen_and_listen_plus_one() {
        let port = free_pair();
        let key = "00000000-0000-4000-8000-000000000001";
        let text = format!(
            "listen = \"127.0.0.1:{port}\"\n[upstreams]\na = \"http://127.0.0.1:1\"\n[routes.r]\nupstreams = [\"a\"]\n[routes.r.methods]\nx = {{}}\n[apiKeys.k]\nkey = \"{key}\"\nroutes = [\"*\"]\n"
        );
        let proxy = Proxy::from_config(&config::parse_toml(&text, &|_| None).unwrap()).unwrap();
        let running = tokio::spawn(proxy.run());
        for p in [port, port + 1] {
            let url = format!("http://127.0.0.1:{p}/health?api-key={key}");
            let mut answered = None;
            for _ in 0..50 {
                if let Ok(r) = reqwest::get(&url).await {
                    answered = Some(r.text().await.unwrap());
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert_eq!(answered.as_deref(), Some("ok"), "port {p}");
        }
        running.abort();
    }

    #[test]
    fn listen_needs_a_fixed_port_below_the_last() {
        let at = |s: &str| Proxy::ports(s.parse().unwrap()).map_err(|e| e.to_string());
        assert_eq!(at("0.0.0.0:8080").unwrap()[1], "0.0.0.0:8081".parse().unwrap());
        assert!(at("0.0.0.0:0").unwrap_err().contains("fixed port"));
        assert!(at("0.0.0.0:65535").unwrap_err().contains("port + 1"));
    }
}
