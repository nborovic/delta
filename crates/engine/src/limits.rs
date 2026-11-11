//! Code defaults. None of these are config fields.

use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Limits {
    /// Timeout of one upstream call.
    pub per_call_timeout: Duration,
    /// Total budget of one client request, counted from its arrival.
    pub request_deadline: Duration,
    /// Concurrent calls per upstream; beyond this, selectors prefer others
    /// and calls wait for a slot.
    pub inflight_per_upstream: usize,
    /// Cache size when `[stores.moka]` is absent.
    pub default_cache_mb: u64,
    /// Responses larger than this are not cached (`[stores.moka]
    /// maxEntryMb`).
    pub max_cached_bytes: usize,
    /// Maximum size of a client request body.
    pub max_body_bytes: usize,
    /// Time a client has to send its request headers, and its body.
    pub client_read_timeout: Duration,
    /// Time a client WebSocket has to accept one message before it is closed.
    pub client_write_timeout: Duration,
    /// Messages one client WebSocket may have in flight (received and not
    /// yet answered); past it delta stops reading from that client.
    pub ws_in_flight: usize,
    /// A client WebSocket with no traffic either way (no message or ping
    /// from the client, nothing sent to it) for this long is closed. Clients
    /// keep connections alive as with RPC providers: web3.js and
    /// @solana/kit send a JSON-RPC `ping` every 5 s.
    pub ws_idle_timeout: Duration,
    /// How long shutdown waits for open requests before exiting anyway.
    pub shutdown_grace: Duration,
    pub health: HealthLimits,
}

#[derive(Clone, Debug)]
pub struct HealthLimits {
    pub window: Duration,
    pub consecutive_failures: u32,
    pub failure_ratio: f64,
    pub min_requests: usize,
    pub overloaded_base: Duration,
    pub overloaded_cap: Duration,
    pub unhealthy_base: Duration,
    pub unhealthy_cap: Duration,
    /// Strikes reset after this long without a failure.
    pub decay: Duration,
    /// ±fraction applied to every backoff.
    pub jitter: f64,
    /// A probe that has not reported back after this long is abandoned and
    /// another one may be let through.
    pub probe_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            per_call_timeout: Duration::from_secs(10),
            request_deadline: Duration::from_secs(30),
            inflight_per_upstream: 256,
            default_cache_mb: 256,
            max_cached_bytes: 16 * 1024 * 1024,
            max_body_bytes: 10 * 1024 * 1024,
            client_read_timeout: Duration::from_secs(10),
            client_write_timeout: Duration::from_secs(30),
            ws_in_flight: 256,
            ws_idle_timeout: Duration::from_secs(600),
            shutdown_grace: Duration::from_secs(10),
            health: HealthLimits::default(),
        }
    }
}

impl Default for HealthLimits {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(30),
            consecutive_failures: 5,
            failure_ratio: 0.5,
            min_requests: 20,
            overloaded_base: Duration::from_secs(1),
            overloaded_cap: Duration::from_secs(30),
            unhealthy_base: Duration::from_secs(5),
            unhealthy_cap: Duration::from_secs(300),
            decay: Duration::from_secs(60),
            jitter: 0.2,
            probe_timeout: Duration::from_secs(30),
        }
    }
}
