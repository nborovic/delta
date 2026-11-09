//! API keys: passed as `?api-key=<uuid>`, checked on every HTTP request and
//! once at the WebSocket upgrade; the rate limits then apply per message.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use ipnet::IpNet;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::glob::Glob;
use crate::resolve::KeySpec;

pub type KeyId = usize;

/// The request gets HTTP 401 with `-33091 unauthorized`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unauthorized;

pub struct Auth {
    keys: Vec<KeyState>,
    trusted_proxies: Vec<IpNet>,
}

struct KeyState {
    spec: KeySpec,
    /// `totalRps`: every request made with the key.
    total: Option<Mutex<Bucket>>,
    /// `rpsPerIp` and `rpmPerIp`: buckets per client.
    per_ip: Option<Mutex<IpBuckets>>,
}

/// A token bucket, per proxy instance: holds up to `capacity` tokens and
/// refills at `rate` per second.
struct Bucket {
    capacity: f64,
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn per_second(n: u32, now: Instant) -> Self {
        Self { capacity: n as f64, rate: n as f64, tokens: n as f64, last: now }
    }

    fn per_minute(n: u32, now: Instant) -> Self {
        Self { capacity: n as f64, rate: n as f64 / 60.0, tokens: n as f64, last: now }
    }

    /// Only moves forward: an older `now` (a request that read the clock,
    /// then waited for the lock) must not move `last` back, or the next
    /// request would be credited the same time twice.
    fn refill(&mut self, now: Instant) {
        if now > self.last {
            self.tokens = (self.tokens + (now - self.last).as_secs_f64() * self.rate).min(self.capacity);
            self.last = now;
        }
    }

    /// Whether `n` tokens can be taken. A batch larger than the bucket may go
    /// into debt as long as the bucket is full, so it is not refused forever.
    fn allows(&mut self, n: u32, now: Instant) -> bool {
        self.refill(now);
        self.tokens + 1e-9 >= (n as f64).min(self.capacity)
    }

    fn spend(&mut self, n: u32) {
        self.tokens -= n as f64;
    }

    /// Full again: dropping it loses nothing, a new one starts full.
    fn is_full(&self, now: Instant) -> bool {
        self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate >= self.capacity
    }
}

/// One client's buckets: `rpsPerIp` caps bursts, `rpmPerIp` sustained use.
struct Client {
    second: Option<Bucket>,
    minute: Option<Bucket>,
}

impl Client {
    fn is_full(&self, now: Instant) -> bool {
        self.second.iter().chain(&self.minute).all(|b| b.is_full(now))
    }
}

/// The per-IP buckets of one key. Clients whose buckets have refilled are
/// dropped whenever the map has doubled since the last sweep, so memory
/// follows the clients active in the last minute.
struct IpBuckets {
    per_second: Option<u32>,
    per_minute: Option<u32>,
    clients: HashMap<IpAddr, Client>,
    sweep_at: usize,
}

const MIN_SWEEP: usize = 1024;

impl IpBuckets {
    fn new(per_second: Option<u32>, per_minute: Option<u32>) -> Self {
        Self { per_second, per_minute, clients: HashMap::new(), sweep_at: MIN_SWEEP }
    }

    fn client(&mut self, ip: IpAddr, now: Instant) -> &mut Client {
        let ip = client_of(ip);
        if self.clients.len() >= self.sweep_at && !self.clients.contains_key(&ip) {
            self.clients.retain(|_, c| !c.is_full(now));
            self.sweep_at = (self.clients.len() * 2).max(MIN_SWEEP);
            // `retain` keeps the allocation, and later sweeps scan all of it:
            // after a spike has passed, give the memory back.
            if self.clients.capacity() > 4 * self.sweep_at {
                self.clients.shrink_to(self.sweep_at);
            }
        }
        let (second, minute) = (self.per_second, self.per_minute);
        self.clients.entry(ip).or_insert_with(|| Client {
            second: second.map(|n| Bucket::per_second(n, now)),
            minute: minute.map(|n| Bucket::per_minute(n, now)),
        })
    }
}

/// What the per-IP limits count as one client: an IPv4 address, or an IPv6 /64,
/// since one host usually gets a whole /64 and could rotate through it.
fn client_of(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => {
            let s = v6.segments();
            IpAddr::V6(std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
        }
        v4 => v4,
    }
}

/// What the caller presented.
pub struct Caller<'a> {
    pub api_key: Option<&'a str>,
    pub ip: IpAddr,
    /// The `Origin` header, if any.
    pub origin: Option<&'a str>,
}

impl Auth {
    pub fn new(specs: Vec<KeySpec>, trusted_proxies: Vec<IpNet>) -> Self {
        Self {
            trusted_proxies,
            keys: specs
                .into_iter()
                .map(|spec| {
                    let now = Instant::now();
                    KeyState {
                        total: spec.total_rps.map(|n| Mutex::new(Bucket::per_second(n, now))),
                        per_ip: (spec.rps_per_ip.is_some() || spec.rpm_per_ip.is_some())
                            .then(|| Mutex::new(IpBuckets::new(spec.rps_per_ip, spec.rpm_per_ip))),
                        spec,
                    }
                })
                .collect(),
        }
    }

    pub fn name(&self, key: KeyId) -> &str {
        &self.keys[key].spec.name
    }

    /// Checks the key, client IP and origin. Every request needs a key: the
    /// config refuses to load without one, and with none configured nothing
    /// is let through.
    pub fn authenticate(&self, caller: &Caller<'_>) -> Result<KeyId, Unauthorized> {
        let presented = caller.api_key.and_then(|k| Uuid::parse_str(k).ok()).ok_or(Unauthorized)?;
        // Compare against every key in constant time; no early exit.
        let mut found: Option<KeyId> = None;
        for (i, k) in self.keys.iter().enumerate() {
            let eq: bool = k.spec.key.as_bytes().ct_eq(presented.as_bytes()).into();
            if eq {
                found = Some(i);
            }
        }
        let id = found.ok_or(Unauthorized)?;
        let spec = &self.keys[id].spec;
        if let Some(nets) = &spec.allowed_ips {
            if !nets.iter().any(|n: &IpNet| n.contains(&caller.ip)) {
                tracing::debug!(key = %spec.name, ip = %caller.ip, "client IP not allowed");
                return Err(Unauthorized);
            }
        }
        if let (Some(domains), Some(origin)) = (&spec.allowed_domains, caller.origin) {
            // Only browsers send Origin, so a request without one passes:
            // this stops other websites, not scripts.
            let host = url::Url::parse(origin)
                .ok()
                .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
                .ok_or(Unauthorized)?;
            if !domains.iter().any(|d| domain_matches(d, &host)) {
                tracing::debug!(key = %spec.name, %host, "origin not allowed");
                return Err(Unauthorized);
            }
        }
        Ok(id)
    }

    /// The client IP. `X-Forwarded-For` is believed only when the TCP peer is
    /// a trusted proxy; then entries are read from the right, skipping
    /// further trusted proxies, and the first other address is the client.
    /// Anyone else could write the header themselves.
    pub fn client_ip(&self, forwarded_for: Option<&str>, peer: IpAddr) -> IpAddr {
        let trusted = |ip: &IpAddr| self.trusted_proxies.iter().any(|n| n.contains(ip));
        if !trusted(&peer) {
            return peer;
        }
        let mut ip = peer;
        for entry in forwarded_for.unwrap_or("").rsplit(',') {
            match entry.trim().parse::<IpAddr>() {
                Ok(e) => {
                    ip = e;
                    if !trusted(&e) {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        ip
    }

    /// Open subscriptions the key may hold, across all its connections.
    pub fn max_subscriptions(&self, key: KeyId) -> Option<usize> {
        self.keys[key].spec.max_subscriptions
    }

    pub fn route_allowed(&self, key: KeyId, route: &str) -> bool {
        self.keys[key].spec.routes.iter().any(|g: &Glob| g.matches(route))
    }

    /// Counts `n` requests from `ip` against the key's `totalRps`,
    /// `rpsPerIp` and `rpmPerIp`. False = rate limited; then none is charged.
    pub fn take(&self, key: KeyId, ip: IpAddr, n: u32) -> bool {
        let k = &self.keys[key];
        // Always this order: the per-IP lock, then the total.
        let mut per_ip = k.per_ip.as_ref().map(|m| m.lock().unwrap());
        let mut total = k.total.as_ref().map(|m| m.lock().unwrap());
        // Read under the locks, so requests reach the buckets in time order.
        let now = Instant::now();
        let (second, minute) = match per_ip.as_deref_mut().map(|b| b.client(ip, now)) {
            Some(c) => (c.second.as_mut(), c.minute.as_mut()),
            None => (None, None),
        };
        let mut limits = [second, minute, total.as_deref_mut()];
        let allowed = limits.iter_mut().flatten().all(|b| b.allows(n, now));
        if allowed {
            limits.into_iter().flatten().for_each(|b| b.spend(n));
        }
        allowed
    }
}

fn domain_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(suffix) => host.len() > suffix.len() && host.ends_with(suffix) && host.as_bytes()[host.len() - suffix.len() - 1] == b'.',
        None => pattern == host,
    }
}



/// The `api-key` query parameter.
pub fn api_key_from_query(query: Option<&str>) -> Option<String> {
    let q = query?;
    url::form_urlencoded::parse(q.as_bytes())
        .find(|(k, _)| k == "api-key")
        .map(|(_, v)| v.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: &str = "3f8a2c1e-7b4d-4e9a-b6c2-1d5e8f0a9b37";

    fn auth(ips: Option<Vec<IpNet>>, domains: Option<Vec<String>>, rps: Option<u32>) -> Auth {
        limited(rps, None)
            .with(|k| {
                k.allowed_ips = ips;
                k.allowed_domains = domains;
            })
            .build()
    }

    struct Spec(KeySpec);

    impl Spec {
        fn with(mut self, f: impl FnOnce(&mut KeySpec)) -> Self {
            f(&mut self.0);
            self
        }
        fn build(self) -> Auth {
            Auth::new(vec![self.0], Vec::new())
        }
    }

    fn limited(total_rps: Option<u32>, rpm_per_ip: Option<u32>) -> Spec {
        Spec(KeySpec {
            name: "k".into(),
            key: Uuid::parse_str(K).unwrap(),
            total_rps,
            rps_per_ip: None,
            rpm_per_ip,
            allowed_ips: None,
            allowed_domains: None,
            routes: vec![Glob::parse("fast").unwrap(), Glob::parse("str*").unwrap()],
            max_subscriptions: None,
        })
    }

    fn caller<'a>(key: Option<&'a str>, ip: &str, origin: Option<&'a str>) -> Caller<'a> {
        Caller { api_key: key, ip: ip.parse().unwrap(), origin }
    }

    #[test]
    fn key_ip_domain_route() {
        let a = auth(Some(vec!["10.0.0.0/8".parse().unwrap()]), Some(vec!["*.example.com".into()]), None);
        assert!(a.authenticate(&caller(None, "10.1.1.1", None)).is_err());
        assert!(a.authenticate(&caller(Some("nope"), "10.1.1.1", None)).is_err());
        assert!(a.authenticate(&caller(Some(K), "11.1.1.1", None)).is_err());
        let id = a.authenticate(&caller(Some(K), "10.1.1.1", None)).unwrap();
        assert!(a.authenticate(&caller(Some(K), "10.1.1.1", Some("https://app.example.com"))).is_ok());
        assert!(a.authenticate(&caller(Some(K), "10.1.1.1", Some("https://example.com"))).is_err());
        assert!(a.authenticate(&caller(Some(K), "10.1.1.1", Some("https://evil.com"))).is_err());
        assert!(a.route_allowed(id, "fast"));
        assert!(a.route_allowed(id, "stream"));
        assert!(!a.route_allowed(id, "safe"));
    }

    #[test]
    fn rate_limit() {
        let a = auth(None, None, Some(5));
        let id = a.authenticate(&caller(Some(K), "1.1.1.1", None)).unwrap();
        assert!(a.take(id, ip("1.1.1.1"), 3));
        assert!(a.take(id, ip("2.2.2.2"), 2), "one budget for every IP");
        assert!(!a.take(id, ip("3.3.3.3"), 1));
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn rpm_per_ip_gives_each_client_its_own_budget() {
        let a = limited(None, Some(3)).build();
        assert!(a.take(0, ip("1.1.1.1"), 2));
        assert!(a.take(0, ip("1.1.1.1"), 1));
        assert!(!a.take(0, ip("1.1.1.1"), 1));
        assert!(a.take(0, ip("2.2.2.2"), 3), "another IP is not affected");
        assert!(!a.take(0, ip("2.2.2.2"), 1));
    }

    #[test]
    fn rpm_per_ip_refills_per_minute() {
        let mut b = Bucket::per_minute(60, Instant::now());
        let start = Instant::now();
        assert!(b.allows(60, start));
        b.spend(60);
        assert!(!b.allows(1, start));
        assert!(b.allows(1, start + std::time::Duration::from_millis(1000)), "60 per minute: one token a second");
        assert!(!b.allows(2, start + std::time::Duration::from_millis(1000)));
    }

    #[test]
    fn ipv6_counts_per_64() {
        let a = limited(None, Some(2)).build();
        assert!(a.take(0, ip("2001:db8:1:2::1"), 1));
        assert!(a.take(0, ip("2001:db8:1:2:ffff::9"), 1));
        assert!(!a.take(0, ip("2001:db8:1:2::abcd"), 1), "same /64");
        assert!(a.take(0, ip("2001:db8:1:3::1"), 1), "another /64");
        assert!(a.take(0, ip("::ffff:1.2.3.4"), 2));
        assert!(!a.take(0, ip("1.2.3.4"), 1), "an IPv4-mapped address is that IPv4 address");
    }

    #[test]
    fn a_refusal_charges_neither_limit() {
        let a = limited(Some(3), Some(2)).build();
        assert!(a.take(0, ip("1.1.1.1"), 2));
        assert!(!a.take(0, ip("1.1.1.1"), 1), "per-IP refuses");
        assert!(a.take(0, ip("2.2.2.2"), 1), "...without spending the total");
        assert!(!a.take(0, ip("3.3.3.3"), 1), "total refuses");
        let a = limited(Some(1), Some(5)).build();
        assert!(a.take(0, ip("1.1.1.1"), 1));
        assert!(!a.take(0, ip("2.2.2.2"), 1), "total refuses");
        let per_ip = a.keys[0].per_ip.as_ref().unwrap().lock().unwrap();
        assert_eq!(per_ip.clients[&ip("2.2.2.2")].minute.as_ref().unwrap().tokens, 5.0, "...without spending 2.2.2.2's budget");
    }

    #[test]
    fn rps_per_ip_caps_bursts_and_rpm_per_ip_the_minute() {
        let a = limited(None, Some(4)).with(|k| k.rps_per_ip = Some(2)).build();
        let start = Instant::now();
        assert!(a.take(0, ip("1.1.1.1"), 2));
        assert!(!a.take(0, ip("1.1.1.1"), 1), "2 per second");
        let mut per_ip = a.keys[0].per_ip.as_ref().unwrap().lock().unwrap();
        let client = per_ip.client(ip("1.1.1.1"), start);
        let later = start + std::time::Duration::from_millis(1100);
        assert!(client.second.as_mut().unwrap().allows(2, later), "a second later the burst budget is back");
        assert!(!client.minute.as_mut().unwrap().allows(3, later), "but only 2 of the minute's 4 are left");
        drop(per_ip);
        let rps_only = limited(None, None).with(|k| k.rps_per_ip = Some(1)).build();
        assert!(rps_only.take(0, ip("1.1.1.1"), 1));
        assert!(!rps_only.take(0, ip("1.1.1.1"), 1));
        assert!(rps_only.take(0, ip("2.2.2.2"), 1), "rpsPerIp alone is per IP too");
    }

    #[test]
    fn an_older_timestamp_does_not_rewind_the_bucket() {
        let start = Instant::now();
        let at = |ms| start + std::time::Duration::from_millis(ms);
        let mut b = Bucket::per_second(1, start);
        assert!(b.allows(1, at(1000)));
        b.spend(1);
        // A request that read the clock earlier but got the lock later.
        assert!(!b.allows(1, at(500)));
        assert!(!b.allows(1, at(1600)), "0.6 s since the spend: the 500 ms before it must not be credited again");
        assert!(b.allows(1, at(2000)));
    }

    #[test]
    fn a_sweep_after_a_spike_gives_the_memory_back() {
        let mut m = IpBuckets::new(None, Some(60));
        let start = Instant::now();
        // 2^17 clients: the next new one is due a sweep.
        for i in 0..1u32 << 17 {
            m.client(IpAddr::from(i.to_be_bytes()), start).minute.as_mut().unwrap().spend(1);
        }
        let later = start + std::time::Duration::from_secs(2);
        m.client(ip("8.8.8.8"), later);
        assert_eq!(m.clients.len(), 1);
        assert!(m.clients.capacity() <= 4 * MIN_SWEEP, "capacity {} kept after the spike", m.clients.capacity());
    }

    #[test]
    fn idle_ip_buckets_are_dropped() {
        let mut m = IpBuckets::new(Some(10), Some(60));
        let start = Instant::now();
        for i in 0..MIN_SWEEP as u32 {
            let c = m.client(IpAddr::from(i.to_be_bytes()), start);
            c.second.as_mut().unwrap().spend(1);
            c.minute.as_mut().unwrap().spend(1);
        }
        assert_eq!(m.clients.len(), MIN_SWEEP);
        // A second later each has refilled its one token, per second and per minute.
        let later = start + std::time::Duration::from_secs(1);
        m.client(ip("8.8.8.8"), later);
        assert_eq!(m.clients.len(), 1, "refilled clients dropped, the new one kept");
    }

    #[test]
    fn no_keys_lets_nothing_through() {
        let a = Auth::new(Vec::new(), Vec::new());
        assert!(a.authenticate(&caller(None, "127.0.0.1", None)).is_err());
        assert!(a.authenticate(&caller(Some(K), "127.0.0.1", None)).is_err());
    }

    #[test]
    fn forwarded_for_only_from_trusted_proxies() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let open = Auth::new(Vec::new(), Vec::new());
        assert_eq!(open.client_ip(Some("10.0.0.1"), ip("9.9.9.9")), ip("9.9.9.9"), "no trusted proxies: header ignored");

        let behind = Auth::new(Vec::new(), vec!["10.0.0.0/8".parse().unwrap()]);
        assert_eq!(behind.client_ip(Some("1.1.1.1, 2.2.2.2"), ip("10.0.0.5")), ip("2.2.2.2"), "what the proxy saw");
        assert_eq!(behind.client_ip(Some("1.1.1.1"), ip("9.9.9.9")), ip("9.9.9.9"), "untrusted peer: header ignored");
        assert_eq!(behind.client_ip(Some("1.1.1.1, 2.2.2.2, 10.0.0.7"), ip("10.0.0.5")), ip("2.2.2.2"), "skips a second trusted hop");
        assert_eq!(behind.client_ip(None, ip("10.0.0.5")), ip("10.0.0.5"));
        assert_eq!(behind.client_ip(Some("garbage"), ip("10.0.0.5")), ip("10.0.0.5"));
        assert_eq!(api_key_from_query(Some("a=1&api-key=xyz")), Some("xyz".into()));
    }
}
