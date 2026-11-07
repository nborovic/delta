//! API keys: passed as `?api-key=<uuid>`, checked on every HTTP request and
//! once at the WebSocket upgrade; `rps` then applies per message.

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
    bucket: Option<Mutex<Bucket>>,
}

/// Token bucket per key, per proxy instance, shared across routes.
struct Bucket {
    rate: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rps: u32) -> Self {
        Self { rate: rps as f64, tokens: rps as f64, last: Instant::now() }
    }

    /// Takes `n` tokens. A batch larger than the bucket may go into debt as
    /// long as the bucket is full, so it is not refused forever.
    fn take(&mut self, n: u32) -> bool {
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate).min(self.rate);
        self.last = now;
        let need = (n as f64).min(self.rate);
        if self.tokens + 1e-9 >= need {
            self.tokens -= n as f64;
            true
        } else {
            false
        }
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
                .map(|spec| KeyState { bucket: spec.rps.map(|r| Mutex::new(Bucket::new(r))), spec })
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

    /// Counts `n` requests against the key's `rps`. False = rate limited.
    pub fn take(&self, key: KeyId, n: u32) -> bool {
        match &self.keys[key].bucket {
            None => true,
            Some(b) => b.lock().unwrap().take(n),
        }
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
        Auth::new(vec![KeySpec {
            name: "k".into(),
            key: Uuid::parse_str(K).unwrap(),
            rps,
            allowed_ips: ips,
            allowed_domains: domains,
            routes: vec![Glob::parse("fast").unwrap(), Glob::parse("str*").unwrap()],
            max_subscriptions: None,
        }], Vec::new())
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
        assert!(a.take(id, 3));
        assert!(a.take(id, 2));
        assert!(!a.take(id, 1));
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
