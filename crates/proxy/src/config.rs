//! The config file: one TOML file with `listen`, `[upstreams]` and
//! `[routes.<name>]`, plus optional `trustedProxies`, `[stores.moka]` and
//! `[apiKeys.<name>]`.
//! Anything not in it is a code default.
//!
//! These types are also the builder's types: a hand-written `main.rs`
//! builds the same structure, and every check runs in the same place.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// Config errors, all of them: the proxy refuses to start on any.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigError(pub Vec<String>);

impl ConfigError {
    pub fn one(msg: impl Into<String>) -> Self {
        ConfigError(vec![msg.into()])
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "config error: {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

#[derive(Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    /// IPs or CIDR ranges of front proxies whose `X-Forwarded-For` is
    /// believed. Empty: the TCP peer is always the client.
    #[serde(default, rename = "trustedProxies")]
    pub trusted_proxies: Vec<String>,
    #[serde(default)]
    pub stores: Stores,
    #[serde(default, rename = "apiKeys")]
    pub api_keys: BTreeMap<String, ApiKey>,
    pub upstreams: BTreeMap<String, Upstream>,
    pub routes: BTreeMap<String, Route>,
}

#[derive(Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Stores {
    pub moka: Option<Moka>,
}

/// `[stores.moka]`: memory limit of the shared in-memory cache.
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Moka {
    pub max_size_mb: u64,
    /// Largest single response that is cached. Default: 16 MB (or
    /// `maxSizeMb`, if smaller).
    #[serde(default)]
    pub max_entry_mb: Option<u64>,
}

impl Moka {
    pub fn max_size_mb(mb: u64) -> Self {
        Moka { max_size_mb: mb, max_entry_mb: None }
    }
    pub fn max_entry_mb(mut self, mb: u64) -> Self {
        self.max_entry_mb = Some(mb);
        self
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiKey {
    /// A UUID, literal or `${VAR}`; parsed after expansion.
    pub key: String,
    pub rps: Option<u32>,
    /// IPs or CIDR ranges.
    pub allowed_ips: Option<Vec<String>>,
    /// `app.example.com` or `*.example.com`.
    pub allowed_domains: Option<Vec<String>>,
    /// Route names or globs; `*` = all.
    pub routes: Vec<String>,
    /// Open subscriptions this key may hold, across all its connections.
    pub max_subscriptions: Option<u32>,
}

impl ApiKey {
    pub fn new(key: impl Into<String>) -> Self {
        ApiKey { key: key.into(), rps: None, allowed_ips: None, allowed_domains: None, routes: Vec::new(), max_subscriptions: None }
    }
    pub fn rps(mut self, rps: u32) -> Self {
        self.rps = Some(rps);
        self
    }
    pub fn allowed_ips<I: IntoIterator<Item = S>, S: Into<String>>(mut self, ips: I) -> Self {
        self.allowed_ips = Some(ips.into_iter().map(Into::into).collect());
        self
    }
    pub fn allowed_domains<I: IntoIterator<Item = S>, S: Into<String>>(mut self, domains: I) -> Self {
        self.allowed_domains = Some(domains.into_iter().map(Into::into).collect());
        self
    }
    pub fn routes<I: IntoIterator<Item = S>, S: Into<String>>(mut self, routes: I) -> Self {
        self.routes = routes.into_iter().map(Into::into).collect();
        self
    }
    pub fn max_subscriptions(mut self, n: u32) -> Self {
        self.max_subscriptions = Some(n);
        self
    }
}

/// An upstream: a URL (the WebSocket URL is derived by swapping the scheme)
/// or `{ http, ws }` when they differ.
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Upstream {
    Url(String),
    Split(UpstreamTable),
}

/// `{ http, ws?, maxSubscriptionsPerConnection?, maxConnections? }`.
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpstreamTable {
    pub http: String,
    /// Default: `http` with `ws://` or `wss://`.
    #[serde(default)]
    pub ws: Option<String>,
    /// The provider's limit of subscriptions per WebSocket connection;
    /// past it delta opens another connection. Default: unlimited.
    #[serde(default)]
    pub max_subscriptions_per_connection: Option<u32>,
    /// WebSocket connections delta may open to this upstream. Default:
    /// as many as needed.
    #[serde(default)]
    pub max_connections: Option<u32>,
}

impl Upstream {
    pub fn url(url: impl Into<String>) -> Self {
        Upstream::Url(url.into())
    }
    pub fn split(http: impl Into<String>, ws: impl Into<String>) -> Self {
        Upstream::Split(UpstreamTable {
            http: http.into(),
            ws: Some(ws.into()),
            max_subscriptions_per_connection: None,
            max_connections: None,
        })
    }
    /// The provider's limit of subscriptions per WebSocket connection.
    pub fn max_subscriptions_per_connection(self, n: u32) -> Self {
        let mut t = self.into_table();
        t.max_subscriptions_per_connection = Some(n);
        Upstream::Split(t)
    }
    /// WebSocket connections delta may open to this upstream.
    pub fn max_connections(self, n: u32) -> Self {
        let mut t = self.into_table();
        t.max_connections = Some(n);
        Upstream::Split(t)
    }
    fn into_table(self) -> UpstreamTable {
        match self {
            Upstream::Url(http) => UpstreamTable { http, ws: None, max_subscriptions_per_connection: None, max_connections: None },
            Upstream::Split(t) => t,
        }
    }
}

/// A route: its upstreams, the policies every method starts from, and the
/// methods it accepts.
#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Route {
    pub upstreams: Vec<String>,
    #[serde(default)]
    pub load_balancing: Option<LoadBalancing>,
    #[serde(default)]
    pub cache: Option<Cache>,
    #[serde(default)]
    pub retry: Option<Setting<Retry>>,
    #[serde(default)]
    pub hedge: Option<Setting<Hedge>>,
    #[serde(default)]
    pub quorum: Option<Setting<Quorum>>,
    pub methods: BTreeMap<String, MethodEntry>,
}

impl Route {
    pub fn new<I: IntoIterator<Item = S>, S: Into<String>>(upstreams: I) -> Self {
        Route {
            upstreams: upstreams.into_iter().map(Into::into).collect(),
            load_balancing: None,
            cache: None,
            retry: None,
            hedge: None,
            quorum: None,
            methods: BTreeMap::new(),
        }
    }
    /// The route's own policies, which every method inherits.
    pub fn policies(&self) -> Policies {
        Policies {
            upstreams: None,
            load_balancing: self.load_balancing.clone(),
            cache: self.cache.clone(),
            retry: self.retry.clone(),
            hedge: self.hedge.clone(),
            quorum: self.quorum.clone(),
        }
    }
    pub fn load_balancing(mut self, lb: LoadBalancing) -> Self {
        self.load_balancing = Some(lb);
        self
    }
    pub fn cache(mut self, cache: Cache) -> Self {
        self.cache = Some(cache);
        self
    }
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(Setting::On(retry));
        self
    }
    pub fn no_retry(mut self) -> Self {
        self.retry = Some(Setting::Off);
        self
    }
    pub fn hedge(mut self, hedge: Hedge) -> Self {
        self.hedge = Some(Setting::On(hedge));
        self
    }
    pub fn no_hedge(mut self) -> Self {
        self.hedge = Some(Setting::Off);
        self
    }
    pub fn quorum(mut self, quorum: Quorum) -> Self {
        self.quorum = Some(Setting::On(quorum));
        self
    }
    pub fn no_quorum(mut self) -> Self {
        self.quorum = Some(Setting::Off);
        self
    }
    /// Accepts a method (exact name, `prefix*` or `*`) with these overrides.
    pub fn method(mut self, key: impl Into<String>, policies: Policies) -> Self {
        self.methods.insert(key.into(), MethodEntry::Accept(policies));
        self
    }
    /// Rejects a method with -32601.
    pub fn reject(mut self, key: impl Into<String>) -> Self {
        self.methods.insert(key.into(), MethodEntry::Reject);
        self
    }
}

/// `false` rejects the method; a table accepts it with these overrides.
#[derive(Clone, Debug, PartialEq)]
pub enum MethodEntry {
    Reject,
    Accept(Policies),
}

impl<'de> Deserialize<'de> for MethodEntry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match toml::Value::deserialize(d)? {
            toml::Value::Boolean(false) => Ok(MethodEntry::Reject),
            toml::Value::Boolean(true) => Err(D::Error::custom(
                "`true` is not accepted; use `{}` to accept a method or `false` to reject it",
            )),
            v => Policies::deserialize(v).map(MethodEntry::Accept).map_err(D::Error::custom),
        }
    }
}

/// A method's settings: its own upstreams and policy overrides.
#[derive(Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Policies {
    /// Replaces the route's upstreams for this method; any upstream may be
    /// named, not only the route's.
    pub upstreams: Option<Vec<String>>,
    pub load_balancing: Option<LoadBalancing>,
    pub cache: Option<Cache>,
    pub retry: Option<Setting<Retry>>,
    pub hedge: Option<Setting<Hedge>>,
    pub quorum: Option<Setting<Quorum>>,
}

impl Policies {
    pub fn new() -> Self {
        Self::default()
    }
    /// This method's own upstreams (names or globs), instead of the route's.
    pub fn upstreams<I: IntoIterator<Item = S>, S: Into<String>>(mut self, upstreams: I) -> Self {
        self.upstreams = Some(upstreams.into_iter().map(Into::into).collect());
        self
    }
    pub fn load_balancing(mut self, lb: LoadBalancing) -> Self {
        self.load_balancing = Some(lb);
        self
    }
    pub fn cache(mut self, cache: Cache) -> Self {
        self.cache = Some(cache);
        self
    }
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(Setting::On(retry));
        self
    }
    pub fn no_retry(mut self) -> Self {
        self.retry = Some(Setting::Off);
        self
    }
    pub fn hedge(mut self, hedge: Hedge) -> Self {
        self.hedge = Some(Setting::On(hedge));
        self
    }
    pub fn no_hedge(mut self) -> Self {
        self.hedge = Some(Setting::Off);
        self
    }
    pub fn quorum(mut self, quorum: Quorum) -> Self {
        self.quorum = Some(Setting::On(quorum));
        self
    }
    pub fn no_quorum(mut self) -> Self {
        self.quorum = Some(Setting::Off);
        self
    }

    /// True when nothing but (maybe) `upstreams` is set.
    pub(crate) fn has_no_policies(&self) -> bool {
        Policies { upstreams: None, ..self.clone() } == Policies::default()
    }
}

/// Undefined = `None`, `false` = `Some(Off)`, table = `Some(On(T))`. `false`
/// is the only boolean accepted.
#[derive(Clone, Debug, PartialEq)]
pub enum Setting<T> {
    Off,
    On(T),
}

impl<T> Setting<T> {
    pub fn on(&self) -> Option<&T> {
        match self {
            Setting::On(t) => Some(t),
            Setting::Off => None,
        }
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for Setting<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match toml::Value::deserialize(d)? {
            toml::Value::Boolean(false) => Ok(Setting::Off),
            toml::Value::Boolean(true) => Err(D::Error::custom(
                "`true` is not accepted for a policy; use a table to turn it on or `false` to turn it off",
            )),
            v => T::deserialize(v).map(Setting::On).map_err(D::Error::custom),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Cache {
    /// Default: moka.
    pub store: Option<Store>,
    /// Default: the route name.
    pub namespace: Option<String>,
    /// Milliseconds; 0 = off.
    pub ttl: Option<u64>,
}

impl Cache {
    pub fn ttl(ms: u64) -> Self {
        Cache { ttl: Some(ms), ..Cache::default() }
    }
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }
    pub fn store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Store {
    Moka,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    /// Extra tries after the first call.
    pub attempts: Option<u32>,
    /// Milliseconds between tries.
    pub delay: Option<u64>,
}

impl Retry {
    pub fn new(attempts: u32, delay_ms: u64) -> Self {
        Retry { attempts: Some(attempts), delay: Some(delay_ms) }
    }
    /// Only `attempts`; `delay` is inherited from the route.
    pub fn attempts(attempts: u32) -> Self {
        Retry { attempts: Some(attempts), delay: None }
    }
    /// Only `delay`; `attempts` is inherited from the route.
    pub fn delay(delay_ms: u64) -> Self {
        Retry { attempts: None, delay: Some(delay_ms) }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Hedge {
    pub upstreams: Option<usize>,
}

impl Hedge {
    pub fn upstreams(n: usize) -> Self {
        Hedge { upstreams: Some(n) }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Quorum {
    pub threshold: Option<usize>,
    pub upstreams: Option<usize>,
}

impl Quorum {
    pub fn new(threshold: usize, upstreams: usize) -> Self {
        Quorum { threshold: Some(threshold), upstreams: Some(upstreams) }
    }
    /// Only `threshold`; `upstreams` is inherited from the route.
    pub fn threshold(threshold: usize) -> Self {
        Quorum { threshold: Some(threshold), upstreams: None }
    }
    /// Only `upstreams`; `threshold` is inherited from the route.
    pub fn upstreams(upstreams: usize) -> Self {
        Quorum { threshold: None, upstreams: Some(upstreams) }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum LoadBalancing {
    RoundRobin,
    Random,
    Weighted { weights: BTreeMap<String, u32> },
    Priority { order: Vec<String> },
}

impl LoadBalancing {
    /// `weighted`: name or glob -> weight.
    pub fn weighted<I: IntoIterator<Item = (S, u32)>, S: Into<String>>(weights: I) -> Self {
        LoadBalancing::Weighted { weights: weights.into_iter().map(|(k, w)| (k.into(), w)).collect() }
    }
    /// `priority`: names or globs, best first.
    pub fn priority<I: IntoIterator<Item = S>, S: Into<String>>(order: I) -> Self {
        LoadBalancing::Priority { order: order.into_iter().map(Into::into).collect() }
    }
}

impl<'de> Deserialize<'de> for LoadBalancing {
    /// Hand-written so that a stray `order` or `weights` on the wrong type is
    /// an error (serde's internally tagged unit variants ignore extra fields).
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            r#type: String,
            weights: Option<BTreeMap<String, u32>>,
            order: Option<Vec<String>>,
        }
        let raw = Raw::deserialize(d)?;
        let stray = |field: &str| D::Error::custom(format!("`{field}` is not a field of loadBalancing type `{}`", raw.r#type));
        let lb = match raw.r#type.as_str() {
            "round-robin" => LoadBalancing::RoundRobin,
            "random" => LoadBalancing::Random,
            "weighted" => LoadBalancing::Weighted {
                weights: raw.weights.clone().ok_or_else(|| D::Error::missing_field("weights"))?,
            },
            "priority" => LoadBalancing::Priority {
                order: raw.order.clone().ok_or_else(|| D::Error::missing_field("order"))?,
            },
            other => {
                return Err(D::Error::unknown_variant(
                    other,
                    &["round-robin", "random", "weighted", "priority"],
                ))
            }
        };
        if raw.weights.is_some() && !matches!(lb, LoadBalancing::Weighted { .. }) {
            return Err(stray("weights"));
        }
        if raw.order.is_some() && !matches!(lb, LoadBalancing::Priority { .. }) {
            return Err(stray("order"));
        }
        Ok(lb)
    }
}

/// Reads an environment variable: the code equivalent of `${VAR}`.
pub fn env(name: &str) -> Result<String, ConfigError> {
    std::env::var(name).map_err(|_| ConfigError::one(format!("environment variable {name} is not set")))
}

/// Parses TOML, then replaces `${VAR}` in every string value from the
/// environment.
pub fn parse_toml(text: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
    parse_toml_in_context(text, &|var, _, _| lookup(var))
}

/// Like [`parse_toml`], but the lookup also gets the dotted path of the value
/// (`upstreams.helius`, `apiKeys.a.allowedIps[0]`) and whether the variable
/// is the whole value. The scaffold uses it to validate with placeholders
/// that fit each field.
pub(crate) fn parse_toml_in_context(text: &str, lookup: &dyn Fn(&str, &str, bool) -> Option<String>) -> Result<Config, ConfigError> {
    // First pass on the raw text: shape errors come with line numbers.
    toml::from_str::<Config>(text).map_err(|e| ConfigError::one(e.to_string()))?;
    let mut value: toml::Value = toml::from_str(text).map_err(|e| ConfigError::one(e.to_string()))?;
    let mut errors = Vec::new();
    expand_value(&mut value, "", lookup, &mut errors);
    if !errors.is_empty() {
        return Err(ConfigError(errors));
    }
    Config::deserialize(value).map_err(|e| ConfigError::one(e.to_string()))
}

fn expand_value(v: &mut toml::Value, path: &str, lookup: &dyn Fn(&str, &str, bool) -> Option<String>, errors: &mut Vec<String>) {
    match v {
        toml::Value::String(s) => match expand(s, &|var| lookup(var, path, s.trim() == format!("${{{var}}}"))) {
            Ok(out) => *s = out,
            Err(var) => errors.push(format!("{path}: environment variable {var} is not set")),
        },
        toml::Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                expand_value(item, &format!("{path}[{i}]"), lookup, errors);
            }
        }
        toml::Value::Table(t) => {
            for (k, item) in t.iter_mut() {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                expand_value(item, &p, lookup, errors);
            }
        }
        _ => {}
    }
}

/// Replaces every `${VAR}`; returns the first missing variable on error.
pub fn expand(s: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        let Some(len) = rest[start + 2..].find('}') else { break };
        out.push_str(&rest[..start]);
        let var = &rest[start + 2..start + 2 + len];
        out.push_str(&lookup(var).ok_or_else(|| var.to_owned())?);
        rest = &rest[start + 3 + len..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    }

    #[test]
    fn expands_whole_and_partial() {
        let l = env_of(&[("A", "x"), ("B", "y")]);
        assert_eq!(expand("${A}", &l).unwrap(), "x");
        assert_eq!(expand("https://h/?k=${A}&j=${B}", &l).unwrap(), "https://h/?k=x&j=y");
        assert_eq!(expand("$A {B}", &l).unwrap(), "$A {B}");
        assert_eq!(expand("${C}", &l).unwrap_err(), "C");
    }

    #[test]
    fn rejects_true_and_unknown_fields() {
        let base = "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\n";
        let e = parse_toml(&format!("{base}retry = true\n[routes.r.methods]\nx = {{}}\n"), &|_| None)
            .unwrap_err();
        assert!(e.to_string().contains("`true` is not accepted"), "{e}");
        let e = parse_toml(&format!("{base}[routes.r.methods]\nx = {{ quroum = {{}} }}\n"), &|_| None).unwrap_err();
        assert!(e.to_string().contains("quroum"), "{e}");
        let e = parse_toml(&format!("{base}[routes.r.methods]\nx = true\n"), &|_| None).unwrap_err();
        assert!(e.to_string().contains("`true` is not accepted"), "{e}");
        let e = parse_toml(&format!("{base}[routes.r.methods]\nx = {{ cache = false }}\n"), &|_| None).unwrap_err();
        assert!(e.to_string().contains("cache") || e.to_string().contains("boolean"), "{e}");
    }

    #[test]
    fn missing_env_is_an_error() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a/${KEY}\"\n[routes.r]\nupstreams = [\"a\"]\n[routes.r.methods]\nx = {}\n";
        let e = parse_toml(text, &|_| None).unwrap_err();
        assert!(e.to_string().contains("upstreams.a: environment variable KEY is not set"), "{e}");
    }

    #[test]
    fn latency_strategy_is_gone() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\nloadBalancing = { type = \"latency\" }\n[routes.r.methods]\nx = {}\n";
        let e = parse_toml(text, &|_| None).unwrap_err().to_string();
        assert!(e.contains("unknown variant `latency`, expected one of `round-robin`, `random`, `weighted`, `priority`"), "{e}");
    }

    #[test]
    fn load_balancing_rejects_stray_fields() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\nloadBalancing = { type = \"round-robin\", order = [\"a\"] }\n[routes.r.methods]\nx = {}\n";
        assert!(parse_toml(text, &|_| None).is_err());
    }
}
