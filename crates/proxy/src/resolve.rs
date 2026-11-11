//! Full config check and resolution. For every (route, method pattern) the
//! effective policies are computed once and stored in a lookup table.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use delta_engine::{solana, CachePolicy, EffectivePolicies, Mode, RetryPolicy, Selector, Strategy, UpstreamId, UpstreamInfo};
use ipnet::IpNet;
use uuid::Uuid;

use crate::config::*;
use crate::glob::Glob;

/// The checked, resolved config the proxy runs on.
pub struct Resolved {
    pub listen: SocketAddr,
    pub cache_bytes: Option<u64>,
    /// `maxEntryMb` in bytes, when set.
    pub max_entry_bytes: Option<u64>,
    pub upstreams: Vec<UpstreamInfo>,
    pub routes: HashMap<String, Arc<RouteTable>>,
    pub api_keys: Vec<KeySpec>,
    pub trusted_proxies: Vec<IpNet>,
    pub warnings: Vec<String>,
}

pub struct RouteTable {
    pub name: String,
    pub upstreams: Vec<UpstreamId>,
    exact: HashMap<String, MethodPlan>,
    /// Longest prefix first.
    prefixes: Vec<(String, MethodPlan)>,
    any: Option<MethodPlan>,
    /// Picks the upstream for the part of a client batch whose methods use
    /// the route's upstreams, with the route's `loadBalancing`.
    pub batch: Selector,
    /// The same for every other upstream list a method uses: round-robin
    /// over that list.
    batch_other: HashMap<Vec<UpstreamId>, Selector>,
}

#[derive(Clone)]
pub enum MethodPlan {
    Reject,
    Accept(Arc<EffectivePolicies>),
}

impl RouteTable {
    /// The most specific matching key decides; `None` when no key matches.
    pub fn lookup(&self, method: &str) -> Option<&MethodPlan> {
        if let Some(p) = self.exact.get(method) {
            return Some(p);
        }
        for (prefix, plan) in &self.prefixes {
            if method.starts_with(prefix.as_str()) {
                return Some(plan);
            }
        }
        self.any.as_ref()
    }

    /// The policies for an accepted method, `None` when it is rejected.
    pub fn accepted(&self, method: &str) -> Option<&Arc<EffectivePolicies>> {
        match self.lookup(method)? {
            MethodPlan::Accept(p) => Some(p),
            MethodPlan::Reject => None,
        }
    }

    fn plans(&self) -> impl Iterator<Item = &Arc<EffectivePolicies>> {
        let all = self.exact.values().chain(self.prefixes.iter().map(|(_, p)| p)).chain(self.any.iter());
        all.filter_map(|p| match p {
            MethodPlan::Accept(p) => Some(p),
            MethodPlan::Reject => None,
        })
    }

    fn plans_mut(&mut self) -> impl Iterator<Item = &mut Arc<EffectivePolicies>> {
        let all = self.exact.values_mut().chain(self.prefixes.iter_mut().map(|(_, p)| p)).chain(self.any.iter_mut());
        all.filter_map(|p| match p {
            MethodPlan::Accept(p) => Some(p),
            MethodPlan::Reject => None,
        })
    }

    /// The selector for the part of a batch that goes to `upstreams` (an
    /// accepted method's list).
    pub fn batch_selector(&self, upstreams: &[UpstreamId]) -> &Selector {
        if upstreams == self.upstreams.as_slice() {
            return &self.batch;
        }
        self.batch_other.get(upstreams).unwrap_or(&self.batch)
    }
}

pub struct KeySpec {
    pub name: String,
    pub key: Uuid,
    pub total_rps: Option<u32>,
    pub rps_per_ip: Option<u32>,
    pub rpm_per_ip: Option<u32>,
    pub allowed_ips: Option<Vec<IpNet>>,
    pub allowed_domains: Option<Vec<String>>,
    pub routes: Vec<Glob>,
    pub max_subscriptions: Option<usize>,
}

/// Checks the whole config and resolves it. Every error is reported.
pub fn resolve(config: &Config) -> Result<Resolved, ConfigError> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let listen = match config.listen.parse::<SocketAddr>() {
        Ok(a) => Some(a),
        Err(_) => {
            errors.push(format!("listen: `{}` is not an address such as 0.0.0.0:8080", config.listen));
            None
        }
    };

    // Upstreams: names are global, ids are their index in name order.
    let mut upstreams = Vec::new();
    for (name, up) in &config.upstreams {
        let (http, ws, per_conn, conns) = match up {
            Upstream::Url(url) => (url.clone(), derive_ws(url), None, None),
            Upstream::Split(s) => (
                s.http.clone(),
                s.ws.clone().or_else(|| derive_ws(&s.http)),
                s.max_subscriptions_per_connection,
                s.max_connections,
            ),
        };
        for (field, v) in [("maxSubscriptionsPerConnection", per_conn), ("maxConnections", conns)] {
            if v == Some(0) {
                errors.push(format!("upstreams.{name}.{field}: must be at least 1 (leave it out for no limit)"));
            }
        }
        check_url(&format!("upstreams.{name}"), &http, &["http", "https"], &mut errors);
        let ws = match ws {
            Some(ws) => {
                check_url(&format!("upstreams.{name}.ws"), &ws, &["ws", "wss"], &mut errors);
                ws
            }
            None => String::new(),
        };
        upstreams.push(UpstreamInfo {
            id: upstreams.len(),
            name: name.clone(),
            http,
            ws,
            max_subscriptions_per_connection: per_conn.map(|n| n as usize),
            max_connections: conns.map(|n| n as usize),
        });
    }
    if upstreams.is_empty() {
        errors.push("upstreams: at least one upstream is required".into());
    }
    let names: Vec<&str> = upstreams.iter().map(|u| u.name.as_str()).collect();

    let mut tables = Vec::new();
    for (route_name, route) in &config.routes {
        if let Some(table) = resolve_route(route_name, route, &names, &mut errors, &mut warnings) {
            tables.push(table);
        }
    }
    keep_for_the_longest_ttl(&mut tables);
    let routes: HashMap<String, Arc<RouteTable>> = tables.into_iter().map(|t| (t.name.clone(), Arc::new(t))).collect();
    if config.routes.is_empty() {
        errors.push("routes: at least one route is required".into());
    }

    let api_keys = resolve_keys(&config.api_keys, &config.routes, &mut errors, &mut warnings);
    if config.api_keys.is_empty() {
        errors.push(
            "apiKeys: at least one API key is required; every request must carry one as ?api-key=<uuid> \
             (add an [apiKeys.<name>] table with a key from `uuidgen`)"
                .into(),
        );
    }

    let trusted_proxies = config
        .trusted_proxies
        .iter()
        .filter_map(|s| match parse_net(s) {
            Some(n) => Some(n),
            None => {
                errors.push(format!("trustedProxies: `{s}` is not an IP or CIDR range"));
                None
            }
        })
        .collect();

    if let Some(m) = &config.stores.moka {
        if m.max_size_mb == 0 {
            errors.push("stores.moka.maxSizeMb: must be at least 1".into());
        }
        match m.max_entry_mb {
            Some(0) => errors.push("stores.moka.maxEntryMb: must be at least 1".into()),
            Some(e) if e > m.max_size_mb && m.max_size_mb > 0 => errors.push(format!(
                "stores.moka.maxEntryMb: {e} is larger than maxSizeMb ({}); no entry that size fits in the cache",
                m.max_size_mb
            )),
            _ => {}
        }
    }

    if !errors.is_empty() {
        return Err(ConfigError(errors));
    }
    Ok(Resolved {
        listen: listen.expect("checked"),
        cache_bytes: config.stores.moka.as_ref().map(|m| m.max_size_mb * 1024 * 1024),
        max_entry_bytes: config.stores.moka.as_ref().and_then(|m| m.max_entry_mb).map(|mb| mb * 1024 * 1024),
        upstreams,
        routes,
        api_keys,
        trusted_proxies,
        warnings,
    })
}

/// Routes sharing a namespace share entries, and each reader judges an
/// entry's age by its own TTL. An entry is kept as long as the longest TTL
/// any method in its namespace uses, so a short TTL never cuts a long one.
fn keep_for_the_longest_ttl(tables: &mut [RouteTable]) {
    let mut longest: HashMap<String, Duration> = HashMap::new();
    for t in tables.iter() {
        for c in t.plans().filter_map(|p| p.cache.as_ref()) {
            let keep = longest.entry(c.namespace.clone()).or_default();
            *keep = (*keep).max(c.ttl);
        }
    }
    for t in tables.iter_mut() {
        for plan in t.plans_mut() {
            // Not shared yet: resolution holds the only reference.
            let p = Arc::get_mut(plan).expect("unshared during resolution");
            if let Some(c) = &mut p.cache {
                c.keep = longest[&c.namespace];
            }
        }
    }
}

fn derive_ws(url: &str) -> Option<String> {
    if let Some(rest) = url.strip_prefix("https://") {
        Some(format!("wss://{rest}"))
    } else {
        url.strip_prefix("http://").map(|rest| format!("ws://{rest}"))
    }
}

fn check_url(path: &str, url: &str, schemes: &[&str], errors: &mut Vec<String>) {
    match url::Url::parse(url) {
        Ok(u) if schemes.contains(&u.scheme()) => {}
        // Never echo the URL: it may hold an API key.
        Ok(u) => errors.push(format!("{path}: scheme `{}` is not one of {}", u.scheme(), schemes.join(", "))),
        Err(e) => errors.push(format!("{path}: invalid URL ({e})")),
    }
}

/// Indices of the upstreams a list of names or globs matches, in upstream
/// order, deduplicated. Reports patterns that match nothing.
fn match_upstreams(path: &str, patterns: &[String], names: &[&str], errors: &mut Vec<String>) -> Vec<UpstreamId> {
    let mut out = Vec::new();
    for p in patterns {
        let Some(glob) = Glob::parse(p) else {
            errors.push(format!("{path}: `{p}` is not a name, a prefix glob or \"*\""));
            continue;
        };
        let matched: Vec<UpstreamId> = (0..names.len()).filter(|&i| glob.matches(names[i])).collect();
        if matched.is_empty() {
            errors.push(format!("{path}: `{p}` matches no upstream"));
        }
        for id in matched {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    out.sort_unstable();
    out
}

fn resolve_route(
    route_name: &str,
    route: &Route,
    names: &[&str],
    errors: &mut Vec<String>,
    warnings: &mut Vec<String>,
) -> Option<RouteTable> {
    let path = format!("routes.{route_name}");
    if route.upstreams.is_empty() {
        errors.push(format!("{path}.upstreams: at least one upstream is required"));
    }
    if route.methods.is_empty() {
        errors.push(format!("{path}.methods: required; list the methods this route accepts"));
    }
    let before = errors.len();
    let candidates = match_upstreams(&format!("{path}.upstreams"), &route.upstreams, names, errors);

    let mut exact = HashMap::new();
    let mut prefixes = Vec::new();
    let mut any = None;
    let mut accepts_any = false;
    for (key, entry) in &route.methods {
        let mpath = format!("{path}.methods.{key}");
        let Some(glob) = Glob::parse(key) else {
            errors.push(format!("{mpath}: a method key is an exact name, a prefix glob (`get*`) or \"*\""));
            continue;
        };
        let plan = match entry {
            MethodEntry::Reject => MethodPlan::Reject,
            MethodEntry::Accept(over) => {
                accepts_any = true;
                // The method's own upstreams replace the route's.
                let own = over.upstreams.as_ref().map(|u| {
                    if u.is_empty() {
                        errors.push(format!("{mpath}.upstreams: at least one upstream is required (leave it out to use the route's)"));
                    }
                    match_upstreams(&format!("{mpath}.upstreams"), u, names, errors)
                });
                let candidates = own.unwrap_or_else(|| candidates.clone());
                match &glob {
                    _ if over.cache.is_none() => {}
                    Glob::Exact(m) if solana::is_write(m) => errors.push(format!(
                        "{mpath}.cache: `{m}` is a write: a cached one is answered from the cache and never sent; \
                         leave cache out"
                    )),
                    Glob::Exact(_) => {}
                    _ => errors.push(format!(
                        "{mpath}.cache: cache is set on exact method names only, since a TTL fits one method; \
                         list the methods to cache"
                    )),
                }
                let subscription = matches!(&glob, Glob::Exact(m) if solana::is_subscription_method(m));
                if subscription {
                    if over.cache.is_some() || over.retry.is_some() {
                        warnings.push(format!("{mpath}: cache and retry are ignored for subscriptions"));
                    }
                    // A subscription's mode is its own: a route's or a glob's
                    // mode is for calls and never reaches it.
                    let mode = match &over.mode {
                        None | Some(crate::config::Mode::Single) => Mode::Single,
                        Some(crate::config::Mode::Redundant) => Mode::Redundant,
                        Some(_) => {
                            errors.push(format!("{mpath}.mode: a subscription is single or redundant"));
                            continue;
                        }
                    };
                    let merged = merge(&route.policies(), over);
                    let strategy = strategy(&mpath, merged.load_balancing.clone(), &candidates, names, errors);
                    MethodPlan::Accept(Arc::new(EffectivePolicies {
                        selector: Selector::new(candidates, strategy),
                        cache: None,
                        mode,
                        retry: None,
                    }))
                } else {
                    let merged = merge(&route.policies(), over);
                    match effective(&mpath, &path, route_name, over, &merged, &candidates, names, errors) {
                        Some(p) => MethodPlan::Accept(Arc::new(p)),
                        None => continue,
                    }
                }
            }
        };
        match glob {
            Glob::Exact(m) => {
                exact.insert(m, plan);
            }
            Glob::Prefix(p) => prefixes.push((p, plan)),
            Glob::Any => any = Some(plan),
        }
    }
    if !route.methods.is_empty() && !accepts_any {
        errors.push(format!("{path}.methods: must accept at least one method"));
    }
    warn_unused_policies(&path, route, warnings);
    prefixes.sort_by_key(|p| std::cmp::Reverse(p.0.len()));
    // Reported once: a method inheriting a broken default already did.
    let mut batch_errors = Vec::new();
    let batch = Selector::new(
        candidates.clone(),
        strategy(&path, route.load_balancing.clone(), &candidates, names, &mut batch_errors),
    );
    if errors.len() == before {
        errors.extend(batch_errors);
    }
    let mut batch_other = HashMap::new();
    for plan in exact.values().chain(prefixes.iter().map(|(_, p)| p)).chain(any.iter()) {
        if let MethodPlan::Accept(p) = plan {
            let ups = p.selector.candidates();
            if ups != candidates.as_slice() && !batch_other.contains_key(ups) {
                batch_other.insert(ups.to_vec(), Selector::new(ups.to_vec(), Strategy::RoundRobin));
            }
        }
    }

    if errors.len() != before {
        return None;
    }
    let table = RouteTable { name: route_name.to_owned(), upstreams: candidates, exact, prefixes, any, batch, batch_other };
    warn_quorum_writes(&table, warnings);
    Some(table)
}

/// Merge rules: unset fields inherit; `retry` merges field by field;
/// `upstreams`, `loadBalancing` and `mode` are replaced whole; `false` turns
/// an inherited retry off. `cache` is never inherited: it is the method's own.
pub fn merge(defaults: &Policies, over: &Policies) -> Policies {
    fn setting<T: Clone>(d: &Option<Setting<T>>, o: &Option<Setting<T>>, f: impl Fn(&T, &T) -> T) -> Option<Setting<T>> {
        match (d, o) {
            (_, Some(Setting::Off)) => Some(Setting::Off),
            (Some(Setting::On(d)), Some(Setting::On(o))) => Some(Setting::On(f(d, o))),
            (_, Some(o)) => Some(o.clone()),
            (d, None) => d.clone(),
        }
    }
    Policies {
        upstreams: over.upstreams.clone().or_else(|| defaults.upstreams.clone()),
        load_balancing: over.load_balancing.clone().or_else(|| defaults.load_balancing.clone()),
        cache: over.cache.clone(),
        mode: over.mode.clone().or_else(|| defaults.mode.clone()),
        retry: setting(&defaults.retry, &over.retry, |d, o| Retry {
            attempts: o.attempts.or(d.attempts),
            delay: o.delay.or(d.delay),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn effective(
    mpath: &str,
    rpath: &str,
    route_name: &str,
    over: &Policies,
    merged: &Policies,
    candidates: &[UpstreamId],
    names: &[&str],
    errors: &mut Vec<String>,
) -> Option<EffectivePolicies> {
    let before = errors.len();
    let matched = candidates.len();

    let cache = match &merged.cache {
        None => None,
        Some(c) => match c.ttl {
            None => {
                errors.push(format!("{mpath}: cache.ttl is required"));
                None
            }
            Some(0) => {
                errors.push(format!("{mpath}: cache.ttl must be at least 1 (leave cache out to not cache)"));
                None
            }
            Some(ttl) => Some(CachePolicy {
                namespace: c.namespace.clone().unwrap_or_default(),
                ttl: Duration::from_millis(ttl),
                // Set once every route is resolved.
                keep: Duration::from_millis(ttl),
                scope: route_name.to_owned(),
            }),
        },
    };

    // Where an error in the mode comes from, when the method did not set it.
    let from = if over.mode.is_none() && merged.mode.is_some() { format!(" (mode set in {rpath})") } else { String::new() };
    let mode = match merged.mode.clone().unwrap_or(crate::config::Mode::Single) {
        crate::config::Mode::Single => Mode::Single,
        crate::config::Mode::Redundant => {
            errors.push(format!("{mpath}: mode redundant is for subscriptions only{from}"));
            Mode::Single
        }
        crate::config::Mode::Hedge { upstreams: n } => {
            if n == 0 || n > matched {
                errors.push(format!(
                    "{mpath}: hedge needs 1 <= upstreams ({n}) <= the {matched} upstreams this method uses{from}"
                ));
            }
            Mode::Hedge { upstreams: n }
        }
        crate::config::Mode::Quorum { threshold: t, upstreams: n } => {
            if t == 0 || t > n || n > matched {
                errors.push(format!(
                    "{mpath}: quorum needs 1 <= threshold ({t}) <= upstreams ({n}) <= the {matched} upstreams this method uses{from}"
                ));
            }
            Mode::Quorum { threshold: t, upstreams: n }
        }
    };
    let mut retry = None;
    if let Some(r) = merged.retry.as_ref().and_then(Setting::on) {
        match (r.attempts, r.delay) {
            (Some(attempts), Some(delay)) => retry = Some(RetryPolicy { attempts, delay: Duration::from_millis(delay) }),
            (a, d) => {
                let missing: Vec<&str> =
                    [("retry.attempts", a.is_none()), ("retry.delay", d.is_none())].into_iter().filter(|x| x.1).map(|x| x.0).collect();
                errors.push(format!("{mpath}: {} is required (set it here or in {rpath})", join_and(&missing)));
            }
        }
    }

    let strategy = strategy(mpath, merged.load_balancing.clone(), candidates, names, errors);
    (errors.len() == before).then(|| EffectivePolicies {
        selector: Selector::new(candidates.to_vec(), strategy),
        cache,
        mode,
        retry,
    })
}

fn strategy(path: &str, lb: Option<LoadBalancing>, candidates: &[UpstreamId], names: &[&str], errors: &mut Vec<String>) -> Strategy {
    match lb.unwrap_or(LoadBalancing::RoundRobin) {
        LoadBalancing::RoundRobin => Strategy::RoundRobin,
        LoadBalancing::Random => Strategy::Random,
        LoadBalancing::Weighted { weights } => Strategy::Weighted(weights_for(path, &weights, candidates, names, errors)),
        LoadBalancing::Priority { order } => Strategy::Priority(ranks_for(path, &order, candidates, names, errors)),
    }
}

fn join_and(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [one] => one.to_string(),
        [init @ .., last] => format!("{} and {last}", init.join(", ")),
    }
}

/// One weight per candidate: a glob gives its weight to each upstream it
/// matches; upstreams not listed get 1.
fn weights_for(
    path: &str,
    weights: &BTreeMap<String, u32>,
    candidates: &[UpstreamId],
    names: &[&str],
    errors: &mut Vec<String>,
) -> Vec<u32> {
    let mut out = vec![None; candidates.len()];
    // More specific keys win when several match one upstream.
    let mut keys: Vec<(Glob, &String, u32)> = Vec::new();
    for (k, &w) in weights {
        match Glob::parse(k) {
            Some(g) => keys.push((g, k, w)),
            None => errors.push(format!("{path}: weights key `{k}` is not a name, a prefix glob or \"*\"")),
        }
        if w == 0 {
            errors.push(format!("{path}: weights.{k} must be at least 1"));
        }
    }
    keys.sort_by_key(|(g, _, _)| std::cmp::Reverse(g.specificity()));
    for (g, k, w) in &keys {
        let mut hit = false;
        for (i, &id) in candidates.iter().enumerate() {
            if g.matches(names[id]) {
                hit = true;
                out[i].get_or_insert(*w);
            }
        }
        if !hit {
            errors.push(format!("{path}: weights key `{k}` matches none of its upstreams"));
        }
    }
    out.into_iter().map(|w| w.unwrap_or(1).max(1)).collect()
}

/// Ranks of candidate indices from `order`; route upstreams missing from it
/// are appended at the end.
fn ranks_for(path: &str, order: &[String], candidates: &[UpstreamId], names: &[&str], errors: &mut Vec<String>) -> Vec<Vec<usize>> {
    let mut ranked = HashSet::new();
    let mut ranks = Vec::new();
    for entry in order {
        let Some(g) = Glob::parse(entry) else {
            errors.push(format!("{path}: order entry `{entry}` is not a name, a prefix glob or \"*\""));
            continue;
        };
        let members: Vec<usize> = (0..candidates.len()).filter(|&i| g.matches(names[candidates[i]])).collect();
        if members.is_empty() {
            errors.push(format!("{path}: order entry `{entry}` matches none of its upstreams"));
        }
        let fresh: Vec<usize> = members.into_iter().filter(|i| ranked.insert(*i)).collect();
        if !fresh.is_empty() {
            ranks.push(fresh);
        }
    }
    for i in 0..candidates.len() {
        if ranked.insert(i) {
            ranks.push(vec![i]);
        }
    }
    ranks
}

/// Writes follow the configured policies like any other method, but quorum
/// on one is almost never meant: it is allowed, with a warning at startup
/// (`delta run` and `build()` alike).
fn warn_quorum_writes(t: &RouteTable, warnings: &mut Vec<String>) {
    let mut methods: Vec<&str> = solana::KNOWN_WRITES.to_vec();
    methods.extend(t.exact.keys().map(String::as_str).filter(|m| solana::is_write(m)));
    methods.sort_unstable();
    methods.dedup();
    let path = format!("routes.{}", t.name);
    for m in methods {
        let Some(p) = t.accepted(m) else { continue };
        if matches!(p.mode, Mode::Quorum { .. }) {
            warnings.push(format!(
                "{path}: `{m}` is a write under quorum: it is sent to every quorum member, and when one answers \
                 \"already processed\" the answers disagree and the client gets -33094 (give it a mode other than quorum)"
            ));
        }
    }
}

/// Warns about route policies no accepted method can use.
fn warn_unused_policies(path: &str, route: &Route, warnings: &mut Vec<String>) {
    let d = &route.policies();
    let accepted: Vec<(&String, &Policies)> = route
        .methods
        .iter()
        .filter_map(|(k, e)| match e {
            MethodEntry::Accept(p) => Some((k, p)),
            MethodEntry::Reject => None,
        })
        .collect();
    if accepted.is_empty() {
        return;
    }
    type Overrides = fn(&Policies) -> bool;
    // loadBalancing is left out: batches always use the route's.
    let checks: [(&str, bool, Overrides); 2] = [
        ("mode", d.mode.is_some(), |p| p.mode.is_some()),
        ("retry", d.retry.is_some(), |p| p.retry.is_some()),
    ];
    for (name, set, overridden) in checks {
        if !set {
            continue;
        }
        let unused = accepted
            .iter()
            .all(|(k, p)| solana::is_subscription_method(k) || overridden(p));
        if unused {
            warnings.push(format!("{path}.{name}: no accepted method uses it"));
        }
    }
}

fn resolve_keys(
    keys: &BTreeMap<String, ApiKey>,
    routes: &BTreeMap<String, Route>,
    errors: &mut Vec<String>,
    warnings: &mut Vec<String>,
) -> Vec<KeySpec> {
    let mut out = Vec::new();
    let mut seen: HashMap<Uuid, &str> = HashMap::new();
    for (name, k) in keys {
        let path = format!("apiKeys.{name}");
        // Never echo the key itself.
        let key = match Uuid::parse_str(k.key.trim()) {
            Ok(u) => Some(u),
            Err(_) => {
                errors.push(format!("{path}.key: not a UUID (generate one with `uuidgen`)"));
                None
            }
        };
        if let Some(u) = key {
            if let Some(other) = seen.insert(u, name) {
                errors.push(format!("{path}.key: same key as apiKeys.{other}"));
            }
        }
        if k.routes.is_empty() {
            errors.push(format!("{path}.routes: required; list the routes this key may use, or [\"*\"]"));
        }
        let mut globs = Vec::new();
        for r in &k.routes {
            match Glob::parse(r) {
                Some(Glob::Exact(e)) if !routes.contains_key(&e) => {
                    errors.push(format!("{path}.routes: `{e}` is not a defined route"))
                }
                Some(g) => globs.push(g),
                None => errors.push(format!("{path}.routes: `{r}` is not a name, a prefix glob or \"*\"")),
            }
        }
        let allowed_ips = k.allowed_ips.as_ref().map(|ips| {
            ips.iter()
                .filter_map(|s| match parse_net(s) {
                    Some(n) => Some(n),
                    None => {
                        errors.push(format!("{path}.allowedIps: `{s}` is not an IP or CIDR range"));
                        None
                    }
                })
                .collect()
        });
        let allowed_domains = k.allowed_domains.as_ref().map(|ds| {
            ds.iter()
                .filter_map(|d| {
                    let d = d.trim().to_ascii_lowercase();
                    let host = d.strip_prefix("*.").unwrap_or(&d);
                    if host.is_empty() || host.contains('*') || host.contains('/') || host.contains(':') {
                        errors.push(format!("{path}.allowedDomains: `{d}` is not a domain or `*.domain`"));
                        None
                    } else {
                        Some(d)
                    }
                })
                .collect()
        });
        if k.total_rps == Some(0) {
            errors.push(format!("{path}.totalRps: must be at least 1 (leave it out for unlimited)"));
        }
        if k.rps_per_ip == Some(0) {
            errors.push(format!("{path}.rpsPerIp: must be at least 1 (leave it out for unlimited)"));
        }
        if k.rpm_per_ip == Some(0) {
            errors.push(format!("{path}.rpmPerIp: must be at least 1 (leave it out for unlimited)"));
        }
        if let (Some(rps), Some(rpm)) = (k.rps_per_ip, k.rpm_per_ip) {
            if u64::from(rpm) >= u64::from(rps) * 60 {
                warnings.push(format!(
                    "{path}.rpmPerIp: {rpm} hardly limits anything: rpsPerIp = {rps} already allows only about {} a minute",
                    u64::from(rps) * 60
                ));
            }
        }
        if k.max_subscriptions == Some(0) {
            errors.push(format!("{path}.maxSubscriptions: must be at least 1 (leave it out for unlimited)"));
        }
        if let Some(key) = key {
            out.push(KeySpec {
                name: name.clone(),
                key,
                total_rps: k.total_rps,
                rps_per_ip: k.rps_per_ip,
                rpm_per_ip: k.rpm_per_ip,
                allowed_ips,
                allowed_domains,
                routes: globs,
                max_subscriptions: k.max_subscriptions.map(|n| n as usize),
            });
        }
    }
    out
}

fn parse_net(s: &str) -> Option<IpNet> {
    let s = s.trim();
    s.parse::<IpNet>().ok().or_else(|| s.parse::<IpAddr>().ok().map(IpNet::from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_toml;

    const UPSTREAMS: &str = r#"
listen = "0.0.0.0:8080"
[upstreams]
helius = "https://mainnet.helius-rpc.com/?api-key=k"
triton = "https://xyz.mainnet.rpcpool.com/t"
own-1  = { http = "http://10.0.0.11:8899", ws = "ws://10.0.0.11:8900" }
own-2  = { http = "http://10.0.0.12:8899", ws = "ws://10.0.0.12:8900" }
"#;

    /// A key every test config gets unless it defines its own.
    const TEST_KEY: &str = "\n[apiKeys.test]\nkey = \"00000000-0000-4000-8000-000000000001\"\nroutes = [\"*\"]\n";

    fn load(routes: &str) -> Result<Resolved, ConfigError> {
        let key = if routes.contains("[apiKeys.") { "" } else { TEST_KEY };
        let cfg = parse_toml(&format!("{UPSTREAMS}{routes}{key}"), &|_| None)?;
        resolve(&cfg)
    }

    fn err(routes: &str) -> String {
        match load(routes) {
            Ok(_) => panic!("expected an error"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn spec_example_resolves() {
        let text = include_str!("../../../config.example.toml");
        let vars = |k: &str| match k {
            "HELIUS_KEY" | "TRITON_TOKEN" => Some("x".to_owned()),
            "BACKEND_KEY" => Some("6a1f0c2e-8a3b-4c5d-9e7f-0123456789ab".to_owned()),
            "INDEXER_KEY" => Some("7b2e1d3f-9b4c-4d6e-8f01-123456789abc".to_owned()),
            _ => None,
        };
        let r = resolve(&parse_toml(text, &vars).unwrap()).unwrap();
        assert_eq!(r.upstreams.len(), 4);
        assert_eq!(r.api_keys.len(), 3);

        let safe = &r.routes["safe"];
        assert!(matches!(safe.lookup("sendTransaction"), Some(MethodPlan::Reject)));
        let once = Some(RetryPolicy { attempts: 1, delay: Duration::from_millis(200) });
        let any = safe.accepted("getBalance").unwrap();
        assert_eq!(any.mode, Mode::Quorum { threshold: 2, upstreams: 3 });
        assert_eq!(any.retry, once);
        assert_eq!(any.cache.as_ref().unwrap().ttl, Duration::from_millis(1000));
        let slot = safe.accepted("getSlot").unwrap();
        assert!(slot.cache.is_none());
        assert_eq!(slot.mode, Mode::Single);
        assert_eq!(slot.retry, Some(RetryPolicy { attempts: 3, delay: Duration::from_millis(50) }));
        let bh = safe.accepted("getLatestBlockhash").unwrap();
        assert_eq!(bh.mode, Mode::Hedge { upstreams: 2 });
        assert_eq!(bh.retry, once, "a method's own mode keeps the route's retry");
        let block = safe.accepted("getBlock").unwrap();
        assert_eq!(block.cache.as_ref().unwrap().ttl, Duration::from_millis(86_400_000));

        let fast = &r.routes["fast"];
        assert_eq!(fast.upstreams.len(), 4);
        assert!(fast.lookup("getSlot").is_none());
        assert!(fast.accepted("getLatestBlockhash").unwrap().cache.is_none());
        assert_eq!(fast.accepted("getBalance").unwrap().cache.as_ref().unwrap().namespace, "", "shared by default");

        let stream = &r.routes["stream"];
        assert!(stream.accepted("accountSubscribe").is_some());
        assert_eq!(stream.upstreams.len(), 2);
    }

    #[test]
    fn most_specific_key_wins() {
        let r = load(
            r#"
[routes.r]
upstreams = ["*"]
[routes.r.methods]
"*" = {}
"get*" = false
"getBlock*" = {}
getBlockTime = false
"#,
        )
        .unwrap();
        let t = &r.routes["r"];
        assert!(t.accepted("sendTransaction").is_some());
        assert!(t.accepted("getSlot").is_none());
        assert!(t.accepted("getBlockHeight").is_some());
        assert!(t.accepted("getBlockTime").is_none());
    }

    #[test]
    fn subscriptions_are_single_or_redundant_set_on_their_own_name() {
        let r = load(
            r#"
[routes.r]
upstreams = ["*"]
mode = { type = "hedge", upstreams = 2 }
[routes.r.methods]
"*" = {}
slotSubscribe = {}
logsSubscribe = { mode = { type = "redundant" } }
"#,
        )
        .unwrap();
        let mode = |m: &str| r.routes["r"].accepted(m).unwrap().mode.clone();
        assert_eq!(mode("slotSubscribe"), Mode::Single, "the route's hedge is for calls");
        assert_eq!(mode("logsSubscribe"), Mode::Redundant);
        assert_eq!(mode("getSlot"), Mode::Hedge { upstreams: 2 });

        for (routes, expected) in [
            (
                "[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nslotSubscribe = { mode = { type = \"hedge\", upstreams = 2 } }\n",
                "routes.r.methods.slotSubscribe.mode: a subscription is single or redundant",
            ),
            (
                "[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\ngetSlot = { mode = { type = \"redundant\" } }\n",
                "routes.r.methods.getSlot: mode redundant is for subscriptions only",
            ),
            (
                "[routes.r]\nupstreams = [\"*\"]\nmode = { type = \"redundant\" }\n[routes.r.methods]\n\"*\" = {}\n",
                "mode redundant is for subscriptions only (mode set in routes.r)",
            ),
        ] {
            let e = err(routes);
            assert!(e.contains(expected), "{e}");
        }
    }

    #[test]
    fn mode_is_replaced_whole_and_retry_inherited_apart() {
        let r = load(
            r#"
[routes.r]
upstreams = ["*"]
mode = { type = "quorum", threshold = 2, upstreams = 3 }
retry = { attempts = 3, delay = 50 }
[routes.r.methods]
getBalance         = {}
getSlot            = { retry = { attempts = 5 } }
getLatestBlockhash = { mode = { type = "hedge", upstreams = 2 } }
getBlock           = { mode = { type = "single" }, retry = false }
"#,
        )
        .unwrap();
        let t = &r.routes["r"];
        let retry = |attempts| Some(RetryPolicy { attempts, delay: Duration::from_millis(50) });
        let p = t.accepted("getBalance").unwrap();
        assert_eq!((&p.mode, &p.retry), (&Mode::Quorum { threshold: 2, upstreams: 3 }, &retry(3)));
        let p = t.accepted("getSlot").unwrap();
        assert_eq!((&p.mode, &p.retry), (&Mode::Quorum { threshold: 2, upstreams: 3 }, &retry(5)));
        let p = t.accepted("getLatestBlockhash").unwrap();
        assert_eq!((&p.mode, &p.retry), (&Mode::Hedge { upstreams: 2 }, &retry(3)));
        let p = t.accepted("getBlock").unwrap();
        assert_eq!((&p.mode, &p.retry), (&Mode::Single, &None));

        let p = load("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n").unwrap();
        let p = p.routes["r"].accepted("x").unwrap().clone();
        assert_eq!((&p.mode, &p.retry), (&Mode::Single, &None), "the default");
    }

    #[test]
    fn required_fields() {
        let e = err("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { retry = { attempts = 3 } }\n");
        assert!(e.contains("retry.delay is required"), "{e}");
        let e = err("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { cache = { namespace = \"n\" } }\n");
        assert!(e.contains("cache.ttl is required"), "{e}");
    }

    #[test]
    fn rule_violations() {
        let cases = [
            ("[routes.r]\nupstreams = [\"own-9\"]\n[routes.r.methods]\nx = {}\n", "`own-9` matches no upstream"),
            ("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\n\"*Subscribe\" = {}\n", "a method key is"),
            ("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = false\n", "must accept at least one method"),
            ("[routes.r]\nupstreams = [\"own-*\"]\n[routes.r.methods]\nx = { mode = { type = \"quorum\", threshold = 2, upstreams = 3 } }\n", "quorum needs 1 <= threshold (2) <= upstreams (3) <= the 2 upstreams"),
            ("[routes.r]\nupstreams = [\"own-1\"]\n[routes.r.methods]\nx = { mode = { type = \"hedge\", upstreams = 2 } }\n", "x: hedge needs 1 <= upstreams (2) <= the 1 upstreams this method uses"),
            ("[routes.r]\nupstreams = [\"own-1\"]\nmode = { type = \"hedge\", upstreams = 0 }\n[routes.r.methods]\nx = {}\n", "(mode set in routes.r)"),
            ("[routes.r]\nupstreams = [\"own-*\"]\n[routes.r.methods]\nx = { loadBalancing = { type = \"priority\", order = [\"triton\"] } }\n", "order entry `triton`"),
            ("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { loadBalancing = { type = \"weighted\", weights = { helius = 0 } } }\n", "weights.helius must be at least 1"),
            ("[routes.r]\nupstreams = [\"*\"]\n", "methods"),
        ];
        for (routes, expected) in cases {
            let e = err(routes);
            assert!(e.contains(expected), "expected `{expected}` in: {e}");
        }
    }

    #[test]
    fn api_key_rules() {
        let route = "[routes.fast]\nupstreams = [\"*\"]\n[routes.fast.methods]\nx = {}\n";
        let e = err(&format!("{route}[apiKeys.a]\nkey = \"test\"\nroutes = [\"*\"]\n"));
        assert!(e.contains("apiKeys.a.key: not a UUID"), "{e}");
        let k = "3f8a2c1e-7b4d-4e9a-b6c2-1d5e8f0a9b37";
        let e = err(&format!("{route}[apiKeys.a]\nkey = \"{k}\"\nroutes = [\"*\"]\n[apiKeys.b]\nkey = \"{k}\"\nroutes = [\"*\"]\n"));
        assert!(e.contains("same key"), "{e}");
        let e = err(&format!("{route}[apiKeys.a]\nkey = \"{k}\"\nroutes = [\"fsat\"]\n"));
        assert!(e.contains("`fsat` is not a defined route"), "{e}");
        let ok = load(&format!("{route}[apiKeys.a]\nkey = \"{k}\"\nroutes = [\"f*\"]\nallowedIps = [\"10.0.0.0/8\", \"1.2.3.4\"]\n")).unwrap();
        assert_eq!(ok.api_keys[0].allowed_ips.as_ref().unwrap().len(), 2);
        let limits = |l: &str| format!("{route}[apiKeys.a]\nkey = \"{k}\"\nroutes = [\"*\"]\n{l}\n");
        for field in ["totalRps", "rpsPerIp", "rpmPerIp"] {
            assert!(err(&limits(&format!("{field} = 0"))).contains(&format!("apiKeys.a.{field}: must be at least 1")), "{field}");
        }
        let ok = load(&limits("totalRps = 100\nrpsPerIp = 5\nrpmPerIp = 120")).unwrap();
        assert_eq!((ok.api_keys[0].total_rps, ok.api_keys[0].rps_per_ip, ok.api_keys[0].rpm_per_ip), (Some(100), Some(5), Some(120)));
        assert!(ok.warnings.is_empty(), "{:?}", ok.warnings);
        let loose = load(&limits("rpsPerIp = 5\nrpmPerIp = 300")).unwrap();
        assert!(loose.warnings.iter().any(|w| w.contains("apiKeys.a.rpmPerIp: 300 hardly limits anything: rpsPerIp = 5 already allows only about 300 a minute")), "{:?}", loose.warnings);
    }

    #[test]
    fn weights_and_priority_ranks() {
        let names = ["helius", "own-1", "own-2", "triton"];
        let mut errors = Vec::new();
        let mut weights = BTreeMap::new();
        weights.insert("own-*".to_string(), 5);
        weights.insert("helius".to_string(), 2);
        weights.insert("triton".to_string(), 1);
        assert_eq!(weights_for("p", &weights, &[0, 1, 2, 3], &names, &mut errors), vec![2, 5, 5, 1]);
        let ranks = ranks_for("p", &["own-*".into(), "helius".into()], &[0, 1, 2, 3], &names, &mut errors);
        assert_eq!(ranks, vec![vec![1, 2], vec![0], vec![3]]);
        assert!(errors.is_empty());
    }

    #[test]
    fn warnings() {
        let r = load(
            r#"
[routes.stream]
upstreams = ["helius"]
retry = { attempts = 1, delay = 10 }
[routes.stream.methods]
accountSubscribe = { retry = { attempts = 1, delay = 10 } }

[routes.r]
upstreams = ["*"]
mode = { type = "hedge", upstreams = 2 }
[routes.r.methods]
getSlot = { mode = { type = "single" } }
"#,
        )
        .unwrap();
        let has = |s: &str| r.warnings.iter().any(|w| w.contains(s));
        assert!(has("routes.stream.methods.accountSubscribe: cache and retry are ignored for subscriptions"), "{:?}", r.warnings);
        assert!(has("routes.stream.retry: no accepted method uses it"), "{:?}", r.warnings);
        assert!(has("routes.r.mode: no accepted method uses it"), "{:?}", r.warnings);
    }

    #[test]
    fn max_entry_mb() {
        let r = load("[stores.moka]\nmaxSizeMb = 64\nmaxEntryMb = 32\n[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n").unwrap();
        assert_eq!(r.max_entry_bytes, Some(32 * 1024 * 1024));
        let r = load("[stores.moka]\nmaxSizeMb = 64\n[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n").unwrap();
        assert_eq!(r.max_entry_bytes, None);
        let e = err("[stores.moka]\nmaxSizeMb = 64\nmaxEntryMb = 128\n[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n");
        assert!(e.contains("maxEntryMb: 128 is larger than maxSizeMb (64)"), "{e}");
        let e = err("[stores.moka]\nmaxSizeMb = 64\nmaxEntryMb = 0\n[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n");
        assert!(e.contains("maxEntryMb: must be at least 1"), "{e}");
    }

    #[test]
    fn routes_share_the_cache_and_keep_entries_for_the_longest_ttl() {
        let r = load(
            r#"
[routes.fast]
upstreams = ["*"]
[routes.fast.methods]
getBalance = { cache = { ttl = 200 } }

[routes.safe]
upstreams = ["*"]
[routes.safe.methods]
getBalance = { cache = { ttl = 2000 } }
getSlot    = { cache = { ttl = 5000 } }

[routes.own]
upstreams = ["*"]
[routes.own.methods]
getBalance = { cache = { namespace = "own", ttl = 100 } }
"#,
        )
        .expect("different TTLs in one namespace load");
        let c = |route: &str, m: &str| r.routes[route].accepted(m).unwrap().cache.clone().unwrap();
        let ms = Duration::from_millis;
        let fast = c("fast", "getBalance");
        assert_eq!((fast.namespace.as_str(), fast.ttl, fast.keep), ("", ms(200), ms(5000)), "shared by default");
        assert_eq!((c("safe", "getBalance").ttl, c("safe", "getBalance").keep), (ms(2000), ms(5000)));
        assert_eq!(c("safe", "getBalance").scope, "safe", "in-flight misses are shared per route");
        let own = c("own", "getBalance");
        assert_eq!((own.namespace.as_str(), own.keep), ("own", ms(100)), "its own namespace, its own keep");
    }

    #[test]
    fn an_api_key_is_required() {
        let text = format!("{UPSTREAMS}[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {{}}\n");
        let e = resolve(&parse_toml(&text, &|_| None).unwrap()).err().unwrap().to_string();
        assert!(e.contains("apiKeys: at least one API key is required"), "{e}");
    }

    #[test]
    fn trusted_proxies_must_parse() {
        let base = format!("listen = \"0.0.0.0:1\"\n{{tp}}\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\n[routes.r.methods]\nx = {{}}\n{TEST_KEY}");
        let ok = resolve(&parse_toml(&base.replace("{tp}", "trustedProxies = [\"10.0.0.0/8\", \"127.0.0.1\"]"), &|_| None).unwrap()).unwrap();
        assert_eq!(ok.trusted_proxies.len(), 2);
        let e = resolve(&parse_toml(&base.replace("{tp}", "trustedProxies = [\"proxy.local\"]"), &|_| None).unwrap()).err().unwrap().to_string();
        assert!(e.contains("trustedProxies: `proxy.local` is not an IP or CIDR range"), "{e}");
    }

    #[test]
    fn subscription_limits() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\nh = { http = \"https://h.example/rpc\", maxSubscriptionsPerConnection = 1000, maxConnections = 4 }\n\
             [routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n\
             [apiKeys.k]\nkey = \"00000000-0000-4000-8000-000000000001\"\nroutes = [\"*\"]\nmaxSubscriptions = 50\n";
        let r = resolve(&parse_toml(text, &|_| None).unwrap()).unwrap();
        let h = &r.upstreams[0];
        assert_eq!(h.ws, "wss://h.example/rpc", "ws is derived when the table leaves it out");
        assert_eq!((h.max_subscriptions_per_connection, h.max_connections), (Some(1000), Some(4)));
        assert_eq!(r.api_keys[0].max_subscriptions, Some(50));

        let zero = text.replace("= 1000", "= 0").replace("= 4", "= 0").replace("= 50", "= 0");
        let e = resolve(&parse_toml(&zero, &|_| None).unwrap()).err().unwrap().to_string();
        for field in ["upstreams.h.maxSubscriptionsPerConnection", "upstreams.h.maxConnections", "apiKeys.k.maxSubscriptions"] {
            assert!(e.contains(&format!("{field}: must be at least 1")), "{field}: {e}");
        }
    }

    #[test]
    fn methods_can_have_their_own_upstreams() {
        let r = load(
            r#"
[routes.r]
upstreams = ["own-*"]
mode = { type = "quorum", threshold = 2, upstreams = 2 }
[routes.r.methods]
getSlot = {}
getBalance = { upstreams = ["helius", "triton"], loadBalancing = { type = "priority", order = ["triton"] } }
accountSubscribe = { upstreams = ["helius"] }
"#,
        )
        .unwrap();
        let route = &r.routes["r"];
        let ids = |m: &str| route.accepted(m).unwrap().selector.candidates().to_vec();
        // Ids are alphabetical: helius 0, own-1 1, own-2 2, triton 3.
        assert_eq!(ids("getSlot"), [1, 2], "the route's");
        assert_eq!(ids("getBalance"), [0, 3], "its own, outside the route's list");
        assert_eq!(ids("accountSubscribe"), [0], "subscriptions too");
        assert_eq!(route.upstreams, [1, 2], "batches keep the route's");

        let e = err("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { upstreams = [\"nope\"] }\ny = { upstreams = [] }\n");
        assert!(e.contains("routes.r.methods.x.upstreams: `nope` matches no upstream"), "{e}");
        assert!(e.contains("routes.r.methods.y.upstreams: at least one upstream is required"), "{e}");
        // Counted against the method's own upstreams.
        let e = err("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { upstreams = [\"helius\"], mode = { type = \"hedge\", upstreams = 2 } }\n");
        assert!(e.contains("<= the 1 upstreams this method uses"), "{e}");
    }

    #[test]
    fn cache_is_set_on_exact_method_names_only() {
        let e = err("[routes.r]\nupstreams = [\"*\"]\ncache = { ttl = 5 }\n[routes.r.methods]\nx = {}\n");
        assert!(e.contains("unknown field `cache`"), "not on the route: {e}");
        for key in ["\"*\"", "\"get*\""] {
            let e = err(&format!("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\n{key} = {{ cache = {{ ttl = 5 }} }}\n"));
            assert!(e.contains(".cache: cache is set on exact method names only"), "{key}: {e}");
        }
        let e = err("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = { cache = { ttl = 0 } }\n");
        assert!(e.contains("cache.ttl must be at least 1 (leave cache out to not cache)"), "{e}");
        for write in ["sendTransaction", "requestAirdrop", "sendBundle"] {
            let e = err(&format!("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\n{write} = {{ cache = {{ ttl = 5 }} }}\n"));
            assert!(e.contains(&format!("routes.r.methods.{write}.cache: `{write}` is a write")), "{e}");
        }
        let r = load("[routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\n\"*\" = {}\nx = { cache = { ttl = 5 } }\n").unwrap();
        assert!(r.routes["r"].accepted("x").unwrap().cache.is_some());
        assert!(r.routes["r"].accepted("y").unwrap().cache.is_none(), "nothing else is cached");
    }

    #[test]
    fn quorum_writes_load_with_a_warning() {
        let r = load(
            r#"
[routes.q]
upstreams = ["*"]
mode = { type = "quorum", threshold = 2, upstreams = 2 }
[routes.q.methods]
"send*" = {}
"#,
        )
        .unwrap();
        let has = |s: &str| r.warnings.iter().any(|w| w.contains(s));
        assert!(has("routes.q: `sendTransaction` is a write under quorum"), "{:?}", r.warnings);
        assert!(has("routes.q: `sendBundle` is a write under quorum"), "{:?}", r.warnings);
    }
}
