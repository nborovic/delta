//! `delta scaffold`: turns a TOML config into the source of a compiled proxy,
//! a Cargo project whose `main.rs` makes the equivalent `Proxy::builder()`
//! calls. `${VAR}` becomes `env("VAR")?`, so secrets stay out of the code and
//! are read at startup.
//!
//! The output is a starting point to edit by hand; it is not regenerated.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::config::*;

/// Where the generated project gets the `delta` crate from.
#[derive(Clone, Debug)]
pub enum DeltaDep {
    Path(PathBuf),
    Git(String),
}

impl DeltaDep {
    fn toml(&self) -> String {
        match self {
            DeltaDep::Path(p) => format!("delta = {{ path = {} }}", toml_str(&p.display().to_string())),
            DeltaDep::Git(url) => format!("delta = {{ git = {} }}", toml_str(url)),
        }
    }
}

/// The default dependency: this source tree, when it still exists.
pub fn default_dep() -> Option<DeltaDep> {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.join("Cargo.toml").exists().then_some(DeltaDep::Path(here))
}

/// Generates `main.rs` from TOML text. The config is fully checked first,
/// with placeholders for environment variables (their real values are never
/// read or written).
pub fn generate_main(text: &str, source_name: &str) -> Result<String, ConfigError> {
    generate(text, source_name).map(|(main, _)| main)
}

/// What [`write_project`] produced.
pub struct Scaffolded {
    pub files: Vec<PathBuf>,
    /// The package name, which is also the binary's name.
    pub package: String,
    /// Environment variables the binary reads at startup.
    pub env_vars: Vec<String>,
}

fn generate(text: &str, source_name: &str) -> Result<(String, BTreeSet<String>), ConfigError> {
    let raw: Config = toml::from_str(text).map_err(|e| ConfigError::one(e.to_string()))?;
    let checked = crate::config::parse_toml_in_context(text, &|var, path, whole| Some(placeholder(var, path, whole)))?;
    crate::resolve::resolve(&checked)?;

    let mut g = Gen::default();
    let mut body = String::new();
    let b = &mut body;
    writeln!(b, "    Ok(Proxy::builder()").unwrap();
    writeln!(b, "        .listen({})", g.expr(&raw.listen).code).unwrap();
    if !raw.trusted_proxies.is_empty() {
        let list: Vec<String> = raw.trusted_proxies.iter().map(|p| g.expr(p).code).collect();
        writeln!(b, "        .trusted_proxies([{}])", list.join(", ")).unwrap();
    }
    if let Some(m) = &raw.stores.moka {
        let entry = m.max_entry_mb.map(|e| format!(".max_entry_mb({e})")).unwrap_or_default();
        writeln!(b, "        .moka(Moka::max_size_mb({}){entry})", m.max_size_mb).unwrap();
    }
    for (name, up) in &raw.upstreams {
        let up = match up {
            Upstream::Url(url) => format!("Upstream::url({})", g.expr(url).code),
            Upstream::Split(s) => {
                let mut code = match &s.ws {
                    Some(ws) => format!("Upstream::split({}, {})", g.expr(&s.http).code, g.expr(ws).code),
                    None => format!("Upstream::url({})", g.expr(&s.http).code),
                };
                if let Some(n) = s.max_subscriptions_per_connection {
                    write!(code, ".max_subscriptions_per_connection({n})").unwrap();
                }
                if let Some(n) = s.max_connections {
                    write!(code, ".max_connections({n})").unwrap();
                }
                code
            }
        };
        writeln!(b, "        .upstream({}, {up})", lit(name)).unwrap();
    }
    for (name, k) in &raw.api_keys {
        let mut key = format!("ApiKey::new({})", g.expr(&k.key).code);
        if let Some(n) = k.total_rps {
            write!(key, ".total_rps({n})").unwrap();
        }
        if let Some(n) = k.rps_per_ip {
            write!(key, ".rps_per_ip({n})").unwrap();
        }
        if let Some(n) = k.rpm_per_ip {
            write!(key, ".rpm_per_ip({n})").unwrap();
        }
        if let Some(ips) = &k.allowed_ips {
            write!(key, ".allowed_ips({})", g.list(ips)).unwrap();
        }
        if let Some(domains) = &k.allowed_domains {
            write!(key, ".allowed_domains({})", g.list(domains)).unwrap();
        }
        write!(key, ".routes({})", g.list(&k.routes)).unwrap();
        if let Some(n) = k.max_subscriptions {
            write!(key, ".max_subscriptions({n})").unwrap();
        }
        writeln!(b, "        .api_key({}, {key})", lit(name)).unwrap();
    }
    for (name, route) in &raw.routes {
        writeln!(b, "        .route(").unwrap();
        writeln!(b, "            {},", lit(name)).unwrap();
        // The route's own policies chain onto Route::new like a method's do
        // onto Policies::new.
        let own = g.policies(&route.policies());
        let own = own.strip_prefix("Policies::new()").unwrap_or(&own).to_owned();
        writeln!(b, "            Route::new({}){own}", g.list(&route.upstreams)).unwrap();
        for (key, entry) in &route.methods {
            match entry {
                MethodEntry::Reject => writeln!(b, "                .reject({})", lit(key)).unwrap(),
                MethodEntry::Accept(p) => {
                    writeln!(b, "                .method({}, {})", lit(key), g.policies(p)).unwrap()
                }
            }
        }
        writeln!(b, "        )").unwrap();
    }
    writeln!(b, "    )").unwrap();

    let mut out = String::new();
    writeln!(out, "//! Generated by `delta scaffold` from {source_name}. Edit freely: it is not").unwrap();
    writeln!(out, "//! regenerated, and every config check still runs in `build()`.").unwrap();
    if !g.vars.is_empty() {
        writeln!(out, "//!").unwrap();
        writeln!(out, "//! Environment variables read at startup:").unwrap();
        for v in &g.vars {
            writeln!(out, "//! - `{v}`").unwrap();
        }
    }
    out.push_str(
        r#"
use delta::*;

/// The proxy's config, as code.
pub fn proxy() -> Result<ProxyBuilder, ConfigError> {
"#,
    );
    out.push_str(&body);
    out.push_str(
        r#"}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    proxy()?.build()?.run().await
}
"#,
    );
    Ok((out, g.vars))
}

/// Writes the Cargo project. Refuses a non-empty directory unless `force`.
pub fn write_project(config: &Path, dir: &Path, dep: &DeltaDep, force: bool) -> Result<Scaffolded, String> {
    let text = std::fs::read_to_string(config).map_err(|e| format!("cannot read {}: {e}", config.display()))?;
    let source_name = config.file_name().map_or("config.toml".into(), |n| n.to_string_lossy().into_owned());
    let (main, vars) = generate(&text, &source_name).map_err(|e| e.to_string())?;

    let occupied = dir.read_dir().map(|mut d| d.next().is_some()).unwrap_or(false);
    if occupied && !force {
        return Err(format!("{} is not empty (pass --force to overwrite)", dir.display()));
    }
    let name = package_name(dir);
    let cargo = format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
{}
anyhow = "1"
tokio = {{ version = "1", features = ["full"] }}
tracing-subscriber = {{ version = "0.3", features = ["env-filter"] }}
"#,
        dep.toml()
    );
    std::fs::create_dir_all(dir.join("src")).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    let files = [
        (dir.join("Cargo.toml"), cargo),
        (dir.join("src/main.rs"), main),
        (dir.join(".gitignore"), "/target\n".to_owned()),
    ];
    for (path, content) in &files {
        std::fs::write(path, content).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    }
    Ok(Scaffolded {
        files: files.into_iter().map(|(p, _)| p).collect(),
        package: name,
        env_vars: vars.into_iter().collect(),
    })
}

fn package_name(dir: &Path) -> String {
    let base = dir
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .or_else(|| dir.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let name: String = base.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '-' }).collect();
    let name = name.trim_matches('-').to_owned();
    if name.starts_with(|c: char| c.is_ascii_alphabetic()) {
        name
    } else {
        "delta-proxy".to_owned()
    }
}

/// A stand-in for `${var}` that is valid where it stands, so the config can
/// be checked without reading the real values. `path` is the dotted path of
/// the value; `whole` is true when the variable is the entire value.
fn placeholder(var: &str, path: &str, whole: bool) -> String {
    let field = path.rsplit('.').next().unwrap_or(path);
    let in_list = |name: &str| field.starts_with(&format!("{name}[")) || path.starts_with(&format!("{name}["));
    match (whole, path) {
        (true, "listen") => "127.0.0.1:1".to_owned(),
        (false, "listen") => "1".to_owned(),
        (true, p) if p.starts_with("upstreams.") && field == "ws" => "ws://placeholder.invalid".to_owned(),
        (true, p) if p.starts_with("upstreams.") => "http://placeholder.invalid".to_owned(),
        (false, p) if p.starts_with("upstreams.") => "placeholder".to_owned(),
        (true, _) if in_list("allowedIps") || in_list("trustedProxies") => "127.0.0.1".to_owned(),
        (false, _) if in_list("allowedIps") || in_list("trustedProxies") => "1".to_owned(),
        (true, _) if in_list("allowedDomains") => "placeholder.invalid".to_owned(),
        (false, _) if in_list("allowedDomains") => "placeholder".to_owned(),
        // API keys, and anything else: a distinct, valid UUID per variable.
        _ => {
            let h = blake3::hash(var.as_bytes());
            let b: [u8; 16] = h.as_bytes()[..16].try_into().unwrap();
            uuid::Uuid::from_bytes(b).to_string()
        }
    }
}

/// A Rust string literal.
fn lit(s: &str) -> String {
    format!("{s:?}")
}

fn toml_str(s: &str) -> String {
    toml::Value::String(s.to_owned()).to_string()
}

struct Expr {
    code: String,
    /// `String` (from `env` or `format!`) rather than `&str`.
    owned: bool,
}

#[derive(Default)]
struct Gen {
    vars: BTreeSet<String>,
}

impl Gen {
    /// A string value: a literal, `env("VAR")?`, or `format!(..)` mixing both.
    fn expr(&mut self, s: &str) -> Expr {
        let mut fmt = String::new();
        let mut args = Vec::new();
        let mut rest = s;
        while let Some(start) = rest.find("${") {
            let Some(len) = rest[start + 2..].find('}') else { break };
            fmt.push_str(&rest[..start].replace('{', "{{").replace('}', "}}"));
            fmt.push_str("{}");
            let var = &rest[start + 2..start + 2 + len];
            self.vars.insert(var.to_owned());
            args.push(format!("env({})?", lit(var)));
            rest = &rest[start + 3 + len..];
        }
        if args.is_empty() {
            return Expr { code: lit(s), owned: false };
        }
        fmt.push_str(&rest.replace('{', "{{").replace('}', "}}"));
        if fmt == "{}" {
            return Expr { code: args.pop().unwrap(), owned: true };
        }
        Expr { code: format!("format!({}, {})", lit(&fmt), args.join(", ")), owned: true }
    }

    /// An array of strings; all `String` when any element is.
    fn list(&mut self, items: &[String]) -> String {
        let exprs: Vec<Expr> = items.iter().map(|s| self.expr(s)).collect();
        let owned = exprs.iter().any(|e| e.owned);
        let parts: Vec<String> = exprs
            .into_iter()
            .map(|e| if owned && !e.owned { format!("{}.to_string()", e.code) } else { e.code })
            .collect();
        format!("[{}]", parts.join(", "))
    }

    fn policies(&mut self, p: &Policies) -> String {
        let mut out = "Policies::new()".to_owned();
        if let Some(u) = &p.upstreams {
            write!(out, ".upstreams({})", self.list(u)).unwrap();
        }
        if let Some(lb) = &p.load_balancing {
            let lb = match lb {
                LoadBalancing::RoundRobin => "LoadBalancing::RoundRobin".to_owned(),
                LoadBalancing::Random => "LoadBalancing::Random".to_owned(),
                LoadBalancing::Priority { order } => format!("LoadBalancing::priority({})", self.list(order)),
                LoadBalancing::Weighted { weights } => {
                    let keys: Vec<String> = weights.keys().cloned().collect();
                    let list = self.list(&keys);
                    let keys = list.trim_start_matches('[').trim_end_matches(']');
                    let pairs: Vec<String> = split_top(keys)
                        .into_iter()
                        .zip(weights.values())
                        .map(|(k, w)| format!("({k}, {w})"))
                        .collect();
                    format!("LoadBalancing::weighted([{}])", pairs.join(", "))
                }
            };
            write!(out, ".load_balancing({lb})").unwrap();
        }
        if let Some(c) = &p.cache {
            let mut cache = match c.ttl {
                Some(ttl) => format!("Cache::ttl({ttl})"),
                None => "Cache::default()".to_owned(),
            };
            if let Some(ns) = &c.namespace {
                write!(cache, ".namespace({})", self.expr(ns).code).unwrap();
            }
            if let Some(Store::Moka) = c.store {
                cache.push_str(".store(Store::Moka)");
            }
            write!(out, ".cache({cache})").unwrap();
        }
        match &p.retry {
            None => {}
            Some(Setting::Off) => out.push_str(".no_retry()"),
            Some(Setting::On(r)) => {
                let r = match (r.attempts, r.delay) {
                    (Some(a), Some(d)) => format!("Retry::new({a}, {d})"),
                    (Some(a), None) => format!("Retry::attempts({a})"),
                    (None, Some(d)) => format!("Retry::delay({d})"),
                    (None, None) => "Retry::default()".to_owned(),
                };
                write!(out, ".retry({r})").unwrap();
            }
        }
        match &p.hedge {
            None => {}
            Some(Setting::Off) => out.push_str(".no_hedge()"),
            Some(Setting::On(h)) => match h.upstreams {
                Some(n) => write!(out, ".hedge(Hedge::upstreams({n}))").unwrap(),
                None => out.push_str(".hedge(Hedge::default())"),
            },
        }
        match &p.quorum {
            None => {}
            Some(Setting::Off) => out.push_str(".no_quorum()"),
            Some(Setting::On(q)) => {
                let q = match (q.threshold, q.upstreams) {
                    (Some(t), Some(n)) => format!("Quorum::new({t}, {n})"),
                    (Some(t), None) => format!("Quorum::threshold({t})"),
                    (None, Some(n)) => format!("Quorum::upstreams({n})"),
                    (None, None) => "Quorum::default()".to_owned(),
                };
                write!(out, ".quorum({q})").unwrap();
            }
        }
        out
    }
}

/// Splits a comma-separated list of expressions at the top level (commas
/// inside parentheses or string literals stay).
fn split_top(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let (mut depth, mut in_str, mut escaped) = (0i32, false, false);
    let mut cur = String::new();
    for c in s.chars() {
        if in_str {
            cur.push(c);
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(cur.trim().to_owned());
                cur.clear();
                continue;
            }
            _ => {}
        }
        cur.push(c);
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_owned());
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_expressions() {
        let mut g = Gen::default();
        assert_eq!(g.expr("plain").code, "\"plain\"");
        assert_eq!(g.expr("${KEY}").code, "env(\"KEY\")?");
        assert_eq!(
            g.expr("https://h/{x}?k=${KEY}&t=${TOK}").code,
            "format!(\"https://h/{{x}}?k={}&t={}\", env(\"KEY\")?, env(\"TOK\")?)"
        );
        assert_eq!(g.list(&["a".into(), "${B}".into()]), "[\"a\".to_string(), env(\"B\")?]");
        assert_eq!(g.list(&["a".into(), "b".into()]), "[\"a\", \"b\"]");
        assert_eq!(g.vars.iter().cloned().collect::<Vec<_>>(), ["B", "KEY", "TOK"]);
    }

    #[test]
    fn generates_spec_example() {
        let main = generate_main(include_str!("../../../config.example.toml"), "config.example.toml").unwrap();
        for expected in [
            ".upstream(\"helius\", Upstream::url(format!(\"https://mainnet.helius-rpc.com/?api-key={}\", env(\"HELIUS_KEY\")?)))",
            ".upstream(\"own-1\", Upstream::split(\"http://10.0.0.11:8899\", \"ws://10.0.0.11:8900\"))",
            ".api_key(\"backend\", ApiKey::new(env(\"BACKEND_KEY\")?).total_rps(500).routes([\"*\"]))",
            "ApiKey::new(\"3f8a2c1e-7b4d-4e9a-b6c2-1d5e8f0a9b37\").total_rps(20).rps_per_ip(5).rpm_per_ip(120)",
            ".reject(\"sendTransaction\")",
            ".method(\"getSlot\", Policies::new().cache(Cache::ttl(0)).retry(Retry::new(3, 50)).no_quorum())",
            "LoadBalancing::priority([\"own-*\", \"helius\"])",
            "//! - `INDEXER_KEY`",
        ] {
            assert!(main.contains(expected), "missing `{expected}` in:\n{main}");
        }
    }

    #[test]
    fn weighted_pairs() {
        let mut g = Gen::default();
        let p = Policies::new().load_balancing(LoadBalancing::weighted([("own-*", 5), ("helius", 2)]));
        assert_eq!(
            g.policies(&p),
            "Policies::new().load_balancing(LoadBalancing::weighted([(\"helius\", 2), (\"own-*\", 5)]))"
        );
    }

    #[test]
    fn invalid_config_is_refused() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"b\"]\n[routes.r.methods]\nx = {}\n";
        assert!(generate_main(text, "c.toml").unwrap_err().to_string().contains("`b` matches no upstream"));
    }

    #[test]
    fn max_entry_mb() {
        let text = "listen = \"0.0.0.0:1\"\n[stores.moka]\nmaxSizeMb = 512\nmaxEntryMb = 32\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\n[routes.r.methods]\nx = {}\n[apiKeys.k]\nkey = \"${K}\"\nroutes = [\"*\"]\n";
        let main = generate_main(text, "c.toml").unwrap();
        assert!(main.contains(".moka(Moka::max_size_mb(512).max_entry_mb(32))"), "{main}");
    }

    #[test]
    fn whole_value_variables_are_deferred_to_runtime() {
        let text = "listen = \"${LISTEN}\"\ntrustedProxies = [\"${LB_IP}\", \"10.0.0.0/8\"]\n[upstreams]\nrpc = \"${RPC_URL}\"\nown = { http = \"${OWN_HTTP}\", ws = \"${OWN_WS}\" }\n\
                    [apiKeys.k]\nkey = \"${KEY}\"\nroutes = [\"*\"]\nallowedIps = [\"${OFFICE}\"]\nallowedDomains = [\"${SITE}\"]\n\
                    [routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n";
        let main = generate_main(text, "c.toml").unwrap();
        for expected in [
            r#".listen(env("LISTEN")?)"#,
            r#".trusted_proxies([env("LB_IP")?, "10.0.0.0/8"])"#,
            r#"Upstream::url(env("RPC_URL")?)"#,
            r#"Upstream::split(env("OWN_HTTP")?, env("OWN_WS")?)"#,
            r#"ApiKey::new(env("KEY")?)"#,
        ] {
            assert!(main.contains(expected), "missing {expected}:\n{main}");
        }
        assert!(!main.contains("placeholder"), "no stand-in value leaks into the code");
    }

    #[test]
    fn refuses_a_non_empty_directory_without_force() {
        let dir = std::env::temp_dir().join(format!("delta-scaffold-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("c.toml");
        std::fs::write(&config, "listen = \"0.0.0.0:1\"\n[upstreams]\na = \"http://a\"\n[routes.r]\nupstreams = [\"a\"]\n[routes.r.methods]\nx = {}\n[apiKeys.k]\nkey = \"${K}\"\nroutes = [\"*\"]\n").unwrap();
        let dep = DeltaDep::Path(PathBuf::from("/nowhere"));
        let e = write_project(&config, &dir, &dep, false).err().expect("refused");
        assert!(e.contains("not empty"), "{e}");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "nothing written");
        write_project(&config, &dir, &dep, true).unwrap();
        assert!(dir.join("src/main.rs").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn subscription_limits_are_generated() {
        let text = "listen = \"0.0.0.0:1\"\n[upstreams]\nh = { http = \"https://h\", maxSubscriptionsPerConnection = 1000, maxConnections = 4 }\n\
                    o = { http = \"http://o:8899\", ws = \"ws://o:8900\", maxSubscriptionsPerConnection = 10 }\n\
                    [routes.r]\nupstreams = [\"*\"]\n[routes.r.methods]\nx = {}\n\
                    [apiKeys.k]\nkey = \"${K}\"\nroutes = [\"*\"]\nmaxSubscriptions = 50\n";
        let main = generate_main(text, "c.toml").unwrap();
        for expected in [
            r#"Upstream::url("https://h").max_subscriptions_per_connection(1000).max_connections(4)"#,
            r#"Upstream::split("http://o:8899", "ws://o:8900").max_subscriptions_per_connection(10)"#,
            ".max_subscriptions(50)",
        ] {
            assert!(main.contains(expected), "missing {expected}:\n{main}");
        }
    }
}
