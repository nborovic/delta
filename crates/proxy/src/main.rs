use std::path::PathBuf;
use std::process::ExitCode;

use delta::scaffold::{self, DeltaDep};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
usage:
  delta [run] <config.toml>        run the proxy (default: config.toml)
  delta scaffold <config.toml> <dir> [--delta-path <path> | --delta-git <url>] [--force]
                                   generate a Cargo project whose main.rs builds the
                                   same proxy with Proxy::builder()
  delta help";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("help" | "-h" | "--help") => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some("scaffold") => scaffold_cmd(&args[1..]),
        Some("run") => run(args.get(1).cloned()).await,
        Some(_) if args.len() == 1 => run(args.first().cloned()).await,
        None => run(None).await,
        Some(_) => fail(USAGE),
    }
}

async fn run(path: Option<String>) -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let path = path.unwrap_or_else(|| "config.toml".to_owned());
    let proxy = match delta::Proxy::from_toml(&path) {
        Ok(p) => p,
        Err(e) => return fail(&e.to_string()),
    };
    match proxy.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&format!("error: {e}")),
    }
}

fn scaffold_cmd(args: &[String]) -> ExitCode {
    let mut positional = Vec::new();
    let mut dep = None;
    let mut force = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--force" => force = true,
            "--delta-path" | "--delta-git" => {
                let Some(v) = it.next() else { return fail(&format!("{a} needs a value\n{USAGE}")) };
                dep = Some(if a == "--delta-path" {
                    let p = PathBuf::from(v);
                    DeltaDep::Path(p.canonicalize().unwrap_or(p))
                } else {
                    DeltaDep::Git(v.clone())
                });
            }
            flag if flag.starts_with("--") => return fail(&format!("unknown option {flag}\n{USAGE}")),
            _ => positional.push(PathBuf::from(a)),
        }
    }
    let [config, dir] = positional.as_slice() else { return fail(USAGE) };
    let Some(dep) = dep.or_else(scaffold::default_dep) else {
        return fail("cannot find the delta sources this binary was built from; pass --delta-path or --delta-git");
    };
    match scaffold::write_project(config, dir, &dep, force) {
        Ok(out) => {
            for f in &out.files {
                println!("wrote {}", f.display());
            }
            println!("\nnext:");
            println!("  1. build   cd {} && cargo build --release", dir.display());
            let env = if out.env_vars.is_empty() {
                String::new()
            } else {
                format!("   (needs {} set)", out.env_vars.join(", "))
            };
            println!("  2. run     ./target/release/{}{env}", out.package);
            ExitCode::SUCCESS
        }
        Err(e) => fail(&e),
    }
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("{msg}");
    ExitCode::FAILURE
}
