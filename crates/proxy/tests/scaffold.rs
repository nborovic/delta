//! Scaffolds the spec's example config, compiles the generated project and
//! checks that its `proxy()` builds exactly the config the TOML parses into.
//!
//! Slow the first time (compiles a separate dependency tree), so ignored by
//! default: `cargo test -p delta --test scaffold -- --ignored`.

use std::path::PathBuf;
use std::process::Command;

use delta::scaffold::{write_project, DeltaDep};

#[test]
#[ignore]
fn generated_project_compiles_and_matches_the_toml() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config = manifest.join("../../config.example.toml").canonicalize().unwrap();
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("scaffolded-proxy");
    write_project(&config, &dir, &DeltaDep::Path(manifest.clone()), true).unwrap();

    // The project sits under delta's target/, inside delta's workspace
    // directory: make it its own workspace so Cargo does not expect it to
    // be a member of delta's.
    let cargo_toml = dir.join("Cargo.toml");
    let mut manifest_text = std::fs::read_to_string(&cargo_toml).unwrap();
    manifest_text.push_str("\n[workspace]\n");
    std::fs::write(&cargo_toml, manifest_text).unwrap();

    // Append an equivalence test to the generated main.rs.
    let main = dir.join("src/main.rs");
    let mut code = std::fs::read_to_string(&main).unwrap();
    code.push_str(&format!(
        r#"
#[cfg(test)]
mod scaffold_check {{
    #[test]
    fn same_config_as_the_toml() {{
        let text = include_str!({config:?});
        let from_toml = delta::config::parse_toml(text, &|k| std::env::var(k).ok()).unwrap();
        let from_code = super::proxy().unwrap();
        assert_eq!(&from_toml, from_code.config());
        from_code.build().unwrap();
    }}
}}
"#
    ));
    std::fs::write(&main, code).unwrap();

    let target = manifest.join("../../target/scaffold-check");
    let status = Command::new(env!("CARGO"))
        .args(["test", "--quiet"])
        .current_dir(&dir)
        .env("CARGO_TARGET_DIR", target)
        .env("HELIUS_KEY", "helius-secret")
        .env("TRITON_TOKEN", "triton-secret")
        .env("BACKEND_KEY", "6a1f0c2e-8a3b-4c5d-9e7f-0123456789ab")
        .env("INDEXER_KEY", "7b2e1d3f-9b4c-4d6e-8f01-123456789abc")
        .status()
        .unwrap();
    assert!(status.success(), "generated project failed its check");
}
