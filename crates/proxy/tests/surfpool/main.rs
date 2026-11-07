//! Integration tests against real Solana RPC nodes: Surfpool simnets behind
//! fault injectors, the real proxy, real clients.
//!
//! Needs `surfpool` on PATH (or `SURFPOOL=/path/to/surfpool`); runs offline.
//! Ignored by default:
//!
//! ```sh
//! cargo test -p delta --test surfpool -- --ignored
//! ```

mod harness;

mod auth;
mod balancing;
mod basics;
mod cache;
mod clients;
mod health;
mod logging;
mod hedge;
mod quorum;
mod retry;
mod transactions;
mod ws;
