//! Effective policies: the route's policies merged with a method's overrides,
//! resolved once at load.

use std::time::Duration;

use crate::selector::Selector;

#[derive(Debug)]
pub struct EffectivePolicies {
    pub selector: Selector,
    /// `None` when caching is off (`ttl = 0`).
    pub cache: Option<CachePolicy>,
    pub mode: Mode,
    /// `None`: a failed attempt is final.
    pub retry: Option<RetryPolicy>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CachePolicy {
    pub namespace: String,
    pub ttl: Duration,
}

/// The upstream calls one attempt makes.
#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    /// One upstream.
    Single,
    /// N upstreams at once; the first success answers.
    Hedge { upstreams: usize },
    /// N upstreams; `threshold` matching answers answer.
    Quorum { threshold: usize, upstreams: usize },
}

/// Runs the mode again after a failed attempt, whatever the mode.
#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Extra attempts after the first.
    pub attempts: u32,
    pub delay: Duration,
}
