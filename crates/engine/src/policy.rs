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
}

#[derive(Clone, Debug, PartialEq)]
pub struct CachePolicy {
    pub namespace: String,
    pub ttl: Duration,
}

/// Which upstream calls a request makes. At most one of retry, hedge and
/// quorum applies.
#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    Single { retry: Option<RetryPolicy> },
    Hedge { upstreams: usize },
    Quorum { threshold: usize, upstreams: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Extra tries after the first call.
    pub attempts: u32,
    pub delay: Duration,
}
