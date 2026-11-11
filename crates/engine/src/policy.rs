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
    /// `""` = shared by every route.
    pub namespace: String,
    /// How old an entry this method accepts.
    pub ttl: Duration,
    /// How long a stored entry is kept: the longest TTL of any method
    /// reading this namespace, so a short TTL never evicts for a long one.
    pub keep: Duration,
    /// Concurrent misses share one execution only within the same scope
    /// (the route), where every request runs the same policies.
    pub scope: String,
}

/// The upstream calls one attempt makes.
#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    /// One upstream.
    Single,
    /// `fanout` upstreams at once; the first success answers.
    Hedge { fanout: usize },
    /// `fanout` upstreams; `threshold` matching answers answer.
    Quorum { threshold: usize, fanout: usize },
    /// Subscriptions only (resolve never gives it to a call): every upstream
    /// at once, the first copy of each notification forwarded.
    Redundant,
}

/// Runs the mode again after a failed attempt, whatever the mode.
#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Extra attempts after the first.
    pub attempts: u32,
    pub delay: Duration,
}
