//! Response cache. The policies talk to a [`CacheBackend`] so a disk layer or
//! a shared layer can be added later without touching them.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use moka::future::Cache;
use moka::Expiry;

use crate::execute::CallResult;

/// A response worth storing, with its time to live.
#[derive(Clone, Debug)]
pub struct Cached {
    pub body: Bytes,
    pub ttl: Duration,
}

/// What a fetch produced: something to store, or an outcome that is passed
/// to every waiter but not stored (errors, `null`, oversized responses).
pub type Fetched = Result<Cached, CallResult>;

pub trait CacheBackend: Send + Sync + 'static {
    /// Returns the stored body for `key` if it is still fresh; otherwise runs
    /// `fetch` once for all concurrent callers of the same key.
    fn get_or_fetch(&self, key: String, fetch: BoxFuture<'static, Fetched>) -> BoxFuture<'static, Result<Bytes, CallResult>>;
}

/// No caching at all (used in tests and when nothing is configured).
pub struct NoCache;

impl CacheBackend for NoCache {
    fn get_or_fetch(&self, _key: String, fetch: BoxFuture<'static, Fetched>) -> BoxFuture<'static, Result<Bytes, CallResult>> {
        async move { fetch.await.map(|c| c.body) }.boxed()
    }
}

/// In-memory cache: size-bounded in bytes, TTL per entry, and identical
/// in-flight requests merged.
pub struct MokaCache {
    inner: Cache<String, Cached>,
}

struct PerEntryTtl;

impl Expiry<String, Cached> for PerEntryTtl {
    fn expire_after_create(&self, _key: &String, value: &Cached, _created_at: Instant) -> Option<Duration> {
        Some(value.ttl)
    }
}

/// Fixed bookkeeping cost counted for every entry.
const ENTRY_OVERHEAD: usize = 128;

impl MokaCache {
    pub fn new(max_bytes: u64) -> Self {
        let inner = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|k: &String, v: &Cached| {
                (k.len() + v.body.len() + ENTRY_OVERHEAD).min(u32::MAX as usize) as u32
            })
            .expire_after(PerEntryTtl)
            .build();
        Self { inner }
    }
}

impl CacheBackend for MokaCache {
    fn get_or_fetch(&self, key: String, fetch: BoxFuture<'static, Fetched>) -> BoxFuture<'static, Result<Bytes, CallResult>> {
        let cache = self.inner.clone();
        async move {
            match cache.entry(key).or_try_insert_with(fetch).await {
                Ok(entry) => Ok(entry.into_value().body),
                Err(outcome) => Err((*outcome).clone()),
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jsonrpc::Answer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn merges_in_flight_and_serves_hits() {
        let cache = Arc::new(MokaCache::new(1 << 20));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                let fetch = async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(Cached { body: Bytes::from_static(b"1"), ttl: Duration::from_secs(10) })
                }
                .boxed();
                cache.get_or_fetch("ns|u|m|h".into(), fetch).await
            }));
        }
        for h in handles {
            assert_eq!(&h.await.unwrap().unwrap()[..], b"1");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn uncacheable_outcomes_are_not_stored() {
        let cache = MokaCache::new(1 << 20);
        for _ in 0..2 {
            let fetch = async { Err(Ok(Answer::Result(Bytes::from_static(b"null")))) }.boxed();
            let r = cache.get_or_fetch("k".into(), fetch).await;
            assert!(matches!(r, Err(Ok(Answer::Result(_)))));
        }
    }
}
