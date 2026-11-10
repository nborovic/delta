//! Response cache. One entry per request (not per upstream call): the answer
//! a request's mode produced. The policies talk to a [`CacheBackend`] so a
//! disk layer or a shared layer can be added later without touching them.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use moka::future::Cache;
use moka::Expiry;

/// A stored answer and when it was fetched. Each reader judges its age
/// against its own TTL, so routes with different TTLs can share it.
#[derive(Clone, Debug)]
pub struct Cached {
    pub body: Bytes,
    pub fetched: Instant,
    /// How long the store keeps it: the longest TTL any reader may use.
    pub keep: Duration,
}

impl Cached {
    /// Fresh enough for a reader whose TTL is `ttl`.
    pub fn fresh_for(&self, ttl: Duration) -> bool {
        self.fetched.elapsed() < ttl
    }
}

pub trait CacheBackend: Send + Sync + 'static {
    /// The stored entry for `key`, however old (until it is dropped).
    fn get(&self, key: &str) -> BoxFuture<'static, Option<Cached>>;
    /// Stores or replaces the entry for `key`.
    fn insert(&self, key: String, value: Cached) -> BoxFuture<'static, ()>;
}

/// No caching at all (used in tests and when nothing is configured).
pub struct NoCache;

impl CacheBackend for NoCache {
    fn get(&self, _key: &str) -> BoxFuture<'static, Option<Cached>> {
        async { None }.boxed()
    }
    fn insert(&self, _key: String, _value: Cached) -> BoxFuture<'static, ()> {
        async {}.boxed()
    }
}

/// In-memory cache: size-bounded in bytes, each entry kept for its `keep`.
pub struct MokaCache {
    inner: Cache<String, Cached>,
}

struct KeepFor;

impl Expiry<String, Cached> for KeepFor {
    fn expire_after_create(&self, _key: &String, value: &Cached, _created_at: Instant) -> Option<Duration> {
        Some(value.keep)
    }
    fn expire_after_update(&self, _key: &String, value: &Cached, _updated_at: Instant, _left: Option<Duration>) -> Option<Duration> {
        Some(value.keep)
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
            .expire_after(KeepFor)
            .build();
        Self { inner }
    }
}

impl CacheBackend for MokaCache {
    fn get(&self, key: &str) -> BoxFuture<'static, Option<Cached>> {
        let (cache, key) = (self.inner.clone(), key.to_owned());
        async move { cache.get(&key).await }.boxed()
    }
    fn insert(&self, key: String, value: Cached) -> BoxFuture<'static, ()> {
        let cache = self.inner.clone();
        async move { cache.insert(key, value).await }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(body: &'static [u8], age: Duration, keep: Duration) -> Cached {
        Cached { body: Bytes::from_static(body), fetched: Instant::now() - age, keep }
    }

    #[tokio::test]
    async fn each_reader_judges_the_age_by_its_own_ttl() {
        let cache = MokaCache::new(1 << 20);
        cache.insert("k".into(), entry(b"1", Duration::from_millis(500), Duration::from_secs(10))).await;
        let got = cache.get("k").await.unwrap();
        assert!(got.fresh_for(Duration::from_secs(1)), "a 1 s reader takes it");
        assert!(!got.fresh_for(Duration::from_millis(200)), "a 200 ms reader does not");
    }

    #[tokio::test]
    async fn an_insert_replaces_the_entry_and_its_keep() {
        let cache = MokaCache::new(1 << 20);
        cache.insert("k".into(), entry(b"old", Duration::ZERO, Duration::from_millis(50))).await;
        cache.insert("k".into(), entry(b"new", Duration::ZERO, Duration::from_secs(10))).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(&cache.get("k").await.unwrap().body[..], b"new", "kept for the new keep");
    }

    #[tokio::test]
    async fn entries_are_dropped_after_their_keep() {
        let cache = MokaCache::new(1 << 20);
        cache.insert("k".into(), entry(b"1", Duration::ZERO, Duration::from_millis(50))).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(cache.get("k").await.is_none());
    }
}
