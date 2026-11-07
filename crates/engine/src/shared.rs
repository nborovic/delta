//! State shared by every route: upstreams, health, connection limits
//! and the cache.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::cache::CacheBackend;
use crate::health::Health;
use crate::limits::Limits;
use crate::upstream::{Transport, UpstreamId, UpstreamInfo};

pub struct Shared {
    pub upstreams: Vec<UpstreamInfo>,
    pub health: Health,
    pub limits: Limits,
    pub cache: Arc<dyn CacheBackend>,
    pub transport: Arc<dyn Transport>,
    permits: Vec<Arc<Semaphore>>,
    /// Calls assigned to each upstream and not finished yet. Counted from
    /// the moment a call is started, before it waits for a permit, so a burst
    /// of requests does not all pick an upstream that only looks free.
    assigned: Vec<AtomicUsize>,
}

/// Counts one call against an upstream until dropped.
pub(crate) struct Assigned {
    shared: Arc<Shared>,
    id: UpstreamId,
}

impl Drop for Assigned {
    fn drop(&mut self) {
        self.shared.assigned[self.id].fetch_sub(1, Ordering::SeqCst);
    }
}

impl Shared {
    pub fn new(
        upstreams: Vec<UpstreamInfo>,
        cache: Arc<dyn CacheBackend>,
        transport: Arc<dyn Transport>,
        limits: Limits,
    ) -> Arc<Self> {
        for (i, u) in upstreams.iter().enumerate() {
            assert_eq!(u.id, i, "upstream ids must be their index");
        }
        let names = upstreams.iter().map(|u| u.name.clone()).collect();
        Arc::new(Self {
            health: Health::new(names, limits.health.clone()),
            assigned: upstreams.iter().map(|_| AtomicUsize::new(0)).collect(),
            permits: upstreams
                .iter()
                .map(|_| Arc::new(Semaphore::new(limits.inflight_per_upstream)))
                .collect(),
            upstreams,
            limits,
            cache,
            transport,
        })
    }

    pub fn name(&self, id: UpstreamId) -> &str {
        &self.upstreams[id].name
    }

    /// True when the upstream already has as many calls assigned as it may
    /// run at once; the selector then prefers others.
    pub fn at_limit(&self, id: UpstreamId) -> bool {
        self.assigned[id].load(Ordering::SeqCst) >= self.limits.inflight_per_upstream
    }

    /// Counts a call against `id` until the returned guard is dropped.
    pub(crate) fn assign(self: &Arc<Self>, id: UpstreamId) -> Assigned {
        self.assigned[id].fetch_add(1, Ordering::SeqCst);
        Assigned { shared: self.clone(), id }
    }

    /// Waits for an in-flight slot, up to the deadline.
    pub(crate) async fn acquire(&self, id: UpstreamId, deadline: Instant) -> Option<OwnedSemaphorePermit> {
        let sem = self.permits[id].clone();
        tokio::time::timeout_at(deadline.into(), sem.acquire_owned())
            .await
            .ok()
            .and_then(Result::ok)
    }
}
