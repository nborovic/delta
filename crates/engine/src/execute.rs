//! Policy execution in a fixed order: hedge, quorum or retry decides the
//! upstream calls; each call goes through the cache, then the upstream.
//!
//! Every upstream call runs in its own task. Hedge, quorum and client
//! disconnects only stop waiting for a call; they never abort it, so every
//! request the proxy starts is fully sent and still records health.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::cache::Cached;
use crate::health::Signal;
use crate::jsonrpc::{self, Answer, RpcError};
use crate::policy::{EffectivePolicies, Mode, RetryPolicy};
use crate::shared::Shared;
use crate::solana::{self, Finality};
use crate::upstream::{TransportError, UpstreamId};

/// The outcome of one upstream call: a final answer (a result, or a JSON-RPC
/// error that another node would repeat), or a failure another upstream may
/// not have.
pub type CallResult = Result<Answer, Failure>;

#[derive(Clone, Debug)]
pub struct Failure {
    /// e.g. `helius: HTTP 503`.
    pub reason: String,
    /// The upstream's JSON-RPC error, for failures that carried one (-32005).
    pub rpc_error: Option<RpcError>,
}

/// One normal (non-subscription) request.
pub struct Request<'a> {
    pub method: &'a str,
    pub params: Option<&'a Value>,
    /// When the request arrived; the total deadline counts from here.
    pub arrived: Instant,
}

/// What happened while answering one request, for the access log.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    /// The upstream whose answer (result or final error) was returned; `None`
    /// when no upstream answered (refused before calling, all failed).
    pub upstream: Option<String>,
    /// Whether that answer came from the cache.
    pub cache: CacheUse,
    /// Upstream HTTP calls this request started (cache hits start none).
    pub calls: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CacheUse {
    /// No cache policy for this request (or a code rule turned it off).
    #[default]
    Off,
    Hit,
    Miss,
}

impl std::fmt::Display for CacheUse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CacheUse::Off => "off",
            CacheUse::Hit => "hit",
            CacheUse::Miss => "miss",
        })
    }
}

/// Runs a request through its effective policies and returns the answer for
/// the client, with a trace of how it was produced.
pub async fn execute(shared: &Arc<Shared>, policies: &EffectivePolicies, req: Request<'_>) -> (Answer, Trace) {
    let deadline = req.arrived + shared.limits.request_deadline;
    // Code rule: data not known to be finalized is never cached for methods
    // whose answer can still change until then. Everything else, writes
    // included, follows the configured policies.
    let finality = solana::finality(req.method, req.params);
    let cache = match &policies.cache {
        Some(c) if finality != Finality::Never => Some((c.namespace.clone(), c.ttl)),
        _ => None,
    };
    let ctx = Arc::new(CallCtx {
        shared: shared.clone(),
        method: req.method.to_owned(),
        body: jsonrpc::upstream_body(req.method, req.params),
        params_hash: cache.as_ref().map(|_| solana::params_hash(req.params)),
        cache,
        finality,
        deadline,
        calls: AtomicU32::new(0),
    });
    let (answer, by) = match &policies.mode {
        Mode::Hedge { upstreams } => hedge(&ctx, policies, *upstreams).await,
        Mode::Quorum { threshold, upstreams } => quorum(&ctx, policies, *threshold, *upstreams).await,
        Mode::Single { retry } => single(&ctx, policies, retry.as_ref()).await,
    };
    let cache = match (&ctx.cache, by) {
        (None, _) => CacheUse::Off,
        (Some(_), Some((_, true))) => CacheUse::Hit,
        (Some(_), _) => CacheUse::Miss,
    };
    let trace = Trace {
        upstream: by.map(|(up, _)| shared.name(up).to_owned()),
        cache,
        calls: ctx.calls.load(Ordering::Relaxed),
    };
    (answer, trace)
}

struct CallCtx {
    shared: Arc<Shared>,
    method: String,
    body: Bytes,
    cache: Option<(String, Duration)>,
    params_hash: Option<blake3::Hash>,
    finality: Finality,
    deadline: Instant,
    /// Upstream HTTP calls started for this request.
    calls: AtomicU32,
}

/// Who answered: the upstream, and whether the answer came from the cache.
type AnsweredBy = Option<(UpstreamId, bool)>;

/// A finished call: which upstream, its outcome, and whether that outcome
/// came from the cache.
struct Served {
    up: UpstreamId,
    result: CallResult,
    from_cache: bool,
}

/// Tracks failures so the client gets the last upstream JSON-RPC error if
/// there was one, otherwise `-33095 upstream failed`.
struct FailureLog {
    last_rpc: Option<RpcError>,
    last_reason: String,
}

impl FailureLog {
    fn new() -> Self {
        Self { last_rpc: None, last_reason: "no upstream available".to_owned() }
    }

    fn add(&mut self, f: Failure) {
        if f.rpc_error.is_some() {
            self.last_rpc = f.rpc_error;
        }
        self.last_reason = f.reason;
    }

    fn into_answer(self) -> Answer {
        match self.last_rpc {
            Some(e) => Answer::Error(e),
            None => Answer::error(jsonrpc::UPSTREAM_FAILED, format!("upstream failed: {}", self.last_reason)),
        }
    }
}

/// Starts one upstream call in its own task. Dropping the returned future
/// stops waiting for it but never aborts the call.
fn spawn_call(ctx: &Arc<CallCtx>, up: UpstreamId) -> impl std::future::Future<Output = Served> {
    let task_ctx = ctx.clone();
    // Counted against `up` right away, in the same step as the pick, and
    // until the call ends.
    let assigned = ctx.shared.assign(up);
    let handle: JoinHandle<(CallResult, bool)> = tokio::spawn(async move {
        let _assigned = assigned;
        cached_call(task_ctx, up).await
    });
    async move {
        match handle.await {
            Ok((result, from_cache)) => Served { up, result, from_cache },
            Err(e) => Served { up, result: Err(Failure { reason: format!("internal: {e}"), rpc_error: None }), from_cache: false },
        }
    }
}

/// Plain call, with optional retries. After a failed call it waits `delay`
/// and asks the selector again with no exclusions.
async fn single(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, retry: Option<&RetryPolicy>) -> (Answer, AnsweredBy) {
    let attempts = 1 + retry.map_or(0, |r| r.attempts);
    let mut log = FailureLog::new();
    for attempt in 0..attempts {
        if attempt > 0 {
            let delay = retry.map_or(Duration::ZERO, |r| r.delay);
            let wake = (Instant::now() + delay).min(ctx.deadline);
            tokio::time::sleep_until(wake.into()).await;
        }
        if Instant::now() >= ctx.deadline {
            log.add(Failure { reason: "deadline".into(), rpc_error: None });
            break;
        }
        let Some(&up) = policies.selector.pick(&ctx.shared, &ctx.method, 1, &[]).first() else {
            log.add(Failure { reason: "no upstream available".into(), rpc_error: None });
            continue;
        };
        match tokio::time::timeout_at(ctx.deadline.into(), spawn_call(ctx, up)).await {
            Err(_) => {
                log.add(Failure { reason: "deadline".into(), rpc_error: None });
                break;
            }
            Ok(s) => match s.result {
                Ok(answer) => return (answer, Some((s.up, s.from_cache))),
                Err(f) => log.add(f),
            },
        }
    }
    (log.into_answer(), None)
}

/// Sends to N upstreams at once and returns the first success. An error
/// never ends the race; the first final error is returned only if no
/// participant succeeds.
async fn hedge(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, n: usize) -> (Answer, AnsweredBy) {
    let picks = policies.selector.pick(&ctx.shared, &ctx.method, n, &[]);
    let mut racing: FuturesUnordered<_> = picks.iter().map(|&up| spawn_call(ctx, up)).collect();
    let mut log = FailureLog::new();
    let mut first_final: Option<(RpcError, UpstreamId, bool)> = None;
    loop {
        match tokio::time::timeout_at(ctx.deadline.into(), racing.next()).await {
            Err(_) => {
                log.add(Failure { reason: "deadline".into(), rpc_error: None });
                break;
            }
            Ok(None) => break,
            Ok(Some(s)) => match s.result {
                Ok(Answer::Result(body)) => return (Answer::Result(body), Some((s.up, s.from_cache))),
                Ok(Answer::Error(e)) => {
                    first_final.get_or_insert((e, s.up, s.from_cache));
                }
                Err(f) => log.add(f),
            },
        }
    }
    match first_final {
        Some((e, up, cached)) => (Answer::Error(e), Some((up, cached))),
        None => (log.into_answer(), None),
    }
}

/// Sends to N upstreams and returns once `threshold` normalised answers
/// match. Never falls back to the most common answer.
async fn quorum(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, threshold: usize, n: usize) -> (Answer, AnsweredBy) {
    let not_reached = || (Answer::error(jsonrpc::QUORUM_NOT_REACHED, "quorum not reached"), None);
    let picks = policies.selector.pick(&ctx.shared, &ctx.method, n, &[]);
    if picks.len() < threshold {
        return not_reached();
    }
    let mut pending: FuturesUnordered<_> = picks.iter().map(|&up| spawn_call(ctx, up)).collect();
    let mut remaining = picks.len();
    let mut groups: HashMap<blake3::Hash, usize> = HashMap::new();
    loop {
        let next = match tokio::time::timeout_at(ctx.deadline.into(), pending.next()).await {
            Err(_) | Ok(None) => return not_reached(),
            Ok(Some(s)) => s,
        };
        remaining -= 1;
        let (up, from_cache) = (next.up, next.from_cache);
        if let Ok(answer) = next.result {
            let key = match &answer {
                Answer::Result(body) => solana::quorum_hash(&ctx.method, body),
                Answer::Error(e) => blake3::hash(format!("error:{}:{}", e.code, e.message).as_bytes()),
            };
            let count = groups.entry(key).or_default();
            *count += 1;
            if *count >= threshold {
                return (answer, Some((up, from_cache)));
            }
        }
        let best = groups.values().copied().max().unwrap_or(0);
        if best + remaining < threshold {
            return not_reached();
        }
    }
}

/// One upstream call through the cache (when on). The flag is true when the
/// outcome came from the cache or from another request's identical call,
/// rather than from a call this request made.
async fn cached_call(ctx: Arc<CallCtx>, up: UpstreamId) -> (CallResult, bool) {
    let (Some((namespace, ttl)), Some(hash)) = (ctx.cache.clone(), ctx.params_hash) else {
        return (raw_call(&ctx, up).await, false);
    };
    let key = format!("{namespace}|{}|{}|{}", ctx.shared.name(up), ctx.method, hash.to_hex());
    let fetch_ctx = ctx.clone();
    let fetched = Arc::new(AtomicBool::new(false));
    let fetched_here = fetched.clone();
    let fetch = async move {
        fetched_here.store(true, Ordering::Relaxed);
        let result = raw_call(&fetch_ctx, up).await;
        match &result {
            Ok(Answer::Result(body)) => match cache_ttl(&fetch_ctx, body, ttl) {
                Some(ttl) => Ok(Cached { body: body.clone(), ttl }),
                None => Err(result),
            },
            _ => Err(result),
        }
    }
    .boxed();
    let outcome = match ctx.shared.cache.get_or_fetch(key, fetch).await {
        Ok(body) => Ok(Answer::Result(body)),
        Err(outcome) => outcome,
    };
    (outcome, !fetched.load(Ordering::Relaxed))
}

/// Code rules on what may be stored. The TTL is whatever the route
/// configured; null/empty, oversized and not-yet-finalized results are never
/// stored.
fn cache_ttl(ctx: &CallCtx, body: &Bytes, configured: Duration) -> Option<Duration> {
    if configured.is_zero() || body.len() > ctx.shared.limits.max_cached_bytes || solana::is_empty_result(body) {
        return None;
    }
    if ctx.finality == Finality::IfStatusesFinalized && !solana::statuses_finalized(body) {
        return None;
    }
    Some(configured)
}

/// One HTTP call to one upstream: classifies the outcome and records health.
async fn raw_call(ctx: &CallCtx, up: UpstreamId) -> CallResult {
    let started = Instant::now();
    let result = upstream_call(ctx, up).await;
    let outcome = match &result {
        Ok(Answer::Result(_)) => "ok".to_owned(),
        Ok(Answer::Error(e)) => format!("error {}", e.code),
        Err(f) => f.reason.clone(),
    };
    tracing::debug!(
        target: "delta::upstream",
        upstream = %ctx.shared.name(up),
        method = %ctx.method,
        outcome = %outcome,
        ms = started.elapsed().as_millis() as u64,
    );
    result
}

async fn upstream_call(ctx: &CallCtx, up: UpstreamId) -> CallResult {
    let shared = &ctx.shared;
    let method = ctx.method.as_str();
    let health = &shared.health;
    let body = post(shared, up, method, ctx.body.clone(), ctx.deadline, &ctx.calls).await?;
    let fail = |reason: String, rpc_error: Option<RpcError>| Failure { reason: format!("{}: {reason}", shared.name(up)), rpc_error };

    let Some(answer) = jsonrpc::parse_upstream(&body) else {
        health.record(up, method, Signal::Failure("invalid response".into()), Signal::Neutral);
        return Err(fail("invalid JSON-RPC response".into(), None));
    };
    match answer {
        Answer::Result(_) => {
            health.record(up, method, Signal::Success, Signal::Success);
            Ok(answer)
        }
        Answer::Error(e) if e.code == solana::NODE_UNHEALTHY => {
            health.record(up, method, Signal::Failure(format!("{}", e.code)), Signal::Neutral);
            Err(fail(format!("{} {}", e.code, e.message), Some(e)))
        }
        Answer::Error(e) if solana::is_retryable_rpc_error(e.code) => {
            health.record(up, method, Signal::Neutral, Signal::Neutral);
            Err(fail(format!("{} {}", e.code, e.message), Some(e)))
        }
        Answer::Error(_) => {
            // A final answer: never counted against health.
            health.record(up, method, Signal::Neutral, Signal::Neutral);
            Ok(answer)
        }
    }
}

/// One HTTP POST to `up` within `deadline`, under its in-flight limit.
/// Records health for the failures it sees (transport, timeout, HTTP
/// status) and returns the body of a 2xx reply; the caller judges that.
async fn post(shared: &Shared, up: UpstreamId, method: &str, body: Bytes, deadline: Instant, calls: &AtomicU32) -> Result<Bytes, Failure> {
    let name = shared.name(up).to_owned();
    let fail = |reason: String| Failure { reason: format!("{name}: {reason}"), rpc_error: None };
    let health = &shared.health;

    let Some(_permit) = shared.acquire(up, deadline).await else {
        health.record(up, method, Signal::Neutral, Signal::Neutral);
        return Err(fail("deadline".into()));
    };
    let left = deadline.saturating_duration_since(Instant::now());
    let per_call = shared.limits.per_call_timeout;
    let cut_by_deadline = left < per_call;

    calls.fetch_add(1, Ordering::Relaxed);
    let reply = match shared.transport.post(&shared.upstreams[up].http, body, per_call.min(left)).await {
        Err(TransportError::Timeout) if cut_by_deadline => {
            // The proxy's own budget ran out: not the upstream's fault.
            health.record(up, method, Signal::Neutral, Signal::Neutral);
            return Err(fail("deadline".into()));
        }
        Err(TransportError::Timeout) => {
            health.record(up, method, Signal::Failure("timeout".into()), Signal::Neutral);
            return Err(fail("timeout".into()));
        }
        Err(TransportError::Connect(e) | TransportError::Other(e)) => {
            health.record(up, method, Signal::Failure("transport".into()), Signal::Neutral);
            return Err(fail(e));
        }
        Ok(reply) => reply,
    };

    match reply.status {
        200..=299 => Ok(reply.body),
        429 => {
            health.record(up, method, Signal::Overloaded, Signal::Neutral);
            Err(fail("HTTP 429".into()))
        }
        401 | 403 => {
            // Providers often reject single methods per plan.
            let reason = format!("HTTP {}", reply.status);
            health.record(up, method, Signal::Neutral, Signal::Failure(reason.clone()));
            Err(fail(reason))
        }
        500..=599 => {
            let reason = format!("HTTP {}", reply.status);
            health.record(up, method, Signal::Failure(reason.clone()), Signal::Neutral);
            Err(fail(reason))
        }
        status => {
            // 400, 413 and the like: the request's fault, not counted.
            health.record(up, method, Signal::Neutral, Signal::Neutral);
            Err(fail(format!("HTTP {status}")))
        }
    }
}

/// One request of a client batch that is forwarded upstream.
pub struct BatchItem<'a> {
    pub method: &'a str,
    pub params: Option<&'a Value>,
    /// A JSON-RPC notification: sent without an id, and gets no answer.
    pub notification: bool,
}

/// The method name batches are tracked under in health and selection.
pub const BATCH_METHOD: &str = "batch";

/// Sends a client batch to one upstream as one batch, picked by `selector`.
/// No cache, retry, hedge or quorum: a batch is forwarded as it is. Returns
/// one answer per item (`None` for notifications).
pub async fn forward_batch(
    shared: &Arc<Shared>,
    selector: &crate::selector::Selector,
    items: &[BatchItem<'_>],
    arrived: Instant,
) -> (Vec<Option<Answer>>, Trace) {
    let deadline = arrived + shared.limits.request_deadline;
    let calls = AtomicU32::new(0);
    let all = |a: Answer| items.iter().map(|i| (!i.notification).then(|| a.clone())).collect::<Vec<_>>();
    let trace = |up: Option<UpstreamId>, calls: &AtomicU32| Trace {
        upstream: up.map(|u| shared.name(u).to_owned()),
        cache: CacheUse::Off,
        calls: calls.load(Ordering::Relaxed),
    };

    let Some(&up) = selector.pick(shared, BATCH_METHOD, 1, &[]).first() else {
        return (all(Answer::error(jsonrpc::UPSTREAM_FAILED, "upstream failed: no upstream available")), trace(None, &calls));
    };
    let _assigned = shared.assign(up);
    // Upstream ids are the items' positions, so answers map back whatever
    // the client's ids were.
    let body: Vec<Value> = items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let mut req = serde_json::json!({"jsonrpc": "2.0", "method": item.method});
            if !item.notification {
                req["id"] = i.into();
            }
            if let Some(p) = item.params {
                req["params"] = p.clone();
            }
            req
        })
        .collect();
    let body = Bytes::from(serde_json::to_vec(&body).expect("serializable"));
    let started = Instant::now();
    let reply = post(shared, up, BATCH_METHOD, body, deadline, &calls).await;
    tracing::debug!(
        target: "delta::upstream",
        upstream = %shared.name(up),
        method = BATCH_METHOD,
        items = items.len(),
        outcome = %reply.as_ref().map_or_else(|f| f.reason.clone(), |_| "ok".to_owned()),
        ms = started.elapsed().as_millis() as u64,
    );
    let body = match reply {
        Ok(b) => b,
        Err(f) => return (all(Answer::error(jsonrpc::UPSTREAM_FAILED, format!("upstream failed: {}", f.reason))), trace(None, &calls)),
    };

    let health = &shared.health;
    let elements: Vec<&serde_json::value::RawValue> = match serde_json::from_slice::<Vec<&serde_json::value::RawValue>>(&body) {
        Ok(e) => e,
        Err(_) => {
            // One error object for the whole batch, e.g. a node refusing it.
            return match jsonrpc::parse_upstream(&body) {
                Some(answer @ Answer::Error(_)) => {
                    health.record(up, BATCH_METHOD, Signal::Neutral, Signal::Neutral);
                    (all(answer), trace(Some(up), &calls))
                }
                _ if body.is_empty() && items.iter().all(|i| i.notification) => {
                    health.record(up, BATCH_METHOD, Signal::Success, Signal::Success);
                    (all(Answer::error(jsonrpc::UPSTREAM_FAILED, "unused")), trace(Some(up), &calls))
                }
                _ => {
                    health.record(up, BATCH_METHOD, Signal::Failure("invalid response".into()), Signal::Neutral);
                    let reason = format!("upstream failed: {}: invalid JSON-RPC response", shared.name(up));
                    (all(Answer::error(jsonrpc::UPSTREAM_FAILED, reason)), trace(None, &calls))
                }
            };
        }
    };
    let mut answers: Vec<Option<Answer>> = vec![None; items.len()];
    let mut unhealthy = false;
    for raw in elements {
        #[derive(serde::Deserialize)]
        struct Id {
            id: Option<usize>,
        }
        let Some(i) = serde_json::from_str::<Id>(raw.get()).ok().and_then(|x| x.id).filter(|&i| i < items.len()) else {
            continue;
        };
        if let Some(answer) = jsonrpc::parse_upstream(raw.get().as_bytes()) {
            if matches!(&answer, Answer::Error(e) if e.code == solana::NODE_UNHEALTHY) {
                unhealthy = true;
            }
            answers[i] = Some(answer);
        }
    }
    let signal = if unhealthy { Signal::Failure(format!("{}", solana::NODE_UNHEALTHY)) } else { Signal::Success };
    health.record(up, BATCH_METHOD, signal, Signal::Success);
    let missing = || Answer::error(jsonrpc::UPSTREAM_FAILED, format!("upstream failed: {}: no answer in the batch response", shared.name(up)));
    let answers = items
        .iter()
        .zip(answers)
        .map(|(item, a)| (!item.notification).then(|| a.unwrap_or_else(missing)))
        .collect();
    (answers, trace(Some(up), &calls))
}
