//! Policy execution: the cache answers first; on a miss, retry runs the
//! attempts and the mode (single, hedge or quorum) decides each attempt's
//! upstream calls. The request's final answer is what gets stored.
//!
//! Every upstream call runs in its own task. Hedge, quorum and client
//! disconnects only stop waiting for a call; they never abort it, so every
//! request the proxy starts is fully sent and still records health.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::cache::Cached;
use crate::health::Signal;
use crate::jsonrpc::{self, Answer, RpcError};
use crate::policy::{EffectivePolicies, Mode};
use crate::shared::Shared;
use crate::solana;
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
    let ctx = Arc::new(CallCtx {
        shared: shared.clone(),
        method: req.method.to_owned(),
        body: jsonrpc::upstream_body(req.method, req.params),
        deadline,
        calls: AtomicU32::new(0),
    });
    let trace = |answer: Answer, by: AnsweredBy, cache: CacheUse| {
        let trace = Trace { upstream: by.map(|up| shared.name(up).to_owned()), cache, calls: ctx.calls.load(Ordering::Relaxed) };
        (answer, trace)
    };
    let cache = match &policies.cache {
        Some(c) => c,
        _ => {
            let (answer, by) = attempts(&ctx, policies).await;
            return trace(answer, by, CacheUse::Off);
        }
    };

    // One entry per request, keyed by how many upstreams stand behind the
    // answer: a quorum's agreed answer never serves as a single upstream's,
    // nor the other way round.
    let agreed = match policies.mode {
        Mode::Quorum { threshold, .. } => threshold,
        Mode::Single | Mode::Hedge { .. } | Mode::Redundant => 1,
    };
    let key = format!("{}|agreed:{agreed}|{}|{}", cache.namespace, req.method, solana::params_hash(req.params).to_hex());
    if let Some(hit) = shared.cache.get(&key).await.filter(|c| c.fresh_for(cache.ttl)) {
        return trace(Answer::Result(hit.body), None, CacheUse::Hit);
    }

    // Concurrent misses on the same route share one execution: the first
    // runs it, the others wait for its answer, error or not.
    let lead = match shared.flights.join(format!("{}|{key}", cache.scope)) {
        Ok(lead) => Some(lead),
        Err(mut flight) => loop {
            if let Some((answer, by)) = flight.borrow_and_update().clone() {
                return trace(answer, by, CacheUse::Hit);
            }
            // The leading request went away (client gone): run our own.
            if flight.changed().await.is_err() {
                break None;
            }
        },
    };
    let (answer, by) = attempts(&ctx, policies).await;
    if let Answer::Result(body) = &answer {
        if storable(shared, body) {
            let entry = Cached { body: body.clone(), fetched: Instant::now(), keep: cache.keep };
            shared.cache.insert(key, entry).await;
        }
    }
    if let Some(lead) = lead {
        lead.land(answer.clone(), by);
    }
    trace(answer, by, CacheUse::Miss)
}

/// A flight's answer, once it lands.
type Landing = watch::Receiver<Option<(Answer, AnsweredBy)>>;

/// Requests whose identical miss is being answered right now.
#[derive(Default)]
pub(crate) struct Flights(Mutex<HashMap<String, Landing>>);

impl Flights {
    /// Leads the flight for `key`, or follows the one already running.
    fn join(&self, key: String) -> Result<Lead<'_>, Landing> {
        let mut flights = self.0.lock().unwrap();
        if let Some(flight) = flights.get(&key) {
            return Err(flight.clone());
        }
        let (tx, rx) = watch::channel(None);
        flights.insert(key.clone(), rx);
        Ok(Lead { flights: self, key, tx })
    }
}

/// The request running a flight. Ends the flight when dropped: landed, or
/// abandoned (its followers then run their own).
struct Lead<'a> {
    flights: &'a Flights,
    key: String,
    tx: watch::Sender<Option<(Answer, AnsweredBy)>>,
}

impl Lead<'_> {
    fn land(self, answer: Answer, by: AnsweredBy) {
        self.tx.send_replace(Some((answer, by)));
    }
}

impl Drop for Lead<'_> {
    fn drop(&mut self) {
        self.flights.0.lock().unwrap().remove(&self.key);
    }
}

/// Code rules on what may be stored: null/empty and oversized results never
/// are. Everything else is cached for the method's TTL: under Alpenglow,
/// history is final once a node returns it, and what still changes (current
/// state, ranges reaching the tip) is bounded by the TTL.
fn storable(shared: &Shared, body: &Bytes) -> bool {
    body.len() <= shared.limits.max_cached_bytes && !solana::is_empty_result(body)
}

struct CallCtx {
    shared: Arc<Shared>,
    method: String,
    body: Bytes,
    deadline: Instant,
    /// Upstream HTTP calls started for this request.
    calls: AtomicU32,
}

/// The upstream whose answer was returned.
type AnsweredBy = Option<UpstreamId>;

/// A finished call: which upstream, and its outcome.
struct Served {
    up: UpstreamId,
    result: CallResult,
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

    fn answer(&self) -> Answer {
        match &self.last_rpc {
            Some(e) => Answer::Error(e.clone()),
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
    let handle: JoinHandle<CallResult> = tokio::spawn(async move {
        let _assigned = assigned;
        raw_call(&task_ctx, up).await
    });
    async move {
        match handle.await {
            Ok(result) => Served { up, result },
            Err(e) => Served { up, result: Err(Failure { reason: format!("internal: {e}"), rpc_error: None }) },
        }
    }
}

/// Runs the mode, and again after `delay` for each retry while an attempt
/// fails and the deadline allows. A failed attempt is one another attempt
/// may turn around: every upstream failed, or quorum was not reached. A
/// result or a node's final JSON-RPC error answers at once. Each attempt
/// asks the selector afresh, which skips unhealthy upstreams.
async fn attempts(ctx: &Arc<CallCtx>, policies: &EffectivePolicies) -> (Answer, AnsweredBy) {
    let retry = policies.retry.as_ref();
    let attempts = 1 + retry.map_or(0, |r| r.attempts);
    // Shared by every attempt: the client gets the last upstream JSON-RPC
    // error of any of them.
    let mut log = FailureLog::new();
    let mut failed = None;
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
        let outcome = match &policies.mode {
            // Redundant is for subscriptions; a call never has it.
            Mode::Single | Mode::Redundant => single(ctx, policies, &mut log).await,
            Mode::Hedge { upstreams } => hedge(ctx, policies, *upstreams, &mut log).await,
            Mode::Quorum { threshold, upstreams } => quorum(ctx, policies, *threshold, *upstreams).await,
        };
        match outcome {
            Attempt::Answered(answer, by) => return (answer, by),
            Attempt::Failed(answer) => failed = answer,
        }
    }
    (failed.unwrap_or_else(|| log.answer()), None)
}

/// How one attempt ended.
enum Attempt {
    /// A result, or a final error another attempt would repeat.
    Answered(Answer, AnsweredBy),
    /// Worth another attempt. `None`: the failure log says why.
    Failed(Option<Answer>),
}

/// One call to one upstream.
async fn single(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, log: &mut FailureLog) -> Attempt {
    let Some(&up) = policies.selector.pick(&ctx.shared, &ctx.method, 1, &[]).first() else {
        log.add(Failure { reason: "no upstream available".into(), rpc_error: None });
        return Attempt::Failed(None);
    };
    match tokio::time::timeout_at(ctx.deadline.into(), spawn_call(ctx, up)).await {
        Err(_) => {
            log.add(Failure { reason: "deadline".into(), rpc_error: None });
            Attempt::Failed(None)
        }
        Ok(s) => match s.result {
            Ok(answer) => Attempt::Answered(answer, Some(s.up)),
            Err(f) => {
                log.add(f);
                Attempt::Failed(None)
            }
        },
    }
}

/// Sends to N upstreams at once and returns the first success. An error
/// never ends the race; the first final error is returned only if no
/// participant succeeds, and the attempt fails only if none answered.
async fn hedge(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, n: usize, log: &mut FailureLog) -> Attempt {
    let picks = policies.selector.pick(&ctx.shared, &ctx.method, n, &[]);
    if picks.is_empty() {
        log.add(Failure { reason: "no upstream available".into(), rpc_error: None });
    }
    let mut racing: FuturesUnordered<_> = picks.iter().map(|&up| spawn_call(ctx, up)).collect();
    let mut first_final: Option<(RpcError, UpstreamId)> = None;
    loop {
        match tokio::time::timeout_at(ctx.deadline.into(), racing.next()).await {
            Err(_) => {
                log.add(Failure { reason: "deadline".into(), rpc_error: None });
                break;
            }
            Ok(None) => break,
            Ok(Some(s)) => match s.result {
                Ok(Answer::Result(body)) => return Attempt::Answered(Answer::Result(body), Some(s.up)),
                Ok(Answer::Error(e)) => {
                    first_final.get_or_insert((e, s.up));
                }
                Err(f) => log.add(f),
            },
        }
    }
    match first_final {
        Some((e, up)) => Attempt::Answered(Answer::Error(e), Some(up)),
        None => Attempt::Failed(None),
    }
}

/// Sends to N upstreams and answers once `threshold` normalised answers
/// match. Never falls back to the most common answer: the attempt fails.
async fn quorum(ctx: &Arc<CallCtx>, policies: &EffectivePolicies, threshold: usize, n: usize) -> Attempt {
    let not_reached = || Attempt::Failed(Some(Answer::error(jsonrpc::QUORUM_NOT_REACHED, "quorum not reached")));
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
        let up = next.up;
        if let Ok(answer) = next.result {
            let key = match &answer {
                Answer::Result(body) => solana::quorum_hash(&ctx.method, body),
                Answer::Error(e) => blake3::hash(format!("error:{}:{}", e.code, e.message).as_bytes()),
            };
            let count = groups.entry(key).or_default();
            *count += 1;
            if *count >= threshold {
                return Attempt::Answered(answer, Some(up));
            }
        }
        let best = groups.values().copied().max().unwrap_or(0);
        if best + remaining < threshold {
            return not_reached();
        }
    }
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
/// No cache, mode or retry: a batch is forwarded as it is. Returns
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
