//! Request handling shared by the HTTP and WebSocket front ends: parse,
//! check the method, compute the effective policies, execute.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use delta_engine::jsonrpc::{self, null_id, Answer};
use delta_engine::{execute, forward_batch, solana, BatchItem, Request, Shared, Trace};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::Value;

use crate::auth::Auth;
use crate::resolve::RouteTable;
use crate::subs::Subscriptions;

/// Who sent a request and how, for the access log. Holds the key's name,
/// never the key.
#[derive(Clone, Debug)]
pub struct Who {
    pub key: String,
    pub ip: IpAddr,
    /// `http`, or `ws/<connection number>`.
    pub via: String,
    /// The route as requested (it may not exist).
    pub route: String,
}

/// One access-log line for an answered request.
pub fn log_answer(who: &Who, method: &str, answer: &Answer, trace: &Trace, arrived: Instant) {
    let ms = arrived.elapsed().as_millis() as u64;
    let upstream = trace.upstream.as_deref().unwrap_or("-");
    match answer {
        Answer::Result(_) => tracing::info!(
            target: "delta::access",
            via = %who.via, key = %who.key, ip = %who.ip, route = %who.route, method = %method,
            outcome = %"ok", ms, upstream = %upstream, cache = %trace.cache, calls = trace.calls,
        ),
        Answer::Error(e) => {
            // Only delta's own messages: an upstream's may echo request
            // parameters, which are never logged.
            let error = if (-33099..=-33090).contains(&e.code) { e.message.as_str() } else { "-" };
            tracing::info!(
                target: "delta::access",
                via = %who.via, key = %who.key, ip = %who.ip, route = %who.route, method = %method,
                outcome = %"error", code = e.code, error = %error, ms, upstream = %upstream, cache = %trace.cache, calls = trace.calls,
            )
        }
    }
}

/// One access-log line for a request refused before it reached a route
/// (`key` is `-` when no valid key was presented).
pub fn log_refused(via: &str, key: &str, ip: IpAddr, path: &str, status: u16, reason: &str) {
    tracing::info!(target: "delta::access", via = %via, key = %key, ip = %ip, path = %path, outcome = %"refused", status, reason = %reason);
}

pub struct App {
    pub shared: Arc<Shared>,
    pub routes: HashMap<String, Arc<RouteTable>>,
    pub auth: Auth,
    pub subs: Arc<Subscriptions>,
    /// Set to true when the server begins shutting down.
    pub shutdown: tokio::sync::watch::Sender<bool>,
}

/// One element of a request body.
pub struct RpcRequest {
    /// None for a notification: a request without an `id` member, which by
    /// JSON-RPC 2.0 gets no response. `"id": null` is a request.
    pub id: Option<Box<RawValue>>,
    pub method: Result<String, Answer>,
    pub params: Option<Value>,
}

impl RpcRequest {
    fn from_raw(raw: &RawValue) -> RpcRequest {
        /// Keeps `"id": null` apart from a missing id.
        fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Box<RawValue>>, D::Error> {
            Box::<RawValue>::deserialize(d).map(Some)
        }
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default, deserialize_with = "present")]
            id: Option<Box<RawValue>>,
            #[serde(default)]
            method: Option<String>,
            #[serde(default)]
            params: Option<Value>,
        }
        match serde_json::from_str::<Wire>(raw.get()) {
            // Without a method it is not a valid notification either: the
            // error goes back with a null id.
            Ok(Wire { id, method: None, .. }) => RpcRequest {
                id: Some(id.unwrap_or_else(null_id)),
                method: Err(Answer::error(jsonrpc::INVALID_REQUEST, "Invalid request")),
                params: None,
            },
            Ok(Wire { id, method: Some(method), params }) => RpcRequest {
                id,
                method: Ok(method),
                params: params.filter(|p| !p.is_null()),
            },
            Err(_) => RpcRequest {
                id: Some(null_id()),
                method: Err(Answer::error(jsonrpc::INVALID_REQUEST, "Invalid request")),
                params: None,
            },
        }
    }

    /// The response to send, or None for a notification.
    pub fn respond(&self, answer: &Answer) -> Option<Vec<u8>> {
        self.id.as_ref().map(|id| answer.to_response(id))
    }
}

/// A parsed body: one request, a batch, or an error for the whole body.
pub enum Body {
    Single(RpcRequest),
    Batch(Vec<RpcRequest>),
    /// An error for the whole body (parse error, empty or oversized batch).
    Invalid(Answer),
}

impl Body {
    pub fn parse(bytes: &[u8]) -> Body {
        let whole_error = |code, msg: &str| Body::Invalid(Answer::error(code, msg));
        let Ok(raw) = serde_json::from_slice::<&RawValue>(bytes) else {
            return whole_error(jsonrpc::PARSE_ERROR, "Parse error");
        };
        if raw.get().trim_start().starts_with('[') {
            let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(raw.get()) else {
                return whole_error(jsonrpc::PARSE_ERROR, "Parse error");
            };
            if items.is_empty() {
                return whole_error(jsonrpc::INVALID_REQUEST, "Invalid request: empty batch");
            }
            Body::Batch(items.into_iter().map(RpcRequest::from_raw).collect())
        } else {
            Body::Single(RpcRequest::from_raw(raw))
        }
    }

    /// `{"jsonrpc":"2.0","method":"ping"}`: the keep-alive web3.js and
    /// @solana/kit send every 5 s over WebSockets. A notification (no `id`),
    /// so nothing answers it; a `ping` with an `id` is a normal request.
    pub fn is_keepalive(&self) -> bool {
        matches!(self, Body::Single(r) if r.id.is_none() && r.method.as_deref().is_ok_and(|m| m == "ping"))
    }

    /// Requests this body counts against `totalRps` and `rpmPerIp`.
    pub fn count(&self) -> u32 {
        match self {
            Body::Single(_) => 1,
            Body::Batch(items) => items.len() as u32,
            Body::Invalid(_) => 1,
        }
    }
}

impl App {
    /// Answers one normal request on a route (`None` = unknown route) and
    /// writes its access-log line.
    pub async fn call(&self, route: Option<&RouteTable>, req: &RpcRequest, arrived: Instant, who: &Who) -> Answer {
        let (answer, trace) = self.call_traced(route, req, arrived).await;
        let method = req.method.as_deref().unwrap_or("-");
        log_answer(who, method, &answer, &trace, arrived);
        answer
    }

    async fn call_traced(&self, route: Option<&RouteTable>, req: &RpcRequest, arrived: Instant) -> (Answer, Trace) {
        if let Some(refused) = refusal(route, req) {
            return (refused, Trace::default());
        }
        let (Some(route), Ok(method)) = (route, &req.method) else { unreachable!("refusal checked both") };
        let policies = route.accepted(method).expect("refusal checked it");
        execute(&self.shared, policies, Request { method, params: req.params.as_ref(), arrived }).await
    }

    /// Answers a batch's normal requests. Elements the route refuses are
    /// answered here, so a batch never gets around the method allow-list.
    /// The rest are grouped by the upstreams their methods use; each group
    /// goes, as one batch, to one upstream from its list (the route's
    /// `loadBalancing` for the route's list, round-robin otherwise). No
    /// cache, mode or retry. One answer per element (a
    /// notification's is never sent).
    pub async fn call_batch(&self, route: Option<&RouteTable>, reqs: &[&RpcRequest], arrived: Instant, who: &Who) -> Vec<Answer> {
        let mut answers: Vec<Option<Answer>> = Vec::with_capacity(reqs.len());
        // (upstream list, element positions, items), in order of first use.
        let mut groups: Vec<(&[delta_engine::UpstreamId], Vec<usize>, Vec<BatchItem>)> = Vec::new();
        for (i, req) in reqs.iter().enumerate() {
            match (refusal(route, req), &req.method, route) {
                (None, Ok(method), Some(route)) => {
                    let ups = route.accepted(method).expect("refusal checked it").selector.candidates();
                    let item = BatchItem { method, params: req.params.as_ref(), notification: req.id.is_none() };
                    match groups.iter_mut().find(|g| g.0 == ups) {
                        Some(g) => {
                            g.1.push(i);
                            g.2.push(item);
                        }
                        None => groups.push((ups, vec![i], vec![item])),
                    }
                    answers.push(None);
                }
                (refused, method, _) => {
                    let refused = refused.unwrap_or_else(|| Answer::error(jsonrpc::INVALID_REQUEST, "Invalid request"));
                    log_answer(who, method.as_deref().unwrap_or("-"), &refused, &Trace::default(), arrived);
                    answers.push(Some(refused));
                }
            }
        }
        if let Some(route) = route {
            let sent = groups.iter().map(|(ups, _, items)| forward_batch(&self.shared, route.batch_selector(ups), items, arrived));
            let results = futures::future::join_all(sent).await;
            for ((_, positions, items), (got, trace)) in groups.iter().zip(results) {
                for ((&i, item), answer) in positions.iter().zip(items).zip(got) {
                    // A notification has no answer; this placeholder is never sent.
                    let answer = answer.unwrap_or_else(|| Answer::Result(bytes::Bytes::from_static(b"null")));
                    log_answer(who, item.method, &answer, &trace, arrived);
                    answers[i] = Some(answer);
                }
            }
        }
        answers.into_iter().map(|a| a.expect("every element answered")).collect()
    }

    /// Answers a whole body over HTTP: batch elements run in parallel and
    /// come back as an array in request order. Notifications are executed
    /// but get no entry; None when nothing is left to send.
    pub async fn handle_body(&self, route: Option<&RouteTable>, body: Body, arrived: Instant, who: &Who) -> Option<Vec<u8>> {
        match body {
            Body::Invalid(answer) => {
                log_answer(who, "-", &answer, &Trace::default(), arrived);
                Some(answer.to_response(&null_id()))
            }
            Body::Single(req) => req.respond(&self.call(route, &req, arrived, who).await),
            Body::Batch(reqs) => {
                let refs: Vec<&RpcRequest> = reqs.iter().collect();
                let answers = self.call_batch(route, &refs, arrived, who).await;
                batch_response(reqs.iter().zip(answers.iter()).filter_map(|(r, a)| r.respond(a)))
            }
        }
    }
}

/// Why a normal request is answered without an upstream call, if it is.
fn refusal(route: Option<&RouteTable>, req: &RpcRequest) -> Option<Answer> {
    let Some(route) = route else {
        return Some(Answer::error(jsonrpc::UNKNOWN_ROUTE, "unknown route"));
    };
    let method = match &req.method {
        Ok(m) => m.as_str(),
        Err(e) => return Some(e.clone()),
    };
    if route.accepted(method).is_none() {
        return Some(Answer::error(jsonrpc::METHOD_NOT_FOUND, "Method not found"));
    }
    if solana::is_subscription_method(method) {
        return Some(Answer::error(jsonrpc::METHOD_NOT_FOUND, "Method not found: subscriptions need a WebSocket"));
    }
    None
}

/// A batch's responses as an array; None when every element was a
/// notification.
pub fn batch_response(items: impl Iterator<Item = Vec<u8>>) -> Option<Vec<u8>> {
    let items: Vec<Vec<u8>> = items.collect();
    (!items.is_empty()).then(|| join_array(items.into_iter()))
}

pub fn join_array(items: impl Iterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut out = vec![b'['];
    for (i, item) in items.enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(&item);
    }
    out.push(b']');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_are_requests_without_an_id_member() {
        let one = |s: &str| match Body::parse(s.as_bytes()) {
            Body::Single(r) => r,
            _ => panic!("{s}"),
        };
        let n = one(r#"{"jsonrpc":"2.0","method":"getSlot"}"#);
        assert!(n.id.is_none() && n.method.is_ok());
        assert!(n.respond(&Answer::Result(bytes::Bytes::from_static(b"1"))).is_none());
        let r = one(r#"{"jsonrpc":"2.0","id":null,"method":"getSlot"}"#);
        assert_eq!(r.id.as_deref().map(RawValue::get), Some("null"), "an explicit null id is a request");
        let bad = one(r#"{"jsonrpc":"2.0"}"#);
        assert_eq!(bad.id.as_deref().map(RawValue::get), Some("null"), "no method: an error with a null id");
        assert!(bad.method.is_err());
        assert_eq!(batch_response([b"1".to_vec(), b"2".to_vec()].into_iter()).as_deref(), Some(&b"[1,2]"[..]));
        assert_eq!(batch_response(std::iter::empty()), None);
    }

    #[test]
    fn parse_bodies() {
        assert!(matches!(Body::parse(b"{"), Body::Invalid(_)));
        match Body::parse(b"[]") {
            Body::Invalid(Answer::Error(e)) => assert_eq!(e.code, -32600),
            _ => panic!(),
        }
        match Body::parse(br#"[{"id":7,"method":"getSlot"}, 5]"#) {
            Body::Batch(items) => {
                assert_eq!(items[0].id.as_deref().unwrap().get(), "7");
                assert!(items[0].method.is_ok());
                assert!(items[1].method.is_err());
            }
            _ => panic!(),
        }
    }
}
