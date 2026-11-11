//! Client WebSockets. The proxy terminates them and parses every message:
//! normal requests go through the same pipeline as HTTP, subscriptions go to
//! the subscription manager.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use delta_engine::jsonrpc::{self, Answer};
use delta_engine::{solana, Trace};
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use hyper_tungstenite::HyperWebsocket;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

use crate::app::{batch_response, log_answer, log_refused, App, Body, RpcRequest, Who};
use crate::auth::KeyId;
use crate::resolve::RouteTable;
use crate::subs::{ConnHandle, Outgoing};

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

pub async fn serve(app: Arc<App>, ws: HyperWebsocket, route: Option<Arc<RouteTable>>, key: KeyId, who: Who) {
    let stream = match ws.await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("websocket handshake failed: {e}");
            return;
        }
    };
    let (mut sink, mut source) = stream.split();
    let (conn, mut rx, mut close_rx) = ConnHandle::new(NEXT_CONN.fetch_add(1, Ordering::Relaxed));
    let who = Arc::new(Who { via: format!("ws/{}", conn.id), ..who });
    let opened = Instant::now();
    let mut messages: u64 = 0;
    tracing::info!(target: "delta::access", via = %who.via, key = %who.key, ip = %who.ip, route = %who.route, event = "websocket opened");

    let write_timeout = app.shared.limits.client_write_timeout;
    // Last traffic either way, in ms since `opened`: the idle timeout counts
    // from it.
    let activity = Arc::new(AtomicU64::new(0));
    let touch = move |activity: &AtomicU64| activity.store(opened.elapsed().as_millis() as u64, Ordering::Relaxed);
    let written = activity.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                // A close (gap, overflow, shutdown) goes out before anything
                // still queued.
                biased;
                changed = close_rx.changed() => {
                    let reason = if changed.is_ok() { close_rx.borrow().clone() } else { None };
                    let frame = reason.map(|r| CloseFrame { code: CloseCode::Again, reason: r.into() });
                    let _ = sink.send(Message::Close(frame)).await;
                    break;
                }
                out = rx.recv() => {
                    // Held until written: frees the message's in-flight slot.
                    let mut _permit = None;
                    let text = match out {
                        None => {
                            // The connection is over; say why if there is a reason.
                            let reason = close_rx.borrow().clone();
                            let frame = reason.map(|r| CloseFrame { code: CloseCode::Again, reason: r.into() });
                            let _ = sink.send(Message::Close(frame)).await;
                            break;
                        }
                        Some(Outgoing::Text(t)) => t,
                        Some(Outgoing::Response(t, permit)) => {
                            _permit = Some(permit);
                            t
                        }
                        Some(Outgoing::Latest(slot)) => match slot.take() {
                            Some(t) => t,
                            None => continue,
                        },
                        Some(Outgoing::Event(counter, t)) => {
                            counter.fetch_sub(1, Ordering::SeqCst);
                            t
                        }
                    };
                    // A client that stops reading is closed, not buffered for.
                    match tokio::time::timeout(write_timeout, sink.send(Message::Text(text.into()))).await {
                        Ok(Ok(())) => touch(&written),
                        _ => break,
                    }
                }
            }
        }
    });

    let in_flight = Arc::new(tokio::sync::Semaphore::new(app.shared.limits.ws_in_flight));
    let mut shutdown = app.shutdown.subscribe();
    loop {
        // At the in-flight limit, stop reading from this client until one of
        // its responses has been written.
        let permit = tokio::select! {
            p = in_flight.clone().acquire_owned() => p.expect("never closed"),
            _ = shutdown.wait_for(|down| *down) => {
                conn.close("server shutting down");
                break;
            }
        };
        let idle = app.shared.limits.ws_idle_timeout;
        let msg = tokio::select! {
            m = source.next() => m,
            _ = shutdown.wait_for(|down| *down) => {
                conn.close("server shutting down");
                break;
            }
            _ = idle_for(&activity, opened, idle) => {
                conn.close(&format!(
                    "idle for {} s: send a message or a ping to keep the connection open",
                    idle.as_secs()
                ));
                break;
            }
        };
        let Some(msg) = msg else { break };
        // Any frame keeps the connection alive, protocol pings and pongs too.
        touch(&activity);
        let text = match msg {
            Ok(Message::Text(t)) => t.as_str().to_owned(),
            Ok(Message::Binary(b)) => String::from_utf8_lossy(&b).into_owned(),
            Ok(Message::Close(_)) | Err(_) => break,
            // Pings are answered by the WebSocket layer.
            Ok(_) => continue,
        };
        let body = Body::parse(text.as_bytes());
        // A client's keep-alive is the proxy's business: no upstream call, no
        // rate-limit charge, no log line, and no reply (a notification).
        if body.is_keepalive() {
            continue;
        }
        let arrived = Instant::now();
        messages += 1;
        if !app.auth.take(key, who.ip, body.count()) {
            log_refused(&who.via, &who.key, who.ip, &format!("/rpc/{}", who.route), 429, "rate limited");
            // No HTTP status over an open WebSocket: the message's response
            // carries the error.
            if let Some(text) = rate_limited(&body) {
                conn.respond(text, permit);
            }
            continue;
        }
        let (app, conn, route, who) = (app.clone(), conn.clone(), route.clone(), who.clone());
        tokio::spawn(async move {
            let mut subscribed = Vec::new();
            let response = match body {
                Body::Invalid(answer) => {
                    log_answer(&who, "-", &answer, &Trace::default(), arrived);
                    Some(answer.to_response(&jsonrpc::null_id()))
                }
                Body::Single(req) => {
                    let (out, id) = respond(&app, &conn, route.as_deref(), &req, arrived, &who, key).await;
                    subscribed.extend(id);
                    out
                }
                Body::Batch(reqs) => {
                    // Subscriptions are delta's own; everything else is
                    // forwarded as one upstream batch.
                    let is_sub = |r: &RpcRequest| route.is_some() && r.method.as_deref().is_ok_and(solana::is_subscription_method);
                    let plain: Vec<&RpcRequest> = reqs.iter().filter(|r| !is_sub(r)).collect();
                    let subs = reqs.iter().filter(|r| is_sub(r)).map(|r| respond(&app, &conn, route.as_deref(), r, arrived, &who, key));
                    let (subs, plain) = futures::join!(join_all(subs), app.call_batch(route.as_deref(), &plain, arrived, &who));
                    let (mut subs, mut plain) = (subs.into_iter(), plain.into_iter());
                    let mut out = Vec::with_capacity(reqs.len());
                    for r in &reqs {
                        if is_sub(r) {
                            let (part, id) = subs.next().expect("one per subscription");
                            subscribed.extend(id);
                            out.extend(part);
                        } else {
                            out.extend(r.respond(&plain.next().expect("one per request")));
                        }
                    }
                    batch_response(out.into_iter())
                }
            };
            // Notifications get nothing back, not even a subscribe's id.
            if let Some(response) = response {
                conn.respond(String::from_utf8(response).unwrap_or_default(), permit);
            }
            // Only now may the new subscriptions' notifications follow.
            app.subs.ack(&subscribed);
        });
        if writer.is_finished() {
            break;
        }
    }

    app.subs.detach_conn(conn.id);
    drop(conn);
    let _ = writer.await;
    tracing::info!(
        target: "delta::access",
        via = %who.via, key = %who.key, ip = %who.ip, route = %who.route, event = "websocket closed",
        messages, secs = opened.elapsed().as_secs(),
    );
}

async fn respond(
    app: &App,
    conn: &Arc<ConnHandle>,
    route: Option<&RouteTable>,
    req: &RpcRequest,
    arrived: Instant,
    who: &Who,
    key: KeyId,
) -> (Option<Vec<u8>>, Option<u64>) {
    let mut subscribed = None;
    let answer = match (route, &req.method) {
        (Some(route), Ok(method)) if solana::is_subscription_method(method) => {
            let answer = match route.accepted(method) {
                None => Answer::error(jsonrpc::METHOD_NOT_FOUND, "Method not found"),
                Some(_) if solana::is_unsubscribe(method) => app.subs.unsubscribe(conn, method, req.params.as_ref()),
                Some(plan) => {
                    let answer = app.subs.subscribe(conn, plan, method, req.params.clone(), key, app.auth.max_subscriptions(key)).await;
                    if let Answer::Result(id) = &answer {
                        subscribed = std::str::from_utf8(id).ok().and_then(|s| s.parse().ok());
                    }
                    answer
                }
            };
            // A subscription answer is the proxy's own id, not an upstream's.
            log_answer(who, method, &answer, &Trace::default(), arrived);
            answer
        }
        _ => app.call(route, req, arrived, who).await,
    };
    (req.respond(&answer), subscribed)
}

fn rate_limited(body: &Body) -> Option<String> {
    let err = Answer::error(jsonrpc::RATE_LIMITED, "rate limited");
    let out = match body {
        Body::Single(r) => r.respond(&err),
        Body::Batch(rs) => batch_response(rs.iter().filter_map(|r| r.respond(&err))),
        Body::Invalid(_) => Some(err.to_response(&jsonrpc::null_id())),
    };
    out.map(|o| String::from_utf8(o).unwrap_or_default())
}

/// Resolves once the connection has had no traffic for `idle`.
async fn idle_for(activity: &AtomicU64, opened: Instant, idle: Duration) {
    loop {
        let last = opened + Duration::from_millis(activity.load(Ordering::Relaxed));
        let deadline = last + idle;
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep_until(deadline.into()).await;
    }
}
