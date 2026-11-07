//! HTTP front end on hyper. Routing is one lookup on the path segment after
//! `/rpc/`; the only other path is `/health`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use delta_engine::jsonrpc::{self, null_id, Answer};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{
    HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, CONTENT_TYPE, ORIGIN,
};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;

use crate::app::{log_refused, App, Body, Who};
use crate::auth::{api_key_from_query, Caller};

type Resp = Response<Full<Bytes>>;

/// Serves every listener until `shutdown` resolves, then waits for open
/// connections.
pub async fn serve(app: Arc<App>, listeners: Vec<TcpListener>, shutdown: impl std::future::Future<Output = ()>) -> std::io::Result<()> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    // A client gets this long to send its request headers.
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(app.shared.limits.client_read_timeout);
    let graceful = GracefulShutdown::new();
    let (accepted, mut incoming) = tokio::sync::mpsc::channel(64);
    let acceptors: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            let accepted = accepted.clone();
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok(x) => {
                            if accepted.send(x).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => tracing::warn!("accept failed: {e}"),
                    }
                }
            })
        })
        .collect();
    drop(accepted);
    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            Some(x) = incoming.recv() => x,
            _ = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let app = app.clone();
        let service = hyper::service::service_fn(move |req| {
            let app = app.clone();
            async move { Ok::<_, Infallible>(with_cors(handle(app, req, peer).await)) }
        });
        let conn = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("connection error: {e}");
            }
        });
    }
    tracing::info!("shutting down");
    for a in &acceptors {
        a.abort();
    }
    // Client WebSockets are told and closed; open requests get a grace period.
    let _ = app.shutdown.send(true);
    let grace = app.shared.limits.shutdown_grace;
    if tokio::time::timeout(grace, graceful.shutdown()).await.is_err() {
        tracing::warn!("connections still open after {} s; exiting anyway", grace.as_secs());
    }
    Ok(())
}

/// CORS: browsers may call delta from any origin. Access is decided by the
/// API key and its `allowedDomains`, checked on the real request; a CORS
/// preflight carries no credentials, so it is answered without a key.
fn with_cors(mut resp: Resp) -> Resp {
    resp.headers_mut().insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    resp
}

fn preflight(req: &Request<Incoming>) -> Resp {
    let mut r = status(StatusCode::NO_CONTENT);
    let h = r.headers_mut();
    h.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, OPTIONS"));
    // Whatever headers the client asks for (web3.js sends `solana-client`).
    let asked = req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS).cloned();
    h.insert(ACCESS_CONTROL_ALLOW_HEADERS, asked.unwrap_or_else(|| HeaderValue::from_static("content-type")));
    h.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    r
}

pub async fn handle(app: Arc<App>, mut req: Request<Incoming>, peer: SocketAddr) -> Resp {
    if req.method() == Method::OPTIONS {
        tracing::debug!(target: "delta::access", via = "http", ip = %peer.ip(), path = %req.uri().path(), outcome = %"preflight");
        return preflight(&req);
    }
    let arrived = Instant::now();
    let path = req.uri().path().to_owned();
    let api_key = api_key_from_query(req.uri().query());
    let header = |name: &str| req.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_owned);
    let forwarded = header("x-forwarded-for");
    let origin = header(ORIGIN.as_str());
    let caller = Caller { api_key: api_key.as_deref(), ip: app.auth.client_ip(forwarded.as_deref(), peer.ip()), origin: origin.as_deref() };

    let ip = caller.ip;
    // The path only: the query string holds the API key.
    let refused = |key: &str, status: u16, reason: &str| log_refused("http", key, ip, &path, status, reason);

    // The API key is checked first, then the route.
    let key = match app.auth.authenticate(&caller) {
        Ok(k) => k,
        Err(_) => {
            refused("-", 401, "unauthorized");
            return unauthorized();
        }
    };
    let key_name = app.auth.name(key).to_owned();
    if path == "/health" {
        tracing::debug!(target: "delta::access", via = "http", key = %key_name, ip = %ip, path = "/health", outcome = %"ok");
        return Response::new(Full::new(Bytes::from_static(b"ok")));
    }
    let Some(route_name) = path.strip_prefix("/rpc/").map(|r| r.trim_end_matches('/')) else {
        refused(&key_name, 404, "no such path");
        return status(StatusCode::NOT_FOUND);
    };
    if route_name.is_empty() || route_name.contains('/') {
        refused(&key_name, 404, "no such path");
        return status(StatusCode::NOT_FOUND);
    }
    if !app.auth.route_allowed(key, route_name) {
        refused(&key_name, 401, "route not allowed for this key");
        return unauthorized();
    }
    let route = app.routes.get(route_name).cloned();
    let who = Who { key: key_name.clone(), ip, via: "http".into(), route: route_name.to_owned() };

    if hyper_tungstenite::is_upgrade_request(&req) {
        return match hyper_tungstenite::upgrade(&mut req, None) {
            Ok((resp, ws)) => {
                tokio::spawn(crate::ws::serve(app, ws, route, key, who));
                resp
            }
            Err(_) => {
                refused(&key_name, 400, "bad websocket upgrade");
                status(StatusCode::BAD_REQUEST)
            }
        };
    }
    if req.method() != Method::POST {
        refused(&key_name, 405, "not a POST");
        return status(StatusCode::METHOD_NOT_ALLOWED);
    }

    let limit = app.shared.limits.max_body_bytes;
    let read = tokio::time::timeout(app.shared.limits.client_read_timeout, Limited::new(req.into_body(), limit).collect());
    let bytes = match read.await {
        Ok(Ok(b)) => b.to_bytes(),
        Ok(Err(_)) => {
            refused(&key_name, 413, "body too large");
            return status(StatusCode::PAYLOAD_TOO_LARGE);
        }
        Err(_) => {
            refused(&key_name, 408, "request body not received in time");
            return status(StatusCode::REQUEST_TIMEOUT);
        }
    };
    let body = Body::parse(&bytes);
    if !app.auth.take(key, body.count()) {
        refused(&key_name, 429, "rate limited");
        return json(StatusCode::TOO_MANY_REQUESTS, Answer::error(jsonrpc::RATE_LIMITED, "rate limited").to_response(&null_id()));
    }
    // Only notifications: HTTP 200 with an empty body, as a Solana node answers.
    json(StatusCode::OK, app.handle_body(route.as_deref(), body, arrived, &who).await.unwrap_or_default())
}

fn json(code: StatusCode, body: Vec<u8>) -> Resp {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = code;
    r.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r
}

fn unauthorized() -> Resp {
    json(StatusCode::UNAUTHORIZED, Answer::error(jsonrpc::UNAUTHORIZED, "unauthorized").to_response(&null_id()))
}

fn status(code: StatusCode) -> Resp {
    let mut r = Response::new(Full::new(Bytes::new()));
    *r.status_mut() = code;
    r
}
