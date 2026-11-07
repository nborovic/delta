//! Upstream descriptors and the HTTP transport.

use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;

pub type UpstreamId = usize;

#[derive(Clone, Debug, Default)]
pub struct UpstreamInfo {
    pub id: UpstreamId,
    pub name: String,
    /// May contain secrets; never logged.
    pub http: String,
    /// May contain secrets; never logged.
    pub ws: String,
    /// Subscriptions per WebSocket connection before another is opened.
    pub max_subscriptions_per_connection: Option<usize>,
    /// WebSocket connections that may be opened to this upstream.
    pub max_connections: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct HttpReply {
    pub status: u16,
    pub body: Bytes,
}

#[derive(Clone, Debug)]
pub enum TransportError {
    Timeout,
    Connect(String),
    Other(String),
}

/// Sends one JSON-RPC body to an upstream. A trait so the engine can be
/// tested without a network.
pub trait Transport: Send + Sync + 'static {
    fn post(&self, url: &str, body: Bytes, timeout: Duration) -> BoxFuture<'static, Result<HttpReply, TransportError>>;
}

/// reqwest client: pooled per host, HTTP/2 when the upstream offers it, rustls.
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(64)
            .build()
            .expect("reqwest client builds");
        Self { client }
    }
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for HttpTransport {
    fn post(&self, url: &str, body: Bytes, timeout: Duration) -> BoxFuture<'static, Result<HttpReply, TransportError>> {
        let req = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(timeout)
            .body(body);
        async move {
            let resp = req.send().await.map_err(classify)?;
            let status = resp.status().as_u16();
            let body = resp.bytes().await.map_err(classify)?;
            Ok(HttpReply { status, body })
        }
        .boxed()
    }
}

fn classify(e: reqwest::Error) -> TransportError {
    // Strip the URL: upstream URLs carry API keys.
    let e = e.without_url();
    if e.is_timeout() {
        TransportError::Timeout
    } else if e.is_connect() {
        TransportError::Connect(e.to_string())
    } else {
        TransportError::Other(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    const SECRET: &str = "s3cret-api-key";

    fn text(e: &TransportError) -> String {
        match e {
            TransportError::Timeout => String::new(),
            TransportError::Connect(s) | TransportError::Other(s) => s.clone(),
        }
    }

    /// Upstream URLs carry API keys; errors end up in client responses
    /// (`-33095 upstream failed: ...`) and logs, so they must not hold them.
    #[tokio::test]
    async fn transport_errors_never_contain_the_url() {
        let t = HttpTransport::new();
        let body = Bytes::from_static(b"{}");

        // Connection refused.
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/?api-key={SECRET}");
        let e = t.post(&url, body.clone(), Duration::from_secs(2)).await.unwrap_err();
        assert!(matches!(e, TransportError::Connect(_)), "{e:?}");
        assert!(!text(&e).contains(SECRET) && !text(&e).contains("127.0.0.1"), "{}", text(&e));

        // Not HTTP at all.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let _ = s.write_all(b"garbage\r\n\r\n").await;
            }
        });
        let url = format!("http://{addr}/{SECRET}/?api-key={SECRET}");
        let e = t.post(&url, body.clone(), Duration::from_secs(2)).await.unwrap_err();
        assert!(!text(&e).contains(SECRET), "{}", text(&e));

        // Timeout: no text at all.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = listener.accept().await {
                held.push(s);
            }
        });
        let url = format!("http://{addr}/?api-key={SECRET}");
        let e = t.post(&url, body, Duration::from_millis(200)).await.unwrap_err();
        assert!(matches!(e, TransportError::Timeout), "{e:?}");
    }
}
