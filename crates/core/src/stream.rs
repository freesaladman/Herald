//! Loopback WebSocket stream of bus events for local tools (CLI, scripts).
//!
//! Access checks: loopback peer, bearer token, and no `Origin` header, so a
//! web page in the user's browser can't reach it.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{Response as HttpResponse, StatusCode};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::bus::Bus;
use crate::config::{MatchTable, Paths};
use crate::event::{ConnectionRef, Event};
use crate::hooks::CompiledHookFilter;

pub const MAX_CLIENTS: usize = 16;
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const PING_EVERY: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
/// Sockets still handshaking; separate from `MAX_CLIENTS` so tokenless sockets can't
/// use up the authenticated client slots.
const MAX_PENDING: usize = 32;
/// Consecutive pings without a pong before a client is dropped.
const MAX_UNANSWERED_PINGS: u32 = 2;
const STREAM_FILE: &str = "stream.json";

/// Receives the connection id of every `credentials-changed` request. A channel rather
/// than an engine handle, so the server and the engine never reference each other.
pub type StreamControl = mpsc::UnboundedSender<String>;

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("could not bind the stream port: {0}")]
    Bind(std::io::Error),
    #[error("could not write {path}: {source}")]
    File {
        path: PathBuf,
        source: std::io::Error,
    },
}

pub struct StreamServer {
    port: u16,
    token: String,
    file: PathBuf,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

struct Shared {
    bus: Arc<Bus>,
    token: String,
    control: StreamControl,
    client_slots: Arc<Semaphore>,
}

impl StreamServer {
    pub async fn start(
        bus: Arc<Bus>,
        paths: Paths,
        port: u16,
        control: StreamControl,
    ) -> Result<StreamServer, StreamError> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .await
            .map_err(StreamError::Bind)?;
        let port = listener.local_addr().map_err(StreamError::Bind)?.port();
        let token = new_token();
        let file = paths.runtime.join(STREAM_FILE);
        write_stream_file(&file, port, &token).map_err(|source| StreamError::File {
            path: file.clone(),
            source,
        })?;
        let shared = Arc::new(Shared {
            bus,
            token: token.clone(),
            control,
            client_slots: Arc::new(Semaphore::new(MAX_CLIENTS)),
        });
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(accept_loop(listener, shared, stop_rx));
        Ok(StreamServer {
            port,
            token,
            file,
            stop,
            task: Some(task),
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    /// Stops accepting, closes every client and deletes `stream.json`.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take()
            && let Err(e) = task.await
        {
            tracing::error!(error = %e, "stream accept task failed");
        }
    }
}

impl Drop for StreamServer {
    fn drop(&mut self) {
        // Also covers a drop without shutdown(): aborting the accept task drops its
        // JoinSet, which aborts the clients.
        if let Some(task) = self.task.take() {
            task.abort();
        }
        remove_own_file(&self.file, &self.token);
    }
}

/// A newer instance may have rewritten the file; only delete it if it is still ours.
fn remove_own_file(path: &Path, token: &str) {
    let ours = std::fs::read_to_string(path)
        .ok()
        .and_then(|body| serde_json::from_str::<Value>(&body).ok())
        .is_some_and(|v| v["token"].as_str() == Some(token));
    if ours {
        let _ = std::fs::remove_file(path);
    }
}

fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn write_stream_file(path: &Path, port: u16, token: &str) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    let body = json!({ "port": port, "token": token, "pid": std::process::id() }).to_string();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(body.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let mut clients = JoinSet::new();
    let pending_slots = Arc::new(Semaphore::new(MAX_PENDING));
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    while let Some(done) = clients.try_join_next() {
                        log_client_end(done);
                    }
                    let Ok(pending) = pending_slots.clone().try_acquire_owned() else {
                        tracing::warn!(%peer, "too many pending stream handshakes; refusing");
                        continue;
                    };
                    clients.spawn(serve_client(socket, peer, shared.clone(), stop.clone(), pending));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "stream accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            Some(done) = clients.join_next(), if !clients.is_empty() => log_client_end(done),
        }
    }
    drop(listener);
    // Clients watch the same stop signal and close themselves; abort stragglers.
    let drained = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(done) = clients.join_next().await {
            log_client_end(done);
        }
    })
    .await;
    if drained.is_err() {
        clients.abort_all();
        while clients.join_next().await.is_some() {}
    }
}

fn log_client_end(done: Result<(), tokio::task::JoinError>) {
    if let Err(e) = done
        && e.is_panic()
    {
        tracing::error!(error = %e, "stream client panicked");
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

type Rejection = (StatusCode, &'static str);

fn reject((status, why): Rejection) -> ErrorResponse {
    let mut response = HttpResponse::new(Some(why.to_string()));
    *response.status_mut() = status;
    response
}

fn check_request(req: &Request, peer: SocketAddr, token: &str) -> Result<(), Rejection> {
    if !peer.ip().is_loopback() {
        return Err((StatusCode::FORBIDDEN, "loopback only"));
    }
    if req.headers().contains_key("origin") {
        return Err((StatusCode::FORBIDDEN, "browser origins are not allowed"));
    }
    if req.uri().path() != "/events" {
        return Err((StatusCode::NOT_FOUND, "not found"));
    }
    let presented = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(t) if constant_time_eq(t.as_bytes(), token.as_bytes()) => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, "missing or invalid token")),
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum Inbound {
    Subscribe {
        #[serde(default)]
        types: Vec<String>,
        #[serde(default, rename = "match")]
        match_table: MatchTable,
        #[serde(default = "yes", rename = "includeCatchUp")]
        include_catch_up: bool,
    },
    Emit {
        event: Value,
    },
    CredentialsChanged {
        #[serde(rename = "connectionId")]
        connection_id: String,
    },
}

fn yes() -> bool {
    true
}

struct Subscription {
    filter: Option<CompiledHookFilter>,
    include_catch_up: bool,
}

impl Subscription {
    fn wants(&self, event: &Event) -> bool {
        if event.catch_up && !self.include_catch_up {
            return false;
        }
        match &self.filter {
            None => true,
            Some(filter) => {
                serde_json::to_value(event).is_ok_and(|v| filter.matches(&event.event_type, &v))
            }
        }
    }
}

fn text(v: &Value) -> Message {
    Message::text(v.to_string())
}

#[allow(clippy::result_large_err)] // the handshake callback's error type is fixed by tungstenite
async fn serve_client(
    socket: TcpStream,
    peer: SocketAddr,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
    pending: OwnedSemaphorePermit,
) {
    // Subscribe before the handshake completes so nothing published after the client
    // sees a successful connect is missed.
    let mut events = shared.bus.subscribe();
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));
    let token = shared.token.clone();
    let slots = shared.client_slots.clone();
    let slot: Arc<Mutex<Option<OwnedSemaphorePermit>>> = Arc::default();
    let slot_in = slot.clone();
    // The client slot is taken after the token check, so only authenticated sockets count.
    let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
        check_request(req, peer, &token).map_err(reject)?;
        let permit = slots
            .try_acquire_owned()
            .map_err(|_| reject((StatusCode::SERVICE_UNAVAILABLE, "too many stream clients")))?;
        if let Ok(mut held) = slot_in.lock() {
            *held = Some(permit);
        }
        Ok(resp)
    };
    let handshake = tokio_tungstenite::accept_hdr_async_with_config(socket, callback, Some(config));
    let mut ws = match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            tracing::debug!(%peer, error = %e, "stream handshake rejected");
            return;
        }
        Err(_) => {
            tracing::debug!(%peer, "stream handshake timed out");
            return;
        }
    };
    drop(pending);
    let _client_slot = slot.lock().ok().and_then(|mut held| held.take());

    let mut sub = Subscription {
        filter: None,
        include_catch_up: true,
    };
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    let mut unanswered_pings = 0u32;
    loop {
        let outgoing: Option<Message> = tokio::select! {
            _ = stop.changed() => {
                let _ = ws.close(None).await;
                return;
            }
            _ = ping.tick() => {
                if unanswered_pings >= MAX_UNANSWERED_PINGS {
                    tracing::debug!(%peer, "stream client missed pongs; dropping");
                    return;
                }
                unanswered_pings += 1;
                Some(Message::Ping(Vec::new().into()))
            }
            received = events.recv() => match received {
                Ok(event) => sub
                    .wants(&event)
                    .then(|| text(&json!({ "type": "event", "event": &*event }))),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    Some(text(&error_reply(format!("lagged {n} events"))))
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            incoming = ws.next() => match incoming {
                Some(Ok(Message::Text(body))) => {
                    Some(text(&handle_text(body.as_str(), &shared, &mut sub)))
                }
                Some(Ok(Message::Binary(_))) => {
                    Some(text(&error_reply("binary messages are not supported")))
                }
                Some(Ok(Message::Pong(_))) => {
                    unanswered_pings = 0;
                    None
                }
                Some(Ok(Message::Close(_))) | None => return,
                Some(Ok(_)) => None,
                Some(Err(e)) => {
                    tracing::debug!(%peer, error = %e, "stream client read failed");
                    return;
                }
            },
        };
        if let Some(message) = outgoing
            && ws.send(message).await.is_err()
        {
            return;
        }
    }
}

fn error_reply(message: impl Into<String>) -> Value {
    json!({ "type": "error", "message": message.into() })
}

fn handle_text(body: &str, shared: &Shared, sub: &mut Subscription) -> Value {
    let message: Inbound = match serde_json::from_str(body) {
        Ok(m) => m,
        Err(e) => return error_reply(format!("invalid message: {e}")),
    };
    match message {
        Inbound::Subscribe {
            types,
            match_table,
            include_catch_up,
        } => {
            let types = if types.is_empty() {
                vec!["*".to_string()]
            } else {
                types
            };
            sub.filter = Some(CompiledHookFilter::new(&types, &match_table));
            sub.include_catch_up = include_catch_up;
            json!({ "type": "ok" })
        }
        Inbound::Emit { event } => match build_test_event(event) {
            Err(message) => error_reply(message),
            Ok(event) => {
                let id = event.id.clone();
                match shared.bus.publish(event) {
                    Some(_) => json!({ "type": "ok", "id": id }),
                    None => error_reply("event was not published"),
                }
            }
        },
        Inbound::CredentialsChanged { connection_id } => {
            if shared.control.send(connection_id).is_err() {
                return error_reply("engine is not accepting credential changes");
            }
            json!({ "type": "ok" })
        }
    }
}

/// Recursive for objects so a partial `actor` or `connection` keeps the other defaults.
fn merge(base: &mut Value, over: Value) {
    match (base, over) {
        (Value::Object(base), Value::Object(over)) => {
            for (key, value) in over {
                match base.get_mut(&key) {
                    Some(slot) if slot.is_object() && value.is_object() => merge(slot, value),
                    _ => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, over) => *base = over,
    }
}

fn build_test_event(partial: Value) -> Result<Event, String> {
    let event_type = partial
        .get("type")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or("event.type is required")?
        .to_string();
    let now = Utc::now();
    let base = Event::new(
        ConnectionRef {
            id: "test".into(),
            provider: "test".into(),
        },
        &event_type,
        now,
        now,
        "Test event",
        "test",
    );
    let mut merged = serde_json::to_value(base).map_err(|e| e.to_string())?;
    merge(&mut merged, partial);
    let mut event: Event =
        serde_json::from_value(merged).map_err(|e| format!("invalid event: {e}"))?;
    event.id = format!("test:{}", uuid::Uuid::new_v4());
    event.test = true;
    event.detected_utc = now;
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::Classifier;
    use crate::config::Config;
    use crate::local_repos::LocalIndex;
    use crate::store::Store;
    use std::collections::BTreeMap;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::Request as HttpRequest;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    struct Harness {
        bus: Arc<Bus>,
        server: StreamServer,
        paths: Paths,
        control: mpsc::UnboundedReceiver<String>,
        _dir: tempfile::TempDir,
    }

    async fn harness() -> Harness {
        let config = Config::from_toml_str(Config::default_toml()).unwrap();
        let bus = Arc::new(Bus::new(
            Arc::new(Store::open_in_memory().unwrap()),
            Classifier::new(&config, &BTreeMap::new()),
            LocalIndex::empty(),
        ));
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            runtime: dir.path().join("runtime"),
        };
        let (tx, control) = mpsc::unbounded_channel();
        let server = StreamServer::start(bus.clone(), paths.clone(), 0, tx)
            .await
            .unwrap();
        Harness {
            bus,
            server,
            paths,
            control,
            _dir: dir,
        }
    }

    fn request(port: u16, path_and_query: &str) -> HttpRequest<()> {
        format!("ws://127.0.0.1:{port}{path_and_query}")
            .into_client_request()
            .unwrap()
    }

    fn bearer(req: &mut HttpRequest<()>, token: &str) {
        req.headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }

    async fn connect(h: &Harness) -> Client {
        let mut req = request(h.server.port(), "/events");
        bearer(&mut req, h.server.token());
        tokio_tungstenite::connect_async(req).await.unwrap().0
    }

    async fn status_of(req: HttpRequest<()>) -> u16 {
        match tokio_tungstenite::connect_async(req).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
            other => panic!("expected an HTTP rejection, got {:?}", other.map(|_| ())),
        }
    }

    async fn send(c: &mut Client, v: Value) {
        c.send(text(&v)).await.unwrap();
    }

    async fn recv(c: &mut Client) -> Value {
        loop {
            let m = tokio::time::timeout(Duration::from_secs(5), c.next())
                .await
                .expect("timed out waiting for a message")
                .expect("stream ended")
                .unwrap();
            if let Message::Text(t) = m {
                return serde_json::from_str(t.as_str()).unwrap();
            }
        }
    }

    async fn subscribe(c: &mut Client, v: Value) {
        send(c, v).await;
        assert_eq!(recv(c).await["type"], "ok");
    }

    fn event(id: &str, event_type: &str) -> Event {
        let now = Utc::now();
        Event::new(
            ConnectionRef {
                id: "c1".into(),
                provider: "p".into(),
            },
            event_type,
            now,
            now,
            "t",
            "k",
        )
        .with_id(id)
    }

    #[tokio::test]
    async fn stream_file_has_port_and_token_and_is_removed_on_shutdown() {
        let h = harness().await;
        let file = h.paths.runtime.join("stream.json");
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(v["port"], h.server.port());
        assert_eq!(v["token"], h.server.token());
        assert_eq!(v["token"].as_str().unwrap().len(), 64);
        assert_eq!(v["pid"], std::process::id());
        h.server.shutdown().await;
        assert!(!file.exists());
    }

    #[tokio::test]
    async fn rejects_missing_token_wrong_token_origin_and_other_paths() {
        let h = harness().await;
        let port = h.server.port();
        assert_eq!(status_of(request(port, "/events")).await, 401);

        let mut wrong = request(port, "/events");
        bearer(&mut wrong, "nope");
        assert_eq!(status_of(wrong).await, 401);

        let mut origin = request(port, "/events");
        bearer(&mut origin, h.server.token());
        origin
            .headers_mut()
            .insert("origin", "http://evil.example".parse().unwrap());
        assert_eq!(status_of(origin).await, 403);

        let mut other = request(port, "/other");
        bearer(&mut other, h.server.token());
        assert_eq!(status_of(other).await, 404);

        let by_query = request(port, &format!("/events?token={}", h.server.token()));
        assert_eq!(status_of(by_query).await, 401);
    }

    #[tokio::test]
    async fn client_receives_published_events() {
        let h = harness().await;
        let mut c = connect(&h).await;
        h.bus.publish(event("a", "pr.created")).unwrap();
        let m = recv(&mut c).await;
        assert_eq!(m["type"], "event");
        assert_eq!(m["event"]["id"], "a");
    }

    #[tokio::test]
    async fn subscribe_narrows_by_type_and_match() {
        let h = harness().await;
        let mut c = connect(&h).await;
        subscribe(
            &mut c,
            json!({"type":"subscribe","types":["pr.*"],"match":{"connection.id":"c1"}}),
        )
        .await;
        h.bus.publish(event("branch", "branch.pushed")).unwrap();
        let mut other = event("other-conn", "pr.created");
        other.connection.id = "c2".into();
        h.bus.publish(other).unwrap();
        h.bus.publish(event("hit", "pr.created")).unwrap();
        assert_eq!(recv(&mut c).await["event"]["id"], "hit");
    }

    #[tokio::test]
    async fn include_catch_up_false_hides_catch_up_events() {
        let h = harness().await;
        let mut c = connect(&h).await;
        subscribe(&mut c, json!({"type":"subscribe","includeCatchUp":false})).await;
        let mut old = event("old", "pr.created");
        old.catch_up = true;
        h.bus.publish(old).unwrap();
        h.bus.publish(event("new", "pr.created")).unwrap();
        assert_eq!(recv(&mut c).await["event"]["id"], "new");
    }

    #[tokio::test]
    async fn emit_publishes_a_test_event() {
        let h = harness().await;
        let mut watcher = h.bus.subscribe();
        let mut c = connect(&h).await;
        send(
            &mut c,
            json!({"type":"emit","event":{"type":"pr.created","title":"Hi"}}),
        )
        .await;
        let reply = recv(&mut c).await;
        assert_eq!(reply["type"], "ok");
        let id = reply["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("test:"));
        let seen = watcher.recv().await.unwrap();
        assert_eq!(seen.id, id);
        assert!(seen.test);
        assert_eq!(seen.title, "Hi");
        assert_eq!(seen.connection.id, "test");
        assert_eq!(seen.subject_key, "test");
        assert_eq!(recv(&mut c).await["event"]["id"], id);
    }

    #[tokio::test]
    async fn credentials_changed_invokes_control() {
        let mut h = harness().await;
        let mut c = connect(&h).await;
        send(
            &mut c,
            json!({"type":"credentials-changed","connectionId":"conn-a"}),
        )
        .await;
        assert_eq!(recv(&mut c).await["type"], "ok");
        assert_eq!(h.control.recv().await.unwrap(), "conn-a");
    }

    #[tokio::test]
    async fn client_limit_applies_to_authenticated_clients_and_frees_slots() {
        let h = harness().await;
        let mut clients = Vec::new();
        for _ in 0..MAX_CLIENTS {
            clients.push(connect(&h).await);
        }
        let mut req = request(h.server.port(), "/events");
        bearer(&mut req, h.server.token());
        assert_eq!(status_of(req).await, 503);

        let mut closed = clients.pop().unwrap();
        closed.close(None).await.unwrap();
        while closed.next().await.is_some() {}
        drop(closed);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut req = request(h.server.port(), "/events");
            bearer(&mut req, h.server.token());
            if tokio_tungstenite::connect_async(req).await.is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "slot was never freed"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn tokenless_sockets_do_not_use_client_slots() {
        let h = harness().await;
        for _ in 0..MAX_CLIENTS + 2 {
            let req = request(h.server.port(), "/events");
            assert_eq!(status_of(req).await, 401);
        }
        connect(&h).await;
    }

    #[tokio::test]
    async fn oversize_message_ends_the_stream() {
        let h = harness().await;
        let mut c = connect(&h).await;
        c.send(Message::text("x".repeat(MAX_MESSAGE_BYTES + 1)))
            .await
            .unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match c.next().await {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                }
            }
        })
        .await;
        assert!(
            ended.is_ok(),
            "stream stayed open after an oversize message"
        );
    }

    #[test]
    fn remove_own_file_keeps_a_file_written_by_another_instance() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("stream.json");
        write_stream_file(&file, 1, "other").unwrap();
        remove_own_file(&file, "mine");
        assert!(file.exists());
        remove_own_file(&file, "other");
        assert!(!file.exists());
    }

    #[tokio::test]
    async fn malformed_message_gets_error_and_connection_stays_open() {
        let h = harness().await;
        let mut c = connect(&h).await;
        c.send(Message::text("not json")).await.unwrap();
        assert_eq!(recv(&mut c).await["type"], "error");
        send(&mut c, json!({"type":"emit","event":{}})).await;
        assert_eq!(recv(&mut c).await["type"], "error");
        subscribe(&mut c, json!({"type":"subscribe"})).await;
    }
}
