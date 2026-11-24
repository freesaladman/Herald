//! `herald` command line: tail and emit events over the app's local stream, query the
//! event store, and manage connection secrets.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Local, NaiveDate, TimeDelta, TimeZone, Utc};
use clap::{ArgGroup, Parser, Subcommand};
use futures_util::{SinkExt, Stream, StreamExt};
use herald_core::config::{Config, ConfigError, ConnectionConfig, DB_FILE, Paths};
use herald_core::event::Event;
use herald_core::provider::ProviderRegistry;
use herald_core::secrets::{KeyringSecretStore, SecretStore};
use herald_core::store::{SqliteStore, Store};
use herald_provider_ado::AdoProviderFactory;
use serde_json::{Map, Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const CONFIG_FILE: &str = "config.toml";
const STREAM_FILE: &str = "stream.json";
const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const NOT_RUNNING: &str = "herald app is not running";

type Client = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Parser)]
#[command(name = "herald", version, about = "Herald command line")]
struct Cli {
    /// Debug logging to stderr.
    #[arg(short, long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Stream live events from the running app. Exits 1 when the app stops; rerun to reconnect.
    Tail {
        /// Event type glob; repeatable.
        #[arg(long = "type")]
        types: Vec<String>,
        /// Field filter `path=glob`; repeatable.
        #[arg(long = "match")]
        matches: Vec<String>,
        /// Skip events replayed from a catch-up poll.
        #[arg(long)]
        no_catch_up: bool,
        /// Replay stored events with a seq above this one, then continue live.
        #[arg(long)]
        after: Option<u64>,
        /// Print raw event JSON, one per line.
        #[arg(long)]
        json: bool,
    },
    /// Publish a test event through the running app.
    EmitTest {
        event_type: String,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        pr_id: Option<i64>,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        connection: Option<String>,
        /// Override any field by dotted path: `key=value` (JSON value or plain string).
        #[arg(long = "set")]
        sets: Vec<String>,
    },
    /// List stored events, oldest first.
    #[command(group(ArgGroup::new("from").required(true).multiple(false).args(["since", "after"])))]
    Events {
        /// RFC3339 timestamp, a date (YYYY-MM-DD, UTC midnight) or a duration like 30m, 2h, 7d.
        #[arg(long)]
        since: Option<String>,
        /// List events with a seq above this one.
        #[arg(long)]
        after: Option<u64>,
        #[arg(long, default_value_t = 200)]
        limit: u32,
        #[arg(long)]
        json: bool,
    },
    /// Manage connection credentials.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Config file helpers.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum AuthAction {
    /// Prompt for a secret and store it in the OS keychain.
    Set {
        #[arg(long)]
        connection: Option<String>,
    },
    /// Connect with the stored secret and show who it identifies.
    Test {
        #[arg(long)]
        connection: Option<String>,
    },
    /// Delete the stored secret.
    Clear {
        #[arg(long)]
        connection: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Print the config file path.
    Path,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    // Never raise tungstenite above DEBUG: it logs the Authorization header at TRACE.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(if cli.verbose {
            tracing::Level::DEBUG
        } else {
            tracing::Level::WARN
        })
        .init();
    if let Err(e) = run(cli.command).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<()> {
    let paths = Paths::from_env();
    match command {
        Command::Tail {
            types,
            matches,
            no_catch_up,
            after,
            json,
        } => {
            let opts = TailOptions {
                types,
                matches: build_match(&matches)?,
                include_catch_up: !no_catch_up,
                after,
                json_out: json,
            };
            tail(&paths, opts).await
        }
        Command::EmitTest {
            event_type,
            title,
            pr_id,
            repo,
            branch,
            url,
            connection,
            sets,
        } => {
            let args = EmitArgs {
                event_type,
                title,
                pr_id,
                repo,
                branch,
                url,
                connection,
                sets,
            };
            emit_test(&paths, build_emit_event(&args)?).await
        }
        Command::Events {
            since,
            after,
            limit,
            json,
        } => {
            let from = match (since, after) {
                (Some(since), None) => EventsFrom::Since(parse_since(&since, Utc::now())?),
                (None, Some(seq)) => EventsFrom::After(seq),
                _ => bail!("pass exactly one of --since or --after"),
            };
            events(&paths, from, limit, json).await
        }
        Command::Auth { action } => match action {
            AuthAction::Set { connection } => auth_set(&paths, connection.as_deref()).await,
            AuthAction::Test { connection } => auth_test(&paths, connection.as_deref()).await,
            AuthAction::Clear { connection } => auth_clear(&paths, connection.as_deref()),
        },
        Command::Config { action } => match action {
            ConfigAction::Path => {
                println!("{}", config_path(&paths).display());
                Ok(())
            }
        },
    }
}

// ---- stream client ----

/// Deliberately not Debug: the token must never reach logs.
struct StreamInfo {
    port: u16,
    token: String,
}

fn read_stream_info(paths: &Paths) -> Result<StreamInfo> {
    let file = paths.runtime.join(STREAM_FILE);
    let text = match std::fs::read_to_string(&file) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => bail!(NOT_RUNNING),
        Err(e) => return Err(e).with_context(|| format!("reading {}", file.display())),
    };
    let v: Value = serde_json::from_str(&text).context("stream.json is not valid JSON")?;
    let port = v
        .get("port")
        .and_then(Value::as_u64)
        .and_then(|p| u16::try_from(p).ok())
        .context("stream.json has no valid port")?;
    let token = v
        .get("token")
        .and_then(Value::as_str)
        .context("stream.json has no token")?
        .to_string();
    Ok(StreamInfo { port, token })
}

async fn connect(paths: &Paths) -> Result<Client> {
    let info = read_stream_info(paths)?;
    let mut request = format!("ws://127.0.0.1:{}/events", info.port)
        .into_client_request()
        .context("building the stream request")?;
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", info.token)
            .parse()
            .context("stream token is not a valid header value")?,
    );
    tracing::debug!(port = info.port, "connecting to the stream");
    match tokio_tungstenite::connect_async(request).await {
        Ok((ws, _)) => Ok(ws),
        Err(tokio_tungstenite::tungstenite::Error::Io(_)) => Err(anyhow!(NOT_RUNNING)),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => Err(anyhow!(
            "stream rejected the connection (HTTP {}); is stream.json stale?",
            resp.status()
        )),
        Err(e) => Err(anyhow!("stream connection failed: {e}")),
    }
}

async fn send_json(ws: &mut Client, v: &Value) -> Result<()> {
    ws.send(Message::text(v.to_string()))
        .await
        .context("sending to the stream")
}

/// Next text frame as JSON; `None` when the stream closed.
async fn next_json<S>(ws: &mut S) -> Result<Option<Value>>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = ws.next().await {
        match msg.context("reading from the stream")? {
            Message::Text(t) => {
                return Ok(Some(
                    serde_json::from_str(t.as_str()).context("stream sent invalid JSON")?,
                ));
            }
            Message::Close(_) => return Ok(None),
            _ => {}
        }
    }
    Ok(None)
}

/// Next non-event frame; broadcast events can arrive before the reply to a request.
async fn next_reply<S>(ws: &mut S) -> Result<Value>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let frame = next_json(ws)
            .await?
            .context("app closed the stream without replying")?;
        if frame.get("type").and_then(Value::as_str) != Some("event") {
            return Ok(frame);
        }
    }
}

/// The timeout covers the whole wait, including skipped event frames.
async fn request_reply(ws: &mut Client, v: &Value) -> Result<Value> {
    send_json(ws, v).await?;
    tokio::time::timeout(REPLY_TIMEOUT, next_reply(ws))
        .await
        .context("timed out waiting for the app to reply")?
}

/// Passes `{"type":"ok"}` replies through; turns the app's error replies into errors.
fn expect_ok(reply: Value) -> Result<Value> {
    match reply.get("type").and_then(Value::as_str) {
        Some("ok") => Ok(reply),
        Some("error") => bail!(
            "{}",
            reply
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        ),
        _ => bail!("unexpected reply from the app"),
    }
}

// ---- tail ----

struct TailOptions {
    types: Vec<String>,
    matches: Map<String, Value>,
    include_catch_up: bool,
    after: Option<u64>,
    json_out: bool,
}

fn subscribe_message(opts: &TailOptions) -> Value {
    let mut msg = json!({
        "type": "subscribe",
        "types": opts.types,
        "match": opts.matches,
        "includeCatchUp": opts.include_catch_up,
    });
    if let Some(after) = opts.after {
        msg["afterSeq"] = json!(after);
    }
    msg
}

/// Human line for replay notices. These go to stderr so `--json` stdout stays events only.
fn notice_line(msg: &Value) -> Option<String> {
    let num = |k: &str| msg.get(k).and_then(Value::as_u64).unwrap_or(0);
    match msg.get("type").and_then(Value::as_str)? {
        "overflow" => {
            let through = num("replayedThrough");
            Some(format!(
                "replay overflow: replayed through {through}, latest {}; run `herald events --after {through}` to fill the gap",
                num("latestSeq")
            ))
        }
        "reset" => Some(format!(
            "replay reset: the store changed, latest seq is {}; discard any saved position",
            num("latestSeq")
        )),
        _ => None,
    }
}

async fn tail(paths: &Paths, opts: TailOptions) -> Result<()> {
    let json_out = opts.json_out;
    let mut ws = connect(paths).await?;
    let reply = expect_ok(request_reply(&mut ws, &subscribe_message(&opts)).await?)?;
    if let Some(after) = opts.after {
        let num = |k: &str| reply.get(k).and_then(Value::as_u64).unwrap_or(0);
        eprintln!("replaying after seq {after}, latest {}", num("latestSeq"));
    }
    let stdout = std::io::stdout();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        let msg = tokio::select! {
            _ = &mut ctrl_c => return Ok(()),
            msg = next_json(&mut ws) => msg?,
        };
        let Some(msg) = msg else {
            bail!("the app closed the stream (app stopped?)");
        };
        match msg.get("type").and_then(Value::as_str) {
            Some("event") => {
                let Some(event) = msg.get("event") else {
                    continue;
                };
                let line = if json_out {
                    event.to_string()
                } else {
                    format_line(event, &Local)
                };
                let mut out = stdout.lock();
                writeln!(out, "{line}")?;
                out.flush()?;
            }
            Some("overflow" | "reset") => {
                if let Some(line) = notice_line(&msg) {
                    eprintln!("{line}");
                }
            }
            Some("error") => {
                let text = msg
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("error");
                eprintln!("stream error: {text}");
            }
            _ => {}
        }
    }
}

/// `[seq  ]HH:MM:SS  type  [level  ]title  url` in the given zone; seq and url omitted when absent.
/// The level shows as `[pane]` or `[ignore]`; alert rows carry no marker.
fn format_line<Tz>(event: &Value, tz: &Tz) -> String
where
    Tz: TimeZone,
    Tz::Offset: std::fmt::Display,
{
    let field = |k: &str| event.get(k).and_then(Value::as_str).unwrap_or("");
    let time = DateTime::parse_from_rfc3339(field("occurredUtc"))
        .map(|t| t.with_timezone(tz).format("%H:%M:%S").to_string())
        .unwrap_or_else(|_| "--:--:--".to_string());
    let mut line = String::new();
    if let Some(seq) = event.get("seq").and_then(Value::as_u64) {
        line.push_str(&format!("{seq}  "));
    }
    line.push_str(&format!("{time}  {}  ", field("type")));
    if let level @ ("pane" | "ignore") = field("level") {
        line.push_str(&format!("[{level}]  "));
    }
    line.push_str(field("title"));
    let url = field("url");
    if !url.is_empty() {
        line.push_str("  ");
        line.push_str(url);
    }
    line
}

/// `key=value` pairs; repeated keys collapse into an any-of array.
fn build_match(pairs: &[String]) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    for pair in pairs {
        let (key, value) = split_kv(pair)?;
        match map.remove(&key) {
            None => map.insert(key, Value::String(value)),
            Some(Value::Array(mut all)) => {
                all.push(Value::String(value));
                map.insert(key, Value::Array(all))
            }
            Some(first) => map.insert(key, Value::Array(vec![first, Value::String(value)])),
        };
    }
    Ok(map)
}

fn split_kv(s: &str) -> Result<(String, String)> {
    match s.split_once('=') {
        Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.to_string())),
        _ => bail!("expected key=value, got '{s}'"),
    }
}

// ---- emit-test ----

struct EmitArgs {
    event_type: String,
    title: Option<String>,
    pr_id: Option<i64>,
    repo: Option<String>,
    branch: Option<String>,
    url: Option<String>,
    connection: Option<String>,
    sets: Vec<String>,
}

fn build_emit_event(a: &EmitArgs) -> Result<Value> {
    let mut event = json!({ "type": a.event_type });
    if let Some(title) = &a.title {
        event["title"] = json!(title);
    }
    if let Some(url) = &a.url {
        event["url"] = json!(url);
    }
    if let Some(id) = &a.connection {
        event["connection"] = json!({ "id": id });
    }
    if let Some(repo) = &a.repo {
        event["repo"] = json!({ "id": repo, "name": repo, "namespace": null });
    }
    if let Some(branch) = &a.branch {
        let short = branch.strip_prefix("refs/heads/").unwrap_or(branch);
        event["branch"] = json!({ "name": format!("refs/heads/{short}"), "shortName": short });
    }
    if let Some(id) = a.pr_id {
        event["pr"] = json!({
            "id": id,
            "title": a.title.clone().unwrap_or_else(|| "Test PR".to_string()),
            "sourceBranch": a.branch.clone().unwrap_or_default(),
            "targetBranch": "",
            "isDraft": false,
            "role": "reviewer",
        });
    }
    for pair in &a.sets {
        let (path, raw) = split_kv(pair)?;
        let value = serde_json::from_str(&raw).unwrap_or(Value::String(raw));
        set_path(&mut event, &path, value);
    }
    Ok(event)
}

/// Sets a dotted path, replacing non-object intermediates.
fn set_path(root: &mut Value, path: &str, value: Value) {
    let mut cur = root;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if !cur.is_object() {
            *cur = Value::Object(Map::new());
        }
        let Some(map) = cur.as_object_mut() else {
            return;
        };
        if parts.peek().is_none() {
            map.insert(part.to_string(), value);
            return;
        }
        cur = map.entry(part.to_string()).or_insert(Value::Null);
    }
}

async fn emit_test(paths: &Paths, event: Value) -> Result<()> {
    let mut ws = connect(paths).await?;
    let request = json!({ "type": "emit", "event": event });
    let reply = expect_ok(request_reply(&mut ws, &request).await?)?;
    let id = reply.get("id").and_then(Value::as_str).unwrap_or("");
    println!("{id}");
    Ok(())
}

// ---- events ----

enum EventsFrom {
    Since(DateTime<Utc>),
    After(u64),
}

impl EventsFrom {
    fn describe(&self) -> String {
        match self {
            Self::Since(t) => format!("since {}", t.to_rfc3339()),
            Self::After(seq) => format!("after seq {seq}"),
        }
    }
}

async fn query_events(store: &dyn Store, from: &EventsFrom, limit: u32) -> Result<Vec<Event>> {
    Ok(match from {
        EventsFrom::Since(t) => store.events_since(*t, limit).await?,
        EventsFrom::After(seq) => store.events_after_seq(*seq, limit).await?,
    })
}

async fn events(paths: &Paths, from: EventsFrom, limit: u32, json_out: bool) -> Result<()> {
    let db = paths.home.join(DB_FILE);
    if !db.exists() {
        eprintln!("no events (no database at {})", db.display());
        return Ok(());
    }
    let store: Arc<dyn Store> =
        Arc::new(SqliteStore::open_read_only(&db).context("opening the event database")?);
    let found = query_events(store.as_ref(), &from, limit).await?;
    if found.is_empty() {
        eprintln!("no events {}", from.describe());
        return Ok(());
    }
    let hit_limit = found.len() >= limit as usize;
    let stdout = std::io::stdout();
    for event in found {
        let value = serde_json::to_value(&event)?;
        let line = if json_out {
            value.to_string()
        } else {
            format_line(&value, &Local)
        };
        let mut out = stdout.lock();
        writeln!(out, "{line}")?;
        out.flush()?;
    }
    if hit_limit {
        eprintln!("showing {limit} events (the limit); more may exist, raise --limit");
    }
    Ok(())
}

fn parse_since(s: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(date.and_time(chrono::NaiveTime::MIN).and_utc());
    }
    let unit = s.chars().last().context("--since is empty")?;
    let amount: i64 = s[..s.len() - unit.len_utf8()].parse().map_err(|_| {
        anyhow!(
            "--since must be an RFC3339 time, a date (YYYY-MM-DD) or a duration like 30m, 2h, 7d"
        )
    })?;
    let delta = match unit {
        's' => TimeDelta::try_seconds(amount),
        'm' => TimeDelta::try_minutes(amount),
        'h' => TimeDelta::try_hours(amount),
        'd' => TimeDelta::try_days(amount),
        'w' => TimeDelta::try_weeks(amount),
        _ => bail!("unknown duration unit '{unit}' (use s, m, h, d or w)"),
    }
    .filter(|d| *d >= TimeDelta::zero())
    .context("duration out of range")?;
    now.checked_sub_signed(delta)
        .context("duration out of range")
}

// ---- auth and config ----

fn config_path(paths: &Paths) -> PathBuf {
    paths.home.join(CONFIG_FILE)
}

fn registry() -> ProviderRegistry {
    ProviderRegistry::new(vec![Arc::new(AdoProviderFactory)])
}

fn load_config(paths: &Paths, registry: &ProviderRegistry) -> Result<Config> {
    let path = config_path(paths);
    if !path.exists() {
        bail!(
            "no config at {}; start the herald app once to create it",
            path.display()
        );
    }
    let config = Config::load(&path).map_err(|errors| config_errors(&path, errors))?;
    let errors = config.validate_providers(registry);
    if !errors.is_empty() {
        return Err(config_errors(&path, errors));
    }
    Ok(config)
}

fn config_errors(path: &Path, errors: Vec<ConfigError>) -> anyhow::Error {
    let lines: Vec<String> = errors
        .iter()
        .map(|e| format!("  {}: {}", e.path, e.message))
        .collect();
    anyhow!("invalid config {}:\n{}", path.display(), lines.join("\n"))
}

/// The wanted id, or the only one when none is given.
fn pick_connection<'a>(ids: &[&'a str], wanted: Option<&str>) -> Result<&'a str> {
    match (wanted, ids) {
        (Some(w), _) => ids
            .iter()
            .copied()
            .find(|id| *id == w)
            .ok_or_else(|| anyhow!("unknown connection '{w}'; configured: {}", ids.join(", "))),
        (None, [only]) => Ok(only),
        (None, []) => bail!("no connections are configured"),
        (None, _) => bail!(
            "several connections are configured; pass --connection <id> ({})",
            ids.join(", ")
        ),
    }
}

fn select_connection<'a>(
    config: &'a Config,
    config_file: &Path,
    wanted: Option<&str>,
) -> Result<&'a ConnectionConfig> {
    if config.connections.is_empty() {
        bail!(
            "no connections are configured; add a [[connections]] block to {}",
            config_file.display()
        );
    }
    let ids: Vec<&str> = config.connections.iter().map(|c| c.id.as_str()).collect();
    let id = pick_connection(&ids, wanted)?;
    config
        .connections
        .iter()
        .find(|c| c.id == id)
        .context("connection vanished")
}

fn keyring() -> Result<KeyringSecretStore> {
    let store = KeyringSecretStore::new();
    if !store.is_native() {
        bail!("no native secret store is available on this system; cannot manage secrets");
    }
    Ok(store)
}

async fn auth_set(paths: &Paths, wanted: Option<&str>) -> Result<()> {
    let registry = registry();
    let config = load_config(paths, &registry)?;
    let conn = select_connection(&config, &config_path(paths), wanted)?;
    let factory = registry
        .get(&conn.provider)
        .with_context(|| format!("unknown provider '{}'", conn.provider))?;
    let store = keyring()?;
    let prompt = format!("{} for {}: ", factory.secret_label(), conn.id);
    if conn.auth.method == "azcli" && !confirm_azcli_override()? {
        println!("nothing stored");
        return Ok(());
    }
    let raw = rpassword::prompt_password(prompt).context("reading the secret")?;
    let secret = clean_secret(&raw)?;
    store.set(&conn.id, secret)?;
    println!("stored secret for {}", conn.id);
    notify_credentials_changed(paths, &conn.id).await;
    Ok(())
}

/// Trims pasted whitespace; an all-blank secret is an error.
fn clean_secret(raw: &str) -> Result<&str> {
    let secret = raw.trim();
    if secret.is_empty() {
        bail!("empty secret; nothing stored");
    }
    Ok(secret)
}

fn confirm_azcli_override() -> Result<bool> {
    eprint!(
        "this connection uses auth.method \"azcli\", so a stored PAT is not used unless you \
         change auth.method to \"pat\". Continue? [y/N] "
    );
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .context("reading the answer")?;
    Ok(is_yes(&answer))
}

fn is_yes(answer: &str) -> bool {
    matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

async fn notify_credentials_changed(paths: &Paths, id: &str) {
    if !paths.runtime.join(STREAM_FILE).exists() {
        println!("app is not running; it will use the secret on next start");
        return;
    }
    let result = async {
        let mut ws = connect(paths).await?;
        let request = json!({ "type": "credentials-changed", "connectionId": id });
        expect_ok(request_reply(&mut ws, &request).await?)
    }
    .await;
    match result {
        Ok(_) => println!("app acknowledged the change"),
        Err(e) => println!("app did not acknowledge the change: {e:#}"),
    }
}

async fn auth_test(paths: &Paths, wanted: Option<&str>) -> Result<()> {
    let registry = registry();
    let config = load_config(paths, &registry)?;
    let conn = select_connection(&config, &config_path(paths), wanted)?;
    let factory = registry
        .get(&conn.provider)
        .with_context(|| format!("unknown provider '{}'", conn.provider))?;
    let connection = factory.connect(conn, Arc::new(KeyringSecretStore::new()))?;
    let me = connection.identify().await?;
    println!("{} ({})", me.display_name, me.unique_name);
    Ok(())
}

fn auth_clear(paths: &Paths, wanted: Option<&str>) -> Result<()> {
    let registry = registry();
    let config = load_config(paths, &registry)?;
    let conn = select_connection(&config, &config_path(paths), wanted)?;
    keyring()?.delete(&conn.id)?;
    println!("cleared secret for {}", conn.id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    type Frame = Result<Message, tokio_tungstenite::tungstenite::Error>;

    fn text(v: Value) -> Frame {
        Ok(Message::text(v.to_string()))
    }

    #[tokio::test]
    async fn next_reply_skips_event_frames_until_the_reply() {
        let mut frames = stream::iter(vec![
            text(json!({"type":"event","event":{}})),
            Ok(Message::Ping(Vec::new().into())),
            text(json!({"type":"event","event":{}})),
            text(json!({"type":"ok","id":"test:1"})),
            text(json!({"type":"event","event":{}})),
        ]);
        let reply = next_reply(&mut frames).await.unwrap();
        assert_eq!(reply["id"], "test:1");
        assert!(expect_ok(reply).is_ok());
    }

    #[tokio::test]
    async fn next_reply_returns_error_replies_and_fails_when_stream_ends() {
        let mut frames = stream::iter(vec![
            text(json!({"type":"event","event":{}})),
            text(json!({"type":"error","message":"nope"})),
        ]);
        let reply = next_reply(&mut frames).await.unwrap();
        assert_eq!(expect_ok(reply).unwrap_err().to_string(), "nope");
        assert!(next_reply(&mut frames).await.is_err());
    }

    #[test]
    fn clean_secret_trims_and_rejects_blank() {
        assert_eq!(clean_secret("  tok\r\n").unwrap(), "tok");
        assert!(clean_secret(" \t\r\n").is_err());
        assert!(clean_secret("").is_err());
    }

    #[test]
    fn is_yes_accepts_only_explicit_yes() {
        assert!(is_yes("y\n"));
        assert!(is_yes(" YES "));
        assert!(!is_yes("\n"));
        assert!(!is_yes("n"));
    }

    #[test]
    fn parse_since_accepts_date_only_as_utc_midnight() {
        let now = Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap();
        assert_eq!(
            parse_since("2026-01-05", now).unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn parse_since_accepts_durations_and_rfc3339() {
        let now = Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap();
        let at = |h, m| Utc.with_ymd_and_hms(2026, 1, 10, h, m, 0).unwrap();
        assert_eq!(parse_since("30m", now).unwrap(), at(11, 30));
        assert_eq!(parse_since("2h", now).unwrap(), at(10, 0));
        assert_eq!(
            parse_since("7d", now).unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 3, 12, 0, 0).unwrap()
        );
        assert_eq!(
            parse_since("2026-01-01T00:00:00+02:00", now).unwrap(),
            Utc.with_ymd_and_hms(2025, 12, 31, 22, 0, 0).unwrap()
        );
        for bad in ["", "h", "5x", "abc", "-2h"] {
            assert!(parse_since(bad, now).is_err(), "{bad}");
        }
    }

    #[test]
    fn build_match_collapses_repeated_keys() {
        let m = build_match(&[
            "pr.id=12".into(),
            "repo.name=a*".into(),
            "repo.name=b*".into(),
        ])
        .unwrap();
        assert_eq!(m["pr.id"], json!("12"));
        assert_eq!(m["repo.name"], json!(["a*", "b*"]));
        assert!(build_match(&["novalue".into()]).is_err());
        assert!(build_match(&["=x".into()]).is_err());
    }

    #[test]
    fn emit_event_sets_fields_and_overrides() {
        let args = EmitArgs {
            event_type: "pr.created".into(),
            title: Some("Hello".into()),
            pr_id: Some(7),
            repo: Some("api".into()),
            branch: Some("feature/x".into()),
            url: None,
            connection: Some("conn-a".into()),
            sets: vec![
                "pr.isDraft=true".into(),
                "data.note=hi there".into(),
                "severity=critical".into(),
            ],
        };
        let e = build_emit_event(&args).unwrap();
        assert_eq!(e["type"], "pr.created");
        assert_eq!(e["connection"], json!({ "id": "conn-a" }));
        assert_eq!(e["pr"]["id"], 7);
        assert_eq!(e["pr"]["isDraft"], true);
        assert_eq!(e["pr"]["sourceBranch"], "feature/x");
        assert_eq!(e["branch"]["name"], "refs/heads/feature/x");
        assert_eq!(e["repo"]["name"], "api");
        assert_eq!(e["data"]["note"], "hi there");
        assert_eq!(e["severity"], "critical");
    }

    #[test]
    fn format_line_shows_time_type_title_and_optional_url() {
        let mut event = json!({
            "occurredUtc": "2026-01-10T09:05:03Z",
            "type": "run.completed",
            "title": "Build failed",
        });
        assert_eq!(
            format_line(&event, &Utc),
            "09:05:03  run.completed  Build failed"
        );
        event["url"] = json!("https://example.com/r/1");
        assert_eq!(
            format_line(&event, &Utc),
            "09:05:03  run.completed  Build failed  https://example.com/r/1"
        );
    }

    #[test]
    fn format_line_marks_pane_and_ignore_levels_but_not_alert() {
        let mut event = json!({
            "seq": 7,
            "occurredUtc": "2026-01-10T09:05:03Z",
            "type": "run.completed",
            "title": "Build failed",
            "level": "alert",
        });
        assert_eq!(
            format_line(&event, &Utc),
            "7  09:05:03  run.completed  Build failed"
        );
        event["level"] = json!("pane");
        assert_eq!(
            format_line(&event, &Utc),
            "7  09:05:03  run.completed  [pane]  Build failed"
        );
        event["level"] = json!("ignore");
        assert_eq!(
            format_line(&event, &Utc),
            "7  09:05:03  run.completed  [ignore]  Build failed"
        );
    }

    #[test]
    fn format_line_leads_with_seq_when_present() {
        let event = json!({
            "seq": 42,
            "occurredUtc": "2026-01-10T09:05:03Z",
            "type": "run.completed",
            "title": "Build failed",
        });
        assert_eq!(
            format_line(&event, &Utc),
            "42  09:05:03  run.completed  Build failed"
        );
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("herald").chain(args.iter().copied()))
    }

    #[test]
    fn events_requires_exactly_one_of_since_and_after() {
        assert!(parse(&["events"]).is_err());
        assert!(parse(&["events", "--since", "1h", "--after", "5"]).is_err());
        assert!(parse(&["events", "--since", "1h"]).is_ok());
        let cli = parse(&["events", "--after", "5", "--limit", "10"]).unwrap();
        let Command::Events {
            since,
            after,
            limit,
            ..
        } = cli.command
        else {
            panic!("expected events");
        };
        assert_eq!((since, after, limit), (None, Some(5), 10));
    }

    #[test]
    fn tail_after_is_sent_as_after_seq_in_subscribe() {
        let cli = parse(&["tail", "--after", "120"]).unwrap();
        let Command::Tail { after, .. } = cli.command else {
            panic!("expected tail");
        };
        let mut opts = TailOptions {
            types: vec![],
            matches: Map::new(),
            include_catch_up: true,
            after,
            json_out: true,
        };
        assert_eq!(subscribe_message(&opts)["afterSeq"], 120);
        opts.after = None;
        assert!(subscribe_message(&opts).get("afterSeq").is_none());
    }

    #[test]
    fn notice_line_describes_overflow_and_reset_and_ignores_events() {
        let overflow =
            notice_line(&json!({"type":"overflow","replayedThrough":1234,"latestSeq":9876}))
                .unwrap();
        assert!(overflow.contains("replayed through 1234, latest 9876"));
        assert!(overflow.contains("herald events --after 1234"));
        let reset = notice_line(&json!({"type":"reset","latestSeq":7,"storeId":"s"})).unwrap();
        assert!(reset.contains("latest seq is 7"));
        assert!(notice_line(&json!({"type":"event","event":{}})).is_none());
    }

    #[tokio::test]
    async fn query_events_after_returns_only_later_events_with_seq() {
        let store = SqliteStore::open_in_memory().unwrap();
        let now = Utc::now();
        for id in ["a", "b", "c"] {
            let conn = herald_core::event::ConnectionRef {
                id: "c".into(),
                provider: "p".into(),
            };
            let e = Event::new(conn, "pr.created", now, now, "t", "c:pr:1:1").with_id(id);
            store.insert_event_if_new(&e).await.unwrap();
        }
        let all = query_events(&store, &EventsFrom::After(0), 10)
            .await
            .unwrap();
        let first = all[0].seq.unwrap();
        let rest = query_events(&store, &EventsFrom::After(first), 1)
            .await
            .unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].id, "b");
        let value = serde_json::to_value(&rest[0]).unwrap();
        assert_eq!(value["seq"], rest[0].seq.unwrap());
    }

    #[test]
    fn pick_connection_handles_default_and_ambiguity() {
        assert_eq!(pick_connection(&["a"], None).unwrap(), "a");
        assert_eq!(pick_connection(&["a", "b"], Some("b")).unwrap(), "b");
        let err = pick_connection(&["a", "b"], None).unwrap_err();
        assert!(err.to_string().contains("a, b"));
        assert!(pick_connection(&["a"], Some("z")).is_err());
        assert!(pick_connection(&[], None).is_err());
    }
}
