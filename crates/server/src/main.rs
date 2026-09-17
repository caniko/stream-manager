//! Central stream-manager service (Nomad).
//!
//! Disabled by default, never auto-deployed. Listens on a private
//! address; public ingress (Kick webhooks) arrives through the
//! operator's own reverse proxy. Clients authenticate per operator
//! with individually revocable bearer tokens (hashes in config).
//!
//! Usage:
//!   stream-manager-server --config /path/server.json
//!   stream-manager-server --gen-token   # prints a token + its sha256

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use stream_manager_chat::runtime::{EventSink, SharedStop, BUS_CAPACITY};
use stream_manager_server::perms::Operator;
use stream_manager_server::protocol::{Command, Receipt, ReceiptStatus};
use stream_manager_server::state::{AccountBinding, ServerConfig, ServerState};
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct FileConfig {
    instance: String,
    listen: String,
    database_path: String,
    token_store_path: String,
    token_key_path: String,
    #[serde(default)]
    client_ids: HashMap<String, String>,
    #[serde(default)]
    kick_secret_path: String,
    #[serde(default)]
    kick_relay_url: String,
    #[serde(default)]
    kick_relay_bearer_path: String,
    #[serde(default)]
    rumble_api_path: String,
    operators: Vec<Operator>,
    #[serde(default)]
    accounts: Vec<AccountBinding>,
    #[serde(default)]
    endpoint_bases: HashMap<String, String>,
}

struct AppState {
    state: Arc<Mutex<ServerState>>,
}

fn bearer_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

async fn authed(st: &AppState, headers: &HeaderMap) -> Result<Operator, Response> {
    let Some(token) = bearer_of(headers) else {
        return Err((StatusCode::UNAUTHORIZED, "missing bearer").into_response());
    };
    let guard = st.state.lock().await;
    match guard.authenticate(&token) {
        Some(op) => Ok(op),
        None => Err((StatusCode::UNAUTHORIZED, "bad bearer").into_response()),
    }
}

async fn health(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let guard = st.state.lock().await;
    Json(serde_json::json!({
        "ok": true,
        "service": "stream-manager-server",
        "instance": guard.instance(),
        "protocol": stream_manager_server::protocol::PROTOCOL_VERSION,
    }))
}

async fn commands(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(cmd): Json<Command>,
) -> Response {
    let op = match authed(&st, &headers).await {
        Ok(op) => op,
        Err(e) => return e,
    };
    if cmd.request_id.trim().is_empty() || cmd.request_id.len() > 128 {
        return (
            StatusCode::BAD_REQUEST,
            Json(Receipt {
                request_id: cmd.request_id,
                status: ReceiptStatus::Rejected,
                intent_id: None,
                error: Some("request_id required (<=128 chars)".into()),
            }),
        )
            .into_response();
    }
    let receipt = ServerState::handle(&st.state, &op, cmd).await;
    let status = match receipt.status {
        ReceiptStatus::Rejected => StatusCode::FORBIDDEN,
        _ => StatusCode::OK,
    };
    (status, Json(receipt)).into_response()
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    #[serde(default)]
    cursor: u64,
}

async fn events(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> Response {
    let op = match authed(&st, &headers).await {
        Ok(op) => op,
        Err(e) => return e,
    };
    let guard = st.state.lock().await;
    Json(guard.events_since_for(&op, q.cursor)).into_response()
}

async fn snapshot(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let op = match authed(&st, &headers).await {
        Ok(op) => op,
        Err(e) => return e,
    };
    let guard = st.state.lock().await;
    Json(guard.snapshot_for(&op)).into_response()
}

fn gen_token() {
    let mut bytes = [0u8; 32];
    let mut f = std::fs::File::open("/dev/urandom").expect("urandom");
    use std::io::Read as _;
    f.read_exact(&mut bytes).expect("urandom read");
    let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    use sha2::{Digest, Sha256};
    println!("token: {token}");
    println!("token_sha256: {:x}", Sha256::digest(token.as_bytes()));
}

fn usage() -> String {
    "usage: stream-manager-server --config PATH | --gen-token".into()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--gen-token") {
        gen_token();
        return Ok(());
    }
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let Some(config_path) = flag("--config") else {
        eprintln!("{}", usage());
        std::process::exit(2);
    };
    let raw = std::fs::read_to_string(&config_path)?;
    let cfg: FileConfig = serde_json::from_str(&raw)?;
    if cfg.operators.is_empty() {
        anyhow::bail!("refusing to start without operators");
    }
    let (tx, rx) = tokio::sync::mpsc::channel(BUS_CAPACITY);
    let sink = EventSink::new(tx);
    let stop = SharedStop::default();
    // Exclusive ownership comes first: the kernel lock on the database
    // anchor is held for the whole process lifetime, before migration,
    // bootstrap, provider connections, or API startup. The guard must
    // stay alive until shutdown; the kernel releases it on crash, so no
    // stale lock can survive us.
    let _service_lock = stream_manager_core::store::acquire_service_lock(&cfg.database_path)?;
    eprintln!("service lock held: {}", _service_lock.path().display());
    // Canonical state loads from SQLite: accounts are the stored rows
    // (config entries seed once, transactionally, behind an explicit
    // marker), receipts rebuild the idempotency log with pre-crash
    // dispatches marked unknown.
    let db_path = cfg.database_path.clone();
    let (accounts, receipts) = tokio::task::spawn_blocking(move || -> anyhow::Result<(
        Vec<AccountBinding>,
        stream_manager_server::state::ReceiptLog,
    )> {
        use stream_manager_core::store as s;
        let conn = s::open(&db_path)?;
        let seed_rows: Vec<s::AccountRow> = cfg
            .accounts
            .iter()
            .map(|a| s::AccountRow {
                id: a.id.clone(),
                provider: a.provider.clone(),
                channel_id: a.channel_id.clone(),
                channel_name: a.channel_name.clone(),
                credential_ref: String::new(),
            })
            .collect();
        s::seed_accounts_once(&conn, &seed_rows)?;
        let accounts = s::list_accounts(&conn)?
            .into_iter()
            .map(|r| AccountBinding {
                id: r.id,
                provider: r.provider,
                channel_id: r.channel_id,
                channel_name: r.channel_name,
            })
            .collect();
        let rows = s::load_chat_receipts(&conn)?;
        Ok((accounts, stream_manager_server::state::ServerState::recover_receipts(rows)))
    })
    .await??;
    let state = ServerState::new(
        ServerConfig {
            instance: cfg.instance.clone(),
            operators: cfg.operators,
            accounts,
            db_path: cfg.database_path.clone(),
            token_store_path: cfg.token_store_path,
            token_key_path: cfg.token_key_path,
            client_ids: cfg.client_ids,
            kick_secret_path: cfg.kick_secret_path,
            kick_relay_url: cfg.kick_relay_url,
            kick_relay_bearer_path: cfg.kick_relay_bearer_path,
            rumble_api_path: cfg.rumble_api_path,
            twitch_oauth: stream_manager_core::credentials::oauth::TWITCH,
            youtube_oauth: stream_manager_core::credentials::oauth::YOUTUBE,
            kick_oauth: stream_manager_core::credentials::oauth::KICK,
            endpoint_bases: cfg.endpoint_bases,
        },
        sink,
        stop,
        receipts,
    );
    // The ONLY path from transport loops into canonical state.
    let pump_state = state.clone();
    tokio::spawn(async move {
        let mut rx = rx;
        while let Some((gen, ev)) = rx.recv().await {
            pump_state.lock().await.ingest_runtime(gen, ev);
        }
    });
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/commands", post(commands))
        .route("/v1/events", get(events))
        .route("/v1/snapshot", get(snapshot))
        .with_state(Arc::new(AppState { state }));
    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    eprintln!(
        "stream-manager-server on {} (private API; no TLS here)",
        cfg.listen
    );
    axum::serve(listener, app).await?;
    Ok(())
}
