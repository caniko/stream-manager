//! Opt-in Kick webhook relay. Never auto-deployed: the operator runs it
//! behind their own reverse proxy with public HTTPS ingress, configures
//! Kick subscriptions toward it, and the desktop connects outbound.
//!
//! - `POST /kick/events`: provider ingress. Verifies the RSA signature,
//!   enforces freshness/channel/dedupe, then broadcasts an envelope with
//!   the verified event type plus the untouched original body.
//!   Bounded: 64 KiB bodies, 10k-id replay window.
//! - `GET /events`: desktop egress. Bearer-authenticated newline-JSON
//!   stream of verified events.
//!
//! Secrets arrive as file paths (public key, bearer, channel allowlist),
//! never as arguments. Nothing is logged per message.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::sync::{Arc, Mutex};
use stream_manager_chat::adapters::kick::{
    envelope_line, ingress_check, parse_chat_message, verify_signature, ReplayWindow,
};
use tokio::sync::broadcast;
use tokio_stream::StreamExt as _;

const MAX_BODY: usize = 64 * 1024;

#[derive(Debug, Clone)]
struct Config {
    public_key_path: String,
    bearer_path: String,
    channels_path: String,
}

struct AppState {
    cfg: Config,
    window: Mutex<ReplayWindow>,
    tx: broadcast::Sender<Vec<u8>>,
}

fn read_trim(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_owned())
        .map_err(|e| format!("cannot read {path}: {e}"))
}

async fn kick_ingress(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > MAX_BODY {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let message_id = get("kick-event-message-id");
    let timestamp = get("kick-event-message-timestamp");
    let signature = get("kick-event-signature");
    let event_type = get("kick-event-type");
    let version = get("kick-event-version");
    if message_id.is_empty() || timestamp.is_empty() || signature.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing kick headers").into_response();
    }
    if event_type.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing kick event type").into_response();
    }
    let public_key = match read_trim(&st.cfg.public_key_path) {
        Ok(k) => k,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    if verify_signature(&public_key, &message_id, &timestamp, &body, &signature).is_err() {
        return (StatusCode::UNAUTHORIZED, "bad signature").into_response();
    }
    let body_json: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "body not JSON").into_response(),
    };
    if event_type != "chat.message.sent" {
        // Verified but not a chat message: acknowledge without fan-out.
        return (StatusCode::ACCEPTED, "ignored").into_response();
    }
    let ev = match parse_chat_message(&event_type, &body_json) {
        Ok(ev) => ev,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let allowed: Vec<String> = read_trim(&st.cfg.channels_path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_lowercase())
        .filter(|l| !l.is_empty())
        .collect();
    let now = chrono::Utc::now().timestamp();
    let decision = ingress_check(
        &mut st.window.lock().expect("replay window"),
        now,
        &timestamp,
        &ev.broadcaster.to_lowercase(),
        &allowed,
        &message_id,
    );
    if decision != stream_manager_chat::adapters::kick::Ingress::Accept {
        return (StatusCode::from_u16(429).unwrap(), "rejected").into_response();
    }
    // Forward the verified type plus the untouched body: desktops parse
    // the official shape and filter by broadcaster per source.
    let _ = st.tx.send(envelope_line(
        &event_type,
        &version,
        &message_id,
        &timestamp,
        &body_json,
    ));
    (StatusCode::ACCEPTED, "ok").into_response()
}

async fn desktop_stream(State(st): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let want = read_trim(&st.cfg.bearer_path).unwrap_or_default();
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .strip_prefix("Bearer ")
        .unwrap_or_default();
    if want.is_empty() || got != want {
        return (StatusCode::UNAUTHORIZED, "bad bearer").into_response();
    }
    let rx = st.tx.subscribe();
    // A lagged receiver terminates the stream instead of silently
    // skipping: the desktop marks a gap and resubscribes.
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).map(|msg| match msg {
        Ok(mut body) => {
            body.push(b'\n');
            Ok(Bytes::from(body))
        }
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "relay lagged behind broadcast",
        )),
    });
    (
        StatusCode::OK,
        [("content-type", "application/x-ndjson")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true, "service": "kick-chat-relay"}))
}

fn usage() -> String {
    "usage: kick-chat-relay --listen 127.0.0.1:8913 --public-key PATH --bearer PATH --channels PATH"
        .into()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let (Some(listen), Some(public_key), Some(bearer), Some(channels)) = (
        flag("--listen"),
        flag("--public-key"),
        flag("--bearer"),
        flag("--channels"),
    ) else {
        eprintln!("{}", usage());
        std::process::exit(2);
    };
    let (tx, _) = broadcast::channel::<Vec<u8>>(512);
    let state = Arc::new(AppState {
        cfg: Config {
            public_key_path: public_key,
            bearer_path: bearer,
            channels_path: channels,
        },
        window: Mutex::new(ReplayWindow::new(10_000)),
        tx,
    });
    let app = Router::new()
        .route("/health", get(health))
        .route("/kick/events", post(kick_ingress))
        .route("/events", get(desktop_stream))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    eprintln!("kick-chat-relay listening on {listen} (operator-managed ingress)");
    axum::serve(listener, app).await?;
    Ok(())
}
