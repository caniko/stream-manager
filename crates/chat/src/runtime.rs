//! Cancellable transport loops: one per connected source.
//!
//! Loops run off the UI thread, push [`RuntimeEvent`]s into an unbounded
//! channel, and exit when the session generation advances (account, plan,
//! or demo switch) or the source is disconnected. Stale loops cannot
//! resurrect state: every item carries the generation it started with.
//!
//! Token refresh is serialized per account by the caller (see
//! `stream_manager_core::credentials::RefreshLocks`); loops only *use*
//! the access token they were given.

use crate::adapters::{rumble, twitch, youtube};
use crate::coordinator::Incoming;
use crate::model::{ConnState, SourceKey};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Message(Box<Incoming>),
    Delete {
        source_id: String,
        remote_id: String,
        room: u64,
    },
    Clear {
        source_id: String,
        room: u64,
    },
    Conn {
        source_id: String,
        state: ConnState,
        /// Room generation the emitting loop started with: results from a
        /// disconnected/rebound loop are dropped, never restoring state.
        room: u64,
    },
    /// A supervised transport loop terminated (clean exit or panic).
    /// Unlike `Conn`, this never overwrites terminal provider states
    /// such as `Ended`: an exited loop reports absence, not a verdict.
    WorkerExited {
        source_id: String,
        room: u64,
        reason: String,
    },
    Gap {
        source_id: String,
        reason: String,
        room: u64,
    },
}

/// Bounded bus capacity: backpressure surfaces as counted drops with a
/// visible gap, never unbounded growth.
pub const BUS_CAPACITY: usize = 4096;

/// Room tag for sink-generated overflow gaps: the sink cannot know the
/// loop's generation, so these apply regardless of rebinds. Harmless by
/// construction: they only report drops that really happened.
pub const OVERFLOW_ROOM: u64 = u64::MAX;

fn event_source(ev: &RuntimeEvent) -> Option<&str> {
    match ev {
        RuntimeEvent::Message(m) => Some(&m.source_id),
        RuntimeEvent::Delete { source_id, .. }
        | RuntimeEvent::Clear { source_id, .. }
        | RuntimeEvent::Conn { source_id, .. }
        | RuntimeEvent::WorkerExited { source_id, .. }
        | RuntimeEvent::Gap { source_id, .. } => Some(source_id),
    }
}

/// Bounded event bus endpoint. `send` never blocks: a full bus drops with
/// a per-source counter, a closed bus reports false so loops exit. Drops
/// are re-announced as a gap on the next successful send for that source.
#[derive(Debug, Clone)]
pub struct EventSink {
    tx: std::sync::Arc<std::sync::Mutex<tokio::sync::mpsc::Sender<(u64, RuntimeEvent)>>>,
    dropped: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
}

impl EventSink {
    pub fn new(tx: tokio::sync::mpsc::Sender<(u64, RuntimeEvent)>) -> Self {
        Self {
            tx: std::sync::Arc::new(std::sync::Mutex::new(tx)),
            dropped: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.lock().map(|tx| tx.is_closed()).unwrap_or(true)
    }

    /// Backpressured control delivery: deletions, clears, connection
    /// changes, and gaps apply backpressure instead of dropping. A full
    /// bus stalls the emitting loop rather than silently losing a
    /// moderation control; a closed bus reports false so loops exit.
    /// Messages stay on the lossy [`Self::send`] path (volume), controls
    /// never do (correctness).
    pub async fn send_control(&self, gen: u64, ev: RuntimeEvent) -> bool {
        let sender = match self.tx.lock() {
            Ok(tx) => tx.clone(),
            Err(_) => return false,
        };
        sender.send((gen, ev)).await.is_ok()
    }

    pub fn send(&self, gen: u64, ev: RuntimeEvent) -> bool {
        let source = event_source(&ev).unwrap_or_default().to_owned();
        let mut tx = match self.tx.lock() {
            Ok(tx) => tx,
            Err(_) => return false,
        };
        // Everything below holds both locks briefly; `dropped` is only
        // ever touched here, so lock order is fixed and uncontended.
        let mut dropped = match self.dropped.lock() {
            Ok(m) => m,
            Err(_) => return false,
        };
        let push_gap =
            |tx: &mut tokio::sync::mpsc::Sender<(u64, RuntimeEvent)>,
             dropped: &mut std::collections::HashMap<String, u64>| {
                let n = dropped.remove(&source).unwrap_or(0);
                if n == 0 || source.is_empty() {
                    if n > 0 {
                        *dropped.entry(source.clone()).or_insert(0) += n;
                    }
                    return;
                }
                // Best effort: if this also drops, the counter keeps growing
                // and a later send carries the larger total.
                if tx
                    .try_send((
                        gen,
                        RuntimeEvent::Gap {
                            source_id: source.clone(),
                            reason: format!("event bus overflow: {n} event(s) dropped"),
                            room: OVERFLOW_ROOM,
                        },
                    ))
                    .is_err()
                {
                    *dropped.entry(source.clone()).or_insert(0) += n;
                }
            };
        use tokio::sync::mpsc::error::TrySendError;
        match tx.try_send((gen, ev)) {
            Ok(()) => {
                push_gap(&mut tx, &mut dropped);
                true
            }
            Err(TrySendError::Full(_)) => {
                *dropped.entry(source.clone()).or_insert(0) += 1;
                push_gap(&mut tx, &mut dropped);
                true
            }
            Err(TrySendError::Closed(_)) => false,
        }
    }
}

/// Shared stop conditions for transport loops: the session generation
/// (account/plan/demo switch cancels everything) plus per-source room
/// generations (disconnect or rebind stops one loop).
#[derive(Debug, Clone)]
pub struct SharedStop {
    pub session: watch::Sender<u64>,
    pub rooms: Arc<Mutex<HashMap<String, u64>>>,
    /// Retained receiver: broadcasts always have a listener, so `send`
    /// reports real delivery state instead of a dropped-receiver error.
    _keep: watch::Receiver<u64>,
}

impl Default for SharedStop {
    /// Session generations start at 1 (matching a fresh [`Coordinator`]);
    /// starting at 0 would cancel the first loops immediately.
    fn default() -> Self {
        let (session, keep) = watch::channel(1u64);
        Self {
            session,
            rooms: Arc::new(Mutex::new(HashMap::new())),
            _keep: keep,
        }
    }
}

impl SharedStop {
    pub fn stale(&self, gen: u64, source_id: &str, room_gen: u64) -> bool {
        if *self.session.borrow() != gen {
            return true;
        }
        self.rooms
            .lock()
            .ok()
            .and_then(|m| m.get(source_id).copied())
            != Some(room_gen)
    }

    pub fn arm(&self, source_id: &str, room_gen: u64) {
        if let Ok(mut m) = self.rooms.lock() {
            m.insert(source_id.to_owned(), room_gen);
        }
    }

    pub fn disarm(&self, source_id: &str) {
        if let Ok(mut m) = self.rooms.lock() {
            m.remove(source_id);
        }
    }
}

fn stale(cancel: &SharedStop, gen: u64, source_id: &str, room_gen: u64) -> bool {
    cancel.stale(gen, source_id, room_gen)
}

/// Shared context for every transport loop: session/room generations for
/// cancellation, the source identity, the event sink, and stop handles.
pub struct LoopCtx {
    pub gen: u64,
    pub room_gen: u64,
    pub source_id: String,
    pub sink: EventSink,
    pub stop: SharedStop,
}

async fn emit(sink: &EventSink, gen: u64, ev: RuntimeEvent) {
    // Closed buses end loops via the `is_closed` check in loop heads.
    // Messages are lossy with counted drops; every other event is a
    // control and applies backpressure rather than dropping.
    if matches!(ev, RuntimeEvent::Message(_)) {
        // Drops are counted and re-announced by the sink itself.
        let _ = sink.send(gen, ev);
    } else {
        let _ = sink.send_control(gen, ev).await;
    }
}

fn backoff_dur(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_secs(2u64.saturating_pow(attempt.min(5)).min(60))
}

/// Cancellable sleep: true when the duration elapsed and the loop is
/// still current; false on session/room/disconnect change so workers
/// exit promptly instead of sleeping out a 60 s backoff.
async fn stop_sleep(
    stop: &SharedStop,
    gen: u64,
    source_id: &str,
    room_gen: u64,
    dur: std::time::Duration,
) -> bool {
    let mut rx = stop.session.subscribe();
    tokio::select! {
        _ = tokio::time::sleep(dur) => {},
        _ = rx.wait_for(|v| *v != gen) => {},
    }
    !stale(stop, gen, source_id, room_gen)
}

/// Twitch EventSub WebSocket loop with Helix-managed subscriptions.
///
/// Subscriptions are created through the Helix EventSub API after each
/// welcome (409 already-subscribed counts as success for reconnects).
/// The source is Live only after Helix confirms; unauthorized
/// credentials stop the loop instead of retrying forever. Requested
/// reconnects resume at the given URL; keepalive misses reconnect with a
/// visible gap.
pub async fn twitch_loop(
    ctx: LoopCtx,
    room_login: String,
    user_id: String,
    broadcaster_id: String,
    access_token: String,
    client_id: String,
    http: reqwest::Client,
) {
    use futures_util::StreamExt as _;
    let LoopCtx {
        gen,
        room_gen,
        source_id,
        sink,
        stop,
    } = ctx;
    let mut attempt = 0u32;
    let mut seen_envelopes = std::collections::HashSet::new();
    let mut url = twitch::EVENTSUB_WS_URL.to_owned();
    // Requested reconnects migrate subscriptions server-side: the next
    // welcome must not recreate them. Fresh connections always do. This
    // lives outside the reconnect loop so the flag survives `continue`.
    let mut migrated = false;
    'outer: loop {
        if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
            return;
        }
        emit(
            &sink,
            gen,
            RuntimeEvent::Conn {
                room: room_gen,
                source_id: source_id.clone(),
                state: if attempt == 0 {
                    ConnState::Connecting
                } else {
                    ConnState::Reconnecting { attempt }
                },
            },
        )
        .await;
        let Ok((ws, _)) = tokio_tungstenite::connect_async(&url).await else {
            attempt += 1;
            emit(
                &sink,
                gen,
                RuntimeEvent::Gap {
                    source_id: source_id.clone(),
                    reason: "eventsub socket unreachable; retrying with backoff".into(),
                    room: room_gen,
                },
            )
            .await;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            // A failed handshake joins no migrated session: the next
            // successful connect is fresh and must subscribe.
            migrated = false;
            continue;
        };
        let mut ws = ws;
        let mut keepalive = std::time::Duration::from_secs(20);
        loop {
            if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
                return;
            }
            let msg = match tokio::time::timeout(keepalive, ws.next()).await {
                Ok(m) => m,
                Err(_) => {
                    // Keepalive missed: the connection is dead even if
                    // the socket looks open. Any migrated session died
                    // with it: the next connect is fresh.
                    migrated = false;
                    attempt += 1;
                    emit(
                        &sink,
                        gen,
                        RuntimeEvent::Gap {
                            source_id: source_id.clone(),
                            reason: "eventsub keepalive missed; reconnecting".into(),
                            room: room_gen,
                        },
                    )
                    .await;
                    continue 'outer;
                }
            };
            let Some(msg) = msg else {
                migrated = false;
                attempt += 1;
                continue 'outer;
            };
            let text = match msg {
                Ok(m) if m.is_text() => m.into_text().unwrap_or_default(),
                Ok(m) if m.is_close() => {
                    migrated = false;
                    attempt += 1;
                    continue 'outer;
                }
                _ => continue,
            };
            let frame = match twitch::parse_envelope(&text) {
                Ok(f) => f,
                Err(_) => continue,
            };
            match frame {
                twitch::WsFrame::Welcome {
                    session_id: id,
                    keepalive_secs,
                } => {
                    // Deadline from the server plus margin.
                    keepalive = std::time::Duration::from_secs(keepalive_secs + 10);
                    attempt = 0;
                    if migrated {
                        // Server-side migration carried the subscriptions;
                        // recreating them would only produce 409 noise.
                        migrated = false;
                    } else {
                        match twitch::subscribe_all(
                            &http,
                            twitch::HELIX_EVENTSUB_URL,
                            &client_id,
                            &access_token,
                            &id,
                            &broadcaster_id,
                            &user_id,
                        )
                        .await
                        {
                            Ok(()) => {}
                            Err(e) if e.contains("unauthorized") => {
                                emit(
                                    &sink,
                                    gen,
                                    RuntimeEvent::Conn {
                                        room: room_gen,
                                        source_id: source_id.clone(),
                                        state: ConnState::Offline { reason: e },
                                    },
                                )
                                .await;
                                return;
                            }
                            Err(e) => {
                                attempt += 1;
                                emit(
                                    &sink,
                                    gen,
                                    RuntimeEvent::Gap {
                                        source_id: source_id.clone(),
                                        reason: format!(
                                            "eventsub subscribe failed ({e}); reconnecting"
                                        ),
                                        room: room_gen,
                                    },
                                )
                                .await;
                                continue 'outer;
                            }
                        }
                    }
                    emit(
                        &sink,
                        gen,
                        RuntimeEvent::Conn {
                            room: room_gen,
                            source_id: source_id.clone(),
                            state: ConnState::Live {
                                since_utc: chrono::Utc::now().to_rfc3339(),
                            },
                        },
                    )
                    .await;
                }
                twitch::WsFrame::Notification(n) => {
                    if !seen_envelopes.insert(n.message_id.clone()) {
                        continue;
                    }
                    if seen_envelopes.len() > 10_000 {
                        seen_envelopes.clear();
                    }
                    if let Some(inc) =
                        twitch::message_notification(gen, room_gen, &source_id, &room_login, &n)
                    {
                        emit(&sink, gen, RuntimeEvent::Message(Box::new(inc))).await;
                    } else if let Some(inc) =
                        twitch::notice_notification(gen, room_gen, &source_id, &room_login, &n)
                    {
                        emit(&sink, gen, RuntimeEvent::Message(Box::new(inc))).await;
                    } else if let Some(action) = twitch::moderation_notification(&n) {
                        match action {
                            twitch::DeleteAction::DeleteMessage { remote_id } => {
                                emit(
                                    &sink,
                                    gen,
                                    RuntimeEvent::Delete {
                                        source_id: source_id.clone(),
                                        remote_id,
                                        room: room_gen,
                                    },
                                )
                                .await;
                            }
                            twitch::DeleteAction::ClearRoom => {
                                emit(
                                    &sink,
                                    gen,
                                    RuntimeEvent::Clear {
                                        source_id: source_id.clone(),
                                        room: room_gen,
                                    },
                                )
                                .await;
                            }
                        }
                    }
                }
                twitch::WsFrame::Reconnect { reconnect_url } => {
                    // Requested reconnect: resume at the given URL (the
                    // server migrates subscriptions); missed events are
                    // not replayed.
                    migrated = true;
                    if let Some(u) = reconnect_url {
                        if !u.trim().is_empty() {
                            url = u;
                        }
                    }
                    emit(
                        &sink,
                        gen,
                        RuntimeEvent::Gap {
                            source_id: source_id.clone(),
                            reason: "twitch requested reconnect; missed events are not replayed"
                                .into(),
                            room: room_gen,
                        },
                    )
                    .await;
                    attempt += 1;
                    continue 'outer;
                }
                twitch::WsFrame::Revocation { status, .. } => {
                    emit(
                        &sink,
                        gen,
                        RuntimeEvent::Conn {
                            room: room_gen,
                            source_id: source_id.clone(),
                            state: ConnState::Offline {
                                reason: format!("subscription revoked: {status}"),
                            },
                        },
                    )
                    .await;
                    return;
                }
                twitch::WsFrame::Close => {
                    attempt += 1;
                    continue 'outer;
                }
                _ => {}
            }
        }
    }
}

/// YouTube list-polling loop with continuation + pollingIntervalMillis.
pub async fn youtube_loop(
    ctx: LoopCtx,
    live_chat_id: String,
    access_token: String,
    http: reqwest::Client,
) {
    let LoopCtx {
        gen,
        room_gen,
        source_id,
        sink,
        stop,
    } = ctx;
    let mut page_token: Option<String> = None;
    let mut attempt = 0u32;
    emit(
        &sink,
        gen,
        RuntimeEvent::Conn {
            room: room_gen,
            source_id: source_id.clone(),
            state: ConnState::Connecting,
        },
    )
    .await;
    loop {
        if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
            return;
        }
        let mut url =
            format!("https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId={live_chat_id}&part=snippet,authorDetails&maxResults=200");
        if let Some(t) = &page_token {
            url.push_str("&pageToken=");
            url.push_str(t);
        }
        let res = http.get(&url).bearer_auth(&access_token).send().await;
        let Ok(res) = res else {
            attempt += 1;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            continue;
        };
        let status = res.status();
        if status.as_u16() == 401 {
            // Revoked/expired credential: stop instead of declaring Live
            // on an error page or retrying a dead token forever.
            emit(
                &sink,
                gen,
                RuntimeEvent::Conn {
                    room: room_gen,
                    source_id: source_id.clone(),
                    state: ConnState::Offline {
                        reason: "youtube unauthorized — reconnect the account".into(),
                    },
                },
            )
            .await;
            return;
        }
        if status.as_u16() == 429 || status.as_u16() == 403 {
            attempt += 1;
            emit(
                &sink,
                gen,
                RuntimeEvent::Conn {
                    room: room_gen,
                    source_id: source_id.clone(),
                    state: ConnState::RateLimited {
                        retry_after_secs: 30,
                    },
                },
            )
            .await;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            continue;
        }
        if !status.is_success() {
            attempt += 1;
            emit(
                &sink,
                gen,
                RuntimeEvent::Gap {
                    source_id: source_id.clone(),
                    reason: format!("youtube list failed ({status}); retrying"),
                    room: room_gen,
                },
            )
            .await;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            continue;
        }
        let Ok(body) = res.json::<serde_json::Value>().await else {
            attempt += 1;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            continue;
        };
        // A disconnect during the fetch must not apply a whole page of
        // stale events.
        if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
            return;
        }
        attempt = 0;
        let page = youtube::parse_list_page(gen, room_gen, &source_id, &live_chat_id, &body);
        // Live is announced only after a successfully parsed page — never
        // on transport errors or error JSON.
        emit(
            &sink,
            gen,
            RuntimeEvent::Conn {
                room: room_gen,
                source_id: source_id.clone(),
                state: ConnState::Live {
                    since_utc: chrono::Utc::now().to_rfc3339(),
                },
            },
        )
        .await;
        for m in page.messages {
            emit(&sink, gen, RuntimeEvent::Message(Box::new(m))).await;
        }
        for id in page.deleted_ids {
            emit(
                &sink,
                gen,
                RuntimeEvent::Delete {
                    source_id: source_id.clone(),
                    remote_id: id,
                    room: room_gen,
                },
            )
            .await;
        }
        if page.chat_ended {
            emit(
                &sink,
                gen,
                RuntimeEvent::Conn {
                    room: room_gen,
                    source_id: source_id.clone(),
                    state: ConnState::Ended {
                        reason: "broadcast ended".into(),
                    },
                },
            )
            .await;
            return;
        }
        page_token = page.next_page_token;
        // Pacing sleep, cancellable; the loop head rechecks staleness.
        stop_sleep(
            &stop,
            gen,
            &source_id,
            room_gen,
            std::time::Duration::from_millis(page.polling_interval_ms),
        )
        .await;
    }
}

/// Rumble read-only polling loop. The API URL is a bearer secret: it is
/// read from the operator's file, used as the request target, and never
/// emitted in events, errors, or logs.
pub async fn rumble_loop(
    ctx: LoopCtx,
    api_url_path: String,
    http: reqwest::Client,
    target_stream: Option<String>,
) {
    let LoopCtx {
        gen,
        room_gen,
        source_id,
        sink,
        stop,
    } = ctx;
    let api_url = match std::fs::read_to_string(&api_url_path) {
        Ok(s) => s.trim().to_owned(),
        Err(_) => {
            emit(
                &sink,
                gen,
                RuntimeEvent::Conn {
                    room: room_gen,
                    source_id,
                    state: ConnState::Offline {
                        reason: "rumble API URL file unreadable — set it in Settings".into(),
                    },
                },
            )
            .await;
            return;
        }
    };
    let mut reconciler = rumble::Reconciler::new(512);
    emit(
        &sink,
        gen,
        RuntimeEvent::Conn {
            room: room_gen,
            source_id: source_id.clone(),
            state: ConnState::Connecting,
        },
    )
    .await;
    loop {
        if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
            return;
        }
        match http.get(&api_url).send().await {
            Ok(res) => match res.text().await {
                // A disconnect during the fetch must not apply a whole
                // poll of stale events: recheck before parsing/emitting.
                Ok(_) if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() => {
                    return;
                }
                Ok(text) => match rumble::parse_snapshot(&text, target_stream.as_deref()) {
                    Ok(snap) => {
                        let out = reconciler.reconcile(gen, room_gen, &source_id, &snap);
                        emit(
                            &sink,
                            gen,
                            RuntimeEvent::Conn {
                                room: room_gen,
                                source_id: source_id.clone(),
                                state: ConnState::Live {
                                    since_utc: chrono::Utc::now().to_rfc3339(),
                                },
                            },
                        )
                        .await;
                        for m in out.items {
                            emit(&sink, gen, RuntimeEvent::Message(Box::new(m))).await;
                        }
                        if out.possible_loss > 0 {
                            emit(
                                &sink,
                                gen,
                                RuntimeEvent::Gap {
                                    source_id: source_id.clone(),
                                    reason: format!(
                                        "rumble window moved past {} message(s); recent window is ID-less",
                                        out.possible_loss,
                                    ),
                                    room: room_gen,
                                },
                            )
                            .await;
                        }
                        if out.ended {
                            emit(
                                &sink,
                                gen,
                                RuntimeEvent::Conn {
                                    room: room_gen,
                                    source_id: source_id.clone(),
                                    state: ConnState::Ended {
                                        reason: "stream not live".into(),
                                    },
                                },
                            )
                            .await;
                            return;
                        }
                    }
                    Err(_) => {
                        if !stop_sleep(
                            &stop,
                            gen,
                            &source_id,
                            room_gen,
                            std::time::Duration::from_secs(10),
                        )
                        .await
                        {
                            return;
                        }
                    }
                },
                Err(_) => {
                    if !stop_sleep(
                        &stop,
                        gen,
                        &source_id,
                        room_gen,
                        std::time::Duration::from_secs(10),
                    )
                    .await
                    {
                        return;
                    }
                }
            },
            Err(_) => {
                if !stop_sleep(
                    &stop,
                    gen,
                    &source_id,
                    room_gen,
                    std::time::Duration::from_secs(10),
                )
                .await
                {
                    return;
                }
            }
        }
        stop_sleep(
            &stop,
            gen,
            &source_id,
            room_gen,
            std::time::Duration::from_secs(15),
        )
        .await;
    }
}

/// Kick relay client: authenticated outbound envelope stream from the
/// operator's relay. The relay verifies provider signatures; without a
/// configured relay URL the UI shows Relay required and never calls this.
/// Events for other channels on the shared stream are dropped (with one
/// visible gap) so messages can never be mislabeled across channels.
pub async fn kick_relay_loop(
    ctx: LoopCtx,
    relay_url: String,
    bearer_path: String,
    http: reqwest::Client,
    channel: String,
) {
    use futures_util::StreamExt as _;
    let LoopCtx {
        gen,
        room_gen,
        source_id,
        sink,
        stop,
    } = ctx;
    let bearer = std::fs::read_to_string(&bearer_path)
        .map(|s| s.trim().to_owned())
        .unwrap_or_default();
    if bearer.is_empty() {
        emit(
            &sink,
            gen,
            RuntimeEvent::Conn {
                room: room_gen,
                source_id,
                state: ConnState::RelayRequired,
            },
        )
        .await;
        return;
    }
    let mut attempt = 0u32;
    loop {
        if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
            return;
        }
        emit(
            &sink,
            gen,
            RuntimeEvent::Conn {
                room: room_gen,
                source_id: source_id.clone(),
                state: if attempt == 0 {
                    ConnState::Connecting
                } else {
                    ConnState::Reconnecting { attempt }
                },
            },
        )
        .await;
        let Ok(res) = http
            .get(format!("{relay_url}/events"))
            .bearer_auth(&bearer)
            .send()
            .await
        else {
            attempt += 1;
            if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
                return;
            }
            continue;
        };
        let mut stream = res.bytes_stream();
        let mut buf = Vec::new();
        attempt = 0;
        emit(
            &sink,
            gen,
            RuntimeEvent::Conn {
                room: room_gen,
                source_id: source_id.clone(),
                state: ConnState::Live {
                    since_utc: chrono::Utc::now().to_rfc3339(),
                },
            },
        )
        .await;
        let mut foreign = 0u64;
        loop {
            // A stalled relay stream must not wedge the loop: time out,
            // report, and reconnect.
            let chunk =
                match tokio::time::timeout(std::time::Duration::from_secs(90), stream.next()).await
                {
                    Ok(c) => c,
                    Err(_) => break,
                };
            let Some(chunk) = chunk else { break };
            if stale(&stop, gen, &source_id, room_gen) || sink.is_closed() {
                return;
            }
            let Ok(bytes) = chunk else { break };
            if buf.len() + bytes.len() > 256 * 1024 {
                buf.clear();
                continue;
            }
            buf.extend_from_slice(&bytes);
            while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=pos).collect();
                let line = line.trim_ascii_end();
                if line.is_empty() {
                    continue;
                }
                let parsed = crate::adapters::kick::parse_envelope_line(line).and_then(
                    |(event_type, body)| {
                        crate::adapters::kick::parse_chat_message(&event_type, &body)
                    },
                );
                match parsed {
                    Ok(ev) => {
                        if !ev.broadcaster.eq_ignore_ascii_case(&channel) {
                            // Shared relay stream, other channel: drop
                            // rather than mislabel; say so once.
                            foreign += 1;
                            if foreign == 1 {
                                emit(
                                    &sink,
                                    gen,
                                    RuntimeEvent::Gap {
                                        source_id: source_id.clone(),
                                        reason: "relay delivered events for other channels; dropped (check relay subscription)".into(),
                                    room: room_gen,
                                    },
                                )
                                .await;
                            }
                            continue;
                        }
                        let inc =
                            crate::adapters::kick::message_event(gen, room_gen, &source_id, &ev);
                        emit(&sink, gen, RuntimeEvent::Message(Box::new(inc))).await;
                    }
                    Err(e) => {
                        if !e.starts_with("unsupported kick event") {
                            emit(
                                &sink,
                                gen,
                                RuntimeEvent::Gap {
                                    source_id: source_id.clone(),
                                    reason: "relay sent an unparsable line; skipped".into(),
                                    room: room_gen,
                                },
                            )
                            .await;
                        }
                    }
                }
            }
        }
        attempt += 1;
        emit(
            &sink,
            gen,
            RuntimeEvent::Gap {
                source_id: source_id.clone(),
                reason: "relay stream dropped; reconnecting".into(),
                room: room_gen,
            },
        )
        .await;
        if !stop_sleep(&stop, gen, &source_id, room_gen, backoff_dur(attempt)).await {
            return;
        }
    }
}

pub fn source_key_of(provider: crate::model::Provider, room: &str) -> SourceKey {
    SourceKey {
        provider,
        room: room.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MessageKind;

    fn message(source_id: &str, remote_id: &str) -> RuntimeEvent {
        RuntimeEvent::Message(Box::new(Incoming {
            session_gen: 1,
            room_gen: 0,
            source_id: source_id.into(),
            remote_id: Some(remote_id.into()),
            idless_key: None,
            author_id: None,
            author_name: "bob".into(),
            provider_ts: None,
            text: "hi".into(),
            fragments: vec![],
            badges: vec![],
            reply_to: None,
            kind: MessageKind::Text,
        }))
    }

    /// Controls never drop: with a full bus the control send waits for
    /// capacity while messages keep their counted-drop behavior. A
    /// dropped final deletion followed by silence therefore cannot lose
    /// the deletion — it arrives once the pump drains.
    #[tokio::test]
    async fn controls_apply_backpressure_while_messages_drop() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let sink = EventSink::new(tx);
        assert!(sink.send(1, message("s", "m1")));
        // Bus full: a further message drops (counted), still reporting
        // success to the loop.
        assert!(sink.send(1, message("s", "m2")));
        // A control instead waits: it must not resolve while full.
        let waiter = sink.clone();
        let control = tokio::spawn(async move {
            waiter
                .send_control(
                    1,
                    RuntimeEvent::Clear {
                        source_id: "s".into(),
                        room: 0,
                    },
                )
                .await
        });
        let mut control = Box::pin(control);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut control)
                .await
                .is_err(),
            "control must wait for capacity, not drop"
        );
        // Drain one slot: the buffered message, then the control.
        let (_, first) = rx.recv().await.expect("message arrives");
        assert!(matches!(first, RuntimeEvent::Message(_)));
        assert!(control.await.expect("control task runs"));
        let (_, second) = rx.recv().await.expect("control arrives");
        assert!(matches!(second, RuntimeEvent::Clear { .. }));
    }
}
