//! Private operator Chat workspace.
//!
//! Layout: sources + connection health (left), merged timeline (center),
//! selected-message details/actions (right, collapsible), composer below.
//! A detached window reuses this same panel — one coordinator, never a
//! second set of subscriptions.
//!
//! Safety rules enforced here:
//! - Connect/Apply activates sources; browsing plans only *proposes*.
//! - Filtering never retargets the composer; Reply binds source+message.
//! - Sends recheck session, room generation, auth scopes, limits, and CTA
//!   policy at execution, not just at the button.
//! - HTTP success never overrides a provider `is_sent=false`/rejection.
//! - Confirmed sends are never retried; ambiguous sends are never
//!   auto-retried. Old drafts are never auto-flushed.

use iced::widget::{
    button, checkbox, column, container, pick_list, row, scrollable, text, text_input,
};
use iced::{window, Element, Length, Task};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use stream_manager_chat::adapters::{kick, twitch, youtube};
use stream_manager_chat::coordinator::{BufferLimits, Coordinator};
use stream_manager_chat::gate::SendGate;
use stream_manager_chat::model::{
    caps_of, ChatSource, ConnState, IntentState, MessageKind, Provider, SourceKey, TimelineEvent,
};
use stream_manager_chat::runtime::{EventSink, RuntimeEvent, SharedStop};
use stream_manager_core::credentials::oauth::{self, OAuthProvider};
use stream_manager_core::credentials::{FileTokenStore, RefreshLocks, Secret, TokenStore};

use crate::Message;

/// Non-secret chat configuration. Precedence: environment, then the
/// Home-Manager-written config file, then built-ins. Only runtime
/// *paths* appear here — never secret contents.
#[derive(Debug, Clone)]
pub struct ChatConfig {
    pub enabled: bool,
    pub auto_connect: bool,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub relay_url: String,
    pub relay_bearer_path: String,
    pub rumble_api_path: String,
    pub token_store_path: String,
    pub token_key_path: String,
    pub twitch_client_id: String,
    pub youtube_client_id: String,
    pub kick_client_id: String,
    pub kick_secret_path: String,
    pub redirect_uri: String,
    /// Central server base URL (empty = standalone desktop mode).
    pub server_url: String,
    /// This client's bearer token file for the central server.
    pub server_token_path: String,
}

impl Default for ChatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_connect: false,
            max_messages: 5_000,
            max_bytes: 8 * 1024 * 1024,
            relay_url: String::new(),
            relay_bearer_path: String::new(),
            rumble_api_path: String::new(),
            token_store_path: shellexpand("~/.config/stream-manager/tokens.json"),
            token_key_path: String::new(),
            twitch_client_id: String::new(),
            youtube_client_id: String::new(),
            kick_client_id: String::new(),
            kick_secret_path: String::new(),
            redirect_uri: "http://127.0.0.1:8912/callback".into(),
            server_url: String::new(),
            server_token_path: String::new(),
        }
    }
}

fn shellexpand(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_string()
}

#[derive(Debug, Clone)]
pub struct AccountView {
    pub id: String,
    pub provider: Provider,
    pub channel_id: String,
    pub channel_name: String,
}

/// One plan destination proposed as a chat source. Proposing never
/// activates: only Connect/Apply does.
#[derive(Debug, Clone)]
pub struct Proposal {
    pub binding_id: String,
    pub provider: Provider,
    pub account_id: Option<String>,
    pub channel: String,
    /// Explicit remote broadcast/stream id from the plan, used to select
    /// one stream when the provider returns several.
    pub remote_id: Option<String>,
    pub selected: bool,
}

pub struct ChatCtx {
    pub demo_mode: bool,
    pub db_path: String,
    pub accounts: Vec<AccountView>,
    pub proposals: Vec<Proposal>,
    pub cta_text: Option<String>,
    pub cta_blocked: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AuthState {
    Unknown,
    Locked { reason: String },
    Connected(AuthSession),
    Failed { error: String },
}

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub identity: String,
    pub user_id: String,
    pub scopes: Vec<String>,
    pub can_write: bool,
    pub can_moderate: bool,
    pub access: Secret,
    pub expires_at: i64,
    pub refresh_present: bool,
}

impl AuthSession {
    fn expired(&self) -> bool {
        chrono::Utc::now().timestamp() >= self.expires_at - 30
    }
}

fn oauth_provider(p: Provider) -> Option<OAuthProvider> {
    match p {
        Provider::Twitch => Some(oauth::TWITCH),
        Provider::Youtube => Some(oauth::YOUTUBE),
        Provider::Kick => Some(oauth::KICK),
        Provider::Rumble => None,
    }
}

fn provider_client_id(cfg: &ChatConfig, p: Provider) -> &str {
    match p {
        Provider::Twitch => &cfg.twitch_client_id,
        Provider::Youtube => &cfg.youtube_client_id,
        Provider::Kick => &cfg.kick_client_id,
        Provider::Rumble => "",
    }
}

/// Shared logout/cancel marks: account id → highest dead auth generation.
/// OAuth tasks check their generation before persisting anything, so a
/// logout or session reset wins over a task that already passed admission.
#[derive(Debug, Clone, Default)]
pub struct AuthKill {
    inner: Arc<Mutex<HashMap<String, u64>>>,
}

impl AuthKill {
    pub fn kill(&self, account_id: &str, gen: u64) {
        if let Ok(mut m) = self.inner.lock() {
            let e = m.entry(account_id.to_owned()).or_insert(0);
            *e = (*e).max(gen);
        }
    }

    pub fn is_dead(&self, account_id: &str, gen: u64) -> bool {
        self.inner
            .lock()
            .ok()
            .and_then(|m| m.get(account_id).copied())
            .is_some_and(|dead| dead >= gen)
    }
}

#[derive(Debug, Clone)]
pub enum ChatMsg {
    BusReady(EventSink),
    BusEvent(u64, Box<RuntimeEvent>),
    StartupLoad,
    SourcesRestored {
        sources: Vec<ChatSource>,
        db_path: String,
    },
    ProposalsRefreshed,
    ToggleProposal(String),
    ConnectSource(String),
    SourceUpserted {
        source: ChatSource,
        session: u64,
        room: u64,
    },
    ConnectFailed {
        source_id: String,
        error: String,
        session: u64,
        room: u64,
    },
    DisconnectSource(String),
    DisconnectAll,
    RetrySource(String),
    SessionReset,
    ComposerSource(String),
    DraftChanged(String),
    ReplyTo(u64),
    ClearReply,
    HideMessage(u64),
    CopyMessage(u64),
    Copied,
    ConfirmDelete(u64),
    DeleteConfirmed,
    DeleteCancelled,
    ConfirmBan(String, String),
    BanConfirmed,
    BanCancelled,
    TimeoutUser(String, String, u64),
    ModDone {
        label: String,
        result: Result<String, String>,
    },
    SendNow,
    CancelIntent(String),
    SendResolved {
        intent_id: String,
        outcome: SendOutcome,
    },
    ToggleMulti(bool),
    ToggleRecipient(String),
    MultiText(String),
    MultiPreview,
    MultiBack,
    MultiConfirm,
    ToggleProviderFilter(Provider),
    QueryChanged(String),
    ToggleMentions,
    TogglePaid,
    TextSize(usize),
    SelectMessage(u64),
    SelectPrev,
    SelectNext,
    ShowOlder,
    ToggleDetails,
    ToggleCompact,
    Follow(bool),
    JumpLive,
    LoadDemoChat,
    ClearChat,
    OpenDetached,
    DetachedOpened(window::Id),
    DetachedClosed(window::Id),
    AuthConnect {
        account_id: String,
        with_write: bool,
        with_moderate: bool,
    },
    AuthResult {
        account_id: String,
        auth_gen: u64,
        result: Result<AuthSession, String>,
    },
    AuthDisconnect(String),
    LogoutDone {
        account_id: String,
        result: Result<String, String>,
    },
    ServerCheck,
    ServerChecked(Result<String, String>),
    InsertCta,
    Persisted(Result<String, String>),
}

#[derive(Debug, Clone)]
pub enum SendOutcome {
    Confirmed {
        remote_id: Option<String>,
        session_moved: bool,
        /// Non-fatal advisory (e.g. rotation not persisted): the send is
        /// real, but the operator should know the store needs attention.
        note: Option<String>,
    },
    Rejected {
        error: String,
    },
    Unknown {
        error: String,
    },
}

/// Bidirectional bus worker: hands the sender to the panel, then forwards
/// every transport event as a message. One bus, shared by the main and
/// detached views alike.
pub fn bus_worker() -> impl iced::futures::Stream<Item = Message> + Send + 'static {
    iced::stream::channel(1024, |mut out| async move {
        use iced::futures::SinkExt as _;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, RuntimeEvent)>(
            stream_manager_chat::runtime::BUS_CAPACITY,
        );
        let _ = out
            .send(Message::Chat(ChatMsg::BusReady(
                stream_manager_chat::runtime::EventSink::new(tx),
            )))
            .await;
        while let Some((gen, ev)) = rx.recv().await {
            let _ = out
                .send(Message::Chat(ChatMsg::BusEvent(gen, Box::new(ev))))
                .await;
        }
    })
}

pub struct ChatPanel {
    coord: Coordinator,
    cfg: ChatConfig,
    bus: Option<EventSink>,
    stop: SharedStop,
    locks: RefreshLocks,
    gate: SendGate,
    auth: HashMap<String, AuthState>,
    auth_gen: u64,
    auth_kill: AuthKill,
    proposals: Vec<Proposal>,
    show_providers: HashMap<Provider, bool>,
    query: String,
    mentions_only: bool,
    paid_only: bool,
    scale_idx: usize,
    selected: Option<u64>,
    details_open: bool,
    compact: bool,
    confirm_delete: Option<u64>,
    confirm_ban: Option<(String, String)>,
    pending_remote_delete: Option<(u64, String, String)>,
    outbox: VecDeque<String>,
    send_note: String,
    multitarget_confirm: bool,
    detached: Option<window::Id>,
    render_window: usize,
    demo_loaded: bool,
    sources_loaded: bool,
    status: String,
}

impl ChatPanel {
    pub fn new(cfg: ChatConfig) -> Self {
        let coord = Coordinator::new(BufferLimits {
            max_messages: cfg.max_messages,
            max_bytes: cfg.max_bytes,
        });
        let mut show_providers = HashMap::new();
        for p in [
            Provider::Youtube,
            Provider::Twitch,
            Provider::Kick,
            Provider::Rumble,
        ] {
            show_providers.insert(p, true);
        }
        let panel = Self {
            coord,
            cfg,
            bus: None,
            stop: SharedStop::default(),
            locks: RefreshLocks::default(),
            gate: SendGate::default(),
            auth: HashMap::new(),
            auth_gen: 0,
            auth_kill: AuthKill::default(),
            proposals: vec![],
            show_providers,
            query: String::new(),
            mentions_only: false,
            paid_only: false,
            scale_idx: 1,
            selected: None,
            details_open: true,
            compact: false,
            confirm_delete: None,
            confirm_ban: None,
            pending_remote_delete: None,
            outbox: VecDeque::new(),
            send_note: String::new(),
            multitarget_confirm: false,
            detached: None,
            render_window: 300,
            demo_loaded: false,
            sources_loaded: false,
            status: "Chat idle.".into(),
        };
        // Gate baseline matches a fresh coordinator (session 1, auth 0);
        // rooms sync as sources activate.
        panel.gate.set_session(1);
        panel.gate.set_auth(0);
        panel
    }

    fn key_id(provider: Provider, channel_id: &str) -> String {
        format!("{}:{channel_id}", provider.slug())
    }

    /// Single activation path: coordinator source plus gate room sync, so
    /// tickets always have a room to admit against.
    fn activate_source(&mut self, source: ChatSource) {
        self.coord.upsert_source(source.clone());
        self.gate
            .set_room(&source.id, self.coord.room_gen_of(&source.id).unwrap_or(0));
    }

    /// Chat activity must never invalidate metadata reviews: this panel
    /// owns no review state and calls no metadata mutation.
    fn note(&mut self, s: impl Into<String>) {
        self.status = s.into();
    }

    // ---------- inbound bus ----------

    /// Room admission shared by every control event: unknown sources
    /// and stale generations are dropped. Overflow gaps (which the sink
    /// cannot tag) always apply — they only report drops that happened.
    fn admit_room(&self, source_id: &str, room: u64) -> bool {
        use stream_manager_chat::runtime::OVERFLOW_ROOM;
        room == OVERFLOW_ROOM || self.coord.room_gen_of(source_id) == Some(room)
    }

    fn apply_bus(&mut self, gen: u64, ev: RuntimeEvent) {
        if gen != self.coord.session_gen() {
            return;
        }
        match ev {
            RuntimeEvent::Message(m) => {
                // Loops exit on disarm and recheck after fetches; a single
                // in-flight message is the residual window, bounded by the
                // session check above.
                let _ = self.coord.ingest(*m);
            }
            RuntimeEvent::Delete {
                source_id,
                remote_id,
                room,
            } => {
                if !self.admit_room(&source_id, room) {
                    return;
                }
                self.coord.apply_delete(&source_id, &remote_id);
            }
            RuntimeEvent::Clear { source_id, room } => {
                if !self.admit_room(&source_id, room) {
                    return;
                }
                self.coord.apply_clear(&source_id);
            }
            RuntimeEvent::Conn {
                source_id,
                state,
                room,
            } => {
                // Per-source generation: a loop disconnected or rebound
                // after emitting cannot restore connection state.
                if !self.admit_room(&source_id, room) {
                    return;
                }
                self.coord.set_conn(gen, &source_id, state);
            }
            RuntimeEvent::WorkerExited {
                source_id,
                room,
                reason,
            } => {
                if !self.admit_room(&source_id, room) {
                    return;
                }
                // Absence, not a verdict: terminal states survive.
                let terminal = matches!(
                    self.coord.conn_of(&source_id),
                    None | Some(ConnState::Ended { .. }) | Some(ConnState::Offline { .. })
                );
                if !terminal {
                    self.coord
                        .set_conn(gen, &source_id, ConnState::Offline { reason });
                }
            }
            RuntimeEvent::Gap {
                source_id,
                reason,
                room,
            } => {
                if !self.admit_room(&source_id, room) {
                    return;
                }
                self.coord.mark_gap(&source_id, &reason);
            }
        }
    }

    // ---------- send eligibility ----------

    /// Single-target send target: Some(source) or a human reason.
    pub fn send_target(&self, ctx: &ChatCtx) -> Result<&ChatSource, String> {
        if let Some(reply) = self.coord.reply() {
            return self
                .coord
                .sources()
                .into_iter()
                .find(|(s, _)| s.id == reply.source_id)
                .map(|(s, _)| s)
                .ok_or_else(|| "reply target is gone".into())
                .and_then(|s| self.check_sendable(s, ctx).map(|_| s));
        }
        let id = self
            .coord
            .composer_source()
            .ok_or_else(|| "no chat source selected".to_string())?;
        let (s, _) = self
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == id)
            .ok_or_else(|| "composer source is gone".to_string())?;
        self.check_sendable(s, ctx).map(|_| s)
    }

    fn check_sendable(&self, s: &ChatSource, ctx: &ChatCtx) -> Result<(), String> {
        if ctx.demo_mode {
            return Err("demo mode: sending disabled".into());
        }
        let caps = caps_of(s.key.provider);
        if !caps.send.implemented {
            return Err(format!("{} is read-only here", s.key.provider.display()));
        }
        match self.coord.conn_of(&s.id) {
            Some(ConnState::Live { .. }) => {}
            Some(other) => return Err(format!("source is {}", other.short())),
            None => return Err("source is gone".into()),
        }
        let account = s
            .account_id
            .as_deref()
            .ok_or_else(|| "no account bound".to_string())?;
        match self.auth.get(account) {
            Some(AuthState::Connected(sess)) if !sess.expired() => {
                if !sess.can_write {
                    return Err(
                        "account authorized for reading only — reconnect with send scope".into(),
                    );
                }
                Ok(())
            }
            Some(AuthState::Connected(_)) => Err("token expired — reconnect".into()),
            Some(AuthState::Locked { reason }) => Err(format!("token store locked: {reason}")),
            Some(AuthState::Failed { error }) => Err(format!("auth failed: {error}")),
            _ => Err("account not connected".into()),
        }
    }

    // ---------- filtering (view-only; never retargets) ----------

    fn visible(&self, ctx: &ChatCtx) -> Vec<&TimelineEvent> {
        let my_names: Vec<String> = ctx
            .accounts
            .iter()
            .map(|a| a.channel_name.to_lowercase())
            .collect();
        self.coord
            .timeline()
            .iter()
            .filter(|ev| match ev {
                TimelineEvent::Message(m) => {
                    if !self
                        .show_providers
                        .get(&m.source.provider)
                        .copied()
                        .unwrap_or(true)
                    {
                        return false;
                    }
                    if self.paid_only && !m.is_paid() {
                        return false;
                    }
                    if self.mentions_only
                        && !my_names.iter().any(|n| m.mentions(n))
                        && !m.badges.iter().any(|b| b == "mention")
                    {
                        return false;
                    }
                    if !self.query.trim().is_empty() {
                        let q = self.query.to_lowercase();
                        if !m.text.to_lowercase().contains(&q)
                            && !m.author_name.to_lowercase().contains(&q)
                        {
                            return false;
                        }
                    }
                    true
                }
                _ => true,
            })
            .collect()
    }

    fn scale(&self) -> f32 {
        [13.0, 15.0, 18.0][self.scale_idx.min(2)]
    }
}

impl ChatPanel {
    pub fn update(&mut self, msg: ChatMsg, ctx: &ChatCtx) -> Task<Message> {
        match msg {
            ChatMsg::BusReady(tx) => {
                self.bus = Some(tx);
                if self.cfg.auto_connect && !ctx.demo_mode && self.sources_loaded {
                    return self.connect_all(ctx);
                }
                Task::none()
            }
            ChatMsg::BusEvent(gen, ev) => {
                self.apply_bus(gen, *ev);
                Task::none()
            }
            ChatMsg::StartupLoad => restore_task(ctx.db_path.clone()),
            ChatMsg::SourcesRestored { sources, db_path } => {
                // Bound to the originating database: a restore racing a
                // demo switch must not plant sources in the new file.
                if db_path != ctx.db_path {
                    return Task::none();
                }
                self.sources_loaded = true;
                for s in sources {
                    self.activate_source(s);
                }
                if self.cfg.auto_connect && !ctx.demo_mode && self.bus.is_some() {
                    return self.connect_all(ctx);
                }
                Task::none()
            }
            ChatMsg::ProposalsRefreshed => {
                let selected: std::collections::HashSet<String> = self
                    .proposals
                    .iter()
                    .filter(|p| p.selected)
                    .map(|p| p.binding_id.clone())
                    .collect();
                self.proposals = ctx.proposals.clone();
                for p in &mut self.proposals {
                    p.selected = selected.contains(&p.binding_id);
                }
                Task::none()
            }
            ChatMsg::ToggleProposal(id) => {
                if let Some(p) = self.proposals.iter_mut().find(|p| p.binding_id == id) {
                    p.selected = !p.selected;
                }
                Task::none()
            }
            ChatMsg::ConnectSource(id) => self.connect_source(id, ctx),
            ChatMsg::SourceUpserted {
                source,
                session,
                room,
            } => {
                // Late results from a previous session/database — or a
                // source disconnected/rebound since dispatch — are
                // dropped: they must not activate sources or persist.
                if session != self.coord.session_gen() {
                    return Task::none();
                }
                if self.coord.room_gen_of(&source.id).unwrap_or(0) != room {
                    return Task::none();
                }
                let id = source.id.clone();
                self.activate_source(source.clone());
                self.note(format!("Source {} active.", id));
                self.persist_source(ctx.db_path.clone(), source)
            }
            ChatMsg::ConnectFailed {
                source_id,
                error,
                session,
                room,
            } => {
                if session != self.coord.session_gen() {
                    return Task::none();
                }
                if self.coord.room_gen_of(&source_id).unwrap_or(0) != room {
                    return Task::none();
                }
                self.coord.set_conn(
                    self.coord.session_gen(),
                    &source_id,
                    ConnState::Offline {
                        reason: error.clone(),
                    },
                );
                self.note(format!("Connect failed: {error}"));
                Task::none()
            }
            ChatMsg::Persisted(Err(e)) => {
                self.note(format!("Source persist failed: {e}"));
                Task::none()
            }
            ChatMsg::Persisted(Ok(_)) => Task::none(),
            ChatMsg::DisconnectSource(id) => {
                self.stop.disarm(&id);
                self.gate.cancel_source(&id);
                self.gate.remove_room(&id);
                // Advance the room generation: pending connect results
                // and late loop events for the old generation are dropped.
                self.coord.rebind_room(&id);
                let gen = self.coord.session_gen();
                self.coord.set_conn(
                    gen,
                    &id,
                    ConnState::Offline {
                        reason: "disconnected by operator".into(),
                    },
                );
                Task::none()
            }
            ChatMsg::DisconnectAll => {
                let gen = self.coord.session_gen() + 1;
                let _ = self.stop.session.send(gen);
                self.stop.rooms.lock().map(|mut m| m.clear()).ok();
                self.gate.clear();
                self.gate.clear_rooms();
                self.coord.reset_for_new_session();
                self.gate.set_session(self.coord.session_gen());
                self.note("All chat sources disconnected.");
                Task::none()
            }
            ChatMsg::RetrySource(id) => self.connect_source(id, ctx),
            ChatMsg::SessionReset => {
                // Full isolation across demo/database switches: in-flight
                // sends are cancelled, sources/drafts and in-memory auth
                // sessions do not cross the boundary (tokens stay sealed
                // in the encrypted store).
                let gen = self.coord.session_gen() + 1;
                let _ = self.stop.session.send(gen);
                self.stop.rooms.lock().map(|mut m| m.clear()).ok();
                self.gate.clear();
                self.gate.clear_rooms();
                self.coord.reset_for_new_session();
                self.gate.set_session(self.coord.session_gen());
                for id in self
                    .coord
                    .sources()
                    .into_iter()
                    .map(|(s, _)| s.id.clone())
                    .collect::<Vec<_>>()
                {
                    self.coord.remove_source(&id);
                }
                self.auth.clear();
                self.auth_gen += 1;
                self.gate.set_auth(self.auth_gen);
                for a in ctx.accounts.iter() {
                    self.auth_kill.kill(&a.id, self.auth_gen);
                }
                self.demo_loaded = false;
                self.note("Chat session reset.");
                Task::none()
            }
            ChatMsg::ComposerSource(id) => {
                self.coord.set_composer_source(&id);
                Task::none()
            }
            ChatMsg::DraftChanged(t) => {
                if let Some(id) = self.coord.composer_source().map(str::to_owned) {
                    self.coord.set_draft(&id, t);
                }
                Task::none()
            }
            ChatMsg::ReplyTo(seq) => {
                let found = self
                    .coord
                    .timeline()
                    .iter()
                    .filter_map(|ev| match ev {
                        TimelineEvent::Message(m) if m.seq == seq => Some(m.clone()),
                        _ => None,
                    })
                    .next();
                if let Some(m) = found {
                    match (m.remote_id.clone(), source_id_of(&self.coord, &m.source)) {
                        (Some(rid), Some(sid)) => {
                            self.coord.set_reply(&sid, &rid, &m.author_name);
                        }
                        _ => self.note(if m.remote_id.is_none() {
                            "No stable id: plain message instead of native reply."
                        } else {
                            "Reply target is gone."
                        }),
                    }
                }
                Task::none()
            }
            ChatMsg::ClearReply => {
                self.coord.clear_reply();
                Task::none()
            }
            ChatMsg::HideMessage(seq) => {
                self.coord.hide_local(seq);
                Task::none()
            }
            ChatMsg::CopyMessage(seq) => {
                let text = self
                    .coord
                    .timeline()
                    .iter()
                    .filter_map(|ev| match ev {
                        TimelineEvent::Message(m) if m.seq == seq => Some(format!(
                            "[{}] {}: {}",
                            m.source.label(),
                            m.author_name,
                            m.text
                        )),
                        _ => None,
                    })
                    .next()
                    .unwrap_or_default();
                if text.is_empty() {
                    return Task::none();
                }
                Task::batch([
                    iced::clipboard::write::<Message>(text),
                    Task::done(Message::Chat(ChatMsg::Copied)),
                ])
            }
            ChatMsg::Copied => {
                self.note("Copied.");
                Task::none()
            }
            ChatMsg::ConfirmDelete(seq) => {
                self.confirm_delete = Some(seq);
                Task::none()
            }
            ChatMsg::DeleteCancelled => {
                self.confirm_delete = None;
                Task::none()
            }
            ChatMsg::DeleteConfirmed => {
                let Some(seq) = self.confirm_delete.take() else {
                    return Task::none();
                };
                self.delete_message(seq, ctx)
            }
            ChatMsg::ConfirmBan(source_id, user) => {
                self.confirm_ban = Some((source_id, user));
                Task::none()
            }
            ChatMsg::BanCancelled => {
                self.confirm_ban = None;
                Task::none()
            }
            ChatMsg::BanConfirmed => {
                let Some((source_id, user)) = self.confirm_ban.take() else {
                    return Task::none();
                };
                self.ban_user(source_id, user, None, ctx)
            }
            ChatMsg::TimeoutUser(source_id, user, secs) => {
                self.ban_user(source_id, user, Some(secs), ctx)
            }
            ChatMsg::ModDone { label, result } => {
                match result {
                    Ok(detail) => {
                        self.note(format!("{label}: {detail}"));
                        if let Some((seq, sid, rid)) = self.pending_remote_delete.take() {
                            if detail.starts_with("deleted") {
                                self.coord.apply_delete(&sid, &rid);
                            }
                            let _ = seq;
                        }
                    }
                    Err(e) => self.note(format!("{label} failed: {e}")),
                }
                Task::none()
            }
            ChatMsg::SendNow => self.send_now(ctx, false),
            ChatMsg::CancelIntent(id) => {
                // Gate only: the spawned task admits (or refuses) and the
                // outcome resolves the intent. Marking the coordinator
                // terminal here would freeze a later Confirmed verdict as
                // "rejected" for a message that was actually sent.
                if self.gate.cancel(&id) {
                    self.send_note = "Cancel requested — in-flight send will not dispatch.".into();
                } else {
                    self.send_note = "Nothing cancellable for that send.".into();
                }
                Task::none()
            }
            ChatMsg::SendResolved { intent_id, outcome } => {
                match outcome {
                    SendOutcome::Confirmed {
                        remote_id,
                        session_moved,
                        note,
                    } => {
                        // Admission and mid-flight moves settle inside the
                        // executor via the gate; the flags arrive here.
                        let annotation = session_moved
                            .then(|| {
                                "sent; cancelled or moved mid-flight — verify before acting"
                                    .to_string()
                            })
                            .or(note);
                        self.coord.resolve_intent(
                            &intent_id,
                            IntentState::Confirmed,
                            remote_id,
                            annotation.clone(),
                        );
                        self.send_note = match annotation {
                            Some(a) => format!("Sent ({a})."),
                            None => "Sent.".into(),
                        };
                    }
                    SendOutcome::Rejected { error } => {
                        self.coord.resolve_intent(
                            &intent_id,
                            IntentState::Rejected,
                            None,
                            Some(error.clone()),
                        );
                        self.send_note = format!("Rejected: {error}");
                    }
                    SendOutcome::Unknown { error } => {
                        self.coord.resolve_intent(
                            &intent_id,
                            IntentState::OutcomeUnknown,
                            None,
                            Some(error.clone()),
                        );
                        self.send_note = format!("Outcome unknown (not retried): {error}");
                    }
                }
                Task::none()
            }
            ChatMsg::ToggleMulti(on) => {
                self.coord.set_multitarget_enabled(on);
                self.multitarget_confirm = false;
                Task::none()
            }
            ChatMsg::ToggleRecipient(id) => {
                self.coord.toggle_multitarget_recipient(&id);
                Task::none()
            }
            ChatMsg::MultiText(t) => {
                self.coord.set_multitarget_text(t);
                Task::none()
            }
            ChatMsg::MultiPreview => {
                self.multitarget_confirm = true;
                Task::none()
            }
            ChatMsg::MultiBack => {
                self.multitarget_confirm = false;
                Task::none()
            }
            ChatMsg::MultiConfirm => {
                self.multitarget_confirm = false;
                self.send_now(ctx, true)
            }
            ChatMsg::ToggleProviderFilter(p) => {
                let v = !self.show_providers.get(&p).copied().unwrap_or(true);
                self.show_providers.insert(p, v);
                Task::none()
            }
            ChatMsg::QueryChanged(q) => {
                self.query = q;
                Task::none()
            }
            ChatMsg::ToggleMentions => {
                self.mentions_only = !self.mentions_only;
                Task::none()
            }
            ChatMsg::TogglePaid => {
                self.paid_only = !self.paid_only;
                Task::none()
            }
            ChatMsg::TextSize(i) => {
                self.scale_idx = i.min(2);
                Task::none()
            }
            ChatMsg::SelectMessage(seq) => {
                self.selected = Some(seq);
                self.details_open = true;
                // Inspecting history pauses follow; Jump to live resumes.
                self.coord.set_following(false);
                Task::none()
            }
            ChatMsg::SelectPrev | ChatMsg::SelectNext => {
                let seqs: Vec<u64> = self
                    .visible(ctx)
                    .into_iter()
                    .filter_map(|ev| match ev {
                        TimelineEvent::Message(m) => Some(m.seq),
                        _ => None,
                    })
                    .collect();
                if seqs.is_empty() {
                    return Task::none();
                }
                let next = match self
                    .selected
                    .and_then(|s| seqs.iter().position(|x| *x == s))
                {
                    Some(i) if matches!(msg, ChatMsg::SelectPrev) => {
                        seqs[(i + seqs.len().saturating_sub(1)) % seqs.len()]
                    }
                    Some(i) => seqs[(i + 1) % seqs.len()],
                    None => match msg {
                        ChatMsg::SelectPrev => *seqs.last().unwrap(),
                        _ => seqs[0],
                    },
                };
                self.selected = Some(next);
                self.details_open = true;
                self.coord.set_following(false);
                Task::none()
            }
            ChatMsg::ShowOlder => {
                self.render_window = self.render_window.saturating_add(300).min(5_000);
                Task::none()
            }
            ChatMsg::ToggleDetails => {
                self.details_open = !self.details_open;
                Task::none()
            }
            ChatMsg::ToggleCompact => {
                self.compact = !self.compact;
                Task::none()
            }
            ChatMsg::Follow(on) => {
                self.coord.set_following(on);
                Task::none()
            }
            ChatMsg::JumpLive => {
                self.coord.set_following(true);
                Task::none()
            }
            ChatMsg::LoadDemoChat => {
                if ctx.demo_mode {
                    let (ok, dup, tomb) = stream_manager_chat::demo::run_demo(&mut self.coord);
                    self.demo_loaded = true;
                    self.note(format!(
                        "Demo feed: {ok} shown, {dup} duplicates suppressed, {tomb} tombstoned."
                    ));
                } else {
                    self.note("Demo feed is only available in demo mode.");
                }
                Task::none()
            }
            ChatMsg::ClearChat => {
                self.coord.reset_for_new_session();
                self.note("Timeline cleared.");
                Task::none()
            }
            ChatMsg::OpenDetached => {
                if self.detached.is_some() {
                    return Task::none();
                }
                let (_id, open) = window::open(window::Settings::default());
                open.map(|id| Message::Chat(ChatMsg::DetachedOpened(id)))
            }
            ChatMsg::DetachedOpened(id) => {
                self.detached = Some(id);
                Task::none()
            }
            ChatMsg::DetachedClosed(id) => {
                if self.detached == Some(id) {
                    self.detached = None;
                }
                Task::none()
            }
            ChatMsg::AuthConnect {
                account_id,
                with_write,
                with_moderate,
            } => self.auth_task(account_id, with_write, with_moderate, ctx),
            ChatMsg::AuthResult {
                account_id,
                auth_gen,
                result,
            } => {
                if auth_gen != self.auth_gen {
                    return Task::none();
                }
                match result {
                    Ok(sess) => {
                        self.auth
                            .insert(account_id.clone(), AuthState::Connected(sess));
                        self.note(format!("Account {account_id} connected."));
                    }
                    Err(e) => {
                        // A locked/missing runtime key is a distinct,
                        // actionable state — never a plaintext fallback.
                        if e.contains("locked") {
                            self.auth
                                .insert(account_id, AuthState::Locked { reason: e.clone() });
                        } else {
                            self.auth
                                .insert(account_id, AuthState::Failed { error: e.clone() });
                        }
                        self.note(format!("Auth failed: {e}"));
                    }
                }
                self.gate.set_auth(self.auth_gen);
                Task::none()
            }
            ChatMsg::AuthDisconnect(account_id) => {
                self.auth_gen += 1;
                self.gate.set_auth(self.auth_gen);
                // In-flight OAuth tasks for this account must not persist
                // anything after this point.
                self.auth_kill.kill(&account_id, self.auth_gen);
                let access = match self.auth.remove(&account_id) {
                    Some(AuthState::Connected(s)) => Some(s.access.clone()),
                    _ => None,
                };
                // Cancel affected loops and any queued sends; stale
                // results die on room mismatch.
                let ids: Vec<String> = self
                    .coord
                    .sources()
                    .into_iter()
                    .filter(|(s, _)| s.account_id.as_deref() == Some(account_id.as_str()))
                    .map(|(s, _)| s.id.clone())
                    .collect();
                let gen = self.coord.session_gen();
                for id in ids {
                    self.stop.disarm(&id);
                    self.gate.cancel_source(&id);
                    self.gate.remove_room(&id);
                    self.coord.rebind_room(&id);
                    self.coord.set_conn(
                        gen,
                        &id,
                        ConnState::Offline {
                            reason: "logged out".into(),
                        },
                    );
                }
                // Local state is already cleared above; the task only
                // revokes at the provider and deletes the stored refresh.
                // Logout never depends on the network succeeding.
                let logout = self.logout_task(&account_id, access, ctx);
                self.note("Account disconnected; revoking and forgetting credentials…");
                logout
            }
            ChatMsg::LogoutDone { account_id, result } => {
                match result {
                    Ok(detail) => self.note(format!("Account {account_id} logged out ({detail}).")),
                    Err(e) => self.note(format!("Account {account_id} logged out locally; {e}")),
                }
                Task::none()
            }
            ChatMsg::ServerChecked(result) => {
                match result {
                    Ok(detail) => self.note(format!("Central server: {detail}.")),
                    Err(e) => self.note(format!("Central server: {e}.")),
                }
                Task::none()
            }
            ChatMsg::ServerCheck => {
                let url = self.cfg.server_url.clone();
                if url.is_empty() {
                    self.note("No central server configured (standalone desktop mode).");
                    return Task::none();
                }
                self.note(format!("Checking {url}…"));
                Task::perform(
                    async move {
                        let http = reqwest::Client::new();
                        match http
                            .get(format!("{url}/health"))
                            .timeout(std::time::Duration::from_secs(5))
                            .send()
                            .await
                        {
                            Ok(res) => {
                                let v: serde_json::Value = res.json().await.unwrap_or_default();
                                if v.get("ok").and_then(|x| x.as_bool()) == Some(true) {
                                    Ok(format!(
                                        "server {} (protocol v{}) reachable",
                                        v.get("instance").and_then(|x| x.as_str()).unwrap_or("?"),
                                        v.get("protocol").and_then(|x| x.as_u64()).unwrap_or(0),
                                    ))
                                } else {
                                    Err("unhealthy server response".into())
                                }
                            }
                            Err(e) => Err(format!("unreachable: {e}")),
                        }
                    },
                    |r| Message::Chat(ChatMsg::ServerChecked(r)),
                )
            }
            ChatMsg::InsertCta => {
                if let Some(text) = &ctx.cta_text {
                    if let Some(id) = self.coord.composer_source().map(str::to_owned) {
                        let mut draft = self.coord.draft_of(&id).to_owned();
                        if !draft.is_empty() {
                            draft.push(' ');
                        }
                        draft.push_str(text);
                        self.coord.set_draft(&id, draft);
                    }
                }
                Task::none()
            }
        }
    }
}

fn source_id_of(coord: &Coordinator, key: &SourceKey) -> Option<String> {
    coord
        .sources()
        .into_iter()
        .find(|(s, _)| &s.key == key)
        .map(|(s, _)| s.id.clone())
}

fn provider_of_slug(slug: &str) -> Option<Provider> {
    match slug {
        "youtube" => Some(Provider::Youtube),
        "twitch" => Some(Provider::Twitch),
        "kick" => Some(Provider::Kick),
        "rumble" => Some(Provider::Rumble),
        _ => None,
    }
}

/// Restore persisted chat sources: resolve each row's provider through
/// its destination binding (never through naming).
fn restore_task(db_path: String) -> Task<Message> {
    let db_tag = db_path.clone();
    Task::perform(
        async move {
            tokio::task::spawn_blocking(move || -> Result<Vec<ChatSource>, String> {
                let conn = stream_manager_core::store::open(&db_path).map_err(|e| e.to_string())?;
                let bindings =
                    stream_manager_core::store::list_bindings(&conn).map_err(|e| e.to_string())?;
                let by_id: HashMap<String, String> =
                    bindings.into_iter().map(|b| (b.id, b.provider)).collect();
                let rows = stream_manager_core::store::list_chat_sources(&conn)
                    .map_err(|e| e.to_string())?;
                Ok(rows
                    .into_iter()
                    .filter(|r| r.enabled)
                    .filter_map(|r| {
                        let provider =
                            by_id.get(&r.binding_id).and_then(|s| provider_of_slug(s))?;
                        Some(ChatSource {
                            key: SourceKey {
                                provider,
                                room: r.room_id.clone().unwrap_or_else(|| r.channel.clone()),
                            },
                            id: r.id,
                            account_id: r.account_id,
                            channel: r.channel,
                            broadcast_id: r.room_id,
                            binding_ids: vec![r.binding_id],
                        })
                    })
                    .collect())
            })
            .await
            .map_err(|e| format!("join: {e}"))?
        },
        move |r| {
            Message::Chat(ChatMsg::SourcesRestored {
                sources: r.unwrap_or_default(),
                db_path: db_tag.clone(),
            })
        },
    )
}

impl ChatPanel {
    fn persist_source(&self, db_path: String, source: ChatSource) -> Task<Message> {
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    let conn =
                        stream_manager_core::store::open(&db_path).map_err(|e| e.to_string())?;
                    stream_manager_core::store::save_chat_source(
                        &conn,
                        &stream_manager_core::store::ChatSourceRow {
                            id: source.id.clone(),
                            binding_id: source
                                .binding_ids
                                .first()
                                .cloned()
                                .unwrap_or_else(|| source.id.clone()),
                            account_id: source.account_id.clone(),
                            channel: source.channel.clone(),
                            room_id: Some(source.key.room.clone()),
                            enabled: true,
                        },
                    )
                    .map_err(|e| e.to_string())?;
                    Ok(source.id.clone())
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            |r| Message::Chat(ChatMsg::Persisted(r)),
        )
    }

    fn connect_all(&mut self, ctx: &ChatCtx) -> Task<Message> {
        let ids: Vec<String> = self
            .coord
            .sources()
            .into_iter()
            .map(|(s, _)| s.id.clone())
            .collect();
        let mut tasks = vec![];
        for id in ids {
            tasks.push(self.connect_source(id, ctx));
        }
        Task::batch(tasks)
    }

    /// Explicit operator action: propose nothing, activate one source.
    fn connect_source(&mut self, id: String, ctx: &ChatCtx) -> Task<Message> {
        // Resolve from active sources first, else from the plan proposal.
        enum Target {
            Active(ChatSource),
            Proposed(Proposal),
        }
        let target = self
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == id)
            .map(|(s, _)| Target::Active(s.clone()))
            .or_else(|| {
                self.proposals
                    .iter()
                    .find(|p| p.binding_id == id || format!("src-{}", p.binding_id) == id)
                    .cloned()
                    .map(Target::Proposed)
            });
        let (provider, account_id, channel, source_id, room_gen, binding_id, target) = match target
        {
            Some(Target::Active(s)) => {
                let gen = self.coord.room_gen_of(&s.id).unwrap_or(0);
                (
                    s.key.provider,
                    s.account_id.clone(),
                    s.channel.clone(),
                    s.id.clone(),
                    gen,
                    s.binding_ids.first().cloned(),
                    None,
                )
            }
            Some(Target::Proposed(p)) => (
                p.provider,
                p.account_id.clone(),
                p.channel.clone(),
                format!("src-{}", p.binding_id),
                0,
                Some(p.binding_id.clone()),
                p.remote_id.clone(),
            ),
            None => {
                self.note("Unknown source.");
                return Task::none();
            }
        };
        if ctx.demo_mode {
            self.note("Demo mode: live connections stay off; use Load demo chat.");
            return Task::none();
        }
        // No duplicate workers: a receiving source owns its loop and its
        // Helix subscriptions. Reconnect only after disconnect.
        if let Some(state) = self.coord.conn_of(&source_id) {
            if state.is_receiving() {
                self.note(format!("Source {source_id} is already connected."));
                return Task::none();
            }
        }
        let Some(sink) = self.bus.clone() else {
            self.note("Chat bus not ready yet.");
            return Task::none();
        };
        // Kick without relay access degrades to a visible state, not silence.
        if provider == Provider::Kick && self.cfg.relay_url.trim().is_empty() {
            let source = ChatSource {
                id: source_id.clone(),
                key: SourceKey {
                    provider,
                    room: channel.clone(),
                },
                account_id: account_id.clone(),
                channel: channel.clone(),
                broadcast_id: None,
                binding_ids: binding_id.clone().into_iter().collect(),
            };
            self.activate_source(source.clone());
            let gen = self.coord.session_gen();
            self.coord
                .set_conn(gen, &source_id, ConnState::RelayRequired);
            return self.persist_source(ctx.db_path.clone(), source);
        }
        // Authenticated providers need a connected account; Rumble does not.
        let auth = account_id
            .as_deref()
            .and_then(|a| self.auth.get(a))
            .cloned();
        let authed = match (&auth, provider) {
            (_, Provider::Rumble) => None,
            (Some(AuthState::Connected(s)), _) if !s.expired() => Some(s.clone()),
            (Some(AuthState::Locked { reason }), _) => {
                self.note(format!("Token store locked: {reason}"));
                return Task::none();
            }
            _ => {
                self.note(
                    "Account not connected — connect it first (reading needs no send scope).",
                );
                return Task::none();
            }
        };
        let session_gen = self.coord.session_gen();
        self.stop.arm(&source_id, room_gen);
        let params = ConnectParams {
            session_gen,
            room_gen,
            source_id,
            provider,
            account_id,
            channel,
            binding_id,
            target,
            authed,
            cfg: self.cfg.clone(),
            sink,
            stop: self.stop.clone(),
        };
        Task::perform(
            async move { connect_and_spawn(params).await },
            move |r| match r {
                Ok(source) => Message::Chat(ChatMsg::SourceUpserted {
                    source,
                    session: session_gen,
                    room: room_gen,
                }),
                Err((sid, e)) => Message::Chat(ChatMsg::ConnectFailed {
                    source_id: sid,
                    error: e,
                    session: session_gen,
                    room: room_gen,
                }),
            },
        )
    }

    fn delete_message(&mut self, seq: u64, _ctx: &ChatCtx) -> Task<Message> {
        let found = self
            .coord
            .timeline()
            .iter()
            .filter_map(|ev| match ev {
                TimelineEvent::Message(m) if m.seq == seq => Some(m.clone()),
                _ => None,
            })
            .next();
        let Some(m) = found else { return Task::none() };
        let Some(sid) = source_id_of(&self.coord, &m.source) else {
            return Task::none();
        };
        let Some(rid) = m.remote_id.clone() else {
            self.note("No stable id: hide locally instead of remote delete.");
            self.coord.hide_local(seq);
            return Task::none();
        };
        let caps = caps_of(m.source.provider);
        if !caps.delete.implemented {
            self.note(format!(
                "{} delete is not wired — hidden locally only.",
                m.source.provider.display()
            ));
            self.coord.hide_local(seq);
            return Task::none();
        }
        let src = self
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == sid)
            .map(|(s, _)| s.clone());
        let Some(src) = src else { return Task::none() };
        let account = match src
            .account_id
            .clone()
            .and_then(|a| self.auth.get(&a).cloned())
        {
            Some(AuthState::Connected(s)) if !s.expired() && s.can_moderate => s,
            _ => {
                self.note("Moderation needs a connected moderator session.");
                return Task::none();
            }
        };
        self.pending_remote_delete = Some((seq, sid.clone(), rid.clone()));
        let client_id = self.cfg.twitch_client_id.clone();
        Task::perform(
            async move {
                let http = reqwest::Client::new();
                delete_remote(&http, &client_id, m.source.provider, &account, &src, &rid)
                    .await
                    .map(|_| format!("deleted {rid}"))
                    .map_err(|e| e.to_string())
            },
            |r| {
                Message::Chat(ChatMsg::ModDone {
                    label: "Delete".into(),
                    result: r,
                })
            },
        )
    }

    fn ban_user(
        &mut self,
        source_id: String,
        user: String,
        duration_secs: Option<u64>,
        _ctx: &ChatCtx,
    ) -> Task<Message> {
        let src = self
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == source_id)
            .map(|(s, _)| s.clone());
        let Some(src) = src else { return Task::none() };
        if src.key.provider != Provider::Twitch {
            self.note("Remote ban is wired for Twitch only; other platforms stay local-hide.");
            return Task::none();
        }
        let account = match src
            .account_id
            .clone()
            .and_then(|a| self.auth.get(&a).cloned())
        {
            Some(AuthState::Connected(s)) if !s.expired() && s.can_moderate => s,
            _ => {
                self.note("Moderation needs a connected moderator session.");
                return Task::none();
            }
        };
        // Never ban across platforms: resolve the user id on THIS provider.
        let label = if duration_secs.is_some() {
            "Timeout"
        } else {
            "Ban"
        }
        .to_string();
        let client_id = self.cfg.twitch_client_id.clone();
        Task::perform(
            async move {
                let http = reqwest::Client::new();
                ban_remote(&http, &client_id, &account, &src, &user, duration_secs)
                    .await
                    .map(|_| format!("{label} applied to {user} on Twitch only"))
                    .map_err(|e| e.to_string())
            },
            |r| {
                Message::Chat(ChatMsg::ModDone {
                    label: "Moderation".into(),
                    result: r,
                })
            },
        )
    }

    /// Capture one outbound intent per target, each executed and reported
    /// independently. Multi-target is broadcaster-authored only — never a
    /// viewer-message relay.
    fn send_now(&mut self, ctx: &ChatCtx, multi: bool) -> Task<Message> {
        if ctx.demo_mode {
            self.send_note = "Demo mode: sending disabled.".into();
            return Task::none();
        }
        let text = if multi {
            self.coord.multitarget().text.clone()
        } else {
            self.coord
                .composer_source()
                .map(|id| self.coord.draft_of(id).to_owned())
                .unwrap_or_default()
        };
        if text.trim().is_empty() {
            self.send_note = "Nothing to send.".into();
            return Task::none();
        }
        let cta_used = ctx
            .cta_text
            .as_deref()
            .is_some_and(|cta| text.contains(cta));
        if cta_used {
            if let Some(blocked) = &ctx.cta_blocked {
                self.send_note = format!("CTA preset blocked by review: {blocked}");
                return Task::none();
            }
        }
        let targets: Vec<String> = if multi {
            if !self.coord.multitarget().enabled {
                self.send_note = "Multi-target mode is off.".into();
                return Task::none();
            }
            self.coord.multitarget().selected.clone()
        } else {
            match self.send_target(ctx) {
                Ok(s) => vec![s.id.clone()],
                Err(reason) => {
                    self.send_note = format!("Cannot send: {reason}");
                    return Task::none();
                }
            }
        };
        if targets.is_empty() {
            self.send_note = "No recipients selected.".into();
            return Task::none();
        }
        let reply = self.coord.reply().cloned();
        // A stale reply (rebound room, deleted message) never retargets.
        if let Some(r) = &reply {
            let fresh = self.coord.room_gen_of(&r.source_id);
            let still_visible = self.coord.timeline().iter().any(|ev| match ev {
                TimelineEvent::Message(m) => {
                    m.remote_id.as_deref() == Some(r.remote_id.as_str())
                        && matches!(m.mod_state, stream_manager_chat::model::ModState::Visible)
                }
                _ => false,
            });
            if fresh.is_none() || !still_visible {
                self.coord.clear_reply();
                self.send_note = "Reply target went stale — cleared, not sent.".into();
                return Task::none();
            }
        }
        let mut tasks = vec![];
        for target in targets {
            let src = match self
                .coord
                .sources()
                .into_iter()
                .find(|(s, _)| s.id == target)
                .map(|(s, _)| s.clone())
            {
                Some(s) => s,
                None => {
                    self.send_note = format!("Recipient gone: {target}");
                    continue;
                }
            };
            if let Err(reason) = self.check_sendable(&src, ctx) {
                self.send_note = format!("{}: {reason}", src.key.label());
                continue;
            }
            // A reply binds its own source; never send it cross-platform.
            let reply_to = match &reply {
                Some(r) if r.source_id == src.id => Some(stream_manager_chat::model::ReplyRef {
                    remote_id: r.remote_id.clone(),
                    author_name: r.author_name.clone(),
                }),
                Some(_) => {
                    self.send_note =
                        "Reply stays on its own platform; other targets send plain text.".into();
                    None
                }
                None => None,
            };
            let account = src
                .account_id
                .clone()
                .and_then(|a| self.auth.get(&a).cloned());
            let Some(AuthState::Connected(sess)) = account else {
                continue;
            };
            let identity = sess.identity.clone();
            let Some(intent) = self.coord.create_intent(
                &src.id,
                &identity,
                text.clone(),
                reply_to,
                vec![src.id.clone()],
            ) else {
                continue;
            };
            self.outbox.push_back(intent.id.clone());
            while self.outbox.len() > 20 {
                self.outbox.pop_front();
            }
            // Same store identity onboarding rotates: channel, not source.
            let key_id = src
                .account_id
                .as_deref()
                .and_then(|a| {
                    ctx.accounts
                        .iter()
                        .find(|x| x.id == a)
                        .map(|x| Self::key_id(src.key.provider, &x.channel_id))
                })
                .unwrap_or_default();
            let client_secret = provider_secret(&self.cfg, src.key.provider).ok().flatten();
            let provider_client_id = provider_client_id(&self.cfg, src.key.provider).to_owned();
            self.gate.issue(&intent.id, &src.id);
            let params = SendParams {
                intent,
                provider: src.key.provider,
                sess,
                broadcast: src.broadcast_id.clone(),
                locks: self.locks.clone(),
                gate: self.gate.clone(),
                store: FileTokenStore::new(&self.cfg.token_store_path, &self.cfg.token_key_path),
                key_id,
                cta_used,
                cta_blocked: ctx.cta_blocked.clone(),
                provider_client_id,
                client_secret,
            };
            tasks.push(Task::perform(
                async move { execute_send(params).await },
                |o| {
                    Message::Chat(ChatMsg::SendResolved {
                        intent_id: o.0,
                        outcome: o.1,
                    })
                },
            ));
        }
        if !multi {
            // Single sends clear the composer draft only after capture.
            if let Some(id) = self.coord.composer_source().map(str::to_owned) {
                self.coord.set_draft(&id, String::new());
            }
            self.coord.clear_reply();
        }
        Task::batch(tasks)
    }

    fn auth_task(
        &mut self,
        account_id: String,
        with_write: bool,
        with_moderate: bool,
        ctx: &ChatCtx,
    ) -> Task<Message> {
        let account = match ctx.accounts.iter().find(|a| a.id == account_id).cloned() {
            Some(a) => a,
            None => return Task::none(),
        };
        let Some(provider_oauth) = oauth_provider(account.provider) else {
            self.note("Rumble has no OAuth chat scope — read-only API URL instead.");
            return Task::none();
        };
        let gen = {
            self.auth_gen += 1;
            self.auth_gen
        };
        self.auth.insert(account_id.clone(), AuthState::Unknown);
        let cfg = self.cfg.clone();
        let locks = self.locks.clone();
        let kill = self.auth_kill.clone();
        Task::perform(
            async move {
                let out = run_auth_flow(AuthFlowParams {
                    account,
                    provider_oauth,
                    with_write,
                    with_moderate,
                    cfg,
                    locks,
                    kill,
                    kill_gen: gen,
                })
                .await;
                (account_id, gen, out)
            },
            |(account_id, auth_gen, result)| {
                Message::Chat(ChatMsg::AuthResult {
                    account_id,
                    auth_gen,
                    result,
                })
            },
        )
    }

    /// Logout task: revoke at the provider and delete the stored refresh.
    /// Memory is already cleared by the caller; failures only annotate.
    fn logout_task(
        &self,
        account_id: &str,
        access: Option<Secret>,
        ctx: &ChatCtx,
    ) -> Task<Message> {
        let account = match ctx.accounts.iter().find(|a| a.id == account_id).cloned() {
            Some(a) => a,
            None => return Task::none(),
        };
        let cfg = self.cfg.clone();
        let account_id = account_id.to_owned();
        Task::perform(
            async move {
                let http = reqwest::Client::new();
                let store = FileTokenStore::new(&cfg.token_store_path, &cfg.token_key_path);
                let key_id = Self::key_id(account.provider, &account.channel_id);
                let removed = store.remove(&key_id).map_err(|e| e.to_string());
                let revoked = match (oauth_provider(account.provider), access) {
                    (Some(o), Some(token)) if o.revoke_url.is_some() => {
                        let client_id = provider_client_id(&cfg, account.provider).to_owned();
                        oauth::revoke_access_token(
                            o.revoke_url.unwrap_or_default(),
                            &client_id,
                            &token,
                            &http,
                        )
                        .await
                        .map_err(|e| e.to_string())
                    }
                    _ => Ok(()),
                };
                let detail = match (removed, revoked) {
                    (Ok(()), Ok(())) => "revoked and forgotten".to_string(),
                    (Ok(()), Err(e)) => {
                        format!("stored token removed; provider revoke failed: {e}")
                    }
                    (Err(e), _) => format!("stored token removal failed: {e}"),
                };
                (account_id, Ok::<_, String>(detail))
            },
            |(account_id, result)| Message::Chat(ChatMsg::LogoutDone { account_id, result }),
        )
    }
}

// ---------- background executors (off the UI thread) ----------

struct ConnectParams {
    session_gen: u64,
    room_gen: u64,
    source_id: String,
    provider: Provider,
    account_id: Option<String>,
    channel: String,
    /// Real destination binding id (never the generated source id):
    /// restoration resolves providers through this binding.
    binding_id: Option<String>,
    /// Explicit stream/broadcast selection from the plan proposal.
    target: Option<String>,
    authed: Option<AuthSession>,
    cfg: ChatConfig,
    sink: EventSink,
    stop: SharedStop,
}

/// Resolve + spawn one transport loop. Returns the active source (with a
/// resolved broadcast id where the provider needs one).
async fn connect_and_spawn(p: ConnectParams) -> Result<ChatSource, (String, String)> {
    let ConnectParams {
        session_gen,
        room_gen,
        source_id,
        provider,
        account_id,
        channel,
        binding_id,
        target,
        authed,
        cfg,
        sink,
        stop,
    } = p;
    let fail = |e: String| (source_id.clone(), e);
    let http = reqwest::Client::new();
    let mut source = ChatSource {
        id: source_id.clone(),
        key: SourceKey {
            provider,
            room: channel.clone(),
        },
        account_id: account_id.clone(),
        channel: channel.clone(),
        broadcast_id: None,
        binding_ids: binding_id.into_iter().collect(),
    };
    let loop_ctx = |sink: EventSink, stop: SharedStop| stream_manager_chat::runtime::LoopCtx {
        gen: session_gen,
        room_gen,
        source_id: source_id.clone(),
        sink,
        stop,
    };
    match provider {
        Provider::Twitch => {
            let sess = authed.ok_or_else(|| fail("twitch needs a connected account".into()))?;
            // Resolve the broadcaster's numeric user id now so sends and
            // moderation never guess it later.
            let broadcaster_id =
                twitch_user_id(&http, &cfg.twitch_client_id, sess.access.expose(), &channel)
                    .await
                    .map_err(fail)?;
            source.broadcast_id = Some(broadcaster_id.clone());
            let user_id = sess.user_id.clone();
            let token = sess.access.expose().to_owned();
            let client_id = cfg.twitch_client_id.clone();
            let room = channel.clone();
            tokio::spawn(stream_manager_chat::runtime::twitch_loop(
                loop_ctx(sink, stop),
                room,
                user_id,
                broadcaster_id,
                token,
                client_id,
                http,
            ));
        }
        Provider::Youtube => {
            let sess = authed.ok_or_else(|| fail("youtube needs a connected account".into()))?;
            let live_chat_id = resolve_youtube_chat(&http, sess.access.expose(), target.as_deref())
                .await
                .map_err(fail)?;
            source.key.room = live_chat_id.clone();
            source.broadcast_id = Some(live_chat_id.clone());
            let token = sess.access.expose().to_owned();
            tokio::spawn(stream_manager_chat::runtime::youtube_loop(
                loop_ctx(sink, stop),
                live_chat_id,
                token,
                http,
            ));
        }
        Provider::Kick => {
            if cfg.relay_url.trim().is_empty() {
                return Err(fail("relay required: set kick_relay_url".into()));
            }
            tokio::spawn(stream_manager_chat::runtime::kick_relay_loop(
                loop_ctx(sink, stop),
                cfg.relay_url.clone(),
                cfg.relay_bearer_path.clone(),
                http,
                channel.clone(),
            ));
        }
        Provider::Rumble => {
            if cfg.rumble_api_path.trim().is_empty() {
                return Err(fail("rumble API URL file not configured (Settings)".into()));
            }
            tokio::spawn(stream_manager_chat::runtime::rumble_loop(
                loop_ctx(sink, stop),
                cfg.rumble_api_path.clone(),
                http,
                target,
            ));
        }
    }
    Ok(source)
}

async fn resolve_youtube_chat(
    http: &reqwest::Client,
    token: &str,
    expect_video: Option<&str>,
) -> Result<String, String> {
    let body: serde_json::Value = http
        .get("https://www.googleapis.com/youtube/v3/liveBroadcasts?part=id,snippet&broadcastStatus=active&broadcastType=all&mine=true")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("broadcast lookup failed: {e}"))?
        .json()
        .await
        .map_err(|e| format!("broadcast lookup unparsable: {e}"))?;
    youtube::resolve_live_chat_id(&body, expect_video)
}

struct AuthFlowParams {
    account: AccountView,
    provider_oauth: OAuthProvider,
    with_write: bool,
    with_moderate: bool,
    cfg: ChatConfig,
    locks: RefreshLocks,
    kill: AuthKill,
    kill_gen: u64,
}

/// Full account onboarding: stored refresh first (serialized per account),
/// otherwise the provider's browser authorization flow with state
/// validation, PKCE, exchange, and identity/scope verification. The kill
/// flag aborts persistence (and success) after logout or session reset.
async fn run_auth_flow(p: AuthFlowParams) -> Result<AuthSession, String> {
    let AuthFlowParams {
        account,
        provider_oauth,
        with_write,
        with_moderate,
        cfg,
        locks,
        kill,
        kill_gen,
    } = p;
    let http = reqwest::Client::new();
    let store = FileTokenStore::new(&cfg.token_store_path, &cfg.token_key_path);
    let key_id = ChatPanel::key_id(account.provider, &account.channel_id);
    let client_id = provider_client_id(&cfg, account.provider).to_owned();
    if client_id.trim().is_empty() {
        return Err("OAuth client id not configured (Settings)".into());
    }
    let dead = || kill.is_dead(&account.id, kill_gen);
    // Fast path: rotate the stored refresh token under the store-identity
    // lock (same key sends use, so onboarding and send refreshes
    // serialize instead of racing rotation).
    if store.load_refresh(&key_id).is_ok() {
        let lock = locks.lock_for(&key_id);
        let _guard = lock.lock().await;
        // Re-read inside the lock: a concurrent refresh may have won.
        if let Ok(refresh) = store.load_refresh(&key_id) {
            let secret = provider_secret(&cfg, account.provider)?;
            match oauth::refresh_access_token(
                provider_oauth.token_url,
                &client_id,
                secret.as_ref(),
                &refresh,
                &http,
            )
            .await
            {
                Ok(r) => {
                    if dead() {
                        return Err("auth cancelled (logged out)".into());
                    }
                    // Verify identity AND scopes before persisting
                    // anything: a refresh token that authenticates a
                    // different account must never rotate this one's
                    // stored credential.
                    match verify_session(&http, account.provider, &r.access_token, &r.scope).await {
                        Ok(v)
                            if scopes_sufficient(
                                account.provider,
                                &v.scopes,
                                with_write,
                                with_moderate,
                            ) =>
                        {
                            if !identity_matches(&account, &v) {
                                return Err("provider identity does not match this account".into());
                            }
                            if dead() {
                                return Err("auth cancelled (logged out)".into());
                            }
                            if let Some(new_refresh) = r.new_refresh.clone() {
                                store
                                    .store_rotated(&key_id, new_refresh)
                                    .map_err(|e| e.to_string())?;
                            }
                            return Ok(finish_session(account.provider, v, r));
                        }
                        Ok(_) => {}
                        Err(e) => {
                            if !is_invalid_grant(&e) {
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    if !is_invalid_grant(&format!("{e:#}")) {
                        return Err(e.to_string());
                    }
                }
            }
        }
    }
    // Browser path: PKCE + state, loopback callback, exchange, verify.
    browser_auth_flow(BrowserFlow {
        http: &http,
        account: &account,
        provider_oauth: &provider_oauth,
        with_write,
        with_moderate,
        cfg: &cfg,
        store: &store,
        key_id: &key_id,
        locks: &locks,
        kill: &kill,
        kill_gen,
    })
    .await
}

/// The verified provider identity must belong to the account being
/// onboarded: exact user-id match, or case-insensitive login match for
/// providers whose display names vary. Anything else refuses before any
/// credential is persisted.
fn identity_matches(account: &AccountView, v: &Verified) -> bool {
    v.user_id == account.channel_id
        || (!v.identity.is_empty() && v.identity.eq_ignore_ascii_case(&account.channel_name))
}

fn provider_secret(cfg: &ChatConfig, p: Provider) -> Result<Option<Secret>, String> {
    if !oauth_provider(p).is_some_and(|o| o.requires_client_secret) {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&cfg.kick_secret_path)
        .map_err(|_| "kick client secret file unreadable (Settings)".to_string())?;
    let trimmed = raw.trim().to_owned();
    if trimmed.is_empty() {
        return Err("kick client secret file is empty".into());
    }
    Ok(Some(Secret::new(trimmed)))
}

fn is_invalid_grant(e: &str) -> bool {
    e.contains("invalid_grant")
}

struct Verified {
    identity: String,
    user_id: String,
    scopes: Vec<String>,
}

async fn verify_session(
    http: &reqwest::Client,
    provider: Provider,
    access: &Secret,
    scope_hint: &str,
) -> Result<Verified, String> {
    match provider {
        Provider::Twitch => {
            let body: serde_json::Value = http
                .get("https://id.twitch.tv/oauth2/validate")
                .bearer_auth(access.expose())
                .send()
                .await
                .map_err(|e| format!("twitch validate failed: {e}"))?
                .json()
                .await
                .map_err(|e| format!("twitch validate unparsable: {e}"))?;
            let id = oauth::parse_twitch_validate(&body)?;
            Ok(Verified {
                identity: id.login,
                user_id: id.user_id,
                scopes: id.scopes,
            })
        }
        Provider::Youtube => {
            // Identity …
            let me_body: serde_json::Value = http
                .get("https://www.googleapis.com/youtube/v3/channels?part=id,snippet&mine=true&maxResults=1")
                .bearer_auth(access.expose())
                .send()
                .await
                .map_err(|e| format!("youtube identity failed: {e}"))?
                .json()
                .await
                .map_err(|e| format!("youtube identity unparsable: {e}"))?;
            let (user_id, identity) = me_body["items"]
                .as_array()
                .and_then(|a| a.first())
                .map(|i| {
                    (
                        i["id"].as_str().unwrap_or("?").to_owned(),
                        i["snippet"]["title"].as_str().unwrap_or("?").to_owned(),
                    )
                })
                .ok_or_else(|| "youtube: no owned channel".to_string())?;
            // … plus granted scopes (recorded, never assumed).
            let info: serde_json::Value = http
                .get("https://www.googleapis.com/oauth2/v3/tokeninfo")
                .bearer_auth(access.expose())
                .send()
                .await
                .map_err(|e| format!("youtube tokeninfo failed: {e}"))?
                .json()
                .await
                .map_err(|e| format!("youtube tokeninfo unparsable: {e}"))?;
            let scopes = oauth::parse_youtube_tokeninfo(&info)?;
            Ok(Verified {
                identity,
                user_id,
                scopes,
            })
        }
        Provider::Kick => {
            let body: serde_json::Value = http
                .get("https://api.kick.com/public/v1/users")
                .bearer_auth(access.expose())
                .send()
                .await
                .map_err(|e| format!("kick users failed: {e}"))?
                .json()
                .await
                .map_err(|e| format!("kick users unparsable: {e}"))?;
            let id = oauth::parse_kick_users(&body)?;
            // Kick's users endpoint does not echo scopes: the granted
            // scope string from the token response is authoritative.
            let scopes: Vec<String> = scope_hint.split_whitespace().map(str::to_owned).collect();
            Ok(Verified {
                identity: id.login,
                user_id: id.user_id,
                scopes,
            })
        }
        Provider::Rumble => Err("rumble has no OAuth chat scope".into()),
    }
}

fn scopes_sufficient(
    provider: Provider,
    scopes: &[String],
    with_write: bool,
    with_moderate: bool,
) -> bool {
    let Some(o) = oauth_provider(provider) else {
        return false;
    };
    let joined = scopes.join(" ");
    if !oauth::scopes_cover(&joined, o.chat_read_scopes) {
        return false;
    }
    if with_write && !oauth::scopes_cover(&joined, o.chat_write_scopes) {
        return false;
    }
    if with_moderate && !oauth::scopes_cover(&joined, o.moderation_scopes) {
        return false;
    }
    true
}

fn finish_session(provider: Provider, v: Verified, r: oauth::Refreshed) -> AuthSession {
    let joined = v.scopes.join(" ");
    let (can_write, can_moderate) = match oauth_provider(provider) {
        Some(o) => (
            oauth::scopes_cover(&joined, o.chat_write_scopes),
            oauth::scopes_cover(&joined, o.moderation_scopes),
        ),
        None => (false, false),
    };
    AuthSession {
        can_moderate,
        identity: v.identity,
        user_id: v.user_id,
        scopes: v.scopes,
        can_write,
        access: r.access_token,
        expires_at: chrono::Utc::now().timestamp() + r.expires_in as i64,
        refresh_present: r.new_refresh.is_some(),
    }
}

/// Helix user lookup on THIS provider only: login -> numeric user id.
/// Used at connect so sends/moderation address the intended channel.
async fn twitch_user_id(
    http: &reqwest::Client,
    client_id: &str,
    token: &str,
    login: &str,
) -> Result<String, String> {
    let body: serde_json::Value = http
        .get("https://api.twitch.tv/helix/users")
        .header("Client-Id", client_id)
        .bearer_auth(token)
        .query(&[("login", login.to_lowercase())])
        .send()
        .await
        .map_err(|e| format!("twitch user lookup failed: {e}"))?
        .json()
        .await
        .map_err(|e| format!("twitch user lookup unparsable: {e}"))?;
    body["data"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|u| u["id"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| format!("no such twitch channel: {login}"))
}

struct BrowserFlow<'a> {
    http: &'a reqwest::Client,
    account: &'a AccountView,
    provider_oauth: &'a OAuthProvider,
    with_write: bool,
    with_moderate: bool,
    cfg: &'a ChatConfig,
    store: &'a FileTokenStore,
    key_id: &'a str,
    locks: &'a RefreshLocks,
    kill: &'a AuthKill,
    kill_gen: u64,
}

async fn browser_auth_flow(f: BrowserFlow<'_>) -> Result<AuthSession, String> {
    let BrowserFlow {
        http,
        account,
        provider_oauth,
        with_write,
        with_moderate,
        cfg,
        store,
        key_id,
        locks,
        kill,
        kill_gen,
    } = f;
    let dead = || kill.is_dead(&account.id, kill_gen);
    let client_id = provider_client_id(cfg, account.provider).to_owned();
    let req = oauth::AuthRequest::begin(
        provider_oauth,
        &client_id,
        &cfg.redirect_uri,
        with_write,
        with_moderate,
    );
    // Bind the loopback callback BEFORE opening the browser: otherwise
    // the provider can redirect before anyone is listening.
    let port = redirect_port(&cfg.redirect_uri)?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|e| format!("loopback bind failed: {e}"))?;
    open_url(&req.authorize_url).await?;
    let (code, state) = tokio::time::timeout(
        std::time::Duration::from_secs(180),
        accept_loopback(&listener),
    )
    .await
    .map_err(|_| "authorization timed out".to_string())?
    .map_err(|e| e.to_string())?;
    let exchange = req.accept_callback(&state, &code)?;
    let secret = provider_secret(cfg, account.provider)?;
    let params = oauth::ExchangeParams {
        token_url: provider_oauth.token_url,
        requires_client_secret: provider_oauth.requires_client_secret,
        client_id: &client_id,
        client_secret: secret.as_ref(),
        redirect_uri: &cfg.redirect_uri,
        code: &code,
        code_verifier: exchange.code_verifier(),
    };
    let r = oauth::exchange_code(&params, http)
        .await
        .map_err(|e| e.to_string())?;
    if dead() {
        return Err("auth cancelled (logged out)".into());
    }
    // Verify identity AND scopes before persisting anything: the grant
    // must belong to the account being onboarded.
    let v = verify_session(http, account.provider, &r.access_token, &r.scope).await?;
    if !scopes_sufficient(account.provider, &v.scopes, with_write, with_moderate) {
        return Err(
            "granted scopes are insufficient — re-authorize with the requested scopes".into(),
        );
    }
    if !identity_matches(account, &v) {
        return Err("provider identity does not match this account".into());
    }
    if dead() {
        return Err("auth cancelled (logged out)".into());
    }
    if let Some(new_refresh) = r.new_refresh.clone() {
        let lock = locks.lock_for(key_id);
        let _guard = lock.lock().await;
        if dead() {
            return Err("auth cancelled (logged out)".into());
        }
        store
            .store_rotated(key_id, new_refresh)
            .map_err(|e| e.to_string())?;
    }
    Ok(finish_session(account.provider, v, r))
}

fn redirect_port(redirect_uri: &str) -> Result<u16, String> {
    let url = url::Url::parse(redirect_uri).map_err(|e| format!("bad redirect uri: {e}"))?;
    if url.host_str() != Some("127.0.0.1") && url.host_str() != Some("localhost") {
        return Err("redirect must be loopback".into());
    }
    url.port()
        .ok_or_else(|| "redirect needs an explicit port".to_string())
}

async fn accept_loopback(listener: &tokio::net::TcpListener) -> Result<(String, String), String> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let (mut sock, _) = listener
        .accept()
        .await
        .map_err(|e| format!("loopback accept failed: {e}"))?;
    let mut buf = vec![0u8; 4096];
    let n = sock
        .read(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;
    let head = String::from_utf8_lossy(&buf[..n]);
    let line = head.lines().next().unwrap_or_default();
    let path = line.split_whitespace().nth(1).unwrap_or_default();
    let query = path.split_once('?').map(|(_, q)| q).unwrap_or_default();
    let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let page = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n<html><body>Authorized - return to stream-manager.</body></html>";
    let _ = sock.write_all(page).await;
    match (params.get("code"), params.get("state")) {
        (Some(code), Some(state)) => Ok((code.clone(), state.clone())),
        _ => Err(params
            .get("error")
            .cloned()
            .unwrap_or_else(|| "callback missing code/state".into())),
    }
}

async fn open_url(url: &str) -> Result<(), String> {
    let mut cmd = tokio::process::Command::new("xdg-open");
    cmd.arg(url).kill_on_drop(true);
    let out = tokio::time::timeout(std::time::Duration::from_secs(10), cmd.output())
        .await
        .map_err(|_| "browser launch timed out".to_string())?
        .map_err(|e| format!("browser launch failed: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err("browser exited non-zero".into())
    }
}

struct SendParams {
    intent: stream_manager_chat::model::OutboundIntent,
    provider: Provider,
    sess: AuthSession,
    broadcast: Option<String>,
    locks: RefreshLocks,
    gate: SendGate,
    store: FileTokenStore,
    /// Token-store key id (`provider:channel_id`) — the same id onboarding
    /// rotates, so refresh reads what login wrote. It is also the refresh
    /// lock key, unifying onboarding and send-time refresh serialization.
    key_id: String,
    cta_used: bool,
    cta_blocked: Option<String>,
    /// This provider's own client id (never another provider's).
    provider_client_id: String,
    client_secret: Option<Secret>,
}

/// Execute one send: gate admission first (a cancelled send never touches
/// the network), inline serialized refresh with the same key/client the
/// onboarding flow uses, CTA recheck, POST, provider verdict. HTTP success
/// never overrides a provider rejection. Mid-flight cancellation is
/// reported, never silently confirmed.
async fn execute_send(p: SendParams) -> (String, SendOutcome) {
    let SendParams {
        intent,
        provider,
        mut sess,
        broadcast,
        locks,
        gate,
        store,
        key_id,
        cta_used,
        cta_blocked,
        provider_client_id,
        client_secret,
    } = p;
    let id = intent.id.clone();
    let finish = |gate: &SendGate, id: String| {
        gate.retire(&id);
    };
    // A failed rotation write does not block a valid access token, but it
    // is surfaced on the confirmed outcome: the next refresh will redo
    // this work instead of assuming the rotation landed.
    let mut rotation_err: Option<String> = None;
    if cta_used {
        if let Some(blocked) = cta_blocked {
            finish(&gate, id.clone());
            return (
                id,
                SendOutcome::Rejected {
                    error: format!("CTA preset blocked by review: {blocked}"),
                },
            );
        }
    }
    if let Err(reason) = gate.admit(&id) {
        finish(&gate, id.clone());
        return (
            id,
            SendOutcome::Rejected {
                error: reason.to_string(),
            },
        );
    }
    let http = reqwest::Client::new();
    if sess.expired() {
        if !sess.refresh_present {
            finish(&gate, id.clone());
            return (
                id,
                SendOutcome::Rejected {
                    error: "token expired and no refresh stored".into(),
                },
            );
        }
        // Admission again inside the lock: cancel/logout wins over refresh.
        // The lock key is the store identity onboarding uses, so the two
        // refresh paths serialize instead of racing rotation.
        let lock = locks.lock_for(&key_id);
        let _guard = lock.lock().await;
        if let Err(reason) = gate.admit(&id) {
            finish(&gate, id.clone());
            return (
                id,
                SendOutcome::Rejected {
                    error: reason.to_string(),
                },
            );
        }
        let Some(o) = oauth_provider(provider) else {
            finish(&gate, id.clone());
            return (
                id,
                SendOutcome::Rejected {
                    error: "no oauth flow".into(),
                },
            );
        };
        // Refresh best-effort; failure is outcome-unknown, never a silent retry.
        match store.load_refresh(&key_id) {
            Ok(refresh) => {
                match oauth::refresh_access_token(
                    o.token_url,
                    &provider_client_id,
                    client_secret.as_ref(),
                    &refresh,
                    &http,
                )
                .await
                {
                    Ok(r) => {
                        if let Some(new_refresh) = r.new_refresh.clone() {
                            if let Err(store_err) = store.store_rotated(&key_id, new_refresh) {
                                rotation_err =
                                    Some(format!("rotation not persisted: {store_err:#}"));
                            }
                        }
                        sess.access = r.access_token;
                        sess.expires_at = chrono::Utc::now().timestamp() + r.expires_in as i64;
                        sess.refresh_present = sess.refresh_present || r.new_refresh.is_some();
                    }
                    Err(e) => {
                        finish(&gate, id.clone());
                        return (
                            id,
                            SendOutcome::Unknown {
                                error: format!("refresh failed: {e:#}"),
                            },
                        );
                    }
                }
            }
            Err(e) => {
                finish(&gate, id.clone());
                return (
                    id,
                    SendOutcome::Unknown {
                        error: format!("refresh load failed: {e:#}"),
                    },
                );
            }
        }
    }
    // Final admission immediately before dispatch.
    if let Err(reason) = gate.admit(&id) {
        finish(&gate, id.clone());
        return (
            id,
            SendOutcome::Rejected {
                error: reason.to_string(),
            },
        );
    }

    // Post-dispatch settlement: a cancel/logout/disconnect that landed
    // mid-flight is annotated, never silently confirmed.
    let (rid, outcome) = match provider {
        Provider::Twitch => {
            let Some(broadcaster_id) = broadcast else {
                return (
                    id,
                    SendOutcome::Rejected {
                        error: "no resolved broadcaster id — reconnect the source".into(),
                    },
                );
            };
            let body = match twitch::send_payload(
                &broadcaster_id,
                &sess.user_id,
                &intent.text,
                intent.reply_to.as_ref().map(|r| r.remote_id.as_str()),
            ) {
                Ok(b) => b,
                Err(e) => return (id, SendOutcome::Rejected { error: e }),
            };
            match http
                .post(twitch::HELIX_CHAT_URL)
                .header("Client-Id", provider_client_id.clone())
                .bearer_auth(sess.access.expose())
                .json(&body)
                .send()
                .await
            {
                Ok(res) => {
                    let status = res.status();
                    let v: serde_json::Value = res.json().await.unwrap_or_default();
                    if !status.is_success() {
                        return (
                            id,
                            SendOutcome::Unknown {
                                error: format!("twitch send {status}: {v}"),
                            },
                        );
                    }
                    match twitch::send_verdict(&v) {
                        Ok(remote_id) => (
                            id,
                            SendOutcome::Confirmed {
                                remote_id,
                                session_moved: false,
                                note: None,
                            },
                        ),
                        Err(e) => (id, SendOutcome::Rejected { error: e }),
                    }
                }
                Err(e) => (
                    id,
                    SendOutcome::Unknown {
                        error: format!("transport: {e}"),
                    },
                ),
            }
        }
        Provider::Youtube => {
            let Some(chat_id) = broadcast else {
                return (
                    id,
                    SendOutcome::Rejected {
                        error: "no liveChatId (chat ended?)".into(),
                    },
                );
            };
            match youtube::insert_message(
                &http,
                youtube::INSERT_URL,
                sess.access.expose(),
                &chat_id,
                &intent.text,
            )
            .await
            {
                youtube::InsertOutcome::Sent(remote_id) => (
                    id,
                    SendOutcome::Confirmed {
                        remote_id,
                        session_moved: false,
                        note: None,
                    },
                ),
                youtube::InsertOutcome::Refused(error) => (id, SendOutcome::Rejected { error }),
                youtube::InsertOutcome::Unknown(error) => (id, SendOutcome::Unknown { error }),
            }
        }
        Provider::Kick => {
            // User sends address the broadcaster's numeric id. Without a
            // channel directory lookup, only the operator's own channel
            // (the authenticated identity) is addressable — anything else
            // is refused, never sent somewhere unexpected.
            let own = intent.source.room.eq_ignore_ascii_case(&sess.identity);
            let broadcaster = if own {
                sess.user_id.clone()
            } else {
                String::new()
            };
            if broadcaster.is_empty() {
                return (
                    id,
                    SendOutcome::Rejected {
                        error: "kick user sends need the target broadcaster id; own channel only for now".into(),
                    },
                );
            }
            let body = match kick::send_payload(
                &broadcaster,
                &intent.text,
                intent.reply_to.as_ref().map(|r| r.remote_id.as_str()),
            ) {
                Ok(b) => b,
                Err(e) => return (id, SendOutcome::Rejected { error: e }),
            };
            match http
                .post("https://api.kick.com/public/v1/chat")
                .bearer_auth(sess.access.expose())
                .json(&body)
                .send()
                .await
            {
                Ok(res) => {
                    let status = res.status();
                    let v: serde_json::Value = res.json().await.unwrap_or_default();
                    if !status.is_success() {
                        return (
                            id,
                            SendOutcome::Unknown {
                                error: format!("kick send {status}: {v}"),
                            },
                        );
                    }
                    match kick::send_verdict(&v) {
                        Ok(remote_id) => (
                            id,
                            SendOutcome::Confirmed {
                                remote_id,
                                session_moved: false,
                                note: None,
                            },
                        ),
                        Err(e) => (id, SendOutcome::Rejected { error: e }),
                    }
                }
                Err(e) => (
                    id,
                    SendOutcome::Unknown {
                        error: format!("transport: {e}"),
                    },
                ),
            }
        }
        Provider::Rumble => (
            id,
            SendOutcome::Rejected {
                error: "rumble is read-only here; use the native-chat action".into(),
            },
        ),
    };
    // Settle BEFORE retiring: `moved_since` on a retired (removed) ticket
    // would always report moved. Cancel-after-dispatch therefore keeps
    // the provider's real verdict, annotated, instead of a false flag on
    // every clean send.
    let moved = gate.moved_since(&rid);
    finish(&gate, rid.clone());
    let outcome = match outcome {
        SendOutcome::Confirmed { remote_id, .. } => SendOutcome::Confirmed {
            remote_id,
            session_moved: moved,
            note: rotation_err,
        },
        SendOutcome::Unknown { error } if moved => SendOutcome::Unknown {
            error: format!("{error} (cancelled mid-flight; may have sent — not retried)"),
        },
        other => other,
    };
    (rid, outcome)
}

async fn delete_remote(
    http: &reqwest::Client,
    client_id: &str,
    provider: Provider,
    account: &AuthSession,
    src: &ChatSource,
    remote_id: &str,
) -> Result<(), String> {
    match provider {
        Provider::Twitch => {
            let Some(broadcaster_id) = src.broadcast_id.clone() else {
                return Err("twitch delete needs a resolved broadcaster id — reconnect".into());
            };
            let url = format!(
                "https://api.twitch.tv/helix/moderation/chat?broadcaster_id={broadcaster_id}&moderator_id={}&message_id={remote_id}",
                account.user_id
            );
            let res = http
                .delete(&url)
                .header("Client-Id", client_id)
                .bearer_auth(account.access.expose())
                .send()
                .await
                .map_err(|e| format!("transport: {e}"))?;
            if res.status().is_success() {
                Ok(())
            } else {
                Err(format!("twitch delete refused: {}", res.status()))
            }
        }
        Provider::Youtube => {
            let res = http
                .delete("https://www.googleapis.com/youtube/v3/liveChat/messages")
                .bearer_auth(account.access.expose())
                .query(&[("id", remote_id)])
                .send()
                .await
                .map_err(|e| format!("transport: {e}"))?;
            if res.status().is_success() {
                Ok(())
            } else {
                Err(format!("youtube delete refused: {}", res.status()))
            }
        }
        Provider::Kick => {
            let res = http
                .delete(kick::delete_path(remote_id))
                .bearer_auth(account.access.expose())
                .send()
                .await
                .map_err(|e| format!("transport: {e}"))?;
            if res.status().is_success() {
                Ok(())
            } else {
                Err(format!("kick delete refused: {}", res.status()))
            }
        }
        _ => Err("remote delete is not wired for this provider".into()),
    }
}

async fn ban_remote(
    http: &reqwest::Client,
    client_id: &str,
    account: &AuthSession,
    src: &ChatSource,
    user_login: &str,
    duration_secs: Option<u64>,
) -> Result<(), String> {
    // Resolve the login to a user id on THIS provider only.
    let users: serde_json::Value = http
        .get("https://api.twitch.tv/helix/users")
        .header("Client-Id", client_id)
        .bearer_auth(account.access.expose())
        .query(&[("login", user_login)])
        .send()
        .await
        .map_err(|e| format!("transport: {e}"))?
        .json()
        .await
        .map_err(|e| format!("unparsable: {e}"))?;
    let user_id = users["data"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|u| u["id"].as_str())
        .ok_or_else(|| format!("no such twitch user: {user_login}"))?
        .to_owned();
    let mut data = serde_json::json!({ "user_id": user_id, "reason": "operator moderation from stream-manager" });
    if let Some(secs) = duration_secs {
        data["duration"] = secs.into();
    }
    let Some(broadcaster_id) = src.broadcast_id.clone() else {
        return Err("twitch moderation needs a resolved broadcaster id — reconnect".into());
    };
    let res = http
        .post(format!(
            "https://api.twitch.tv/helix/moderation/bans?broadcaster_id={broadcaster_id}&moderator_id={}",
            account.user_id
        ))
        .bearer_auth(account.access.expose())
        .json(&serde_json::json!({ "data": data }))
        .send()
        .await
        .map_err(|e| format!("transport: {e}"))?;
    if res.status().is_success() {
        Ok(())
    } else {
        Err(format!("twitch moderation refused: {}", res.status()))
    }
}

// ---------- views ----------

impl ChatPanel {
    pub fn view(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        let banner = text(
            "Private operator console — viewer messages stay here. Never auto-forward between platforms; captures may include personal data.",
        )
        .size(12);
        let body = if self.compact {
            self.view_timeline(ctx)
        } else {
            row![
                container(self.view_sources(ctx)).width(Length::Fixed(270.0)),
                container(self.view_center(ctx)).width(Length::Fill),
                container(self.view_details(ctx)).width(Length::Fixed(if self.details_open {
                    300.0
                } else {
                    36.0
                })),
            ]
            .spacing(8)
            .into()
        };
        column![banner, body].spacing(6).padding(8).into()
    }

    fn view_sources(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        let mut col = column![row![
            text("Sources").size(16),
            button(if self.compact { "wide" } else { "compact" })
                .on_press(Message::Chat(ChatMsg::ToggleCompact)),
        ]
        .spacing(8),]
        .spacing(6);
        if self.coord.sources().is_empty() {
            col =
                col.push(text("No active sources. Propose from the plan, then Connect.").size(13));
        }
        for (s, conn) in self.coord.sources() {
            let caps = caps_of(s.key.provider);
            let rw = if caps.send.implemented {
                "R/W"
            } else {
                "read-only"
            };
            let conn_label = match conn {
                ConnState::Offline { reason } => format!("offline: {reason}"),
                other => other.short().to_owned(),
            };
            let mut rowbtns = row![].spacing(4);
            if conn.is_receiving() {
                rowbtns = rowbtns.push(
                    button("Disconnect")
                        .on_press(Message::Chat(ChatMsg::DisconnectSource(s.id.clone()))),
                );
            } else {
                rowbtns = rowbtns
                    .push(
                        button("Connect")
                            .on_press(Message::Chat(ChatMsg::ConnectSource(s.id.clone()))),
                    )
                    .push(
                        button("Retry").on_press(Message::Chat(ChatMsg::RetrySource(s.id.clone()))),
                    );
            }
            col = col.push(column![
                text(format!("[{}] {} ({rw})", s.badge(), s.channel)).size(14),
                text(conn_label).size(12),
                rowbtns,
            ]);
        }
        col = col.push(text("Proposed from plan").size(14));
        for p in &self.proposals {
            let active = self
                .coord
                .sources()
                .iter()
                .any(|(s, _)| s.binding_ids.contains(&p.binding_id));
            if active {
                continue;
            }
            col = col.push(row![
                checkbox(
                    format!("{} {}", p.provider.display(), p.channel),
                    p.selected
                )
                .on_toggle(|_| Message::Chat(ChatMsg::ToggleProposal(p.binding_id.clone()))),
                button("Connect")
                    .on_press(Message::Chat(ChatMsg::ConnectSource(p.binding_id.clone()))),
            ]);
        }
        col = col.push(text("Accounts").size(14));
        for a in &ctx.accounts {
            let state = match self.auth.get(&a.id) {
                Some(AuthState::Connected(s)) => format!(
                    "connected as {}{}{} [{}]",
                    s.identity,
                    if s.can_write { " (+send)" } else { "" },
                    if s.can_moderate { " (+mod)" } else { "" },
                    s.scopes.join(" "),
                ),
                Some(AuthState::Locked { reason }) => format!("locked: {reason}"),
                Some(AuthState::Failed { error }) => format!("failed: {error}"),
                _ => "not connected".into(),
            };
            let mut abtns = row![].spacing(4);
            if matches!(self.auth.get(&a.id), Some(AuthState::Connected(_))) {
                abtns = abtns.push(
                    button("Logout").on_press(Message::Chat(ChatMsg::AuthDisconnect(a.id.clone()))),
                );
            } else if a.provider != Provider::Rumble {
                abtns = abtns
                    .push(
                        button("Connect").on_press(Message::Chat(ChatMsg::AuthConnect {
                            account_id: a.id.clone(),
                            with_write: false,
                            with_moderate: false,
                        })),
                    )
                    .push(
                        button("+send").on_press(Message::Chat(ChatMsg::AuthConnect {
                            account_id: a.id.clone(),
                            with_write: true,
                            with_moderate: false,
                        })),
                    )
                    .push(button("+mod").on_press(Message::Chat(ChatMsg::AuthConnect {
                        account_id: a.id.clone(),
                        with_write: true,
                        with_moderate: true,
                    })));
            } else {
                abtns = abtns.push(text("read-only: API URL file").size(12));
            }
            col = col.push(column![
                text(format!("{} {}", a.provider.display(), a.channel_name)).size(13),
                text(state).size(12),
                abtns,
            ]);
        }
        col = col.push(row![
            button("Disconnect all").on_press(Message::Chat(ChatMsg::DisconnectAll)),
            button("Demo feed").on_press(Message::Chat(ChatMsg::LoadDemoChat)),
            button("Clear").on_press(Message::Chat(ChatMsg::ClearChat)),
            button("Pop out").on_press(Message::Chat(ChatMsg::OpenDetached)),
            button("Check server").on_press(Message::Chat(ChatMsg::ServerCheck)),
        ]);
        col = col.push(text(self.status.clone()).size(12));
        scrollable(col).height(Length::Fill).into()
    }

    fn view_center(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        let mut filters = row![].spacing(6);
        for p in [
            Provider::Youtube,
            Provider::Twitch,
            Provider::Kick,
            Provider::Rumble,
        ] {
            let on = self.show_providers.get(&p).copied().unwrap_or(true);
            filters = filters.push(
                checkbox(p.display(), on)
                    .on_toggle(move |_| Message::Chat(ChatMsg::ToggleProviderFilter(p))),
            );
        }
        filters = filters
            .push(
                checkbox("mentions", self.mentions_only)
                    .on_toggle(|_| Message::Chat(ChatMsg::ToggleMentions)),
            )
            .push(
                checkbox("paid", self.paid_only).on_toggle(|_| Message::Chat(ChatMsg::TogglePaid)),
            )
            .push(button(text("A")).on_press(Message::Chat(ChatMsg::TextSize(0))))
            .push(button(text("A+")).on_press(Message::Chat(ChatMsg::TextSize(1))))
            .push(button(text("A++")).on_press(Message::Chat(ChatMsg::TextSize(2))));
        let follow_row = row![
            text_input("search", &self.query).on_input(|q| Message::Chat(ChatMsg::QueryChanged(q))),
            checkbox("follow", self.coord.is_following())
                .on_toggle(|v| Message::Chat(ChatMsg::Follow(v))),
        ]
        .spacing(6);
        let unread = self.coord.unread();
        let timeline: Element<'_, Message> = if self.visible(ctx).is_empty() {
            container(text(match self.coord.sources().is_empty() {
                true => "Empty: connect a source or load the demo feed.",
                false => "No messages match the current filters.",
            }))
            .padding(12)
            .into()
        } else {
            self.view_timeline(ctx)
        };
        let mut col = column![filters, follow_row, timeline].spacing(6);
        if unread > 0 && !self.coord.is_following() {
            col = col.push(
                button(text(format!("Jump to live ({unread} unread)")))
                    .on_press(Message::Chat(ChatMsg::JumpLive)),
            );
        }
        col = col.push(self.view_composer(ctx));
        col.into()
    }

    fn view_timeline(&self, _ctx: &ChatCtx) -> Element<'_, Message> {
        let size = self.scale();
        let mut col = column![].spacing(2);
        // Windowed render over the bounded coordinator buffer.
        let visible = self.visible(_ctx);
        let total = visible.len();
        let take = self.render_window.min(total);
        let mut events: Vec<&TimelineEvent> = visible.into_iter().rev().take(take).collect();
        events.reverse();
        if take < total {
            col = col.push(
                button(text(format!("Show older ({} of {})", take, total)))
                    .on_press(Message::Chat(ChatMsg::ShowOlder)),
            );
        }
        for ev in events {
            col = col.push(self.view_event(ev, size));
        }
        if self.coord.evicted() > 0 {
            col = col.push(
                text(format!(
                    "… {} oldest message(s) evicted by the live buffer (bounded, not lossless)",
                    self.coord.evicted()
                ))
                .size(12),
            );
        }
        let timeline = scrollable(col).id(scrollable::Id::new("chat-timeline"));
        let timeline = if self.coord.is_following() {
            timeline.anchor_bottom()
        } else {
            timeline
        };
        timeline.height(Length::Fill).into()
    }

    fn view_event(&self, ev: &TimelineEvent, size: f32) -> Element<'_, Message> {
        match ev {
            TimelineEvent::Message(m) => self.view_message(m, size),
            TimelineEvent::Deletion {
                source, remote_id, ..
            } => text(format!("(deleted on {}: {remote_id})", source.label()))
                .size(12)
                .into(),
            TimelineEvent::Clear { source, .. } => {
                text(format!("(chat cleared on {})", source.label()))
                    .size(12)
                    .into()
            }
            TimelineEvent::ConnChange { source, state, .. } => {
                text(format!("— {}: {} —", source.label(), state.short()))
                    .size(12)
                    .into()
            }
            TimelineEvent::Gap { source, reason, .. } => {
                text(format!("… possible gap on {}: {reason}", source.label()))
                    .size(12)
                    .into()
            }
        }
    }

    fn view_message(
        &self,
        m: &stream_manager_chat::model::ChatMessage,
        size: f32,
    ) -> Element<'_, Message> {
        use stream_manager_chat::model::ModState;
        let badge = match m.source.provider {
            Provider::Youtube => "YT",
            Provider::Twitch => "TW",
            Provider::Kick => "KK",
            Provider::Rumble => "RU",
        };
        let kind_tag = match &m.kind {
            MessageKind::Text => "",
            MessageKind::Paid(_) => " [paid]",
            MessageKind::Membership => " [member]",
            MessageKind::System => " [system]",
        };
        let state_tag = match m.mod_state {
            ModState::Visible => "",
            ModState::HiddenLocal => " (hidden locally)",
            ModState::DeletedRemote => " (deleted)",
        };
        let reply = m
            .reply_to
            .as_ref()
            .map(|r| format!("↩ {} ", r.author_name))
            .unwrap_or_default();
        let body = format!("{reply}{}", m.text);
        let selected = self.selected == Some(m.seq);
        let mut btns = row![].spacing(4);
        if m.remote_id.is_some() {
            btns = btns.push(button("Reply").on_press(Message::Chat(ChatMsg::ReplyTo(m.seq))));
        }
        btns = btns
            .push(button("Copy").on_press(Message::Chat(ChatMsg::CopyMessage(m.seq))))
            .push(button("Hide").on_press(Message::Chat(ChatMsg::HideMessage(m.seq))));
        if m.remote_id.is_some() {
            btns =
                btns.push(button("Delete…").on_press(Message::Chat(ChatMsg::ConfirmDelete(m.seq))));
        }
        if m.source.provider == Provider::Twitch {
            let (sid, user) = (
                source_id_of(&self.coord, &m.source).unwrap_or_default(),
                m.author_name.clone(),
            );
            btns =
                btns.push(button("Ban…").on_press(Message::Chat(ChatMsg::ConfirmBan(sid, user))));
        }
        let mut col = column![
            text(format!(
                "[{badge}] {} · {}{kind_tag}{state_tag}",
                m.author_name,
                m.provider_ts.as_deref().unwrap_or(""),
            ))
            .size(size - 2.0),
            text(body).size(size),
        ];
        if selected {
            col = col.push(btns);
        } else {
            col = col.push(button("···").on_press(Message::Chat(ChatMsg::SelectMessage(m.seq))));
        }
        if self.confirm_delete == Some(m.seq) {
            col = col.push(row![
                text("Delete remotely? This cannot be undone.").size(12),
                button("Confirm").on_press(Message::Chat(ChatMsg::DeleteConfirmed)),
                button("Cancel").on_press(Message::Chat(ChatMsg::DeleteCancelled)),
            ]);
        }
        container(col).padding(4).into()
    }

    fn view_details(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        if !self.details_open {
            return button("»")
                .on_press(Message::Chat(ChatMsg::ToggleDetails))
                .into();
        }
        let mut col = column![row![
            text("Details").size(16),
            button("«").on_press(Message::Chat(ChatMsg::ToggleDetails)),
        ]
        .spacing(8),]
        .spacing(6);
        if let Some(seq) = self.selected {
            let found = self
                .coord
                .timeline()
                .iter()
                .filter_map(|ev| match ev {
                    TimelineEvent::Message(m) if m.seq == seq => Some(m.clone()),
                    _ => None,
                })
                .next();
            match found {
                Some(m) => {
                    col = col.push(text(format!("Source: {}", m.source.label())).size(13));
                    col = col.push(
                        text(format!(
                            "Remote id: {}",
                            m.remote_id
                                .as_deref()
                                .unwrap_or("(none — ID-less provider)")
                        ))
                        .size(13),
                    );
                    col = col.push(
                        text(format!(
                            "Author id: {}",
                            m.author_id.as_deref().unwrap_or("(not supplied)")
                        ))
                        .size(13),
                    );
                    col = col.push(text(format!("Local seq: #{}", m.seq)).size(13));
                    if !m.badges.is_empty() {
                        col = col.push(text(format!("Badges: {}", m.badges.join(", "))).size(13));
                    }
                    let emotes: Vec<String> = m
                        .fragments
                        .iter()
                        .filter_map(|f| f.emote_id.clone())
                        .collect();
                    if !emotes.is_empty() {
                        col = col.push(text(format!("Emotes: {}", emotes.join(", "))).size(13));
                    }
                    if let Some(r) = &m.reply_to {
                        col = col.push(
                            text(format!("Reply to {} ({})", r.author_name, r.remote_id)).size(13),
                        );
                    }
                    col = col.push(text(format!("Moderation: {:?}", m.mod_state)).size(13));
                }
                None => col = col.push(text("Message left the window.").size(13)),
            }
        } else {
            col = col.push(text("Select a message (···) to inspect it.").size(13));
        }
        if let Some((sid, user)) = &self.confirm_ban {
            let (sid, user) = (sid.clone(), user.clone());
            col = col.push(column![
                text(format!(
                    "Ban {user} on Twitch only? Same names elsewhere are untouched."
                ))
                .size(12),
                row![
                    button("Ban").on_press(Message::Chat(ChatMsg::BanConfirmed)),
                    button("60s timeout").on_press(Message::Chat(ChatMsg::TimeoutUser(
                        sid.clone(),
                        user.clone(),
                        60
                    ))),
                    button("Cancel").on_press(Message::Chat(ChatMsg::BanCancelled)),
                ],
            ]);
        }
        col = col.push(text("Outbox").size(14));
        for id in self.outbox.iter().rev().take(8) {
            if let Some(intent) = self.coord.intent(id) {
                let state = match intent.state {
                    IntentState::Pending => "pending",
                    IntentState::Confirmed => "confirmed",
                    IntentState::Rejected => "rejected",
                    IntentState::OutcomeUnknown => "outcome unknown",
                };
                let mut r = row![text(format!("{}: {state}", short_id(&intent.id))).size(12)];
                if intent.state == IntentState::Pending {
                    r = r.push(
                        button("Cancel")
                            .on_press(Message::Chat(ChatMsg::CancelIntent(intent.id.clone()))),
                    );
                }
                col = col.push(r);
                if let Some(e) = &intent.error {
                    col = col.push(text(e.clone()).size(12));
                }
            }
        }
        let _ = ctx;
        scrollable(col).height(Length::Fill).into()
    }

    fn view_composer(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        let multi = self.coord.multitarget().enabled;
        if multi {
            return self.view_multitarget(ctx);
        }
        let options: Vec<String> = self
            .coord
            .sources()
            .into_iter()
            .map(|(s, _)| s.id.clone())
            .collect();
        let selected = self.coord.composer_source().map(str::to_owned);
        let draft = selected
            .as_deref()
            .map(|id| self.coord.draft_of(id).to_owned())
            .unwrap_or_default();
        let target_line = match self.send_target(ctx) {
            Ok(s) => {
                let identity = s
                    .account_id
                    .as_deref()
                    .and_then(|a| self.auth.get(a))
                    .and_then(|a| match a {
                        AuthState::Connected(sess) => Some(sess.identity.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "(unverified identity)".into());
                format!("To {} as {identity}", s.key.label())
            }
            Err(reason) => format!("Cannot send: {reason}"),
        };
        let reply_chip: Element<'_, Message> = match self.coord.reply() {
            Some(r) => row![
                text(format!(
                    "Replying to {} ({} only)",
                    r.author_name, r.source_id
                ))
                .size(12),
                button("clear").on_press(Message::Chat(ChatMsg::ClearReply)),
            ]
            .into(),
            None => text("").size(1).into(),
        };
        let mut col = column![
            row![
                pick_list(options, selected, |id| {
                    Message::Chat(ChatMsg::ComposerSource(id))
                })
                .placeholder("composer source"),
                checkbox("multi-target", multi)
                    .on_toggle(|v| Message::Chat(ChatMsg::ToggleMulti(v))),
            ]
            .spacing(6),
            text(target_line).size(12),
            reply_chip,
            row![
                text_input("message…", &draft)
                    .on_input(|t| Message::Chat(ChatMsg::DraftChanged(t)))
                    .on_submit(Message::Chat(ChatMsg::SendNow)),
                button("Send").on_press(Message::Chat(ChatMsg::SendNow)),
            ]
            .spacing(6),
        ]
        .spacing(4);
        if ctx.cta_text.is_some() {
            let blocked = ctx.cta_blocked.clone();
            col = col.push(row![
                button("Insert stream link").on_press(Message::Chat(ChatMsg::InsertCta)),
                text(
                    blocked
                        .map(|b| format!("CTA blocked: {b}"))
                        .unwrap_or_else(|| "CTA from reviewed plan".into())
                )
                .size(12),
            ]);
        }
        if !self.send_note.is_empty() {
            col = col.push(text(self.send_note.clone()).size(12));
        }
        container(col).padding(6).into()
    }

    fn view_multitarget(&self, ctx: &ChatCtx) -> Element<'_, Message> {
        let mt = self.coord.multitarget();
        let mut col = column![
            row![
                text("Multi-target (broadcaster-authored, off by default)").size(14),
                checkbox("enabled", mt.enabled)
                    .on_toggle(|v| Message::Chat(ChatMsg::ToggleMulti(v))),
            ]
            .spacing(8),
            text("This sends YOUR message to selected channels. Never a viewer relay.").size(12),
        ]
        .spacing(4);
        for (s, _) in self.coord.sources() {
            let eligible = self.check_sendable(s, ctx).is_ok();
            let selected = mt.selected.iter().any(|id| id == &s.id);
            let label = if eligible {
                format!("{} {}", s.key.label(), s.channel)
            } else {
                format!("{} {} (ineligible)", s.key.label(), s.channel)
            };
            col = col.push(
                checkbox(label, selected)
                    .on_toggle(|_| Message::Chat(ChatMsg::ToggleRecipient(s.id.clone()))),
            );
        }
        col = col.push(
            text_input("announcement…", &mt.text)
                .on_input(|t| Message::Chat(ChatMsg::MultiText(t))),
        );
        if self.multitarget_confirm {
            col = col.push(column![
                text(format!(
                    "Send to {} channel(s)? Preview: {}",
                    mt.selected.len(),
                    mt.text
                ))
                .size(13),
                row![
                    button("Confirm send").on_press(Message::Chat(ChatMsg::MultiConfirm)),
                    button("Back").on_press(Message::Chat(ChatMsg::MultiBack)),
                ],
            ]);
        } else {
            col = col
                .push(button("Preview & confirm").on_press(Message::Chat(ChatMsg::MultiPreview)));
        }
        if !self.send_note.is_empty() {
            col = col.push(text(self.send_note.clone()).size(12));
        }
        container(col).padding(6).into()
    }
}

fn short_id(id: &str) -> String {
    id.chars().take(18).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_manager_chat::coordinator::BufferLimits;
    use stream_manager_chat::demo;

    fn test_ctx(demo_mode: bool) -> ChatCtx {
        ChatCtx {
            demo_mode,
            db_path: ":memory:".into(),
            accounts: vec![AccountView {
                id: "tw-acc".into(),
                provider: Provider::Twitch,
                channel_id: "111".into(),
                channel_name: "alice".into(),
            }],
            proposals: vec![],
            cta_text: None,
            cta_blocked: None,
        }
    }

    fn live_panel() -> ChatPanel {
        let mut panel = ChatPanel::new(ChatConfig::default());
        for s in demo::demo_sources() {
            panel.activate_source(s);
        }
        let gen = panel.coord.session_gen();
        for id in ["tw", "yt", "kk", "ru"] {
            panel.coord.set_conn(
                gen,
                id,
                ConnState::Live {
                    since_utc: "2026-09-15T10:00:00Z".into(),
                },
            );
        }
        panel
    }

    fn connect_twitch(panel: &mut ChatPanel) {
        panel.auth.insert(
            "tw-acc".into(),
            AuthState::Connected(AuthSession {
                identity: "alice".into(),
                user_id: "111".into(),
                scopes: vec!["user:read:chat".into(), "user:write:chat".into()],
                can_write: true,
                can_moderate: true,
                access: Secret::new("access".into()),
                expires_at: chrono::Utc::now().timestamp() + 3600,
                refresh_present: false,
            }),
        );
        // Bind the demo twitch source to the test account.
        panel.coord.remove_source("tw");
        panel.activate_source(ChatSource {
            id: "tw".into(),
            key: SourceKey {
                provider: Provider::Twitch,
                room: "alice".into(),
            },
            account_id: Some("tw-acc".into()),
            channel: "alice".into(),
            broadcast_id: Some("111".into()),
            binding_ids: vec![],
        });
        let gen = panel.coord.session_gen();
        panel.coord.set_conn(
            gen,
            "tw",
            ConnState::Live {
                since_utc: "2026-09-15T10:00:00Z".into(),
            },
        );
    }

    #[test]
    fn reply_binds_source_and_filters_never_retarget() {
        let mut panel = live_panel();
        let ctx = test_ctx(true);
        let _ = panel.update(ChatMsg::LoadDemoChat, &ctx);
        // Reply to a kick message binds the kick source…
        let seq = panel
            .coord
            .timeline()
            .iter()
            .filter_map(|ev| match ev {
                TimelineEvent::Message(m) if m.source.provider == Provider::Kick => Some(m.seq),
                _ => None,
            })
            .next()
            .unwrap();
        let _ = panel.update(ChatMsg::ReplyTo(seq), &ctx);
        assert_eq!(panel.coord.composer_source(), Some("kk"));
        // …and filtering the feed cannot move the draft or the reply.
        let _ = panel.update(ChatMsg::ToggleProviderFilter(Provider::Kick), &ctx);
        let _ = panel.update(ChatMsg::QueryChanged("something else".into()), &ctx);
        assert_eq!(panel.coord.composer_source(), Some("kk"));
        assert!(panel.coord.reply().is_some());
    }

    #[test]
    fn stale_bus_events_die_on_session_reset() {
        let mut panel = live_panel();
        let ctx = test_ctx(false);
        let old_gen = panel.coord.session_gen();
        let _ = panel.update(ChatMsg::SessionReset, &ctx);
        assert!(panel.coord.timeline().is_empty());
        // Full isolation: sources, drafts, and auth do not cross the
        // boundary with the demo/live switch.
        assert!(panel.coord.sources().is_empty());
        panel.apply_bus(
            old_gen,
            RuntimeEvent::Conn {
                source_id: "tw".into(),
                state: ConnState::Live {
                    since_utc: String::new(),
                },
                room: 0,
            },
        );
        assert!(panel.coord.conn_of("tw").is_none());
    }

    #[test]
    fn conn_events_carry_room_generations() {
        let mut panel = live_panel();
        // A Live event from a rebound room is dropped, not applied.
        panel.coord.rebind_room("tw");
        panel.apply_bus(
            panel.coord.session_gen(),
            RuntimeEvent::Conn {
                source_id: "tw".into(),
                state: ConnState::Live {
                    since_utc: String::new(),
                },
                room: 0,
            },
        );
        assert!(!panel.coord.conn_of("tw").is_some_and(|c| c.is_receiving()));
        // The current generation applies.
        let room = panel.coord.room_gen_of("tw").unwrap();
        panel.apply_bus(
            panel.coord.session_gen(),
            RuntimeEvent::Conn {
                source_id: "tw".into(),
                state: ConnState::Live {
                    since_utc: String::new(),
                },
                room,
            },
        );
        assert!(panel.coord.conn_of("tw").is_some_and(|c| c.is_receiving()));
    }

    #[test]
    fn duplicate_connect_is_refused_while_receiving() {
        let mut panel = live_panel();
        let ctx = test_ctx(false);
        let _ = panel.update(ChatMsg::ConnectSource("tw".into()), &ctx);
        assert!(panel.status.contains("already connected"));
    }

    #[test]
    fn cancel_blocks_queued_send_ticket() {
        let mut panel = live_panel();
        let ctx = test_ctx(false);
        connect_twitch(&mut panel);
        let _ = panel.update(ChatMsg::MultiText("x".into()), &ctx);
        // Single-target draft send path issues a ticket on SendNow.
        panel.coord.set_composer_source("tw");
        panel.coord.set_draft("tw", "hello".into());
        let _ = panel.update(ChatMsg::SendNow, &ctx);
        let id = panel.coord.intents().into_iter().next().unwrap().id.clone();
        assert!(panel.gate.admit(&id).is_ok());
        let _ = panel.update(ChatMsg::CancelIntent(id.clone()), &ctx);
        assert!(panel.gate.admit(&id).is_err());
    }

    #[test]
    fn demo_mode_disables_sending_but_keeps_reading() {
        let mut panel = live_panel();
        let ctx = test_ctx(true);
        let _ = panel.update(ChatMsg::LoadDemoChat, &ctx);
        assert!(!panel.coord.timeline().is_empty());
        let _ = panel.update(ChatMsg::SendNow, &ctx);
        assert!(panel.send_note.contains("Demo mode"));
    }

    #[test]
    fn ban_refuses_cross_platform_by_construction() {
        let mut panel = live_panel();
        let ctx = test_ctx(false);
        let _ = panel.update(ChatMsg::ConfirmBan("kk".into(), "sam".into()), &ctx);
        let _ = panel.update(ChatMsg::BanConfirmed, &ctx);
        assert!(panel.status.contains("Twitch only"));
    }

    #[test]
    fn multitarget_skips_ineligible_targets() {
        let mut panel = live_panel();
        let ctx = test_ctx(false);
        connect_twitch(&mut panel);
        // Rumble is read-only: selecting it must not send.
        let _ = panel.update(ChatMsg::ToggleMulti(true), &ctx);
        let _ = panel.update(ChatMsg::ToggleRecipient("tw".into()), &ctx);
        let _ = panel.update(ChatMsg::ToggleRecipient("ru".into()), &ctx);
        let _ = panel.update(ChatMsg::MultiText("hello all".into()), &ctx);
        let _ = panel.update(ChatMsg::MultiConfirm, &ctx);
        let confirmed_targets: Vec<_> = panel
            .coord
            .intents()
            .into_iter()
            .map(|i| i.source_id.clone())
            .collect();
        assert_eq!(confirmed_targets, vec!["tw".to_string()]);
        assert!(panel.send_note.contains("read-only"));
    }

    #[tokio::test]
    async fn execute_send_rejects_without_network() {
        let mut coord = Coordinator::new(BufferLimits::default());
        coord.upsert_source(ChatSource {
            id: "ru".into(),
            key: SourceKey {
                provider: Provider::Rumble,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec![],
        });
        let sess = AuthSession {
            identity: "op".into(),
            user_id: "1".into(),
            scopes: vec![],
            can_write: false,
            can_moderate: false,
            access: Secret::new("x".into()),
            expires_at: chrono::Utc::now().timestamp() + 60,
            refresh_present: false,
        };
        let intent = coord
            .create_intent("ru", "op", "hi".into(), None, vec!["ru".into()])
            .unwrap();
        let gate = SendGate::new();
        gate.set_session(1);
        gate.set_room("ru", 0);
        gate.issue(&intent.id, "ru");
        let params = SendParams {
            intent,
            provider: Provider::Rumble,
            sess,
            broadcast: None,
            locks: RefreshLocks::default(),
            gate,
            store: FileTokenStore::new("/nonexistent/tokens.json", "/nonexistent/key"),
            key_id: "rumble:alice".into(),
            cta_used: false,
            cta_blocked: None,
            provider_client_id: String::new(),
            client_secret: None,
        };
        let (_, outcome) = execute_send(params).await;
        assert!(matches!(outcome, SendOutcome::Rejected { .. }));
    }

    #[tokio::test]
    async fn cancelled_send_never_touches_network() {
        let mut coord = Coordinator::new(BufferLimits::default());
        coord.upsert_source(ChatSource {
            id: "ru".into(),
            key: SourceKey {
                provider: Provider::Rumble,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec![],
        });
        let sess = AuthSession {
            identity: "op".into(),
            user_id: "1".into(),
            scopes: vec![],
            can_write: false,
            can_moderate: false,
            access: Secret::new("x".into()),
            expires_at: chrono::Utc::now().timestamp() + 60,
            refresh_present: false,
        };
        let intent = coord
            .create_intent("ru", "op", "hi".into(), None, vec!["ru".into()])
            .unwrap();
        let gate = SendGate::new();
        gate.set_session(1);
        gate.set_room("ru", 0);
        gate.issue(&intent.id, "ru");
        gate.cancel(&intent.id);
        let params = SendParams {
            intent,
            provider: Provider::Rumble,
            sess,
            broadcast: None,
            locks: RefreshLocks::default(),
            gate,
            store: FileTokenStore::new("/nonexistent/tokens.json", "/nonexistent/key"),
            key_id: "rumble:alice".into(),
            cta_used: false,
            cta_blocked: None,
            provider_client_id: String::new(),
            client_secret: None,
        };
        let (_, outcome) = execute_send(params).await;
        assert!(matches!(outcome, SendOutcome::Rejected { error } if error.contains("cancelled")));
    }

    #[test]
    fn load_probe_reports_bounds() {
        let mut coord = Coordinator::new(BufferLimits::default());
        coord.upsert_source(ChatSource {
            id: "tw".into(),
            key: SourceKey {
                provider: Provider::Twitch,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec![],
        });
        // 100 events/s x 60 s with long bodies + moderation events.
        let (n, elapsed_ms, evicted) = demo::load_probe(&mut coord, "tw");
        assert_eq!(n, 6_000);
        assert_eq!(coord.timeline().len(), 5_000);
        assert!(
            (1_000..=1_006).contains(&evicted),
            "bounded eviction, got {evicted}"
        );
        eprintln!("chat load probe: {n} ingested in {elapsed_ms}ms, evicted={evicted}");
    }
}
