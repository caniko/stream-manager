//! Single-writer server state.
//!
//! One async mutex guards the canonical session: coordinator, gate,
//! accounts, idempotency receipts, and the bounded event backlog.
//! Provider network calls run WITHOUT the lock held; admission,
//! resolution, and receipting re-lock in order. Nothing here is
//! reachable without passing [`crate::perms`] checks first.

use crate::perms::Operator;
use crate::protocol::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use stream_manager_chat::coordinator::{BufferLimits, Coordinator};
use stream_manager_chat::gate::SendGate;
use stream_manager_chat::model::{
    caps_of, ChatSource, ConnState, IntentState, Provider, TimelineEvent,
};
use stream_manager_chat::runtime::{EventSink, RuntimeEvent, SharedStop};
use stream_manager_core::credentials::{FileTokenStore, RefreshLocks, Secret, TokenStore};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountBinding {
    pub id: String,
    pub provider: String,
    pub channel_id: String,
    pub channel_name: String,
}

#[derive(Debug, Clone)]
struct LiveSession {
    access: Secret,
    expires_at: i64,
    /// Verified granted scopes. Capabilities derive from these at every
    /// use — never from a stale boolean cached at login.
    scopes: Vec<String>,
    user_id: String,
    identity: String,
}

impl LiveSession {
    fn expired(&self) -> bool {
        chrono::Utc::now().timestamp() >= self.expires_at - 30
    }

    fn can_write(&self, provider: Provider) -> bool {
        caps_for(provider, &self.scopes).0
    }

    fn can_moderate(&self, provider: Provider) -> bool {
        caps_for(provider, &self.scopes).1
    }
}

/// Capability derivation from verified grants: nothing is assumed.
/// Write needs the provider's write scopes; moderation needs its
/// moderation scopes.
fn caps_for(provider: Provider, scopes: &[String]) -> (bool, bool) {
    use stream_manager_core::credentials::oauth as o;
    let (write, moderate) = match provider {
        Provider::Twitch => (o::TWITCH.chat_write_scopes, o::TWITCH.moderation_scopes),
        Provider::Youtube => (o::YOUTUBE.chat_write_scopes, o::YOUTUBE.moderation_scopes),
        Provider::Kick => (o::KICK.chat_write_scopes, o::KICK.moderation_scopes),
        Provider::Rumble => (&[][..], &[][..]),
    };
    let joined = scopes.join(" ");
    (
        o::scopes_cover(&joined, write),
        o::scopes_cover(&joined, moderate),
    )
}

pub struct ServerConfig {
    pub instance: String,
    pub operators: Vec<Operator>,
    /// Canonical accounts, loaded from the SQLite database by the caller
    /// (the database — not this config — is the source of truth at rest).
    pub accounts: Vec<AccountBinding>,
    /// Canonical database path: account registry and durable receipts.
    pub db_path: String,
    pub token_store_path: String,
    pub token_key_path: String,
    pub client_ids: HashMap<String, String>,
    pub kick_secret_path: String,
    pub kick_relay_url: String,
    pub kick_relay_bearer_path: String,
    pub rumble_api_path: String,
    pub twitch_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    pub youtube_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    pub kick_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    /// Identity-endpoint base overrides by provider slug (tests/proxies).
    pub endpoint_bases: HashMap<String, String>,
}

pub struct ServerState {
    instance: String,
    /// Random per-process boot id: operator approvals captured in one
    /// boot never authorize actions in another. Snapshots publish it;
    /// mutations echo it back.
    boot: u64,
    operators: Vec<Operator>,
    accounts: Vec<AccountBinding>,
    client_ids: HashMap<String, String>,
    kick_secret_path: String,
    kick_relay_url: String,
    kick_relay_bearer_path: String,
    rumble_api_path: String,
    twitch_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    youtube_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    kick_oauth: stream_manager_core::credentials::oauth::OAuthProvider,
    endpoint_bases: HashMap<String, String>,
    coord: Coordinator,
    gate: SendGate,
    auth_epoch: u64,
    /// Per-account operation eras: bumped on logout/account removal so a
    /// slow import that started earlier aborts instead of persisting.
    acct_epoch: HashMap<String, u64>,
    db_path: String,
    stop: SharedStop,
    sink: EventSink,
    sessions: HashMap<String, LiveSession>,
    /// Live transport workers by source: tracked so disconnect/reconnect
    /// aborts the previous loop instead of stacking duplicates, and so a
    /// loop that exits unexpectedly can be observed rather than leak.
    workers: HashMap<String, tokio::task::JoinHandle<()>>,
    locks: RefreshLocks,
    store: FileTokenStore,
    token_store_path: String,
    token_key_path: String,
    /// Receipts live behind their own blocking lock: recording a receipt
    /// must never wait on (or deadlock against) the state mutex held by
    /// executors. Entries are tiny; contention is negligible.
    receipts: Arc<std::sync::Mutex<ReceiptLog>>,
    backlog: VecDeque<ServerEvent>,
    cursor: u64,
}

/// Digest versions: 1 = legacy u64 decimal carried by the v6 migration
/// (map-hit duplicates only), 2 = SHA-256 hex over the canonical
/// payload (full comparison).
const DIGEST_V2: i64 = 2;

/// Internal log key: request ids are only unique per operator, so every
/// map below is keyed by client + id. The protocol keeps the bare id;
/// namespacing is a server-side invariant.
fn ns_key(actor: &str, request_id: &str) -> String {
    format!("{actor}\0{request_id}")
}

#[derive(Debug, Default)]
pub struct ReceiptLog {
    map: HashMap<String, Receipt>,
    /// Completed digests: key → (digest version, digest). Reuse with a
    /// different v2 digest is rejected, never executed twice.
    digests: HashMap<String, (i64, String)>,
    /// In-flight reservations: key → v2 digest. Reserves the id before
    /// execution so concurrent duplicates serialize on one action.
    in_flight: HashMap<String, String>,
    /// Expired terminal ids (bounded in memory; retained wide in the
    /// database): reuse is rejected with an explicit "issue a new id"
    /// error instead of executing as new.
    tombstones: HashSet<String>,
    tombstone_order: VecDeque<String>,
    order: VecDeque<String>,
}

enum ReserveOutcome {
    Fresh,
    Duplicate(Receipt),
    Expired,
    PayloadMismatch,
}

/// Bound the memory receipt log: drop evicted entries, never tombstone
/// them. Shared by every path that inserts into the log so none can
/// grow it without bound.
fn trim_receipt_order(log: &mut ReceiptLog) {
    const ORDER_CAP: usize = 1000;
    while log.order.len() > ORDER_CAP {
        if let Some(old) = log.order.pop_front() {
            log.map.remove(&old);
            log.digests.remove(&old);
        }
    }
}

/// One admitted provider mutation: everything the executor needs,
/// captured under a single lock hold. The ticket is issued in the same
/// critical section it was validated in, so a disconnect, rebind, or
/// auth change landing between validation and ticketing cannot bless
/// stale source/session data.
struct MutationPlan {
    intent_id: String,
    source: ChatSource,
    sess: LiveSession,
    /// Room generation validated at admission: confirmation effects
    /// apply only while the source still lives in this room.
    room: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationKind {
    Send,
    Delete,
    Ban,
}

/// Where-and-when for one mutation: the source plus the generations the
/// operator approved against. Bundled so executor signatures stay
/// reviewable.
struct MutationTarget<'a> {
    source_id: &'a str,
    expected_boot: u64,
    expected_room: u64,
}

/// Post-network outcome classes. `Refused` is a definitive provider
/// verdict; `Unknown` means no verdict (never auto-retried).
enum MutationVerdict {
    Confirmed { remote_id: Option<String> },
    Refused { error: String },
    Unknown { error: String },
}

/// Stable digest of a command payload for idempotency comparison:
/// SHA-256 hex over the canonical JSON. Not a secret; a collision only
/// risks a false duplicate, never a double execution.
fn payload_digest(cmd: &Command) -> String {
    use sha2::{Digest, Sha256};
    let canonical = serde_json::to_string(&cmd.kind).unwrap_or_default();
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

impl ServerState {
    pub fn new(
        cfg: ServerConfig,
        bus: EventSink,
        stop: SharedStop,
        receipts: ReceiptLog,
    ) -> Arc<Mutex<Self>> {
        // Boot id: process-start nanos folded with the pid. Collisions
        // require two boots in the same nanosecond with the same pid —
        // not a planning input.
        let boot = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ ((std::process::id() as u64).wrapping_mul(0x9E3779B97F4A7C15));
        // Gate generations sync from the coordinator at every admission
        // (see plan_mutation): the gate never invents its own session.
        Arc::new(Mutex::new(Self {
            instance: cfg.instance,
            boot,
            operators: cfg.operators,
            accounts: cfg.accounts,
            db_path: cfg.db_path,
            client_ids: cfg.client_ids,
            kick_secret_path: cfg.kick_secret_path,
            kick_relay_url: cfg.kick_relay_url,
            kick_relay_bearer_path: cfg.kick_relay_bearer_path,
            rumble_api_path: cfg.rumble_api_path,
            twitch_oauth: cfg.twitch_oauth,
            youtube_oauth: cfg.youtube_oauth,
            kick_oauth: cfg.kick_oauth,
            endpoint_bases: cfg.endpoint_bases,
            coord: Coordinator::new(BufferLimits::default()),
            gate: SendGate::default(),
            auth_epoch: 0,
            acct_epoch: HashMap::new(),
            stop,
            sink: bus,
            sessions: HashMap::new(),
            workers: HashMap::new(),
            locks: RefreshLocks::default(),
            store: FileTokenStore::new(&cfg.token_store_path, &cfg.token_key_path),
            token_store_path: cfg.token_store_path,
            token_key_path: cfg.token_key_path,
            receipts: Arc::new(std::sync::Mutex::new(receipts)),
            backlog: VecDeque::new(),
            cursor: 0,
        }))
    }

    /// Rebuild the receipt log from durable rows. Dispatch markers without
    /// a terminal outcome become outcome-unknown: the action may have run
    /// before the crash, so it is reported — never silently retried.
    /// Expired rows rebuild tombstones. Legacy v1 digests keep map-hit
    /// duplicate semantics (documented upgrade path, pre-release data).
    pub fn recover_receipts(rows: Vec<stream_manager_core::store::ChatReceiptRow>) -> ReceiptLog {
        let mut log = ReceiptLog::default();
        for r in rows {
            let key = ns_key(&r.client_id, &r.request_id);
            match r.status.as_str() {
                "expired" => {
                    log.tombstones.insert(key.clone());
                    log.tombstone_order.push_back(key);
                }
                "accepted" | "rejected" | "unknown" => {
                    let status = match r.status.as_str() {
                        "accepted" => ReceiptStatus::Accepted,
                        "rejected" => ReceiptStatus::Rejected,
                        _ => ReceiptStatus::Unknown,
                    };
                    log.map.insert(
                        key.clone(),
                        Receipt {
                            request_id: r.request_id.clone(),
                            status,
                            intent_id: r.intent_id,
                            error: r.error,
                        },
                    );
                    log.digests
                        .insert(key.clone(), (r.digest_v, r.digest.clone()));
                    log.order.push_back(key);
                }
                _ => {
                    // Dispatched without verdict: possible pre-crash run.
                    log.map.insert(
                        key.clone(),
                        Receipt {
                            request_id: r.request_id.clone(),
                            status: ReceiptStatus::Unknown,
                            intent_id: r.intent_id,
                            error: Some("possible dispatch before restart — not retried".into()),
                        },
                    );
                    log.digests
                        .insert(key.clone(), (r.digest_v, r.digest.clone()));
                    log.order.push_back(key);
                }
            }
        }
        log
    }

    fn oauth(&self, provider: Provider) -> stream_manager_core::credentials::oauth::OAuthProvider {
        match provider {
            Provider::Twitch => self.twitch_oauth.clone(),
            Provider::Youtube => self.youtube_oauth.clone(),
            Provider::Kick => self.kick_oauth.clone(),
            Provider::Rumble => self.youtube_oauth.clone(),
        }
    }

    fn client_id(&self, provider: Provider) -> String {
        self.client_ids
            .get(provider.slug())
            .cloned()
            .unwrap_or_default()
    }

    /// Provider API host, overridable per host for mock transports.
    /// Twitch splits identity (`id.`) from API (`api.`), so it gets its
    /// own key; YouTube and Kick share one host for identity and chat.
    /// Callers append the documented endpoint path.
    fn api_base(&self, provider: Provider) -> String {
        let (key, production) = match provider {
            Provider::Twitch => ("twitch_api", "https://api.twitch.tv"),
            Provider::Youtube => ("youtube", "https://www.googleapis.com"),
            Provider::Kick => ("kick", "https://api.kick.com"),
            Provider::Rumble => ("rumble", "https://rumble.local.invalid"),
        };
        self.endpoint_bases
            .get(key)
            .cloned()
            .unwrap_or_else(|| production.into())
    }

    fn kick_secret(&self) -> anyhow::Result<Option<Secret>> {
        let raw = std::fs::read_to_string(&self.kick_secret_path)
            .map_err(|_| anyhow::anyhow!("kick client secret file unreadable"))?;
        let trimmed = raw.trim().to_owned();
        if trimmed.is_empty() {
            anyhow::bail!("kick client secret file is empty");
        }
        Ok(Some(Secret::new(trimmed)))
    }

    pub fn authenticate(&self, bearer: &str) -> Option<Operator> {
        self.operators
            .iter()
            .find(|o| o.authenticates(bearer))
            .cloned()
    }

    fn provider_of(&self, name: &str) -> Option<Provider> {
        match name {
            "youtube" => Some(Provider::Youtube),
            "twitch" => Some(Provider::Twitch),
            "kick" => Some(Provider::Kick),
            "rumble" => Some(Provider::Rumble),
            _ => None,
        }
    }

    fn account(&self, id: &str) -> Option<AccountBinding> {
        self.accounts.iter().find(|a| a.id == id).cloned()
    }

    fn push_event(&mut self, kind: EventKind) {
        self.cursor += 1;
        self.backlog.push_back(ServerEvent {
            cursor: self.cursor,
            kind,
        });
        while self.backlog.len() > 2000 {
            self.backlog.pop_front();
        }
    }

    /// Receipt log handle for failure paths: grabbed before any planning
    /// lock is taken, so recording a rejection can never self-deadlock
    /// against a guard the caller already holds.
    async fn receipts_of(self_arc: &Arc<Mutex<Self>>) -> Arc<std::sync::Mutex<ReceiptLog>> {
        self_arc.lock().await.receipts.clone()
    }

    /// Authorization context for a receipt row: the provider and channel
    /// the action was admitted against, resolved from its intent. Rows
    /// carry this so reconciliation stays authorizable after a restart,
    /// when the in-memory coordinator is gone.
    fn action_context(&self, intent_id: Option<&str>) -> (String, String) {
        let Some(id) = intent_id else {
            return (String::new(), String::new());
        };
        self.coord
            .intent(id)
            .and_then(|i| {
                self.coord
                    .sources()
                    .into_iter()
                    .find(|(s, _)| s.id == i.source_id)
                    .map(|(s, _)| (s.key.provider.slug().to_owned(), s.channel.clone()))
            })
            .unwrap_or_default()
    }

    /// Single settlement path for every admitted command outcome: persist
    /// the terminal row, mirror rotation victims, and surface durability
    /// loss on the receipt. Every arm of `handle` funnels through here so
    /// no admitted command skips durable outcome recording.
    async fn settle_terminal(
        self_arc: &Arc<Mutex<Self>>,
        db_path: &str,
        actor: &str,
        request_id: &str,
        digest: &str,
        mut receipt: Receipt,
    ) -> Receipt {
        if receipt.status == ReceiptStatus::Duplicate {
            return receipt;
        }
        let status = match receipt.status {
            ReceiptStatus::Accepted => "accepted",
            ReceiptStatus::Rejected => "rejected",
            ReceiptStatus::Unknown => "unknown",
            ReceiptStatus::Duplicate => "dispatched",
        };
        let (provider, channel) = {
            self_arc
                .lock()
                .await
                .action_context(receipt.intent_id.as_deref())
        };
        let row = stream_manager_core::store::ChatReceiptRow {
            client_id: actor.to_owned(),
            request_id: request_id.to_owned(),
            digest: digest.to_owned(),
            digest_v: DIGEST_V2,
            status: status.to_owned(),
            intent_id: receipt.intent_id.clone(),
            error: receipt.error.clone(),
            provider,
            channel,
            updated_utc: Self::now_utc(),
        };
        match Self::persist_terminal(db_path, row).await {
            Ok(victims) => {
                Self::mirror_tombstones(self_arc, victims).await;
            }
            Err(e) => {
                let note = format!("warning: durable outcome write failed ({e:#}); in-memory only");
                receipt.error = Some(match receipt.error.take() {
                    Some(prev) => format!("{prev} | {note}"),
                    None => note,
                });
                // The cached entry must carry the same warning: it was
                // recorded by finish() before persistence was attempted,
                // and a later duplicate must not hide the lost durability.
                let key = ns_key(actor, request_id);
                let receipts = self_arc.lock().await.receipts.clone();
                Self::annotate_cached(receipts, &key, receipt.error.clone()).await;
            }
        }
        receipt
    }

    /// Durable rejection for paths that return before the command tail.
    async fn reject_durable(
        self_arc: &Arc<Mutex<Self>>,
        db_path: &str,
        actor: &str,
        request_id: &str,
        digest: &str,
        error: String,
    ) -> Receipt {
        let receipt = {
            let st = self_arc.lock().await;
            st.finish(
                actor,
                request_id.to_owned(),
                ReceiptStatus::Rejected,
                None,
                Some(error),
            )
        };
        Self::settle_terminal(self_arc, db_path, actor, request_id, digest, receipt).await
    }

    /// Sync rejection constructor over a pre-grabbed receipts handle.
    /// Deliberately lock-free against state: safe inside planning scopes.
    /// Keys are namespaced by operator: request ids are only unique per
    /// client, so another operator's id can never collide.
    fn reject(
        receipts: &Arc<std::sync::Mutex<ReceiptLog>>,
        actor: &str,
        request_id: &str,
        error: String,
    ) -> Receipt {
        Self::finish_receipts(
            receipts,
            &ns_key(actor, request_id),
            request_id.to_owned(),
            ReceiptStatus::Rejected,
            None,
            Some(error),
        )
    }

    /// Room → (provider, channel) directory for permission filtering.
    /// Events from retired sources are unverifiable and therefore hidden.
    fn channel_dir(&self) -> HashMap<(Provider, String), (Provider, String)> {
        self.coord
            .sources()
            .into_iter()
            .map(|(s, _)| {
                (
                    (s.key.provider, s.key.room.clone()),
                    (s.key.provider, s.channel.clone()),
                )
            })
            .collect()
    }

    fn visible_event(
        op: &Operator,
        dir: &HashMap<(Provider, String), (Provider, String)>,
        ev: &TimelineEvent,
    ) -> bool {
        use stream_manager_chat::model::TimelineEvent as TE;
        let key: (Provider, String) = match ev {
            TE::Message(m) => (m.source.provider, m.source.room.clone()),
            TE::Deletion { source, .. }
            | TE::Clear { source, .. }
            | TE::ConnChange { source, .. }
            | TE::Gap { source, .. } => (source.provider, source.room.clone()),
        };
        let Some((provider, channel)) = dir.get(&key) else {
            return false;
        };
        op.can_read(*provider, channel)
    }

    fn source_view(&self, s: &ChatSource, conn: &ConnState) -> SourceView {
        SourceView {
            source: s.clone(),
            conn: conn.clone(),
            caps: caps_of(s.key.provider),
            room_gen: self.coord.room_gen_of(&s.id).unwrap_or(0),
        }
    }

    fn visible_sources(&self, op: &Operator) -> Vec<SourceView> {
        self.coord
            .sources()
            .into_iter()
            .filter(|(s, _)| op.can_read(s.key.provider, &s.channel))
            .map(|(s, conn)| self.source_view(s, conn))
            .collect()
    }

    fn visible_recent(&self, op: &Operator) -> Vec<TimelineEvent> {
        let dir = self.channel_dir();
        self.coord
            .timeline()
            .iter()
            .rev()
            .filter(|ev| Self::visible_event(op, &dir, ev))
            .take(300)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn boot(&self) -> u64 {
        self.boot
    }

    pub fn snapshot_for(&self, op: &Operator) -> Snapshot {
        Snapshot {
            instance: self.instance.clone(),
            boot: self.boot,
            session: self.coord.session_gen(),
            sources: self.visible_sources(op),
            recent: self.visible_recent(op),
            backlog_start: self.cursor.saturating_sub(self.backlog.len() as u64),
        }
    }

    fn filter_backlog(&self, op: &Operator, events: Vec<ServerEvent>) -> Vec<ServerEvent> {
        let dir = self.channel_dir();
        events
            .into_iter()
            .filter_map(|e| match e.kind {
                // Snapshots are built per operator at read time; gaps
                // carry no channel content.
                EventKind::Snapshot { .. } => Some(ServerEvent {
                    cursor: e.cursor,
                    kind: EventKind::Snapshot {
                        snapshot: self.snapshot_for(op),
                    },
                }),
                EventKind::Gap { reason } => Some(ServerEvent {
                    cursor: e.cursor,
                    kind: EventKind::Gap { reason },
                }),
                EventKind::Sources { .. } => {
                    let sources = self.visible_sources(op);
                    if sources.is_empty() {
                        None
                    } else {
                        Some(ServerEvent {
                            cursor: e.cursor,
                            kind: EventKind::Sources { sources },
                        })
                    }
                }
                EventKind::Timeline { events } => {
                    let kept: Vec<TimelineEvent> = events
                        .into_iter()
                        .filter(|ev| Self::visible_event(op, &dir, ev))
                        .collect();
                    if kept.is_empty() {
                        None
                    } else {
                        Some(ServerEvent {
                            cursor: e.cursor,
                            kind: EventKind::Timeline { events: kept },
                        })
                    }
                }
                EventKind::Intent {
                    intent_id,
                    state,
                    error,
                } => self
                    .coord
                    .intent(&intent_id)
                    .and_then(|i| {
                        self.coord
                            .sources()
                            .into_iter()
                            .find(|(s, _)| s.id == i.source_id)
                    })
                    .filter(|(s, _)| op.can_read(s.key.provider, &s.channel))
                    .map(|_| ServerEvent {
                        cursor: e.cursor,
                        kind: EventKind::Intent {
                            intent_id,
                            state,
                            error,
                        },
                    }),
            })
            .collect()
    }

    /// Events since `cursor` (exclusive), filtered to the operator's
    /// channels. An expired or unknown cursor yields a fresh snapshot
    /// plus a visible gap — never silent replay.
    pub fn events_since_for(&self, op: &Operator, cursor: u64) -> Vec<ServerEvent> {
        if cursor == 0 {
            return vec![ServerEvent {
                cursor: self.cursor,
                kind: EventKind::Snapshot {
                    snapshot: self.snapshot_for(op),
                },
            }];
        }
        // A future cursor means the client is ahead of this process —
        // after a restart cursors reset to zero, so any larger cursor is
        // stale by construction. Resync visibly instead of stalling the
        // client on events that will never arrive.
        if cursor > self.cursor {
            let mut out = vec![ServerEvent {
                cursor,
                kind: EventKind::Gap {
                    reason:
                        "cursor is ahead of this server (restart?) — state re-synced from snapshot"
                            .into(),
                },
            }];
            out.push(ServerEvent {
                cursor: self.cursor,
                kind: EventKind::Snapshot {
                    snapshot: self.snapshot_for(op),
                },
            });
            return out;
        }
        let oldest = self.cursor.saturating_sub(self.backlog.len() as u64);
        if cursor < oldest {
            let mut out = vec![ServerEvent {
                cursor,
                kind: EventKind::Gap {
                    reason: "event backlog expired; state re-synced from snapshot".into(),
                },
            }];
            out.push(ServerEvent {
                cursor: self.cursor,
                kind: EventKind::Snapshot {
                    snapshot: self.snapshot_for(op),
                },
            });
            return out;
        }
        let events: Vec<ServerEvent> = self
            .backlog
            .iter()
            .filter(|e| e.cursor > cursor)
            .cloned()
            .collect();
        self.filter_backlog(op, events)
    }

    /// Pump one runtime event into the coordinator and the backlog.
    /// The ONLY place bus events enter canonical state.
    pub fn ingest_runtime(&mut self, gen: u64, ev: RuntimeEvent) {
        if gen != self.coord.session_gen() {
            return;
        }
        match ev {
            RuntimeEvent::Message(m) => {
                let _ = self.coord.ingest(*m);
                self.push_timeline_tail(8);
            }
            RuntimeEvent::Delete {
                source_id,
                remote_id,
                room,
            } => {
                if self.coord.room_gen_of(&source_id) != Some(room) {
                    return;
                }
                self.coord.apply_delete(&source_id, &remote_id);
                self.push_timeline_tail(4);
            }
            RuntimeEvent::Clear { source_id, room } => {
                if self.coord.room_gen_of(&source_id) != Some(room) {
                    return;
                }
                self.coord.apply_clear(&source_id);
                self.push_timeline_tail(4);
            }
            RuntimeEvent::Conn {
                source_id,
                state,
                room,
            } => {
                if self.coord.room_gen_of(&source_id) != Some(room) {
                    return;
                }
                self.coord.set_conn(gen, &source_id, state.clone());
                self.push_sources_event();
            }
            RuntimeEvent::WorkerExited {
                source_id,
                room,
                reason,
            } => {
                if self.coord.room_gen_of(&source_id) != Some(room) {
                    return;
                }
                // An exited loop reports absence, never a verdict:
                // terminal provider states (notably Ended) survive the
                // worker. Anything else parks offline with the cause.
                let terminal = matches!(
                    self.coord.conn_of(&source_id),
                    None | Some(ConnState::Ended { .. }) | Some(ConnState::Offline { .. })
                );
                if terminal {
                    return;
                }
                self.coord
                    .set_conn(gen, &source_id, ConnState::Offline { reason });
                self.push_sources_event();
            }
            RuntimeEvent::Gap {
                source_id,
                reason,
                room,
            } => {
                if self.coord.room_gen_of(&source_id) != Some(room)
                    && room != stream_manager_chat::runtime::OVERFLOW_ROOM
                {
                    return;
                }
                self.coord.mark_gap(&source_id, &reason);
                self.push_timeline_tail(4);
            }
        }
    }

    fn push_timeline_tail(&mut self, n: usize) {
        let events: Vec<_> = self
            .coord
            .timeline()
            .iter()
            .rev()
            .take(n)
            .cloned()
            .collect();
        let mut events = events;
        events.reverse();
        if !events.is_empty() {
            self.push_event(EventKind::Timeline { events });
        }
    }

    /// Admit + execute one command. Permission checks precede everything;
    /// idempotency precedes execution; network runs lock-free via a
    /// planned action returned to the caller loop.
    pub async fn handle(self_arc: &Arc<Mutex<Self>>, op: &Operator, cmd: Command) -> Receipt {
        if cmd.v != PROTOCOL_VERSION {
            return Receipt {
                request_id: cmd.request_id,
                status: ReceiptStatus::Rejected,
                intent_id: None,
                error: Some("unsupported protocol version".into()),
            };
        }
        if cmd.actor != op.id {
            return Receipt {
                request_id: cmd.request_id,
                status: ReceiptStatus::Rejected,
                intent_id: None,
                error: Some("actor does not match authenticated operator".into()),
            };
        }
        // Atomic idempotency, keyed by (operator, request id): reserve
        // the digest under the receipts lock before doing anything else.
        // A completed id returns its receipt; an in-flight id is answered
        // Duplicate, never executed twice. Tombstoned (expired) ids are
        // rejected outright — reuse requires a fresh id.
        let digest = payload_digest(&cmd);
        let key = ns_key(&op.id, &cmd.request_id);
        {
            let st = self_arc.lock().await;
            let Ok(mut log) = st.receipts.lock() else {
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Rejected,
                    intent_id: None,
                    error: Some("receipt log unavailable".into()),
                };
            };
            if log.tombstones.contains(&key) {
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Rejected,
                    intent_id: None,
                    error: Some("expired request id — issue a new id".into()),
                };
            }
            // Payload comparison precedes the receipt hit: the same id
            // with a different payload is a new (rejected) action, never
            // the old receipt. Legacy v1 digests have no comparable
            // digest and keep map-hit duplicate semantics.
            if let Some((ver, prev)) = log
                .digests
                .get(&key)
                .cloned()
                .or_else(|| log.in_flight.get(&key).map(|d| (DIGEST_V2, d.clone())))
            {
                if ver == DIGEST_V2 && prev != digest {
                    return Receipt {
                        request_id: cmd.request_id,
                        status: ReceiptStatus::Rejected,
                        intent_id: None,
                        error: Some("request id reused with a different payload".into()),
                    };
                }
            }
            // Completed repeats return the original receipt unchanged:
            // the outcome was already decided, and restating it cannot
            // confuse a client tracking per-request results. Only
            // in-flight repeats report `Duplicate` (no outcome exists).
            if let Some(r) = log.map.get(&key).cloned() {
                return r;
            }
            if let Some(prev) = log.in_flight.get(&key).cloned() {
                if prev != digest {
                    return Receipt {
                        request_id: cmd.request_id,
                        status: ReceiptStatus::Rejected,
                        intent_id: None,
                        error: Some("request id reused with a different payload".into()),
                    };
                }
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Duplicate,
                    intent_id: None,
                    error: Some("request already in flight".into()),
                };
            }
            log.in_flight.insert(key.clone(), digest.clone());
        }
        let db_path = {
            let st = self_arc.lock().await;
            st.db_path.clone()
        };
        // The database is authoritative for repeats the live log has
        // forgotten: consult it before executing, then record a durable
        // dispatch marker. A crash from here on recovers as
        // outcome-unknown, never as a silent retry.
        match Self::reserve_durable(&db_path, &op.id, &cmd.request_id, &digest).await {
            Ok(ReserveOutcome::Fresh) => {}
            Ok(ReserveOutcome::Duplicate(receipt)) => {
                let st = self_arc.lock().await;
                if let Ok(mut log) = st.receipts.lock() {
                    log.in_flight.remove(&key);
                    log.map.insert(key.clone(), receipt.clone());
                    log.digests.insert(key.clone(), (DIGEST_V2, digest.clone()));
                    log.order.push_back(key);
                    trim_receipt_order(&mut log);
                }
                return receipt;
            }
            Ok(ReserveOutcome::Expired) => {
                let st = self_arc.lock().await;
                if let Ok(mut log) = st.receipts.lock() {
                    log.in_flight.remove(&key);
                    log.tombstones.insert(key.clone());
                    log.tombstone_order.push_back(key);
                }
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Rejected,
                    intent_id: None,
                    error: Some("expired request id — issue a new id".into()),
                };
            }
            Ok(ReserveOutcome::PayloadMismatch) => {
                let st = self_arc.lock().await;
                if let Ok(mut log) = st.receipts.lock() {
                    log.in_flight.remove(&key);
                }
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Rejected,
                    intent_id: None,
                    error: Some("request id reused with a different payload".into()),
                };
            }
            Err(e) => {
                let st = self_arc.lock().await;
                if let Ok(mut log) = st.receipts.lock() {
                    log.in_flight.remove(&key);
                }
                return Receipt {
                    request_id: cmd.request_id,
                    status: ReceiptStatus::Rejected,
                    intent_id: None,
                    error: Some(format!("receipt store unavailable: {e:#}")),
                };
            }
        }
        let receipt = match cmd.kind.clone() {
            CommandKind::ConnectSource { source } => {
                Self::connect(self_arc, op, &cmd.request_id, source).await
            }
            CommandKind::ImportToken {
                account_id,
                refresh_token,
            } => {
                Self::import_token(self_arc, op, &cmd.request_id, &account_id, &refresh_token).await
            }
            CommandKind::DisconnectSource { source_id } => {
                // Topology first: parking the source is atomic against a
                // concurrent connect installing it. Order: topo → state.
                let _topo_guard = Self::topo_guard(self_arc).await;
                let mut st = self_arc.lock().await;
                // Known sources require channel permission; unknown ids
                // stay an idempotent no-op.
                if let Some((s, _)) = st
                    .coord
                    .sources()
                    .into_iter()
                    .find(|(s, _)| s.id == source_id)
                    .map(|(s, c)| (s.clone(), c.clone()))
                {
                    if let Err(e) = Self::check_channel(
                        op,
                        Operator::can_read,
                        "disconnect",
                        s.key.provider,
                        &s.channel,
                    ) {
                        let db_path = st.db_path.clone();
                        drop(st);
                        return Self::reject_durable(
                            self_arc,
                            &db_path,
                            &op.id,
                            &cmd.request_id,
                            &digest,
                            e,
                        )
                        .await;
                    }
                }
                st.stop.disarm(&source_id);
                st.drop_worker(&source_id);
                st.gate.cancel_source(&source_id);
                st.gate.remove_room(&source_id);
                st.coord.rebind_room(&source_id);
                let gen = st.coord.session_gen();
                st.coord.set_conn(
                    gen,
                    &source_id,
                    ConnState::Offline {
                        reason: "disconnected by operator".into(),
                    },
                );
                let views: Vec<SourceView> = st
                    .coord
                    .sources()
                    .into_iter()
                    .map(|(s, conn)| {
                        let room_gen = st.coord.room_gen_of(&s.id).unwrap_or(0);
                        SourceView {
                            source: s.clone(),
                            conn: conn.clone(),
                            caps: caps_of(s.key.provider),
                            room_gen,
                        }
                    })
                    .collect();
                st.push_event(EventKind::Sources { sources: views });
                st.finish(
                    &op.id,
                    cmd.request_id.clone(),
                    ReceiptStatus::Accepted,
                    None,
                    None,
                )
            }
            CommandKind::SendMessage {
                source_id,
                expected_boot,
                expected_room,
                text,
                reply_to,
            } => {
                let target = MutationTarget {
                    source_id: &source_id,
                    expected_boot,
                    expected_room,
                };
                Self::send(
                    self_arc,
                    op,
                    &cmd.request_id,
                    &target,
                    &text,
                    reply_to.as_deref(),
                )
                .await
            }
            CommandKind::CancelIntent { intent_id } => {
                let st = self_arc.lock().await;
                // Cancelling someone else's send is a cross-channel action:
                // resolve the intent's source and check it first.
                if let Some((s, _)) = st.coord.intent(&intent_id).and_then(|i| {
                    st.coord
                        .sources()
                        .into_iter()
                        .find(|(s, _)| s.id == i.source_id)
                        .map(|(s, c)| (s.clone(), c.clone()))
                }) {
                    if let Err(e) = Self::check_channel(
                        op,
                        Operator::can_send,
                        "cancel",
                        s.key.provider,
                        &s.channel,
                    ) {
                        let db_path = st.db_path.clone();
                        drop(st);
                        return Self::reject_durable(
                            self_arc,
                            &db_path,
                            &op.id,
                            &cmd.request_id,
                            &digest,
                            e,
                        )
                        .await;
                    }
                }
                // A cancel recorded after dispatch cannot un-run the
                // provider call: the receipt says so explicitly instead
                // of implying the action never ran.
                match st.gate.cancel_state(&intent_id) {
                    stream_manager_chat::gate::CancelState::Queued => st.finish(
                        &op.id,
                        cmd.request_id.clone(),
                        ReceiptStatus::Accepted,
                        None,
                        None,
                    ),
                    stream_manager_chat::gate::CancelState::Dispatched => st.finish(
                        &op.id,
                        cmd.request_id.clone(),
                        ReceiptStatus::Accepted,
                        None,
                        Some(
                            "already dispatched — the provider verdict stands; verify before acting"
                                .into(),
                        ),
                    ),
                    stream_manager_chat::gate::CancelState::Missing => st.finish(
                        &op.id,
                        cmd.request_id.clone(),
                        ReceiptStatus::Rejected,
                        None,
                        Some("nothing cancellable for that send".into()),
                    ),
                }
            }
            CommandKind::LogoutAccount { account_id } => {
                Self::logout(self_arc, op, &cmd.request_id, &account_id).await
            }
            CommandKind::UpsertAccount { account } => {
                Self::upsert_account(self_arc, op, &cmd.request_id, account).await
            }
            CommandKind::RemoveAccount { account_id } => {
                Self::remove_account(self_arc, op, &cmd.request_id, &account_id).await
            }
            CommandKind::DeleteMessage {
                source_id,
                expected_boot,
                expected_room,
                remote_id,
            } => {
                let target = MutationTarget {
                    source_id: &source_id,
                    expected_boot,
                    expected_room,
                };
                Self::moderate(self_arc, op, &cmd.request_id, &target, &remote_id).await
            }
            CommandKind::ModerateUser {
                source_id,
                expected_boot,
                expected_room,
                user,
                duration_secs,
            } => {
                let target = MutationTarget {
                    source_id: &source_id,
                    expected_boot,
                    expected_room,
                };
                Self::moderate_user(self_arc, op, &cmd.request_id, &target, &user, duration_secs)
                    .await
            }
            CommandKind::ReconcileUnknown {
                target_request_id,
                outcome,
                evidence,
            } => {
                Self::reconcile(
                    self_arc,
                    op,
                    &cmd.request_id,
                    &target_request_id,
                    outcome,
                    &evidence,
                )
                .await
            }
        };
        Self::settle_terminal(
            self_arc,
            &db_path,
            &op.id,
            &cmd.request_id,
            &digest,
            receipt,
        )
        .await
    }

    async fn persist_terminal(
        db_path: &str,
        row: stream_manager_core::store::ChatReceiptRow,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let db = db_path.to_owned();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<(String, String)>> {
            let conn = stream_manager_core::store::open(&db)?;
            stream_manager_core::store::save_chat_receipt(&conn, &row)?;
            // Tombstones are retained wide (100k small rows): expiry must
            // stay a rejection for any id the database still remembers.
            // Request ids must be UUIDs — legitimate reuse across that
            // horizon is not a planning input.
            stream_manager_core::store::rotate_chat_receipts(&conn, 1000)
        })
        .await?
    }

    fn now_utc() -> String {
        chrono::Utc::now().to_rfc3339()
    }

    /// Annotate one cached receipt's error field (durability warnings).
    /// Takes the receipts handle directly (as mirror_tombstones does):
    /// field access through a held guard trips borrowck here, so the
    /// handle is cloned out of a temporary instead.
    async fn annotate_cached(
        receipts: Arc<std::sync::Mutex<ReceiptLog>>,
        key: &str,
        error: Option<String>,
    ) {
        if let Ok(mut log) = receipts.lock() {
            if let Some(cached) = log.map.get_mut(key) {
                cached.error = error;
            }
        }
    }

    /// Mirror rotation victims into the memory log as tombstones.
    async fn mirror_tombstones(self_arc: &Arc<Mutex<Self>>, victims: Vec<(String, String)>) {
        let receipts = self_arc.lock().await.receipts.clone();
        let Ok(mut log) = receipts.lock() else {
            return;
        };
        for (client, id) in victims {
            let k = ns_key(&client, &id);
            log.map.remove(&k);
            log.digests.remove(&k);
            if log.tombstones.insert(k.clone()) {
                log.tombstone_order.push_back(k);
            }
        }
        while log.tombstone_order.len() > 10_000 {
            if let Some(old) = log.tombstone_order.pop_front() {
                log.tombstones.remove(&old);
            }
        }
    }

    /// Database-backed reservation: the claim runs in one IMMEDIATE
    /// transaction, so two processes racing the same id serialize and
    /// exactly one observes absence. A stored terminal row answers
    /// repeats without executing; an expired row refuses; a dispatched
    /// marker reports outcome-unknown.
    async fn reserve_durable(
        db_path: &str,
        client_id: &str,
        request_id: &str,
        digest: &str,
    ) -> anyhow::Result<ReserveOutcome> {
        let db = db_path.to_owned();
        let client = client_id.to_owned();
        let id = request_id.to_owned();
        let digest = digest.to_owned();
        tokio::task::spawn_blocking(move || -> anyhow::Result<ReserveOutcome> {
            use stream_manager_core::store as s;
            let conn = s::open(&db)?;
            let now = chrono::Utc::now().to_rfc3339();
            match s::claim_chat_receipt(&conn, &client, &id, &digest, &now)? {
                s::ReceiptClaim::Fresh => Ok(ReserveOutcome::Fresh),
                s::ReceiptClaim::Existing(row) => match row.status.as_str() {
                    "expired" => Ok(ReserveOutcome::Expired),
                    "accepted" | "rejected" | "unknown" => {
                        if row.digest_v == DIGEST_V2 && row.digest != digest {
                            return Ok(ReserveOutcome::PayloadMismatch);
                        }
                        let status = match row.status.as_str() {
                            "accepted" => ReceiptStatus::Accepted,
                            "rejected" => ReceiptStatus::Rejected,
                            _ => ReceiptStatus::Unknown,
                        };
                        Ok(ReserveOutcome::Duplicate(Receipt {
                            request_id: row.request_id,
                            status,
                            intent_id: row.intent_id,
                            error: row.error,
                        }))
                    }
                    _ => Ok(ReserveOutcome::Duplicate(Receipt {
                        request_id: row.request_id,
                        status: ReceiptStatus::Unknown,
                        intent_id: row.intent_id,
                        error: Some("possible dispatch before restart — not retried".into()),
                    })),
                },
            }
        })
        .await?
    }

    fn finish(
        &self,
        actor: &str,
        request_id: String,
        status: ReceiptStatus,
        intent_id: Option<String>,
        error: Option<String>,
    ) -> Receipt {
        Self::finish_receipts(
            &self.receipts,
            &ns_key(actor, &request_id),
            request_id,
            status,
            intent_id,
            error,
        )
    }

    async fn upsert_account(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        account: AccountBinding,
    ) -> Receipt {
        let receipts = self_arc.lock().await.receipts.clone();
        let fail = |e: String| Self::reject(&receipts, &op.id, request_id, e);
        if !op.manages_accounts() {
            return fail("account administration requires the admin role".into());
        }
        if account.id.trim().is_empty()
            || account.channel_id.trim().is_empty()
            || account.channel_name.trim().is_empty()
        {
            return fail("account id, channel id, and channel name are required".into());
        }
        if self_arc
            .lock()
            .await
            .provider_of(&account.provider)
            .is_none()
        {
            return fail(format!("unknown provider: {}", account.provider));
        }
        // Serialize by stable account id across the durable write and
        // the memory update, so concurrent account operations cannot
        // leave database and memory reflecting different winners. The
        // topology lock makes the identity-change parking atomic
        // against concurrent connects. Order: acct-op → topo → state.
        let acct_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("acct:{}", account.id))
        };
        let _acct_guard = acct_lock.lock().await;
        let db_path = self_arc.lock().await.db_path.clone();
        let row = stream_manager_core::store::AccountRow {
            id: account.id.clone(),
            provider: account.provider.clone(),
            channel_id: account.channel_id.clone(),
            channel_name: account.channel_name.clone(),
            credential_ref: String::new(),
        };
        let saved = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let conn = stream_manager_core::store::open(&db_path)?;
            stream_manager_core::store::save_account(&conn, &row)?;
            Ok(())
        })
        .await;
        if !matches!(saved, Ok(Ok(()))) {
            return fail("storing account failed".into());
        }
        let _topo_guard = Self::topo_guard(self_arc).await;
        let mut st = self_arc.lock().await;
        let account_id = account.id.clone();
        let identity_changed = st
            .accounts
            .iter()
            .find(|a| a.id == account_id)
            .is_some_and(|a| {
                a.provider != account.provider
                    || a.channel_id != account.channel_id
                    || a.channel_name != account.channel_name
            });
        if let Some(existing) = st.accounts.iter_mut().find(|a| a.id == account_id) {
            *existing = account;
        } else {
            st.accounts.push(account);
        }
        // A changed canonical identity revokes the live session admitted
        // under the old one: bound sources park offline and re-resolve
        // against the new binding instead of acting as the old identity.
        let note = if identity_changed {
            st.drop_account_live(&account_id, "account identity changed");
            Some("account identity changed — live session dropped, reconnect sources".to_string())
        } else {
            None
        };
        st.finish(
            &op.id,
            request_id.to_owned(),
            ReceiptStatus::Accepted,
            None,
            note,
        )
    }

    async fn remove_account(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        account_id: &str,
    ) -> Receipt {
        let receipts = {
            let st = self_arc.lock().await;
            st.receipts.clone()
        };
        let reject = |e: String| Self::reject(&receipts, &op.id, request_id, e);
        if !op.manages_accounts() {
            return reject("account administration requires the admin role".into());
        }
        // Serialize against concurrent refresh/import/logout for this
        // account, and against concurrent connects while sessions drop.
        // Order: acct-op → topo → state.
        let acct_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("acct:{account_id}"))
        };
        let _acct_guard = acct_lock.lock().await;
        let _topo_guard = Self::topo_guard(self_arc).await;
        {
            let st = self_arc.lock().await;
            if st.accounts.iter().all(|a| a.id != account_id) {
                return reject("unknown account".into());
            }
            if st
                .coord
                .sources()
                .into_iter()
                .any(|(s, _)| s.account_id.as_deref() == Some(account_id))
            {
                return reject("account still has bound sources — disconnect them first".into());
            }
        }
        let db_path = self_arc.lock().await.db_path.clone();
        let id = account_id.to_owned();
        let removed = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let conn = stream_manager_core::store::open(&db_path)?;
            stream_manager_core::store::delete_account(&conn, &id)?;
            Ok(())
        })
        .await;
        if !matches!(removed, Ok(Ok(()))) {
            return reject("removing account failed".into());
        }
        let mut st = self_arc.lock().await;
        st.sessions.remove(account_id);
        st.accounts.retain(|a| a.id != account_id);
        // Bound sources are refused while any exist, but an in-flight
        // ticket admitted earlier must still die here, not dispatch.
        *st.acct_epoch.entry(account_id.to_owned()).or_insert(0) += 1;
        st.auth_epoch += 1;
        st.gate.set_auth(st.auth_epoch);
        st.finish(
            &op.id,
            request_id.to_owned(),
            ReceiptStatus::Accepted,
            None,
            None,
        )
    }

    /// Receipt without touching the state mutex: failure paths call this
    /// while a planning guard is alive. Locking state here would
    /// self-deadlock; receipts have their own lock precisely for this.
    /// `key` is the namespaced log key; the stored receipt keeps the
    /// bare protocol id. Eviction drops without tombstoning (see
    /// [`trim_receipt_order`]); only database-mirrored tombstones
    /// expire ids.
    fn finish_receipts(
        receipts: &Arc<std::sync::Mutex<ReceiptLog>>,
        key: &str,
        request_id: String,
        status: ReceiptStatus,
        intent_id: Option<String>,
        error: Option<String>,
    ) -> Receipt {
        const TOMBSTONE_CAP: usize = 10_000;
        let r = Receipt {
            request_id,
            status,
            intent_id,
            error,
        };
        if let Ok(mut log) = receipts.lock() {
            // Retire the in-flight reservation into the digest registry:
            // later reuse compares payloads instead of executing twice.
            // In-flight reservations always carry v2 digests.
            if let Some(d) = log.in_flight.remove(key) {
                log.digests.insert(key.to_owned(), (DIGEST_V2, d));
            }
            log.order.push_back(key.to_owned());
            log.map.insert(key.to_owned(), r.clone());
            // Pure cache eviction: the entry is dropped, never tombstoned.
            // Every terminal outcome is persisted by the command tail, so
            // a repeat of an evicted id reconsults the database and gets
            // the stored receipt. Tombstoning here would wrongly expire
            // live (especially unknown) outcomes the database retains.
            trim_receipt_order(&mut log);
            while log.tombstone_order.len() > TOMBSTONE_CAP {
                if let Some(old) = log.tombstone_order.pop_front() {
                    log.tombstones.remove(&old);
                }
            }
        }
        r
    }

    /// Park every live source bound to an account: disarm loops, abort
    /// workers, cancel tickets, advance rooms, and mark offline. Shared
    /// by logout and account-identity changes — both revoke the live
    /// session the sources were admitted under.
    fn drop_account_live(&mut self, account_id: &str, reason: &str) {
        let ids: Vec<String> = self
            .coord
            .sources()
            .into_iter()
            .filter(|(s, _)| s.account_id.as_deref() == Some(account_id))
            .map(|(s, _)| s.id.clone())
            .collect();
        for id in ids {
            self.stop.disarm(&id);
            self.drop_worker(&id);
            self.gate.cancel_source(&id);
            self.gate.remove_room(&id);
            self.coord.rebind_room(&id);
            let gen = self.coord.session_gen();
            self.coord.set_conn(
                gen,
                &id,
                ConnState::Offline {
                    reason: reason.into(),
                },
            );
        }
        // Era bump first: any import that started earlier aborts instead
        // of reinstalling a session or token afterwards.
        *self.acct_epoch.entry(account_id.to_owned()).or_insert(0) += 1;
        self.sessions.remove(account_id);
        self.auth_epoch += 1;
        self.gate.set_auth(self.auth_epoch);
        self.push_sources_event();
    }

    /// Admin-only logout: forget the session and the stored refresh,
    /// cancel the account's sources and queued sends. Best effort on
    /// provider revocation; local state is always cleared first.
    async fn logout(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        account_id: &str,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        if !op.manages_accounts() {
            return fail("account administration requires the admin role".into()).await;
        }
        // Serialize this account's lifecycle first (stable id — the
        // credential key can change under an upsert), then topology:
        // parking sources must be atomic against concurrent connects.
        // Lock order everywhere is acct-op → topo → key → state.
        let acct_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("acct:{account_id}"))
        };
        let _acct_guard = acct_lock.lock().await;
        let _topo_guard = Self::topo_guard(self_arc).await;
        let key = {
            let st = self_arc.lock().await;
            let Some(a) = st.account(account_id) else {
                return fail("unknown account".into()).await;
            };
            let Some(p) = st.provider_of(&a.provider) else {
                return fail("unknown provider".into()).await;
            };
            Self::key_id(p, &a.channel_id)
        };
        // Hold the account refresh lock across the whole logout: a
        // concurrent import/refresh holding the same lock cannot persist
        // after this point, and vice versa.
        let refresh_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&key)
        };
        let _refresh_guard = refresh_lock.lock().await;
        let (provider, channel_id, access, client_id, revoke_url) = {
            let mut st = self_arc.lock().await;
            let Some(a) = st.account(account_id) else {
                return fail("unknown account".into()).await;
            };
            let provider = st.provider_of(&a.provider).unwrap_or(Provider::Twitch);
            let access = st.sessions.get(account_id).map(|s| s.access.clone());
            st.drop_account_live(account_id, "logged out");
            let client_id = st.client_id(provider);
            let revoke_url = st.oauth(provider).revoke_url;
            (
                provider,
                a.channel_id.clone(),
                access,
                client_id,
                revoke_url,
            )
        };
        // Topology served its purpose (atomic parking): release it before
        // the unbounded provider/disk calls below. The account and
        // refresh locks stay held so no import can reinstall mid-logout.
        drop(_topo_guard);
        // Network and disk outside the lock; memory is already cleared.
        if let (Some(url), Some(token)) = (revoke_url, access.as_ref()) {
            let http = reqwest::Client::new();
            let _ = stream_manager_core::credentials::oauth::revoke_access_token(
                url, &client_id, token, &http,
            )
            .await;
        }
        let key = Self::key_id(provider, &channel_id);
        let removed = {
            let st = self_arc.lock().await;
            st.store.remove(&key).map_err(|e| format!("{e:#}"))
        };
        let st = self_arc.lock().await;
        match removed {
            Ok(()) => st.finish(
                &op.id,
                request_id.to_owned(),
                ReceiptStatus::Accepted,
                None,
                None,
            ),
            Err(e) => st.finish(
                &op.id,
                request_id.to_owned(),
                ReceiptStatus::Accepted,
                None,
                Some(format!(
                    "logged out locally; stored token removal failed: {e}"
                )),
            ),
        }
    }

    /// Topology ownership: parking/installing sources is atomic against
    /// concurrent connects, disconnects, logouts, and account changes.
    /// Lock order everywhere is acct-op → topo → key → state; the owned
    /// guard lets callers hold it across install + worker registration
    /// with no reentrant locking.
    async fn topo_guard(self_arc: &Arc<Mutex<Self>>) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = { self_arc.lock().await.locks.lock_for("topology") };
        lock.lock_owned().await
    }

    fn check_channel(
        op: &Operator,
        needs: fn(&Operator, Provider, &str) -> bool,
        what: &str,
        provider: Provider,
        channel: &str,
    ) -> Result<(), String> {
        if needs(op, provider, channel) {
            Ok(())
        } else {
            Err(format!(
                "{} not permitted on {}:{}",
                what,
                provider.display(),
                channel
            ))
        }
    }

    /// Admit one mutation and ticket it, atomically: permission,
    /// capability, server boot, room generation, account session, intent
    /// creation, and ticket issue happen under one lock hold. Network
    /// runs lock-free afterwards; execution must `dispatch` immediately
    /// before the mutating request and `settle` right after.
    async fn plan_mutation(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        kind: MutationKind,
        target: &MutationTarget<'_>,
        intent_text: String,
        reply_to: Option<&str>,
    ) -> Result<MutationPlan, String> {
        use stream_manager_chat::model::ReplyRef;
        let mut st = self_arc.lock().await;
        let Some((s, _)) = st
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == target.source_id)
            .map(|(s, c)| (s.clone(), c.clone()))
        else {
            return Err("unknown source".into());
        };
        let (needs, what) = match kind {
            MutationKind::Send => (
                Operator::can_send as fn(&Operator, Provider, &str) -> bool,
                "send",
            ),
            MutationKind::Delete | MutationKind::Ban => (
                Operator::can_moderate as fn(&Operator, Provider, &str) -> bool,
                "moderate",
            ),
        };
        Self::check_channel(op, needs, what, s.key.provider, &s.channel)?;
        // Approvals do not survive a restart: post-crash outcomes are
        // unknown, so a pre-crash snapshot cannot authorize a new action.
        if st.boot != target.expected_boot {
            return Err("server restarted since selection — refresh and retry".into());
        }
        let caps = caps_of(s.key.provider);
        match kind {
            MutationKind::Send if !caps.send.implemented => {
                return Err(format!("{} is read-only here", s.key.provider.display()));
            }
            MutationKind::Delete if !caps.delete.implemented => {
                return Err(format!("{} delete is not wired", s.key.provider.display()));
            }
            MutationKind::Ban if s.key.provider != Provider::Twitch || !caps.ban.implemented => {
                return Err("remote ban is wired for Twitch only".into());
            }
            _ => {}
        }
        if kind == MutationKind::Send && !st.coord.conn_of(&s.id).is_some_and(|c| c.is_receiving())
        {
            return Err("source is not receiving".into());
        }
        // The operator approved against a snapshot generation: a rebind
        // or reconnect since then rejects instead of acting on a room the
        // operator never saw.
        if st.coord.room_gen_of(&s.id) != Some(target.expected_room) {
            return Err("source changed since selection — refresh and retry".into());
        }
        let Some(account_id) = s.account_id.clone() else {
            return Err("source has no bound account".into());
        };
        let Some(sess) = st.sessions.get(&account_id).cloned() else {
            return Err("account has no live session".into());
        };
        if sess.expired() {
            return Err("session expired — reconnect the account".into());
        }
        match kind {
            MutationKind::Send if !sess.can_write(s.key.provider) => {
                return Err("session is read-only".into());
            }
            MutationKind::Delete | MutationKind::Ban if !sess.can_moderate(s.key.provider) => {
                return Err("no live moderator session".into());
            }
            _ => {}
        }
        let reply = match (kind, reply_to) {
            (MutationKind::Send, Some(rid)) => {
                let found = st.coord.timeline().iter().find_map(|ev| match ev {
                    stream_manager_chat::model::TimelineEvent::Message(m)
                        if m.remote_id.as_deref() == Some(rid) && m.source == s.key =>
                    {
                        Some((m.remote_id.clone(), m.author_name.clone()))
                    }
                    _ => None,
                });
                match found {
                    Some((Some(id), author)) => Some(ReplyRef {
                        remote_id: id,
                        author_name: author,
                    }),
                    _ => return Err("reply target is gone or foreign".into()),
                }
            }
            _ => None,
        };
        // The gate never invents generations: sync from the coordinator in
        // the same critical section as the ticket, so the ticket binds
        // exactly the generations validated above.
        st.gate.set_session(st.coord.session_gen());
        // Captured alongside the ticket: confirmation effects later apply
        // only while the source still lives in this room.
        let room = st.coord.room_gen_of(&s.id).unwrap_or(0);
        let Some(intent) = st.coord.create_intent(
            &s.id,
            &sess.identity,
            intent_text,
            reply,
            vec![s.id.clone()],
        ) else {
            return Err("source is gone".into());
        };
        st.gate.issue(&intent.id, &s.id);
        st.push_event(EventKind::Intent {
            intent_id: intent.id.clone(),
            state: "pending".into(),
            error: None,
        });
        Ok(MutationPlan {
            intent_id: intent.id,
            source: s,
            sess,
            room,
        })
    }

    /// Resolve one ticketed-but-never-dispatched intent as rejected and
    /// retire it. For preparation failures (bad payload, unknown user,
    /// transport error on a read): nothing ran, so there is no verdict
    /// to annotate — but the pending intent must still resolve, never
    /// leak.
    async fn abort_mutation(self_arc: &Arc<Mutex<Self>>, intent_id: &str, error: String) {
        let mut st = self_arc.lock().await;
        st.coord
            .resolve_intent(intent_id, IntentState::Rejected, None, Some(error.clone()));
        st.push_event(EventKind::Intent {
            intent_id: intent_id.to_owned(),
            state: "rejected".into(),
            error: Some(error),
        });
        st.gate.retire(intent_id);
    }

    /// Rejection receipt for an admitted-but-never-dispatched command:
    /// carries the intent id so clients can link it to the pending
    /// intent they observed. Plain `reject` stays for pre-admission
    /// failures, which have no intent.
    fn abort_receipt(
        receipts: &Arc<std::sync::Mutex<ReceiptLog>>,
        actor: &str,
        request_id: &str,
        intent_id: &str,
        error: String,
    ) -> Receipt {
        Self::finish_receipts(
            receipts,
            &ns_key(actor, request_id),
            request_id.to_owned(),
            ReceiptStatus::Rejected,
            Some(intent_id.to_owned()),
            Some(error),
        )
    }

    /// Abort one ticketed command end to end: resolve the intent
    /// rejected, retire the ticket, and return the linked receipt.
    async fn abort_command(
        self_arc: &Arc<Mutex<Self>>,
        receipts: &Arc<std::sync::Mutex<ReceiptLog>>,
        actor: &str,
        request_id: &str,
        intent_id: &str,
        error: String,
    ) -> Receipt {
        Self::abort_mutation(self_arc, intent_id, error.clone()).await;
        Self::abort_receipt(receipts, actor, request_id, intent_id, error)
    }

    /// Atomic queued-to-dispatched transition immediately before the
    /// mutating request. A cancel or move landing first prevents
    /// execution; on failure the intent resolves rejected here so no
    /// caller path can leak a pending ticket.
    async fn dispatch_mutation(self_arc: &Arc<Mutex<Self>>, intent_id: &str) -> Result<(), String> {
        let gate = { self_arc.lock().await.gate.clone() };
        if let Err(reason) = gate.dispatch(intent_id) {
            Self::abort_mutation(self_arc, intent_id, reason.to_string()).await;
            return Err(reason.to_string());
        }
        Ok(())
    }

    /// The single post-network exit for every provider mutation: settle
    /// before retiring, resolve the intent with any mid-flight
    /// annotation, emit the Intent event. An optional confirmation side
    /// effect (e.g. marking a message deleted locally) runs under the
    /// same lock. Returns the receipt status and error: confirmed
    /// receipts carry the movement warning instead of hiding it.
    async fn settle_mutation(
        self_arc: &Arc<Mutex<Self>>,
        intent_id: &str,
        verdict: MutationVerdict,
        on_confirmed: impl FnOnce(&mut ServerState),
    ) -> (ReceiptStatus, Option<String>) {
        use stream_manager_chat::gate::Settle;
        let settle = Self::settle_and_retire(self_arc, intent_id).await;
        let moved_note = match settle {
            Settle::Clean => None,
            Settle::CancelledAfterDispatch => Some(
                "cancelled after dispatch — provider verdict stands; verify before acting"
                    .to_string(),
            ),
            Settle::Moved => Some("moved mid-flight — verify before acting".to_string()),
        };
        let with_note = |error: String| {
            moved_note
                .clone()
                .map(|n| format!("{error} ({n})"))
                .unwrap_or(error)
        };
        let mut st = self_arc.lock().await;
        match verdict {
            MutationVerdict::Confirmed { remote_id } => {
                on_confirmed(&mut st);
                st.coord.resolve_intent(
                    intent_id,
                    IntentState::Confirmed,
                    remote_id,
                    moved_note.clone(),
                );
                st.push_event(EventKind::Intent {
                    intent_id: intent_id.to_owned(),
                    state: "confirmed".into(),
                    error: moved_note.clone(),
                });
                (ReceiptStatus::Accepted, moved_note)
            }
            MutationVerdict::Refused { error } => {
                let error = with_note(error);
                st.coord.resolve_intent(
                    intent_id,
                    IntentState::Rejected,
                    None,
                    Some(error.clone()),
                );
                st.push_event(EventKind::Intent {
                    intent_id: intent_id.to_owned(),
                    state: "rejected".into(),
                    error: Some(error.clone()),
                });
                (ReceiptStatus::Rejected, Some(error))
            }
            MutationVerdict::Unknown { error } => {
                let error = with_note(error);
                st.coord.resolve_intent(
                    intent_id,
                    IntentState::OutcomeUnknown,
                    None,
                    Some(error.clone()),
                );
                st.push_event(EventKind::Intent {
                    intent_id: intent_id.to_owned(),
                    state: "outcome-unknown".into(),
                    error: Some(error.clone()),
                });
                (ReceiptStatus::Unknown, Some(error))
            }
        }
    }

    async fn moderate(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        target: &MutationTarget<'_>,
        remote_id: &str,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        // One atomic admission (see plan_mutation): the ticket binds the
        // generations validated here, closing the rebind gap between the
        // old split planning locks.
        let plan = match Self::plan_mutation(
            self_arc,
            op,
            MutationKind::Delete,
            target,
            format!("delete:{remote_id}"),
            None,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => return fail(e).await,
        };
        let source = plan.source.clone();
        let sess = plan.sess.clone();
        // Preparation finishes before dispatch: the broadcaster id is a
        // clone, so the only network below is the mutating request.
        if source.key.provider == Provider::Twitch && source.broadcast_id.is_none() {
            let e = "no resolved broadcaster id — reconnect".to_string();
            return Self::abort_command(
                self_arc,
                &receipts,
                &op.id,
                request_id,
                &plan.intent_id,
                e,
            )
            .await;
        }
        // Full preparation before dispatch: endpoint, credentials, and
        // the request target resolve here, so the dispatch-to-request
        // gap holds no further state reads — a cancel landing during
        // preparation still prevents execution.
        let (api, client_id) = {
            let st = self_arc.lock().await;
            (
                st.api_base(source.key.provider),
                st.client_id(Provider::Twitch),
            )
        };
        let twitch_url = match source.key.provider {
            Provider::Twitch => match source.broadcast_id.clone() {
                // Checked above; the None arm is unreachable defense
                // that settles safely instead of panicking.
                Some(broadcaster_id) => Some(format!(
                    "{api}/helix/moderation/chat?broadcaster_id={broadcaster_id}&moderator_id={}&message_id={remote_id}",
                    sess.user_id
                )),
                None => None,
            },
            _ => None,
        };
        if let Err(reason) = Self::dispatch_mutation(self_arc, &plan.intent_id).await {
            return Self::abort_receipt(&receipts, &op.id, request_id, &plan.intent_id, reason);
        }
        let http = reqwest::Client::new();
        // One mutating request, already dispatched: map the response to
        // a verdict. Transport failure is outcome-unknown (the delete
        // may have landed); a provider refusal is a definitive verdict.
        let verdict = match source.key.provider {
            Provider::Twitch => match twitch_url {
                Some(url) => match http
                    .delete(&url)
                    .header("Client-Id", client_id)
                    .bearer_auth(sess.access.expose())
                    .send()
                    .await
                {
                    Ok(res) if res.status().is_success() => MutationVerdict::Confirmed {
                        remote_id: Some(remote_id.to_owned()),
                    },
                    Ok(res) => MutationVerdict::Refused {
                        error: format!("twitch delete refused: {}", res.status()),
                    },
                    Err(e) => MutationVerdict::Unknown {
                        error: format!("transport: {e}"),
                    },
                },
                None => MutationVerdict::Refused {
                    error: "no resolved broadcaster id — reconnect".into(),
                },
            },
            Provider::Youtube => {
                match http
                    .delete(format!("{api}/youtube/v3/liveChat/messages"))
                    .bearer_auth(sess.access.expose())
                    .query(&[("id", remote_id)])
                    .send()
                    .await
                {
                    Ok(res) if res.status().is_success() => MutationVerdict::Confirmed {
                        remote_id: Some(remote_id.to_owned()),
                    },
                    Ok(res) => MutationVerdict::Refused {
                        error: format!("youtube delete refused: {}", res.status()),
                    },
                    Err(e) => MutationVerdict::Unknown {
                        error: format!("transport: {e}"),
                    },
                }
            }
            Provider::Kick => {
                match http
                    .delete(format!("{api}/public/v1/chat/{remote_id}"))
                    .bearer_auth(sess.access.expose())
                    .send()
                    .await
                {
                    Ok(res) if res.status().is_success() => MutationVerdict::Confirmed {
                        remote_id: Some(remote_id.to_owned()),
                    },
                    Ok(res) => MutationVerdict::Refused {
                        error: format!("kick delete refused: {}", res.status()),
                    },
                    Err(e) => MutationVerdict::Unknown {
                        error: format!("transport: {e}"),
                    },
                }
            }
            // Unreachable: admission rejects unwired deletes. Fail closed
            // with no network rather than omitting the arm.
            Provider::Rumble => MutationVerdict::Refused {
                error: "remote delete is not wired for this provider".into(),
            },
        };
        let sid = target.source_id.to_owned();
        let rid = remote_id.to_owned();
        let room = plan.room;
        let (status, error) = Self::settle_mutation(self_arc, &plan.intent_id, verdict, |st| {
            // Apply the local deletion only while the source still lives
            // in the admitted room: a rebind mid-flight moves the source
            // id to a new room, and the old room's timeline must not
            // absorb effects into the new one. The moved annotation on
            // the verdict tells the operator to verify instead.
            if st.coord.room_gen_of(&sid) != Some(room) {
                return;
            }
            st.coord.apply_delete(&sid, &rid);
            let tail: Vec<_> = st
                .coord
                .timeline()
                .iter()
                .rev()
                .take(2)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            st.push_event(EventKind::Timeline { events: tail });
        })
        .await;
        let st = self_arc.lock().await;
        st.finish(
            &op.id,
            request_id.to_owned(),
            status,
            Some(plan.intent_id),
            error,
        )
    }

    async fn moderate_user(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        target: &MutationTarget<'_>,
        user: &str,
        duration_secs: Option<u64>,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        // One atomic admission (see plan_mutation): validation, intent,
        // and ticket share a single critical section.
        let plan = match Self::plan_mutation(
            self_arc,
            op,
            MutationKind::Ban,
            target,
            format!("ban:{user}"),
            None,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => return fail(e).await,
        };
        let source = plan.source.clone();
        let sess = plan.sess.clone();
        // Preparation finishes before dispatch: user lookup is a
        // read-only GET, so a cancel landing during it still prevents
        // the later mutation.
        let Some(broadcaster_id) = source.broadcast_id.clone() else {
            let e = "no resolved broadcaster id — reconnect".to_string();
            return Self::abort_command(
                self_arc,
                &receipts,
                &op.id,
                request_id,
                &plan.intent_id,
                e,
            )
            .await;
        };
        let http = reqwest::Client::new();
        let (client_id, api) = {
            let st = self_arc.lock().await;
            (
                st.client_id(Provider::Twitch),
                st.api_base(Provider::Twitch),
            )
        };
        // Resolve the login on THIS provider only: same names elsewhere
        // are never touched. A transport error here attempted no
        // mutation, so it resolves rejected — never outcome-unknown.
        let users: serde_json::Value = match http
            .get(format!("{api}/helix/users"))
            .header("Client-Id", client_id.clone())
            .bearer_auth(sess.access.expose())
            .query(&[("login", user.to_lowercase())])
            .send()
            .await
        {
            Ok(res) => res.json().await.unwrap_or_default(),
            Err(e) => {
                let e = format!("user lookup transport: {e}");
                return Self::abort_command(
                    self_arc,
                    &receipts,
                    &op.id,
                    request_id,
                    &plan.intent_id,
                    e,
                )
                .await;
            }
        };
        let Some(user_id) = users["data"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|u| u["id"].as_str())
            .map(str::to_owned)
        else {
            let e = format!("no such twitch user: {user}");
            return Self::abort_command(
                self_arc,
                &receipts,
                &op.id,
                request_id,
                &plan.intent_id,
                e,
            )
            .await;
        };
        let mut data = serde_json::json!({ "user_id": user_id, "reason": "operator moderation from stream-manager" });
        if let Some(secs) = duration_secs {
            data["duration"] = secs.into();
        }
        // Dispatch only now that preparation is complete: the next
        // await is the mutating request itself.
        if let Err(reason) = Self::dispatch_mutation(self_arc, &plan.intent_id).await {
            return Self::abort_receipt(&receipts, &op.id, request_id, &plan.intent_id, reason);
        }
        let res = http
            .post(format!(
                "{api}/helix/moderation/bans?broadcaster_id={broadcaster_id}&moderator_id={}",
                sess.user_id
            ))
            .header("Client-Id", client_id)
            .bearer_auth(sess.access.expose())
            .json(&serde_json::json!({ "data": data }))
            .send()
            .await;
        let verdict = match res {
            Ok(r) if r.status().is_success() => MutationVerdict::Confirmed { remote_id: None },
            Ok(r) => MutationVerdict::Refused {
                error: format!("twitch moderation refused: {}", r.status()),
            },
            Err(e) => MutationVerdict::Unknown {
                error: format!("transport: {e}"),
            },
        };
        let (status, error) =
            Self::settle_mutation(self_arc, &plan.intent_id, verdict, |_| {}).await;
        let st = self_arc.lock().await;
        st.finish(
            &op.id,
            request_id.to_owned(),
            status,
            Some(plan.intent_id),
            error,
        )
    }

    /// Operator attestation for an outcome-unknown receipt: record what
    /// the provider actually did (checked out of band) as evidence.
    /// Never replays the action. Only still-unknown outcomes move; a
    /// decided verdict is never rewritten. The attestation itself is an
    /// admitted command with its own idempotent receipt.
    async fn reconcile(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        target_request_id: &str,
        outcome: ReconciledOutcome,
        evidence: &str,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        let evidence = evidence.trim();
        if evidence.is_empty() {
            return fail("evidence is required — what did the provider actually do?".into()).await;
        }
        if evidence.len() > 512 {
            return fail("evidence too long (<=512 chars)".into()).await;
        }
        // The database moves first, transactionally: concurrent
        // attestations serialize there, and the committed row is then
        // published to memory and clients. Memory alone is never the
        // authority — it may have evicted the outcome or lost it to a
        // restart.
        let key = ns_key(&op.id, target_request_id);
        let in_flight = receipts
            .lock()
            .map(|log| log.in_flight.contains_key(&key))
            .unwrap_or(false);
        if in_flight {
            return fail("action still in flight — wait for its outcome".into()).await;
        }
        let (status_str, word) = match outcome {
            ReconciledOutcome::Confirmed => ("accepted", "confirmed"),
            ReconciledOutcome::Rejected => ("rejected", "rejected"),
        };
        let note = format!("reconciled by {} as {word}: {evidence}", op.id);
        // spawn_blocking needs 'static: clone the borrowed ids out.
        let db_path = self_arc.lock().await.db_path.clone();
        let actor = op.id.clone();
        let target = target_request_id.to_owned();
        let note_stored = note.clone();
        let claimed = tokio::task::spawn_blocking(
            move || -> anyhow::Result<stream_manager_core::store::ReconcileClaim> {
                let conn = stream_manager_core::store::open(&db_path)?;
                stream_manager_core::store::claim_reconcile(
                    &conn,
                    &actor,
                    &target,
                    status_str,
                    &note_stored,
                    &chrono::Utc::now().to_rfc3339(),
                )
            },
        )
        .await;
        use stream_manager_core::store::ReconcileClaim;
        // (committed status, intent, provider, channel, memory-only note)
        let (final_status, target_intent, row_provider, row_channel, memory_only): (
            &str,
            String,
            String,
            String,
            Option<String>,
        ) = match claimed {
            Ok(Ok(ReconcileClaim::Moved(row))) => {
                let Some(intent) = row.intent_id.clone() else {
                    return fail("receipt carries no intent".into()).await;
                };
                (
                    status_str,
                    intent,
                    row.provider.clone(),
                    row.channel.clone(),
                    None,
                )
            }
            Ok(Ok(ReconcileClaim::Decided(row))) => {
                // Converge memory to the decided row, then refuse: the
                // verdict was already attested and must never rewrite.
                Self::publish_receipt(
                    &receipts,
                    &key,
                    target_request_id,
                    row.status.as_str(),
                    row.intent_id.clone(),
                    row.error.clone(),
                );
                return fail("only outcome-unknown receipts can be reconciled".into()).await;
            }
            Ok(Ok(ReconcileClaim::Missing)) => {
                // No durable row: only a memory-only unknown (after an
                // earlier durability loss) may still attest, visibly.
                let mem = receipts
                    .lock()
                    .ok()
                    .and_then(|log| log.map.get(&key).cloned());
                match mem {
                    Some(r) if r.status == ReceiptStatus::Unknown => {
                        let Some(intent) = r.intent_id.clone() else {
                            return fail("receipt carries no intent".into()).await;
                        };
                        (
                            status_str,
                            intent,
                            String::new(),
                            String::new(),
                            Some(
                                "reconciled in memory; no durable unknown row found (earlier durability loss?)"
                                    .to_string(),
                            ),
                        )
                    }
                    _ => {
                        return fail("no completed outcome for that request id".into()).await;
                    }
                }
            }
            Ok(Err(e)) => {
                return fail(format!("reconcile store unavailable: {e:#}")).await;
            }
            Err(e) => {
                return fail(format!("reconcile store unavailable: {e}")).await;
            }
        };
        let mut st = self_arc.lock().await;
        // Authorization comes from the committed row first
        // (restart-safe): fall back to the live coordinator only for
        // rows persisted before the v8 context columns existed.
        let context = if !row_provider.is_empty() && !row_channel.is_empty() {
            st.provider_of(&row_provider)
                .map(|p| (p, row_channel.clone()))
        } else {
            None
        };
        let context = context.or_else(|| {
            st.coord.intent(&target_intent).and_then(|i| {
                st.coord
                    .sources()
                    .into_iter()
                    .find(|(s, _)| s.id == i.source_id)
                    .map(|(s, _)| (s.key.provider, s.channel.clone()))
            })
        });
        let Some((provider, channel)) = context else {
            return fail("no authorization context for that outcome — refusing".into()).await;
        };
        if !(op.can_send(provider, &channel) || op.can_moderate(provider, &channel)) {
            return fail(format!(
                "reconcile not permitted on {}:{}",
                provider.display(),
                channel
            ))
            .await;
        }
        let state = match outcome {
            ReconciledOutcome::Confirmed => IntentState::Confirmed,
            ReconciledOutcome::Rejected => IntentState::Rejected,
        };
        // The intent may be gone after a restart: resolve when present,
        // and only then emit the Intent event (events for unknown
        // intents are filtered downstream anyway).
        if st
            .coord
            .resolve_intent(&target_intent, state, None, Some(note.clone()))
        {
            st.push_event(EventKind::Intent {
                intent_id: target_intent.clone(),
                state: word.into(),
                error: Some(note.clone()),
            });
        }
        Self::publish_receipt(
            &receipts,
            &key,
            target_request_id,
            final_status,
            Some(target_intent.clone()),
            Some(note),
        );
        st.finish(
            &op.id,
            request_id.to_owned(),
            ReceiptStatus::Accepted,
            Some(target_intent),
            memory_only,
        )
    }

    /// Publish one committed outcome into the memory log (insert or
    /// overwrite, never tombstone): repeats and reconciliation converge
    /// here after the database has moved.
    fn publish_receipt(
        receipts: &Arc<std::sync::Mutex<ReceiptLog>>,
        key: &str,
        request_id: &str,
        status: &str,
        intent_id: Option<String>,
        error: Option<String>,
    ) {
        if let Ok(mut log) = receipts.lock() {
            let status = match status {
                "accepted" => ReceiptStatus::Accepted,
                "rejected" => ReceiptStatus::Rejected,
                _ => ReceiptStatus::Unknown,
            };
            log.map.insert(
                key.to_owned(),
                Receipt {
                    request_id: request_id.to_owned(),
                    status,
                    intent_id,
                    error,
                },
            );
            log.order.push_back(key.to_owned());
            trim_receipt_order(&mut log);
        }
    }

    async fn send(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        target: &MutationTarget<'_>,
        text: &str,
        reply_to: Option<&str>,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        if text.trim().is_empty() {
            return fail("message is empty".into()).await;
        }
        // One atomic admission (see plan_mutation): the ticket binds the
        // generations validated here.
        let plan = match Self::plan_mutation(
            self_arc,
            op,
            MutationKind::Send,
            target,
            text.to_owned(),
            reply_to,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => return fail(e).await,
        };
        // Preparation finishes before dispatch: payload building and the
        // reply re-check run first, so a failure aborts with nothing
        // dispatched and no network touched.
        let prepared = match Self::prepare_send(self_arc, &plan, text).await {
            Ok(p) => p,
            Err(e) => {
                return Self::abort_command(
                    self_arc,
                    &receipts,
                    &op.id,
                    request_id,
                    &plan.intent_id,
                    e,
                )
                .await;
            }
        };
        // Dispatch only now: the next await is the mutating request.
        if let Err(reason) = Self::dispatch_mutation(self_arc, &plan.intent_id).await {
            return Self::abort_receipt(&receipts, &op.id, request_id, &plan.intent_id, reason);
        }
        let http = reqwest::Client::new();
        let verdict = Self::fire_send(&http, &plan.sess, text, prepared).await;
        // Unknown stays Unknown end to end: the provider gave no
        // verdict, so nothing may claim success. It persists as
        // unknown and is never auto-retried.
        let (status, error) =
            Self::settle_mutation(self_arc, &plan.intent_id, verdict, |_| {}).await;
        let st = self_arc.lock().await;
        st.finish(
            &op.id,
            request_id.to_owned(),
            status,
            Some(plan.intent_id),
            error,
        )
    }

    /// Settle one dispatched ticket and retire it: the single exit for
    /// every provider mutation (sends and moderation alike). A cancel
    /// recorded before dispatch prevents execution via `dispatch`; a
    /// cancel recorded after dispatch annotates the real provider
    /// verdict instead of pretending the action never ran.
    async fn settle_and_retire(
        self_arc: &Arc<Mutex<Self>>,
        intent_id: &str,
    ) -> stream_manager_chat::gate::Settle {
        let gate = { self_arc.lock().await.gate.clone() };
        let settle = gate.settle_status(intent_id);
        gate.retire(intent_id);
        settle
    }

    /// Pure preparation for a planned send: resolve the target, re-check
    /// the reply binding, build the provider payload. No dispatch, no
    /// network — failures abort before anything is marked as running.
    async fn prepare_send(
        self_arc: &Arc<Mutex<Self>>,
        plan: &MutationPlan,
        text: &str,
    ) -> Result<PreparedSend, String> {
        use stream_manager_chat::adapters::{kick, twitch};
        let source = &plan.source;
        let sess = &plan.sess;
        let api = { self_arc.lock().await.api_base(source.key.provider) };
        match source.key.provider {
            Provider::Twitch => {
                let Some(broadcaster_id) = source.broadcast_id.clone() else {
                    return Err("no resolved broadcaster id — reconnect the source".into());
                };
                let client_id = { self_arc.lock().await.client_id(Provider::Twitch) };
                let reply = {
                    let st = self_arc.lock().await;
                    st.coord
                        .intent(&plan.intent_id)
                        .and_then(|i| i.reply_to.clone())
                        .filter(|r| {
                            st.coord.timeline().iter().any(|ev| match ev {
                                stream_manager_chat::model::TimelineEvent::Message(m) => {
                                    m.remote_id.as_deref() == Some(r.remote_id.as_str())
                                }
                                _ => false,
                            })
                        })
                        .map(|r| r.remote_id)
                };
                let body =
                    twitch::send_payload(&broadcaster_id, &sess.user_id, text, reply.as_deref())?;
                Ok(PreparedSend::Twitch {
                    url: format!("{api}/helix/chat/messages"),
                    client_id,
                    body,
                })
            }
            Provider::Youtube => {
                let Some(chat_id) = source.broadcast_id.clone() else {
                    return Err("no liveChatId (chat ended?)".into());
                };
                Ok(PreparedSend::Youtube {
                    url: format!("{api}/youtube/v3/liveChat/messages"),
                    chat_id,
                })
            }
            Provider::Kick => {
                // Own channel only until a channel directory exists.
                if !source.channel.eq_ignore_ascii_case(&sess.identity) {
                    return Err(
                        "kick user sends need the target broadcaster id; own channel only for now"
                            .into(),
                    );
                }
                let reply = {
                    let st = self_arc.lock().await;
                    st.coord
                        .intent(&plan.intent_id)
                        .and_then(|i| i.reply_to.clone())
                        .map(|r| r.remote_id)
                };
                let body = kick::send_payload(&sess.user_id, text, reply.as_deref())?;
                Ok(PreparedSend::Kick {
                    url: format!("{api}/public/v1/chat"),
                    body,
                })
            }
            // Unreachable: admission rejects unwired sends. Fail closed
            // with no network rather than omitting the arm.
            Provider::Rumble => Err("rumble is read-only here; use the native-chat action".into()),
        }
    }

    /// Fire one prepared send: exactly one mutating request, already
    /// dispatched. Maps the response to a verdict — never settles; the
    /// caller runs the single shared settlement.
    async fn fire_send(
        http: &reqwest::Client,
        sess: &LiveSession,
        text: &str,
        call: PreparedSend,
    ) -> MutationVerdict {
        use stream_manager_chat::adapters::{kick, twitch, youtube};
        match call {
            PreparedSend::Twitch {
                url,
                client_id,
                body,
            } => match http
                .post(url)
                .header("Client-Id", client_id)
                .bearer_auth(sess.access.expose())
                .json(&body)
                .send()
                .await
            {
                Ok(res) => {
                    let status = res.status();
                    let v: serde_json::Value = res.json().await.unwrap_or_default();
                    if !status.is_success() {
                        MutationVerdict::Unknown {
                            error: format!("twitch send {status}"),
                        }
                    } else {
                        match twitch::send_verdict(&v) {
                            Ok(remote_id) => MutationVerdict::Confirmed { remote_id },
                            Err(e) => MutationVerdict::Refused { error: e },
                        }
                    }
                }
                Err(e) => MutationVerdict::Unknown {
                    error: format!("transport: {e}"),
                },
            },
            PreparedSend::Youtube { url, chat_id } => {
                match youtube::insert_message(http, &url, sess.access.expose(), &chat_id, text)
                    .await
                {
                    youtube::InsertOutcome::Sent(remote_id) => {
                        MutationVerdict::Confirmed { remote_id }
                    }
                    youtube::InsertOutcome::Refused(error) => MutationVerdict::Refused { error },
                    youtube::InsertOutcome::Unknown(error) => MutationVerdict::Unknown { error },
                }
            }
            PreparedSend::Kick { url, body } => match http
                .post(url)
                .bearer_auth(sess.access.expose())
                .json(&body)
                .send()
                .await
            {
                Ok(res) => {
                    let status = res.status();
                    let v: serde_json::Value = res.json().await.unwrap_or_default();
                    if !status.is_success() {
                        MutationVerdict::Unknown {
                            error: format!("kick send {status}"),
                        }
                    } else {
                        match kick::send_verdict(&v) {
                            Ok(remote_id) => MutationVerdict::Confirmed { remote_id },
                            Err(e) => MutationVerdict::Refused { error: e },
                        }
                    }
                }
                Err(e) => MutationVerdict::Unknown {
                    error: format!("transport: {e}"),
                },
            },
        }
    }

    /// Ensure a fresh access token for the account (serialized refresh,
    /// unified store key). Returns the session to send with.
    async fn live_session(
        self_arc: &Arc<Mutex<Self>>,
        account_id: &str,
    ) -> Result<LiveSession, String> {
        let (provider, key, client_id, secret) = {
            let st = self_arc.lock().await;
            let Some(a) = st.account(account_id) else {
                return Err("unknown account".into());
            };
            let Some(p) = st.provider_of(&a.provider) else {
                return Err("unknown provider".into());
            };
            let key = Self::key_id(p, &a.channel_id);
            let client_id = st.client_id(p);
            let secret = match p {
                Provider::Kick => st.kick_secret().map_err(|e| format!("{e:#}"))?,
                _ => None,
            };
            (p, key, client_id, secret)
        };
        let oauth = {
            let st = self_arc.lock().await;
            st.oauth(provider)
        };
        // Serialize by stable account id first (the credential key can
        // change under an upsert), then by credential key: logout,
        // import, and refresh for one account are mutually exclusive, so
        // a logout cannot interleave a refresh's verify-and-install.
        // Lock order everywhere is acct-op → key → state.
        let acct_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("acct:{account_id}"))
        };
        let _acct_guard = acct_lock.lock().await;
        let lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&key)
        };
        let _guard = lock.lock().await;
        // Era capture: with the acct-op lock held no logout/import can
        // interleave, but verify at install anyway — installing a
        // session for a revoked era must fail closed, never silently.
        let era = {
            self_arc
                .lock()
                .await
                .acct_epoch
                .get(account_id)
                .copied()
                .unwrap_or(0)
        };
        // Re-read under the lock: a concurrent refresh may have won, and
        // an existing fresh session is reused without network.
        {
            let st = self_arc.lock().await;
            if let Some(s) = st.sessions.get(account_id) {
                if !s.expired() {
                    return Ok(s.clone());
                }
            }
        }
        let http = reqwest::Client::new();
        let (store, bases) = {
            let st = self_arc.lock().await;
            (
                FileTokenStore::new(&st.token_store_path, &st.token_key_path),
                st.endpoint_bases.clone(),
            )
        };
        let refresh = store
            .load_refresh(&key)
            .map_err(|e| format!("no stored credential: {e:#}"))?;
        let r = stream_manager_core::credentials::oauth::refresh_access_token(
            oauth.token_url,
            &client_id,
            secret.as_ref(),
            &refresh,
            &http,
        )
        .await
        .map_err(|e| format!("refresh failed: {e:#}"))?;
        let v = Self::verify_identity_static(&bases, &http, provider, &r.access_token, &r.scope)
            .await?;
        // The refreshed grant must belong to the canonical account before
        // anything is persisted or cached: the stable provider id must
        // match exactly. A matching mutable display name never overrides
        // a different id — names can be renamed or squatted.
        {
            let st = self_arc.lock().await;
            let Some(expected) = st.account(account_id) else {
                return Err("account removed during refresh".into());
            };
            if v.user_id != expected.channel_id {
                return Err("refreshed grant does not match this account".into());
            }
        }
        if let Some(new_refresh) = r.new_refresh.clone() {
            store
                .store_rotated(&key, Secret::new(new_refresh.expose().to_owned()))
                .map_err(|e| format!("storing rotated token: {e:#}"))?;
        }
        let sess = LiveSession {
            access: r.access_token,
            expires_at: chrono::Utc::now().timestamp() + r.expires_in as i64,
            scopes: v.scopes,
            user_id: v.user_id,
            identity: v.identity,
        };
        let mut st = self_arc.lock().await;
        if st.acct_epoch.get(account_id).copied().unwrap_or(0) != era {
            return Err("account changed during refresh — aborted".into());
        }
        st.sessions.insert(account_id.to_owned(), sess.clone());
        Ok(sess)
    }

    async fn connect(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        request_id: &str,
        mut source: ChatSource,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        if let Err(e) = Self::check_channel(
            op,
            Operator::can_read,
            "connect",
            source.key.provider,
            &source.channel,
        ) {
            return fail(e).await;
        }
        // Serialize connects per source: two concurrent connects must
        // not both pass the already-connected check and spawn two loops
        // behind one conn state.
        let connect_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("connect:{}", source.id))
        };
        let _connect_guard = connect_lock.lock().await;
        let (session, room, sink, stop, cfg) = {
            let st = self_arc.lock().await;
            // Canonical binding: the source's account must exist, belong
            // to the same provider, and name this channel. The
            // permission check above authorizes the *operator*; this
            // binds the *target* — a permitted channel label alone never
            // selects the account or broadcast. (Rumble is receive-only
            // with no OAuth account, so it carries no binding.)
            if source.key.provider != Provider::Rumble {
                let Some(account_id) = source.account_id.as_deref() else {
                    return fail("source must bind a canonical account".into()).await;
                };
                let Some(acct) = st.account(account_id) else {
                    return fail("unknown account".into()).await;
                };
                if st.provider_of(&acct.provider) != Some(source.key.provider)
                    || !acct.channel_name.eq_ignore_ascii_case(&source.channel)
                {
                    return fail("source does not match its bound account".into()).await;
                }
            } else {
                // Operator-attested feed: Rumble has no OAuth account to
                // anchor the target, so the room key derives from the
                // permitted channel and the broadcast selector cannot
                // name anything else. The permission check above is the
                // authorization; this normalization removes aliasing.
                if let Some(b) = source.broadcast_id.as_deref() {
                    if !b.eq_ignore_ascii_case(&source.channel) {
                        return fail("rumble broadcast must name the connected channel".into())
                            .await;
                    }
                }
                source.key.room = source.channel.to_lowercase();
            }
            if st
                .coord
                .conn_of(&source.id)
                .is_some_and(|c| c.is_receiving())
            {
                return fail(format!("source {} is already connected", source.id)).await;
            }
            (
                st.coord.session_gen(),
                st.coord.room_gen_of(&source.id).unwrap_or(0),
                st.sink.clone(),
                st.stop.clone(),
                (
                    st.client_id(source.key.provider),
                    st.kick_relay_url.clone(),
                    st.kick_relay_bearer_path.clone(),
                    st.rumble_api_path.clone(),
                    st.api_base(Provider::Twitch),
                    st.api_base(Provider::Youtube),
                ),
            )
        };
        let http = reqwest::Client::new();
        let (client_id, relay_url, relay_bearer, rumble_path, twitch_api, youtube_api) = cfg;
        match source.key.provider {
            Provider::Twitch => {
                let lookup = match Self::session_for_source(self_arc, &source).await {
                    Ok(s) => s,
                    Err(e) => return fail(e).await,
                };
                let broadcaster = match Self::twitch_user_id(
                    &http,
                    &twitch_api,
                    &client_id,
                    lookup.access.expose(),
                    &source.channel,
                )
                .await
                {
                    Ok(id) => id,
                    Err(e) => return fail(e).await,
                };
                // Server-resolved identity: the room and broadcast id
                // come from the provider lookup, never the client.
                source.broadcast_id = Some(broadcaster.clone());
                source.key.room = broadcaster.clone();
                // Install revalidates room, binding, and session: a
                // disconnect or logout during the lookups aborts instead
                // of resurrecting stale state. The loop runs as the
                // current grant, never the pre-lookup clone. Topology is
                // held across install + registration: no disconnect can
                // orphan the handle between them.
                let topo = Self::topo_guard(self_arc).await;
                let (armed, sess) =
                    match Self::install_source(self_arc, op, &source, session, room, true, &topo)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return fail(e).await,
                    };
                let Some(sess) = sess else {
                    return fail("account session changed during connect — retry".into()).await;
                };
                Self::track_worker(
                    self_arc,
                    &source.id,
                    session,
                    armed,
                    stream_manager_chat::runtime::twitch_loop(
                        stream_manager_chat::runtime::LoopCtx {
                            gen: session,
                            room_gen: armed,
                            source_id: source.id.clone(),
                            sink: sink.clone(),
                            stop: stop.clone(),
                        },
                        source.channel.clone(),
                        sess.user_id.clone(),
                        broadcaster,
                        sess.access.expose().to_owned(),
                        client_id,
                        http,
                    ),
                    &topo,
                )
                .await;
            }
            Provider::Youtube => {
                let lookup = match Self::session_for_source(self_arc, &source).await {
                    Ok(s) => s,
                    Err(e) => return fail(e).await,
                };
                let live_chat_id = match Self::resolve_live_chat(
                    &http,
                    &youtube_api,
                    lookup.access.expose(),
                    source.broadcast_id.as_deref(),
                )
                .await
                {
                    Ok(id) => id,
                    Err(e) => return fail(e).await,
                };
                source.key.room = live_chat_id.clone();
                source.broadcast_id = Some(live_chat_id.clone());
                let topo = Self::topo_guard(self_arc).await;
                let (armed, sess) =
                    match Self::install_source(self_arc, op, &source, session, room, true, &topo)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return fail(e).await,
                    };
                let Some(sess) = sess else {
                    return fail("account session changed during connect — retry".into()).await;
                };
                Self::track_worker(
                    self_arc,
                    &source.id,
                    session,
                    armed,
                    stream_manager_chat::runtime::youtube_loop(
                        stream_manager_chat::runtime::LoopCtx {
                            gen: session,
                            room_gen: armed,
                            source_id: source.id.clone(),
                            sink: sink.clone(),
                            stop: stop.clone(),
                        },
                        live_chat_id,
                        sess.access.expose().to_owned(),
                        http,
                    ),
                    &topo,
                )
                .await;
            }
            Provider::Kick => {
                // The relay filters by channel; normalize the room key so
                // the client cannot alias two rooms onto one channel.
                source.key.room = source.channel.to_lowercase();
                let topo = Self::topo_guard(self_arc).await;
                let (armed, _) =
                    match Self::install_source(self_arc, op, &source, session, room, false, &topo)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return fail(e).await,
                    };
                if relay_url.trim().is_empty() {
                    let mut st = self_arc.lock().await;
                    st.coord
                        .set_conn(session, &source.id, ConnState::RelayRequired);
                    st.push_sources_event();
                    return st.finish(
                        &op.id,
                        request_id.to_owned(),
                        ReceiptStatus::Accepted,
                        None,
                        None,
                    );
                }
                Self::track_worker(
                    self_arc,
                    &source.id,
                    session,
                    armed,
                    stream_manager_chat::runtime::kick_relay_loop(
                        stream_manager_chat::runtime::LoopCtx {
                            gen: session,
                            room_gen: armed,
                            source_id: source.id.clone(),
                            sink: sink.clone(),
                            stop: stop.clone(),
                        },
                        relay_url,
                        relay_bearer,
                        http,
                        source.channel.clone(),
                    ),
                    &topo,
                )
                .await;
            }
            Provider::Rumble => {
                if rumble_path.trim().is_empty() {
                    return fail("rumble API URL file not configured".into()).await;
                }
                let topo = Self::topo_guard(self_arc).await;
                let (armed, _) =
                    match Self::install_source(self_arc, op, &source, session, room, false, &topo)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return fail(e).await,
                    };
                Self::track_worker(
                    self_arc,
                    &source.id,
                    session,
                    armed,
                    stream_manager_chat::runtime::rumble_loop(
                        stream_manager_chat::runtime::LoopCtx {
                            gen: session,
                            room_gen: armed,
                            source_id: source.id.clone(),
                            sink: sink.clone(),
                            stop: stop.clone(),
                        },
                        rumble_path,
                        http,
                        source.broadcast_id.clone(),
                    ),
                    &topo,
                )
                .await;
            }
        }
        let st = self_arc.lock().await;
        st.finish(
            &op.id,
            request_id.to_owned(),
            ReceiptStatus::Accepted,
            None,
            None,
        )
    }

    /// Sources needed a live session (Twitch/YouTube): the source's bound
    /// account, refreshed through the unified path.
    async fn session_for_source(
        self_arc: &Arc<Mutex<Self>>,
        source: &ChatSource,
    ) -> Result<LiveSession, String> {
        let Some(account_id) = source.account_id.clone() else {
            return Err("source has no bound account".into());
        };
        Self::live_session(self_arc, &account_id).await
    }

    /// Install a provider-resolved source: revalidate the room
    /// generation, account binding, and live session captured before the
    /// lookups, then activate. A disconnect, logout, or account change
    /// during the lookups fails the install instead of resurrecting a
    /// stale source with stale credentials. Returns the armed room
    /// generation and the current live session (when the source binds an
    /// account and `need_session` is set). The caller must hold the
    /// topology lock (`_topo`, see [`Self::topo_guard`]) across this
    /// call and the subsequent worker registration, so no disconnect
    /// can slip between install and registration.
    async fn install_source(
        self_arc: &Arc<Mutex<Self>>,
        op: &Operator,
        source: &ChatSource,
        session: u64,
        expected_room: u64,
        need_session: bool,
        _topo: &tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<(u64, Option<LiveSession>), String> {
        let mut st = self_arc.lock().await;
        // Identity change on an existing id: the operator must also see
        // the old channel (no hijacking an id you cannot read), and the
        // room advances so tickets bound to the old identity die at the
        // admission check.
        let mut expected = expected_room;
        let changed = st
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == source.id)
            .is_some_and(|(old, _)| {
                old.key.provider != source.key.provider
                    || old.channel != source.channel
                    || old.account_id != source.account_id
            });
        if changed {
            let (old_provider, old_channel) = st
                .coord
                .sources()
                .into_iter()
                .find(|(s, _)| s.id == source.id)
                .map(|(s, _)| (s.key.provider, s.channel.clone()))
                .expect("source observed above");
            Self::check_channel(
                op,
                Operator::can_read,
                "connect",
                old_provider,
                &old_channel,
            )?;
            st.gate.remove_room(&source.id);
            st.coord.rebind_room(&source.id);
            expected = st.coord.room_gen_of(&source.id).unwrap_or(0);
        }
        // A never-before-seen id has no room until the upsert below;
        // anything else must still match the pre-lookup capture.
        match st.coord.room_gen_of(&source.id) {
            None => {
                if expected != 0 {
                    return Err("source changed during connect — retry".into());
                }
            }
            Some(generation) => {
                if generation != expected {
                    return Err("source changed during connect — retry".into());
                }
            }
        }
        let sess: Option<LiveSession> = match (&source.account_id, need_session) {
            (Some(account_id), true) => {
                // Re-resolve the binding: the account may have been
                // removed or rebound during the lookups.
                let acct = st.account(account_id).ok_or("unknown account")?;
                if st.provider_of(&acct.provider) != Some(source.key.provider)
                    || !acct.channel_name.eq_ignore_ascii_case(&source.channel)
                {
                    return Err("source does not match its bound account".into());
                }
                // Use the current session, never the pre-lookup clone: a
                // logout or rotation during the lookups must not install
                // a loop authenticated as the old grant.
                Some(
                    st.sessions
                        .get(account_id)
                        .cloned()
                        .filter(|s| !s.expired())
                        .ok_or("account session changed during connect — retry")?,
                )
            }
            _ => None,
        };
        st.coord.upsert_source(source.clone());
        st.gate
            .set_room(&source.id, st.coord.room_gen_of(&source.id).unwrap_or(0));
        st.stop
            .arm(&source.id, st.coord.room_gen_of(&source.id).unwrap_or(0));
        st.coord
            .set_conn(session, &source.id, ConnState::Connecting);
        st.push_sources_event();
        Ok((st.coord.room_gen_of(&source.id).unwrap_or(0), sess))
    }

    /// Spawn a transport loop under supervision: abort any previous loop
    /// for the source (reconnects never stack duplicates), and report
    /// termination — clean exit or panic — so the source cannot sit at
    /// its last state forever. The report is a `WorkerExited` event:
    /// ingest drops it when the room moved on, and preserves terminal
    /// provider states such as `Ended`. The caller must hold the
    /// topology lock (`_topo`) across install and this call, so a
    /// disconnect cannot land between them and orphan the handle.
    async fn track_worker(
        self_arc: &Arc<Mutex<Self>>,
        source_id: &str,
        session: u64,
        room: u64,
        loop_fut: impl std::future::Future<Output = ()> + Send + 'static,
        _topo: &tokio::sync::OwnedMutexGuard<()>,
    ) {
        use futures_util::FutureExt as _;
        let sink = { self_arc.lock().await.sink.clone() };
        let sid = source_id.to_owned();
        let handle = tokio::spawn(async move {
            let outcome = std::panic::AssertUnwindSafe(loop_fut).catch_unwind().await;
            let reason = match outcome {
                Ok(()) => "transport loop exited".to_string(),
                Err(_) => "transport loop panicked".to_string(),
            };
            let _ = sink
                .send_control(
                    session,
                    RuntimeEvent::WorkerExited {
                        source_id: sid,
                        room,
                        reason,
                    },
                )
                .await;
        });
        // Insert-then-abort-old under one lock: the map never forgets a
        // live handle, and no window runs two loops for one source.
        let mut st = self_arc.lock().await;
        if let Some(old) = st.workers.insert(source_id.to_owned(), handle) {
            old.abort();
        }
    }

    /// Abort and forget a source's transport loop, if any.
    fn drop_worker(&mut self, source_id: &str) {
        if let Some(old) = self.workers.remove(source_id) {
            old.abort();
        }
    }

    fn push_sources_event(&mut self) {
        let sources = self
            .coord
            .sources()
            .into_iter()
            .map(|(s, conn)| {
                let room_gen = self.coord.room_gen_of(&s.id).unwrap_or(0);
                SourceView {
                    source: s.clone(),
                    conn: conn.clone(),
                    caps: caps_of(s.key.provider),
                    room_gen,
                }
            })
            .collect();
        self.push_event(EventKind::Sources { sources });
    }

    async fn twitch_user_id(
        http: &reqwest::Client,
        api_base: &str,
        client_id: &str,
        token: &str,
        login: &str,
    ) -> Result<String, String> {
        let body: serde_json::Value = http
            .get(format!("{api_base}/helix/users"))
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

    async fn resolve_live_chat(
        http: &reqwest::Client,
        api_base: &str,
        token: &str,
        expect_video: Option<&str>,
    ) -> Result<String, String> {
        let body: serde_json::Value = http
            .get(format!("{api_base}/youtube/v3/liveBroadcasts?part=id,snippet&broadcastStatus=active&broadcastType=all&mine=true"))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| format!("broadcast lookup failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("broadcast lookup unparsable: {e}"))?;
        stream_manager_chat::adapters::youtube::resolve_live_chat_id(&body, expect_video)
    }

    fn key_id(provider: Provider, channel_id: &str) -> String {
        format!("{}:{channel_id}", provider.slug())
    }

    /// Admin-only one-time bootstrap: refresh → verify identity AND scopes
    /// → bind to the account → store. Nothing persists before the grant
    /// proves it belongs to the named account.
    async fn import_token(
        self_arc: &Arc<Mutex<ServerState>>,
        op: &Operator,
        request_id: &str,
        account_id: &str,
        refresh_token: &str,
    ) -> Receipt {
        let receipts = Self::receipts_of(self_arc).await;
        let fail = |e: String| {
            let receipts = receipts.clone();
            let rid = request_id.to_owned();
            let actor = op.id.clone();
            async move { Self::reject(&receipts, &actor, &rid, e) }
        };
        if !op.manages_accounts() {
            return fail("account administration requires the admin role".into()).await;
        }
        let (account, oauth, client_id, secret) = {
            let st = self_arc.lock().await;
            let Some(account) = st.account(account_id) else {
                return fail("unknown account".into()).await;
            };
            let Some(provider) = st.provider_of(&account.provider) else {
                return fail("unknown provider".into()).await;
            };
            if provider == Provider::Rumble {
                return fail("rumble has no OAuth flow".into()).await;
            }
            let oauth = st.oauth(provider);
            let client_id = st.client_id(provider);
            let secret = match provider {
                Provider::Kick => match st.kick_secret() {
                    Ok(s) => s,
                    Err(e) => return fail(format!("{e:#}")).await,
                },
                _ => None,
            };
            (account, oauth, client_id, secret)
        };
        if client_id.trim().is_empty() {
            return fail("OAuth client id not configured".into()).await;
        }
        // Serialize by stable account id (the credential key can change
        // under an upsert): import, logout, and refresh for one account
        // are mutually exclusive. Lock order everywhere is acct-op →
        // key → state.
        let acct_lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&format!("acct:{account_id}"))
        };
        let _acct_guard = acct_lock.lock().await;
        // Era capture: a logout bumping this account's era aborts the
        // import below instead of reinstalling credentials afterwards.
        let era = {
            let st = self_arc.lock().await;
            st.acct_epoch.get(account_id).copied().unwrap_or(0)
        };
        let http = reqwest::Client::new();
        let lock = {
            let st = self_arc.lock().await;
            st.locks.lock_for(&ServerState::key_id(
                st.provider_of(&account.provider)
                    .unwrap_or(Provider::Twitch),
                &account.channel_id,
            ))
        };
        let _guard = lock.lock().await;
        let refreshed = match stream_manager_core::credentials::oauth::refresh_access_token(
            oauth.token_url,
            &client_id,
            secret.as_ref(),
            &Secret::new(refresh_token.to_owned()),
            &http,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return fail(format!("token validation failed: {e:#}")).await,
        };
        let (provider_enum, key, bases) = {
            let st = self_arc.lock().await;
            let p = st
                .provider_of(&account.provider)
                .unwrap_or(Provider::Twitch);
            (
                p,
                ServerState::key_id(p, &account.channel_id),
                st.endpoint_bases.clone(),
            )
        };
        let verified = match Self::verify_identity_static(
            &bases,
            &http,
            provider_enum,
            &refreshed.access_token,
            &refreshed.scope,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => return fail(e).await,
        };
        // Exact provider id, never the mutable display name: a matching
        // name with a different id is a foreign grant, not this account.
        if verified.user_id != account.channel_id {
            return fail("provider identity does not match this account".into()).await;
        }
        {
            let st = self_arc.lock().await;
            let read_ok = {
                let o = st.oauth(provider_enum);
                stream_manager_core::credentials::oauth::scopes_cover(
                    &verified.scopes.join(" "),
                    o.chat_read_scopes,
                )
            };
            if !read_ok {
                return fail("granted scopes lack chat read access".into()).await;
            }
        }
        {
            let st = self_arc.lock().await;
            if st.acct_epoch.get(account_id).copied().unwrap_or(0) != era {
                return fail("account changed during import — aborted".into()).await;
            }
            if let Some(new_refresh) = refreshed.new_refresh.clone() {
                if let Err(e) = st
                    .store
                    .store_rotated(&key, Secret::new(new_refresh.expose().to_owned()))
                {
                    return fail(format!("storing rotated token: {e:#}")).await;
                }
            } else if let Err(e) = st
                .store
                .store_rotated(&key, Secret::new(refresh_token.to_owned()))
            {
                return fail(format!("storing token: {e:#}")).await;
            }
            let mut st = st;
            st.sessions.insert(
                account.id.clone(),
                LiveSession {
                    access: refreshed.access_token,
                    expires_at: chrono::Utc::now().timestamp() + refreshed.expires_in as i64,
                    scopes: verified.scopes.clone(),
                    user_id: verified.user_id,
                    identity: verified.identity,
                },
            );
            // A new live session is a new auth generation for the gate.
            st.auth_epoch += 1;
            st.gate.set_auth(st.auth_epoch);
        }
        let st = self_arc.lock().await;
        st.finish(
            &op.id,
            request_id.to_owned(),
            ReceiptStatus::Accepted,
            None,
            None,
        )
    }

    async fn verify_identity_static(
        bases: &HashMap<String, String>,
        http: &reqwest::Client,
        provider: Provider,
        access: &Secret,
        scope_hint: &str,
    ) -> Result<VerifiedIdentity, String> {
        use stream_manager_core::credentials::oauth as o;
        let base = |slug: &str, production: &str| {
            bases
                .get(slug)
                .cloned()
                .unwrap_or_else(|| production.into())
        };
        match provider {
            Provider::Twitch => {
                let body: serde_json::Value = http
                    .get(format!(
                        "{}/oauth2/validate",
                        base("twitch", "https://id.twitch.tv")
                    ))
                    .bearer_auth(access.expose())
                    .send()
                    .await
                    .map_err(|e| format!("twitch validate failed: {e}"))?
                    .json()
                    .await
                    .map_err(|e| format!("twitch validate unparsable: {e}"))?;
                let id = o::parse_twitch_validate(&body)?;
                Ok(VerifiedIdentity {
                    identity: id.login,
                    user_id: id.user_id,
                    scopes: id.scopes,
                })
            }
            Provider::Youtube => {
                let me_body: serde_json::Value = http
                    .get(format!(
                        "{}/youtube/v3/channels?part=id,snippet&mine=true&maxResults=1",
                        base("youtube", "https://www.googleapis.com")
                    ))
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
                let info: serde_json::Value = http
                    .get(format!(
                        "{}/oauth2/v3/tokeninfo",
                        base("youtube", "https://www.googleapis.com")
                    ))
                    .bearer_auth(access.expose())
                    .send()
                    .await
                    .map_err(|e| format!("youtube tokeninfo failed: {e}"))?
                    .json()
                    .await
                    .map_err(|e| format!("youtube tokeninfo unparsable: {e}"))?;
                Ok(VerifiedIdentity {
                    identity,
                    user_id,
                    scopes: o::parse_youtube_tokeninfo(&info)?,
                })
            }
            Provider::Kick => {
                let body: serde_json::Value = http
                    .get(format!(
                        "{}/public/v1/users",
                        base("kick", "https://api.kick.com")
                    ))
                    .bearer_auth(access.expose())
                    .send()
                    .await
                    .map_err(|e| format!("kick users failed: {e}"))?
                    .json()
                    .await
                    .map_err(|e| format!("kick users unparsable: {e}"))?;
                let id = o::parse_kick_users(&body)?;
                Ok(VerifiedIdentity {
                    identity: id.login,
                    user_id: id.user_id,
                    scopes: scope_hint.split_whitespace().map(str::to_owned).collect(),
                })
            }
            Provider::Rumble => Err("rumble has no OAuth flow".into()),
        }
    }
}

#[derive(Debug, Clone)]
struct VerifiedIdentity {
    identity: String,
    user_id: String,
    scopes: Vec<String>,
}

/// A send with everything resolved except the network: built before
/// dispatch, fired immediately after. Nothing here touches the gate.
enum PreparedSend {
    Twitch {
        url: String,
        client_id: String,
        body: serde_json::Value,
    },
    Youtube {
        url: String,
        chat_id: String,
    },
    Kick {
        url: String,
        body: serde_json::Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perms::{ChannelScope, Role};
    use stream_manager_chat::model::{Provider, SourceKey};

    async fn test_state() -> (
        Arc<Mutex<ServerState>>,
        Operator,
        Operator,
        tempfile::TempDir,
    ) {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let can = Operator {
            id: "can".into(),
            role: Role::Admin,
            token_sha256: crate::perms::token_hash_hex("can-token"),
            channels: vec![],
        };
        let dejana = Operator {
            id: "dejana".into(),
            role: Role::Operator,
            token_sha256: crate::perms::token_hash_hex("de-token"),
            channels: vec![ChannelScope {
                provider: "twitch".into(),
                channel: "alice".into(),
            }],
        };
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.db");
        let db_str = db.to_string_lossy().into_owned();
        stream_manager_core::store::open(&db_str).unwrap();
        let cfg = ServerConfig {
            instance: "test".into(),
            operators: vec![can.clone(), dejana.clone()],
            accounts: vec![],
            db_path: db_str,
            token_store_path: "/nonexistent/t.json".into(),
            token_key_path: "/nonexistent/k".into(),
            client_ids: HashMap::new(),
            kick_secret_path: String::new(),
            kick_relay_url: String::new(),
            kick_relay_bearer_path: String::new(),
            rumble_api_path: String::new(),
            twitch_oauth: stream_manager_core::credentials::oauth::TWITCH,
            youtube_oauth: stream_manager_core::credentials::oauth::YOUTUBE,
            kick_oauth: stream_manager_core::credentials::oauth::KICK,
            endpoint_bases: HashMap::new(),
        };
        let st = ServerState::new(
            cfg,
            EventSink::new(tx),
            SharedStop::default(),
            ReceiptLog::default(),
        );
        // Baseline gate generations match a fresh coordinator.
        {
            let guard = st.lock().await;
            guard.gate.set_session(1);
        }
        (st, can, dejana, dir)
    }

    fn cmd(actor: &str, request_id: &str, kind: CommandKind) -> Command {
        Command {
            v: PROTOCOL_VERSION,
            request_id: request_id.into(),
            actor: actor.into(),
            kind,
        }
    }

    #[tokio::test]
    async fn version_actor_and_idempotency() {
        let (st, can, _, _tmp) = test_state().await;
        let bad_version = Command {
            v: 999,
            ..cmd(
                "can",
                "r1",
                CommandKind::DisconnectSource {
                    source_id: "x".into(),
                },
            )
        };
        let r = ServerState::handle(&st, &can, bad_version).await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        let forged = cmd(
            "dejana",
            "r2",
            CommandKind::DisconnectSource {
                source_id: "x".into(),
            },
        );
        let r = ServerState::handle(&st, &can, forged).await;
        assert!(r.error.unwrap().contains("actor"));
        // Unknown source disconnect is accepted-but-noop; repeats are duplicates.
        let mk = || {
            cmd(
                "can",
                "r3",
                CommandKind::DisconnectSource {
                    source_id: "ghost".into(),
                },
            )
        };
        let r1 = ServerState::handle(&st, &can, mk()).await;
        assert_eq!(r1.status, ReceiptStatus::Accepted);
        // Completed repeats return the original receipt unchanged — the
        // outcome was already decided. Only in-flight repeats report
        // `Duplicate` (no outcome exists yet).
        let r2 = ServerState::handle(&st, &can, mk()).await;
        assert_eq!(r2.status, r1.status);
        assert_eq!(r2.request_id, r1.request_id);
    }

    #[tokio::test]
    async fn operator_cannot_touch_unassigned_channels() {
        let (st, _, dejana, _tmp) = test_state().await;
        {
            let mut guard = st.lock().await;
            guard.coord.upsert_source(ChatSource {
                id: "tw".into(),
                key: SourceKey {
                    provider: Provider::Twitch,
                    room: "mallory".into(),
                },
                account_id: None,
                channel: "mallory".into(),
                broadcast_id: None,
                binding_ids: vec![],
            });
        }
        // Bind the boot first: a lock guard inside the handle() call
        // would live across the await and self-deadlock.
        let boot = st.lock().await.boot();
        let r = ServerState::handle(
            &st,
            &dejana,
            cmd(
                "dejana",
                "r1",
                CommandKind::SendMessage {
                    source_id: "tw".into(),
                    expected_boot: boot,
                    expected_room: 0,
                    text: "hi".into(),
                    reply_to: None,
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("not permitted"));
        // Same for moderation on another provider.
        let r = ServerState::handle(
            &st,
            &dejana,
            cmd(
                "dejana",
                "r2",
                CommandKind::ModerateUser {
                    source_id: "tw".into(),
                    expected_boot: boot,
                    expected_room: 0,
                    user: "bob".into(),
                    duration_secs: None,
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
    }

    #[tokio::test]
    async fn import_is_admin_only() {
        let (st, _, dejana, _tmp) = test_state().await;
        let r = ServerState::handle(
            &st,
            &dejana,
            cmd(
                "dejana",
                "r1",
                CommandKind::ImportToken {
                    account_id: "a".into(),
                    refresh_token: "sekret".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("admin"));
    }

    #[tokio::test]
    async fn concurrent_duplicates_execute_once() {
        let (st, can, _, _tmp) = test_state().await;
        let mk = || {
            cmd(
                "can",
                "race-1",
                CommandKind::DisconnectSource {
                    source_id: "ghost".into(),
                },
            )
        };
        let (r1, r2) = tokio::join!(
            ServerState::handle(&st, &can, mk()),
            ServerState::handle(&st, &can, mk()),
        );
        // Exactly one owner executes; the other never duplicates the
        // action, whether it arrives during or after execution.
        let statuses = [r1.status, r2.status];
        assert!(
            statuses.contains(&ReceiptStatus::Accepted)
                && !statuses.contains(&ReceiptStatus::Rejected),
            "{statuses:?}"
        );
        assert!(
            statuses
                .iter()
                .filter(|s| **s == ReceiptStatus::Accepted)
                .count()
                == 1,
            "{statuses:?}"
        );
        // A later repeat with the same payload returns the original
        // Accepted receipt unchanged.
        let r3 = ServerState::handle(&st, &can, mk()).await;
        assert_eq!(r3.status, ReceiptStatus::Accepted);
        // Same id, different payload is rejected, never executed.
        let r4 = ServerState::handle(
            &st,
            &can,
            cmd(
                "can",
                "race-1",
                CommandKind::DisconnectSource {
                    source_id: "other".into(),
                },
            ),
        )
        .await;
        assert_eq!(r4.status, ReceiptStatus::Rejected);
        assert!(r4.error.unwrap().contains("different payload"));
    }

    #[tokio::test]
    async fn settle_failure_warns_cached_and_returned_receipts() {
        let (st, can, _, tmp) = test_state().await;
        // Point settlement at a directory, not a database: every durable
        // write fails deterministically.
        let bad = tmp.path().to_string_lossy().into_owned();
        // Mirror the real flow: finish() records in memory, then the tail
        // settles durably.
        {
            let st = st.lock().await;
            st.finish(&can.id, "x1".into(), ReceiptStatus::Accepted, None, None);
        }
        let receipt = Receipt {
            request_id: "x1".into(),
            status: ReceiptStatus::Accepted,
            intent_id: None,
            error: None,
        };
        let out = ServerState::settle_terminal(&st, &bad, &can.id, "x1", "ab12", receipt).await;
        assert_eq!(out.status, ReceiptStatus::Accepted);
        let note = out.error.expect("durability loss must be reported");
        assert!(note.contains("durable outcome write failed"), "{note}");
        // The cached entry carries the same warning: a later duplicate
        // cannot hide the lost durability.
        let guard = st.lock().await;
        let cached = guard
            .receipts
            .lock()
            .unwrap()
            .map
            .get(&ns_key(&can.id, "x1"))
            .cloned()
            .expect("cached receipt");
        drop(guard);
        assert!(cached
            .error
            .unwrap()
            .contains("durable outcome write failed"));
    }

    #[tokio::test]
    async fn disconnect_and_cancel_need_channel_permission() {
        let (st, _, dejana, _tmp) = test_state().await;
        {
            let mut guard = st.lock().await;
            guard.coord.upsert_source(ChatSource {
                id: "tw".into(),
                key: SourceKey {
                    provider: Provider::Twitch,
                    room: "mallory".into(),
                },
                account_id: None,
                channel: "mallory".into(),
                broadcast_id: None,
                binding_ids: vec![],
            });
        }
        let r = ServerState::handle(
            &st,
            &dejana,
            cmd(
                "dejana",
                "d1",
                CommandKind::DisconnectSource {
                    source_id: "tw".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("not permitted"));
    }

    #[tokio::test]
    async fn expired_cursor_resyncs_with_gap() {
        let (st, can, _, _tmp) = test_state().await;
        let snap = {
            let guard = st.lock().await;
            guard.events_since_for(&can, 0)
        };
        assert_eq!(snap.len(), 1);
        // Any nonzero cursor on an empty backlog is ahead of this
        // process: gap plus snapshot, never a stall. (Flooding the
        // backlog past its bound is impractical here; the large-cursor
        // case is covered by future_cursor_resyncs_with_gap.)
        let back = {
            let guard = st.lock().await;
            guard.events_since_for(&can, 1)
        };
        assert_eq!(back.len(), 2);
        assert!(matches!(back[0].kind, EventKind::Gap { .. }));
        assert!(matches!(back[1].kind, EventKind::Snapshot { .. }));
    }

    #[tokio::test]
    async fn reads_are_channel_filtered() {
        use stream_manager_chat::model::{Provider, SourceKey};
        let (st, can, dejana, _tmp) = test_state().await;
        {
            let mut guard = st.lock().await;
            guard.coord.upsert_source(ChatSource {
                id: "tw".into(),
                key: SourceKey {
                    provider: Provider::Twitch,
                    room: "mallory".into(),
                },
                account_id: None,
                channel: "mallory".into(),
                broadcast_id: None,
                binding_ids: vec![],
            });
            // A message on the unassigned channel, ingested directly.
            let gen = guard.coord.session_gen();
            let room = guard.coord.room_gen_of("tw").unwrap_or(0);
            let _ = guard
                .coord
                .ingest(stream_manager_chat::coordinator::Incoming {
                    session_gen: gen,
                    room_gen: room,
                    source_id: "tw".into(),
                    remote_id: Some("m1".into()),
                    idless_key: None,
                    author_id: None,
                    author_name: "bob".into(),
                    provider_ts: None,
                    text: "secret".into(),
                    fragments: vec![],
                    badges: vec![],
                    reply_to: None,
                    kind: stream_manager_chat::model::MessageKind::Text,
                });
        }
        let admin_view = {
            let guard = st.lock().await;
            guard.snapshot_for(&can)
        };
        assert_eq!(admin_view.sources.len(), 1);
        assert_eq!(admin_view.recent.len(), 1);
        let op_view = {
            let guard = st.lock().await;
            guard.snapshot_for(&dejana)
        };
        assert!(
            op_view.sources.is_empty(),
            "unassigned channel must be hidden"
        );
        assert!(
            op_view.recent.is_empty(),
            "unassigned messages must be hidden"
        );
        let evs = {
            let guard = st.lock().await;
            guard.events_since_for(&dejana, 0)
        };
        assert_eq!(evs.len(), 1);
        match &evs[0].kind {
            EventKind::Snapshot { snapshot } => assert!(snapshot.sources.is_empty()),
            other => panic!("expected filtered snapshot, got {other:?}"),
        }
    }

    fn leaked(s: String) -> &'static str {
        Box::leak(s.into_boxed_str())
    }

    struct MockEnv {
        st: Arc<Mutex<ServerState>>,
        can: Operator,
        _server: wiremock::MockServer,
        _tmp: tempfile::TempDir,
    }

    /// Test state whose Twitch token + identity endpoints hit a local mock,
    /// with one canonical account and a stored refresh token.
    async fn mock_env(validate_body: serde_json::Value) -> MockEnv {
        use stream_manager_core::credentials::oauth as o;
        let server = wiremock::MockServer::start().await;
        let uri = server.uri();
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/token"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "a1",
                    "refresh_token": "r2",
                    "expires_in": 3600,
                    "scope": "user:read:chat",
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/oauth2/validate"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(validate_body))
            .mount(&server)
            .await;
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let can = Operator {
            id: "can".into(),
            role: Role::Admin,
            token_sha256: crate::perms::token_hash_hex("can-token"),
            channels: vec![],
        };
        let dir = tempfile::tempdir().unwrap();
        let db_str = dir.path().join("test.db").to_string_lossy().into_owned();
        stream_manager_core::store::open(&db_str).unwrap();
        let token_uri = format!("{uri}/token");
        let twitch = o::OAuthProvider {
            token_url: leaked(token_uri.clone()),
            ..o::TWITCH
        };
        let mut bases = HashMap::new();
        bases.insert("twitch".to_string(), uri.clone());
        let cfg = ServerConfig {
            instance: "test".into(),
            operators: vec![can.clone()],
            accounts: vec![AccountBinding {
                id: "tw".into(),
                provider: "twitch".into(),
                channel_id: "111".into(),
                channel_name: "alice".into(),
            }],
            db_path: db_str.clone(),
            token_store_path: dir
                .path()
                .join("tokens.json")
                .to_string_lossy()
                .into_owned(),
            token_key_path: dir.path().join("key").to_string_lossy().into_owned(),
            client_ids: [("twitch".to_string(), "cid".to_string())].into(),
            kick_secret_path: String::new(),
            kick_relay_url: String::new(),
            kick_relay_bearer_path: String::new(),
            rumble_api_path: String::new(),
            twitch_oauth: twitch,
            youtube_oauth: o::YOUTUBE,
            kick_oauth: o::KICK,
            endpoint_bases: bases,
        };
        // Runtime key + one stored refresh for the canonical account.
        std::fs::write(dir.path().join("key"), [9u8; 32]).unwrap();
        let store = stream_manager_core::credentials::FileTokenStore::new(
            &dir.path().join("tokens.json").to_string_lossy(),
            &dir.path().join("key").to_string_lossy(),
        );
        store
            .store_rotated(
                "twitch:111",
                stream_manager_core::credentials::Secret::new("old-refresh".into()),
            )
            .unwrap();
        let st = ServerState::new(
            cfg,
            EventSink::new(tx),
            SharedStop::default(),
            ReceiptLog::default(),
        );
        {
            let guard = st.lock().await;
            guard.gate.set_session(1);
        }
        MockEnv {
            st,
            can,
            _server: server,
            _tmp: dir,
        }
    }

    fn twitch_validate(login: &str, user_id: &str) -> serde_json::Value {
        serde_json::json!({
            "login": login,
            "user_id": user_id,
            "scopes": ["user:read:chat"],
        })
    }

    #[tokio::test]
    async fn import_rejects_foreign_grant_and_stores_nothing() {
        let env = mock_env(twitch_validate("mallory", "999")).await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "imp-1",
                CommandKind::ImportToken {
                    account_id: "tw".into(),
                    refresh_token: "submitted".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("does not match"));
        // Nothing from the foreign grant may persist: no session, and the
        // pre-existing stored refresh is untouched (not rotated to the
        // foreign grant's token).
        let guard = env.st.lock().await;
        assert!(!guard.sessions.contains_key("tw"));
        assert_eq!(
            guard.store.load_refresh("twitch:111").unwrap().expose(),
            "old-refresh"
        );
    }

    #[tokio::test]
    async fn import_derives_read_only_caps_from_scopes() {
        let env = mock_env(twitch_validate("alice", "111")).await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "imp-1",
                CommandKind::ImportToken {
                    account_id: "tw".into(),
                    refresh_token: "submitted".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        let guard = env.st.lock().await;
        let sess = guard.sessions.get("tw").expect("session installed");
        assert_eq!(sess.identity, "alice");
        // Read-only grant: no write or moderation capability, derived at
        // every use from the stored scopes — never cached booleans.
        assert!(!sess.can_write(Provider::Twitch));
        assert!(!sess.can_moderate(Provider::Twitch));
    }

    struct LiveEnv {
        st: Arc<Mutex<ServerState>>,
        can: Operator,
        _server: wiremock::MockServer,
        _tmp: tempfile::TempDir,
    }

    /// Live mock environment: token + identity mocks, a `twitch_api`
    /// override for chat endpoints, a receiving source, and an installed
    /// session. The chat mock is caller-supplied so each lifecycle test
    /// controls the provider verdict.
    async fn live_env_with(chat: wiremock::ResponseTemplate, scopes: Vec<&str>) -> LiveEnv {
        use stream_manager_chat::model::ConnState;
        use stream_manager_core::credentials::oauth as o;
        let server = wiremock::MockServer::start().await;
        let uri = server.uri();
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/token"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "a1",
                    "expires_in": 3600,
                    "scope": scopes.join(" "),
                })),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/oauth2/validate"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "login": "alice",
                    "user_id": "111",
                    "scopes": scopes,
                })),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/helix/chat/messages"))
            .respond_with(chat)
            .mount(&server)
            .await;
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let can = Operator {
            id: "can".into(),
            role: Role::Admin,
            token_sha256: crate::perms::token_hash_hex("can-token"),
            channels: vec![],
        };
        let dir = tempfile::tempdir().unwrap();
        let db_str = dir.path().join("test.db").to_string_lossy().into_owned();
        stream_manager_core::store::open(&db_str).unwrap();
        let token_uri = format!("{uri}/token");
        let twitch = o::OAuthProvider {
            token_url: leaked(token_uri.clone()),
            ..o::TWITCH
        };
        let mut bases = HashMap::new();
        bases.insert("twitch".to_string(), uri.clone());
        bases.insert("twitch_api".to_string(), uri.clone());
        let cfg = ServerConfig {
            instance: "test".into(),
            operators: vec![can.clone()],
            accounts: vec![AccountBinding {
                id: "tw".into(),
                provider: "twitch".into(),
                channel_id: "111".into(),
                channel_name: "alice".into(),
            }],
            db_path: db_str,
            token_store_path: dir
                .path()
                .join("tokens.json")
                .to_string_lossy()
                .into_owned(),
            token_key_path: dir.path().join("key").to_string_lossy().into_owned(),
            client_ids: [("twitch".to_string(), "cid".to_string())].into(),
            kick_secret_path: String::new(),
            kick_relay_url: String::new(),
            kick_relay_bearer_path: String::new(),
            rumble_api_path: String::new(),
            twitch_oauth: twitch,
            youtube_oauth: o::YOUTUBE,
            kick_oauth: o::KICK,
            endpoint_bases: bases,
        };
        std::fs::write(dir.path().join("key"), [9u8; 32]).unwrap();
        let store = stream_manager_core::credentials::FileTokenStore::new(
            &dir.path().join("tokens.json").to_string_lossy(),
            &dir.path().join("key").to_string_lossy(),
        );
        store
            .store_rotated(
                "twitch:111",
                stream_manager_core::credentials::Secret::new("old-refresh".into()),
            )
            .unwrap();
        let st = ServerState::new(
            cfg,
            EventSink::new(tx),
            SharedStop::default(),
            ReceiptLog::default(),
        );
        {
            let guard = st.lock().await;
            guard.gate.set_session(1);
        }
        {
            let mut guard = st.lock().await;
            guard.coord.upsert_source(ChatSource {
                id: "live".into(),
                key: SourceKey {
                    provider: Provider::Twitch,
                    room: "alice".into(),
                },
                account_id: Some("tw".into()),
                channel: "alice".into(),
                broadcast_id: Some("111".into()),
                binding_ids: vec![],
            });
            guard.gate.set_room("live", 0);
            guard.stop.arm("live", 0);
            let gen = guard.coord.session_gen();
            guard.coord.set_conn(
                gen,
                "live",
                ConnState::Live {
                    since_utc: "2026-01-01T00:00:00Z".into(),
                },
            );
        }
        // Installs the session through the mocked token + identity flow.
        ServerState::live_session(&st, "tw").await.unwrap();
        LiveEnv {
            st,
            can,
            _server: server,
            _tmp: dir,
        }
    }

    fn chat_sent() -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"message_id": "m1", "is_sent": true}],
        }))
    }

    /// Count provider requests received by the mock, proving
    /// zero-request paths (and no re-execution on repeats) without
    /// relying on expectation panics.
    async fn hits(server: &wiremock::MockServer, part: &str) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.url.path().contains(part))
            .count()
    }

    async fn chat_hits(server: &wiremock::MockServer) -> usize {
        hits(server, "helix/chat").await
    }

    async fn boot_of(st: &Arc<Mutex<ServerState>>) -> u64 {
        st.lock().await.boot()
    }

    fn send_cmd(actor: &str, id: &str, boot: u64, room: u64) -> Command {
        cmd(
            actor,
            id,
            CommandKind::SendMessage {
                source_id: "live".into(),
                expected_boot: boot,
                expected_room: room,
                text: "hi".into(),
                reply_to: None,
            },
        )
    }

    #[tokio::test]
    async fn send_confirmed_repeats_identically() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        let r1 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r1.status, ReceiptStatus::Accepted);
        let intent_id = r1.intent_id.clone().expect("confirmed send carries intent");
        {
            let guard = env.st.lock().await;
            let intent = guard.coord.intent(&intent_id).expect("intent tracked");
            assert_eq!(intent.remote_id.as_deref(), Some("m1"));
        }
        // A repeat returns the original receipt unchanged — same status,
        // same intent — rather than a generic Duplicate.
        let r2 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r2.status, ReceiptStatus::Accepted);
        assert_eq!(r2.intent_id, r1.intent_id);
        // The repeat re-ran nothing: exactly one provider mutation total.
        assert_eq!(chat_hits(&env._server).await, 1);
    }

    #[tokio::test]
    async fn send_unknown_stays_unknown() {
        let env = live_env_with(
            wiremock::ResponseTemplate::new(500),
            vec!["user:read:chat", "user:write:chat"],
        )
        .await;
        let boot = boot_of(&env.st).await;
        let r1 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r1.status, ReceiptStatus::Unknown);
        // The unknown outcome persists durably and repeats identically —
        // never retried, never promoted to success.
        let r2 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r2.status, ReceiptStatus::Unknown);
        assert_eq!(r2.intent_id, r1.intent_id);
        // The unknown repeat was not retried: one attempt total.
        assert_eq!(chat_hits(&env._server).await, 1);
    }

    /// Moderation runs the unified gate lifecycle: ticket, atomic
    /// dispatch, provider verdict, settle, retire — with the same
    /// mid-flight guarantees as sends.
    #[tokio::test]
    async fn moderation_delete_lifecycle() {
        use stream_manager_chat::model::IntentState;
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        // Grant moderation scopes for this scenario.
        {
            let mut guard = env.st.lock().await;
            let sess = guard.sessions.get_mut("tw").unwrap();
            sess.scopes.push("moderator:manage:chat_messages".into());
            sess.scopes.push("moderator:manage:banned_users".into());
        }
        wiremock::Mock::given(wiremock::matchers::method("DELETE"))
            .and(wiremock::matchers::path("/helix/moderation/chat"))
            .respond_with(wiremock::ResponseTemplate::new(204))
            .mount(&env._server)
            .await;
        // Seed one moderator session token lookup is already installed by
        // live_env; the delete resolves against the mock API base.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "del-1",
                CommandKind::DeleteMessage {
                    source_id: "live".into(),
                    expected_boot: boot,
                    expected_room: 0,
                    remote_id: "m1".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        let intent_id = r.intent_id.clone().expect("delete carries intent");
        {
            let guard = env.st.lock().await;
            let intent = guard.coord.intent(&intent_id).expect("intent tracked");
            assert_eq!(intent.state, IntentState::Confirmed);
        }
        // Repeat returns the identical receipt.
        let r2 = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "del-1",
                CommandKind::DeleteMessage {
                    source_id: "live".into(),
                    expected_boot: boot,
                    expected_room: 0,
                    remote_id: "m1".into(),
                },
            ),
        )
        .await;
        assert_eq!(r2.status, ReceiptStatus::Accepted);
        assert_eq!(r2.intent_id, r.intent_id);
        // The repeat re-ran nothing: exactly one provider delete total.
        assert_eq!(hits(&env._server, "helix/moderation/chat").await, 1);
    }

    /// A rebind landing mid-delete must not apply the confirmation to
    /// the new room: the provider verdict settles with its moved
    /// annotation, but the local timeline of the rebound source stays
    /// untouched.
    #[tokio::test]
    async fn delete_rebind_mid_flight_skips_local_apply() {
        use stream_manager_chat::model::IntentState;
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        {
            let mut guard = env.st.lock().await;
            let sess = guard.sessions.get_mut("tw").unwrap();
            sess.scopes.push("moderator:manage:chat_messages".into());
            sess.scopes.push("moderator:manage:banned_users".into());
        }
        wiremock::Mock::given(wiremock::matchers::method("DELETE"))
            .and(wiremock::matchers::path("/helix/moderation/chat"))
            .respond_with(
                wiremock::ResponseTemplate::new(204)
                    .set_delay(std::time::Duration::from_millis(300)),
            )
            .mount(&env._server)
            .await;
        let del = CommandKind::DeleteMessage {
            source_id: "live".into(),
            expected_boot: boot,
            expected_room: 0,
            remote_id: "m1".into(),
        };
        let st = env.st.clone();
        let can = env.can.clone();
        let handle =
            tokio::spawn(
                async move { ServerState::handle(&st, &can, cmd("can", "del-9", del)).await },
            );
        // Wait for admission, let the dispatch land, then rebind
        // mid-request: the 300 ms mock keeps the window open.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let found = {
                    let guard = env.st.lock().await;
                    guard
                        .coord
                        .intents()
                        .into_iter()
                        .any(|i| i.source_id == "live" && i.text == "delete:m1")
                };
                if found {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("delete admits an intent");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let d = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "d9",
                CommandKind::DisconnectSource {
                    source_id: "live".into(),
                },
            ),
        )
        .await;
        assert_eq!(d.status, ReceiptStatus::Accepted);
        let receipt = handle.await.unwrap();
        assert_eq!(receipt.status, ReceiptStatus::Accepted);
        assert!(receipt.error.unwrap().contains("cancelled after dispatch"));
        let guard = env.st.lock().await;
        let intent = guard
            .coord
            .intent(receipt.intent_id.as_deref().unwrap())
            .expect("intent tracked");
        assert_eq!(intent.state, IntentState::Confirmed);
        // The rebound room absorbed no local deletion.
        assert!(
            !guard.coord.timeline().iter().any(|ev| matches!(
                ev,
                stream_manager_chat::model::TimelineEvent::Deletion { .. }
            )),
            "rebound timeline must not carry the old room's deletion"
        );
    }

    #[tokio::test]
    async fn stale_room_rejects_without_network() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        let r = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 999)).await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("changed since selection"));
        assert_eq!(chat_hits(&env._server).await, 0);
    }

    #[tokio::test]
    async fn stale_boot_rejects_without_network() {
        // Approvals do not cross a restart: a snapshot from a previous
        // boot cannot authorize a new action.
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            send_cmd("can", "s1", boot.wrapping_add(1), 0),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("restarted since selection"));
        assert_eq!(chat_hits(&env._server).await, 0);
    }

    fn connect_cmd(actor: &str, id: &str, source: ChatSource) -> Command {
        cmd(actor, id, CommandKind::ConnectSource { source })
    }

    fn twitch_source(id: &str, account: Option<&str>, channel: &str) -> ChatSource {
        ChatSource {
            id: id.into(),
            key: SourceKey {
                provider: Provider::Twitch,
                room: "client-supplied".into(),
            },
            account_id: account.map(str::to_owned),
            channel: channel.into(),
            broadcast_id: None,
            binding_ids: vec![],
        }
    }

    /// Connect binds the canonical account: unbound, unknown-account,
    /// and channel-mismatched sources reject before any network, while a
    /// bound source resolves its room from the provider — the client's
    /// room label is discarded.
    #[tokio::test]
    async fn connect_binds_canonical_account() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        // No account binding: rejected, no network.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c1", twitch_source("evil", None, "alice")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("must bind"));
        // Unknown account: rejected.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c2", twitch_source("evil", Some("ghost"), "alice")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("unknown account"));
        // Channel does not match the bound account: rejected. The
        // operator may read `alice`, but `mallory` is not this account.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c3", twitch_source("evil", Some("tw"), "mallory")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("bound account"));
        // Bound correctly: accepted, with the server-resolved room — the
        // client-supplied "client-supplied" label is gone.
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/helix/users"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "data": [{"id": "111", "login": "alice"}],
                })),
            )
            .mount(&env._server)
            .await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c4", twitch_source("tw2", Some("tw"), "alice")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        let guard = env.st.lock().await;
        let (s, _) = guard
            .coord
            .sources()
            .into_iter()
            .find(|(s, _)| s.id == "tw2")
            .expect("connected source stored");
        assert_eq!(s.key.room, "111");
        assert_eq!(s.broadcast_id.as_deref(), Some("111"));
    }

    /// A disconnect landing during the provider lookups aborts the
    /// install: the generation captured before the lookup no longer
    /// matches, so no source is activated and no worker is registered.
    #[tokio::test]
    async fn disconnect_during_connect_lookup_aborts() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/helix/users"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_delay(std::time::Duration::from_millis(300))
                    .set_body_json(serde_json::json!({
                        "data": [{"id": "111", "login": "alice"}],
                    })),
            )
            .mount(&env._server)
            .await;
        // Establish the source first so the disconnect bumps a real room.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c0", twitch_source("race1", Some("tw"), "alice")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        // Reconnect (slow lookup) while a disconnect lands mid-lookup.
        let st = env.st.clone();
        let can = env.can.clone();
        let handle = tokio::spawn(async move {
            ServerState::handle(
                &st,
                &can,
                connect_cmd("can", "c1", twitch_source("race1", Some("tw"), "alice")),
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let d = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "d1",
                CommandKind::DisconnectSource {
                    source_id: "race1".into(),
                },
            ),
        )
        .await;
        assert_eq!(d.status, ReceiptStatus::Accepted);
        let r = handle.await.unwrap();
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("changed during connect"));
        let guard = env.st.lock().await;
        assert!(!guard.workers.contains_key("race1"));
        assert!(matches!(
            guard.coord.conn_of("race1"),
            Some(ConnState::Offline { .. })
        ));
    }

    /// A logout during the provider lookups revokes the session the
    /// connect was resolved with: the install fails instead of starting
    /// a loop authenticated as the revoked grant.
    #[tokio::test]
    async fn logout_during_connect_drops_session() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/helix/users"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_delay(std::time::Duration::from_millis(300))
                    .set_body_json(serde_json::json!({
                        "data": [{"id": "111", "login": "alice"}],
                    })),
            )
            .mount(&env._server)
            .await;
        let st = env.st.clone();
        let can = env.can.clone();
        let handle = tokio::spawn(async move {
            ServerState::handle(
                &st,
                &can,
                connect_cmd("can", "c1", twitch_source("race2", Some("tw"), "alice")),
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let lo = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "lo1",
                CommandKind::LogoutAccount {
                    account_id: "tw".into(),
                },
            ),
        )
        .await;
        assert_eq!(lo.status, ReceiptStatus::Accepted);
        let r = handle.await.unwrap();
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("session changed during connect"));
        let guard = env.st.lock().await;
        assert!(!guard.sessions.contains_key("tw"));
        assert!(!guard.workers.contains_key("race2"));
    }

    /// Reconnecting an existing id with a different identity advances
    /// the room (old tickets die) instead of silently overwriting the
    /// source another operator approved against.
    #[tokio::test]
    async fn connect_identity_change_rebinds_room() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/helix/users"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "data": [{"id": "111", "login": "alice"}],
                })),
            )
            .mount(&env._server)
            .await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c0", twitch_source("x1", Some("tw"), "alice")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        // Same id, textually different channel: identity change, room
        // advances. (The account binding still matches case-insensitively.)
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd("can", "c1", twitch_source("x1", Some("tw"), "ALICE")),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        let guard = env.st.lock().await;
        assert_eq!(guard.coord.room_gen_of("x1"), Some(1));
    }

    /// Rumble has no OAuth account, so the broadcast selector cannot
    /// name anything but the connected channel — and the room key
    /// derives from it. Forged selectors reject before any network.
    #[tokio::test]
    async fn rumble_forged_broadcast_rejected() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            connect_cmd(
                "can",
                "c1",
                ChatSource {
                    id: "ru".into(),
                    key: SourceKey {
                        provider: Provider::Rumble,
                        room: "anything".into(),
                    },
                    account_id: None,
                    channel: "alice".into(),
                    broadcast_id: Some("mallory".into()),
                    binding_ids: vec![],
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("must name the connected channel"));
    }

    /// Changing an account's canonical identity parks its sources,
    /// drops its session, and kills tickets admitted under the old
    /// identity: nothing dispatches as the replaced account.
    #[tokio::test]
    async fn upsert_identity_change_parks_sources() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        // Ticket admitted under the old identity.
        let target = MutationTarget {
            source_id: "live",
            expected_boot: boot,
            expected_room: 0,
        };
        let plan = ServerState::plan_mutation(
            &env.st,
            &env.can,
            MutationKind::Send,
            &target,
            "hi".into(),
            None,
        )
        .await
        .expect("admission plans");
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "up1",
                CommandKind::UpsertAccount {
                    account: AccountBinding {
                        id: "tw".into(),
                        provider: "twitch".into(),
                        channel_id: "999".into(),
                        channel_name: "alice".into(),
                    },
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        assert!(r.error.unwrap().contains("identity changed"));
        {
            let guard = env.st.lock().await;
            assert!(!guard.sessions.contains_key("tw"));
            assert!(matches!(
                guard.coord.conn_of("live"),
                Some(ConnState::Offline { .. })
            ));
            // The pre-change ticket cannot dispatch anymore.
            assert!(guard.gate.dispatch(&plan.intent_id).is_err());
        }
        // Sends now fail at admission: the source parked offline (and
        // the session is gone, asserted above).
        let r = ServerState::handle(&env.st, &env.can, send_cmd("can", "s9", boot, 0)).await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("not receiving"));
    }

    /// A matching display name with a different provider id is a foreign
    /// grant, not this account: exact id match, never the mutable name.
    #[tokio::test]
    async fn import_rejects_name_match_id_mismatch() {
        let env = mock_env(twitch_validate("alice", "999")).await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "imp-1",
                CommandKind::ImportToken {
                    account_id: "tw".into(),
                    refresh_token: "submitted".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("does not match"));
        let guard = env.st.lock().await;
        assert!(!guard.sessions.contains_key("tw"));
    }

    /// Ban preparation failures resolve the intent as rejected and retire
    /// the ticket: nothing ran, nothing leaks, nothing stays pending.
    #[tokio::test]
    async fn ban_unknown_user_resolves_rejected_without_leak() {
        use stream_manager_chat::model::IntentState;
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        {
            let mut guard = env.st.lock().await;
            let sess = guard.sessions.get_mut("tw").unwrap();
            sess.scopes.push("moderator:manage:chat_messages".into());
            sess.scopes.push("moderator:manage:banned_users".into());
        }
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/helix/users"))
            .and(wiremock::matchers::query_param("login", "ghost"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "data": [] })),
            )
            .mount(&env._server)
            .await;
        let r = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "ban-1",
                CommandKind::ModerateUser {
                    source_id: "live".into(),
                    expected_boot: boot,
                    expected_room: 0,
                    user: "ghost".into(),
                    duration_secs: None,
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("no such twitch user"));
        let intent_id = r.intent_id.clone().expect("rejection carries intent");
        let guard = env.st.lock().await;
        let intent = guard.coord.intent(&intent_id).expect("intent tracked");
        assert_eq!(intent.state, IntentState::Rejected);
        // Retired: a later cancel finds nothing, and no mutation ran.
        assert!(!guard.gate.cancel(&intent_id));
        drop(guard);
        assert_eq!(chat_hits(&env._server).await, 0);
    }

    /// Unknown outcomes reconcile through operator evidence — never
    /// through replay. The attested receipt replaces the unknown one,
    /// repeats return it, and decided verdicts refuse reconciliation.
    #[tokio::test]
    async fn reconcile_unknown_flow() {
        let env = live_env_with(
            wiremock::ResponseTemplate::new(500),
            vec!["user:read:chat", "user:write:chat"],
        )
        .await;
        let boot = boot_of(&env.st).await;
        let r1 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r1.status, ReceiptStatus::Unknown);
        let reconcile = |id: &str, target: &str, outcome: ReconciledOutcome, evidence: &str| {
            cmd(
                "can",
                id,
                CommandKind::ReconcileUnknown {
                    target_request_id: target.into(),
                    outcome,
                    evidence: evidence.into(),
                },
            )
        };
        // Evidence is required.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            reconcile("r0", "s1", ReconciledOutcome::Confirmed, "  "),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        // Unknown targets refuse.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            reconcile("r0", "nope", ReconciledOutcome::Confirmed, "saw it"),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        // Attest confirmed with out-of-band evidence.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            reconcile(
                "r1",
                "s1",
                ReconciledOutcome::Confirmed,
                "provider dashboard shows the message as sent",
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        // The original id now returns the attested receipt — still no
        // second provider mutation.
        let r2 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r2.status, ReceiptStatus::Accepted);
        assert!(r2.error.unwrap().contains("reconciled by can as confirmed"));
        // Attestation replayed nothing: still the single failed attempt.
        assert_eq!(chat_hits(&env._server).await, 1);
        // Decided verdicts never rewrite.
        let r = ServerState::handle(
            &env.st,
            &env.can,
            reconcile("r2", "s1", ReconciledOutcome::Rejected, "changed my mind"),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("only outcome-unknown"));
    }

    /// Restart-safe reconciliation: an unknown outcome settled before a
    /// restart reconciles afterwards from the durable row alone. The
    /// in-memory coordinator is gone, but the row carries provider,
    /// channel, and intent for authorization and publication.
    #[tokio::test]
    async fn reconcile_survives_restart() {
        use stream_manager_core::store as s;
        let env = live_env_with(
            wiremock::ResponseTemplate::new(500),
            vec!["user:read:chat", "user:write:chat"],
        )
        .await;
        let boot = boot_of(&env.st).await;
        let r1 = ServerState::handle(&env.st, &env.can, send_cmd("can", "s1", boot, 0)).await;
        assert_eq!(r1.status, ReceiptStatus::Unknown);
        let intent_id = r1.intent_id.clone().expect("unknown carries intent");
        // Simulate restart: drop all memory, rebuild from the database.
        let LiveEnv {
            st,
            can,
            _server,
            _tmp,
        } = env;
        let db_path = _tmp.path().join("test.db").to_string_lossy().into_owned();
        drop(st);
        let st2 = {
            let recovered = {
                let conn = s::open(&db_path).unwrap();
                ServerState::recover_receipts(s::load_chat_receipts(&conn).unwrap())
            };
            let (tx, _rx) = tokio::sync::mpsc::channel(16);
            ServerState::new(
                ServerConfig {
                    instance: "test".into(),
                    operators: vec![can.clone()],
                    accounts: vec![AccountBinding {
                        id: "tw".into(),
                        provider: "twitch".into(),
                        channel_id: "111".into(),
                        channel_name: "alice".into(),
                    }],
                    db_path: db_path.clone(),
                    token_store_path: _tmp
                        .path()
                        .join("tokens.json")
                        .to_string_lossy()
                        .into_owned(),
                    token_key_path: _tmp.path().join("key").to_string_lossy().into_owned(),
                    client_ids: [("twitch".to_string(), "cid".to_string())].into(),
                    kick_secret_path: String::new(),
                    kick_relay_url: String::new(),
                    kick_relay_bearer_path: String::new(),
                    rumble_api_path: String::new(),
                    twitch_oauth: stream_manager_core::credentials::oauth::TWITCH,
                    youtube_oauth: stream_manager_core::credentials::oauth::YOUTUBE,
                    kick_oauth: stream_manager_core::credentials::oauth::KICK,
                    endpoint_bases: HashMap::new(),
                },
                EventSink::new(tx),
                SharedStop::default(),
                recovered,
            )
        };
        // The coordinator is empty after restart, but the durable row
        // authorizes and publishes the attestation.
        let r = ServerState::handle(
            &st2,
            &can,
            cmd(
                "can",
                "r1",
                CommandKind::ReconcileUnknown {
                    target_request_id: "s1".into(),
                    outcome: ReconciledOutcome::Confirmed,
                    evidence: "provider dashboard shows the message as sent".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        assert_eq!(r.intent_id.as_deref(), Some(intent_id.as_str()));
        // No provider mutation ran during reconciliation, before or
        // after the restart.
        let conn = s::open(&db_path).unwrap();
        let row = s::load_chat_receipt(&conn, "can", "s1").unwrap().unwrap();
        assert_eq!(row.status, "accepted");
        assert!(row
            .error
            .unwrap()
            .contains("reconciled by can as confirmed"));
        // A second attestation refuses: the verdict is decided.
        let r = ServerState::handle(
            &st2,
            &can,
            cmd(
                "can",
                "r2",
                CommandKind::ReconcileUnknown {
                    target_request_id: "s1".into(),
                    outcome: ReconciledOutcome::Rejected,
                    evidence: "changed my mind".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
    }

    /// Supervised workers: a panicking loop is observed with its cause
    /// (not silent), and a clean exit never overwrites a terminal
    /// provider state such as `Ended`.
    #[tokio::test]
    async fn worker_exit_and_panic_are_observed() {
        use stream_manager_chat::model::ConnState as CS;
        use stream_manager_chat::runtime::{EventSink, RuntimeEvent as RE};
        let (st, _, _, _tmp) = test_state().await;
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(16);
        let gen = {
            let mut guard = st.lock().await;
            guard.sink = EventSink::new(tx2);
            for id in ["w1", "w2"] {
                guard.coord.upsert_source(ChatSource {
                    id: id.into(),
                    key: SourceKey {
                        provider: Provider::Twitch,
                        room: id.into(),
                    },
                    account_id: None,
                    channel: id.into(),
                    broadcast_id: None,
                    binding_ids: vec![],
                });
                guard.gate.set_room(id, 0);
                guard.stop.arm(id, 0);
            }
            let gen = guard.coord.session_gen();
            guard.coord.set_conn(gen, "w1", CS::Connecting);
            guard.coord.set_conn(
                gen,
                "w2",
                CS::Ended {
                    reason: "stream over".into(),
                },
            );
            gen
        };
        let topo = ServerState::topo_guard(&st).await;
        // w1's loop panics; w2's exits cleanly over a terminal state.
        ServerState::track_worker(&st, "w1", gen, 0, async { panic!("boom") }, &topo).await;
        ServerState::track_worker(&st, "w2", gen, 0, async {}, &topo).await;
        drop(topo);
        // Drain both exit reports through the bus into canonical state.
        for _ in 0..2 {
            let (g, ev) = tokio::time::timeout(std::time::Duration::from_secs(5), rx2.recv())
                .await
                .expect("worker exit reported")
                .expect("bus open");
            // WorkerExited is server-ingested; other variants cannot
            // appear here.
            assert!(
                matches!(ev, RE::WorkerExited { .. }),
                "expected worker exit, got {ev:?}"
            );
            st.lock().await.ingest_runtime(g, ev);
        }
        let guard = st.lock().await;
        match guard.coord.conn_of("w1") {
            Some(CS::Offline { reason }) => {
                assert!(reason.contains("panicked"), "unexpected: {reason}")
            }
            other => panic!("panicked worker must park offline, got {other:?}"),
        }
        assert!(
            matches!(guard.coord.conn_of("w2"), Some(CS::Ended { .. })),
            "Ended must survive its worker"
        );
    }

    /// Memory eviction drops entries without tombstoning: the database
    /// remains authoritative, so an evicted (especially unknown) outcome
    /// still repeats from its durable row instead of expiring wrongly.
    #[test]
    fn memory_eviction_never_tombstones() {
        let receipts = Arc::new(std::sync::Mutex::new(ReceiptLog::default()));
        for i in 0..1100 {
            let id = format!("req-{i:04}");
            ServerState::finish_receipts(
                &receipts,
                &ns_key("can", &id),
                id,
                ReceiptStatus::Unknown,
                Some(format!("intent-{i}")),
                None,
            );
        }
        let log = receipts.lock().unwrap();
        assert!(log.map.len() <= 1000, "memory stays bounded");
        assert!(
            log.tombstones.is_empty(),
            "eviction must not manufacture tombstones"
        );
    }

    /// A cursor ahead of this process (e.g. pre-restart) resyncs visibly
    /// instead of stalling the client on events that will never arrive.
    #[tokio::test]
    async fn future_cursor_resyncs_with_gap() {
        let (st, can, _, _tmp) = test_state().await;
        let guard = st.lock().await;
        let events = guard.events_since_for(&can, 9999);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0].kind, EventKind::Gap { .. }));
        assert!(matches!(events[1].kind, EventKind::Snapshot { .. }));
    }

    #[tokio::test]
    async fn cancel_before_dispatch_sends_nothing() {
        let env = live_env_with(chat_sent(), vec!["user:read:chat", "user:write:chat"]).await;
        let boot = boot_of(&env.st).await;
        // Admit through the real planning path, then cancel before the
        // dispatch step: preparation succeeds but dispatch refuses, and
        // no provider request runs.
        let target = MutationTarget {
            source_id: "live",
            expected_boot: boot,
            expected_room: 0,
        };
        let plan = ServerState::plan_mutation(
            &env.st,
            &env.can,
            MutationKind::Send,
            &target,
            "hi".into(),
            None,
        )
        .await
        .expect("admission plans");
        {
            let guard = env.st.lock().await;
            assert!(guard.gate.cancel(&plan.intent_id));
        }
        let _prepared = ServerState::prepare_send(&env.st, &plan, "hi")
            .await
            .expect("preparation is pure");
        let reason = ServerState::dispatch_mutation(&env.st, &plan.intent_id)
            .await
            .expect_err("cancelled ticket must not dispatch");
        assert!(reason.contains("cancelled"));
        assert_eq!(chat_hits(&env._server).await, 0);
        // Dispatch failure retires the ticket: nothing left to cancel.
        assert!(!env.st.lock().await.gate.cancel(&plan.intent_id));
    }

    /// Cancel-after-dispatch through the operator's own command path:
    /// the send runs in a task, CancelIntent lands mid-flight, and the
    /// real provider verdict settles with its cancellation note. Exactly
    /// one provider mutation runs.
    #[tokio::test]
    async fn cancel_after_dispatch_keeps_verdict() {
        let env = live_env_with(
            wiremock::ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(500))
                .set_body_json(serde_json::json!({
                    "data": [{"message_id": "m9", "is_sent": true}],
                })),
            vec!["user:read:chat", "user:write:chat"],
        )
        .await;
        let boot = boot_of(&env.st).await;
        let st = env.st.clone();
        let can = env.can.clone();
        let handle = tokio::spawn(async move {
            ServerState::handle(&st, &can, send_cmd("can", "s1", boot, 0)).await
        });
        // Wait for the admitted intent to go pending: the send task has
        // planned and (almost surely) dispatched before the 500 ms mock
        // answers. Polling avoids assuming scheduling delays.
        let intent_id = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let found = {
                    let guard = env.st.lock().await;
                    guard
                        .coord
                        .intents()
                        .into_iter()
                        .find(|i| i.source_id == "live" && i.text == "hi")
                        .map(|i| i.id.clone())
                };
                if let Some(id) = found {
                    break id;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("send admits an intent");
        // Let the dispatch land ahead of the cancel: the mock answers in
        // 500 ms, so a short sleep puts the cancel firmly mid-flight.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let cancel = ServerState::handle(
            &env.st,
            &env.can,
            cmd(
                "can",
                "cx",
                CommandKind::CancelIntent {
                    intent_id: intent_id.clone(),
                },
            ),
        )
        .await;
        assert_eq!(cancel.status, ReceiptStatus::Accepted);
        assert!(cancel.error.unwrap().contains("already dispatched"));
        let receipt = handle.await.unwrap();
        assert_eq!(receipt.status, ReceiptStatus::Accepted);
        assert_eq!(receipt.intent_id.as_deref(), Some(intent_id.as_str()));
        // The cancellation warning rides on the receipt: a client reading
        // only receipts learns the verdict stands.
        assert!(receipt.error.unwrap().contains("cancelled after dispatch"));
        assert_eq!(chat_hits(&env._server).await, 1);
    }

    #[tokio::test]
    async fn concurrent_refresh_hits_network_once() {
        let env = mock_env(twitch_validate("alice", "111")).await;
        let (a, b) = tokio::join!(
            ServerState::live_session(&env.st, "tw"),
            ServerState::live_session(&env.st, "tw"),
        );
        assert!(a.is_ok());
        assert!(b.is_ok());
        // The refresh mock expects exactly one POST; the loser reuses the
        // winner's installed session without network. (Verified on mock
        // drop.)
    }

    #[tokio::test]
    async fn recovery_marks_dispatched_unknown() {
        use stream_manager_core::store as s;
        let (st, can, _, tmp) = test_state().await;
        let db = tmp.path().join("test.db").to_string_lossy().into_owned();
        let done_cmd = cmd(
            "can",
            "done-1",
            CommandKind::DisconnectSource {
                source_id: "ghost".into(),
            },
        );
        let done_digest = payload_digest(&done_cmd);
        let crashed_cmd = cmd(
            "can",
            "crashed-1",
            CommandKind::DisconnectSource {
                source_id: "ghost".into(),
            },
        );
        let crashed_digest = payload_digest(&crashed_cmd);
        {
            let conn = s::open(&db).unwrap();
            let row = |id: &str, digest: &str, status: &str| s::ChatReceiptRow {
                client_id: "can".into(),
                request_id: id.into(),
                digest: digest.into(),
                digest_v: DIGEST_V2,
                status: status.into(),
                intent_id: None,
                error: None,
                provider: String::new(),
                channel: String::new(),
                updated_utc: "2026-09-16T00:00:00Z".into(),
            };
            s::save_chat_receipt(&conn, &row("done-1", &done_digest, "accepted")).unwrap();
            s::save_chat_receipt(&conn, &row("crashed-1", &crashed_digest, "dispatched")).unwrap();
        }
        // Simulate restart: rebuild the log from durable rows.
        let recovered = {
            let conn = s::open(&db).unwrap();
            ServerState::recover_receipts(s::load_chat_receipts(&conn).unwrap())
        };
        {
            let guard = st.lock().await;
            *guard.receipts.lock().unwrap() = recovered;
        }
        // A crashed dispatch repeats its outcome-unknown receipt
        // unchanged — never re-runs, never obscured as generic Duplicate.
        let r = ServerState::handle(&st, &can, crashed_cmd).await;
        assert_eq!(r.status, ReceiptStatus::Unknown);
        assert!(r.error.unwrap().contains("not retried"));
        // The completed id still answers from its receipt.
        let r = ServerState::handle(&st, &can, done_cmd).await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
    }

    #[tokio::test]
    async fn expired_ids_are_rejected_not_rerun() {
        use stream_manager_core::store as s;
        let (st, can, _, tmp) = test_state().await;
        let db = tmp.path().join("test.db").to_string_lossy().into_owned();
        let gone_cmd = cmd(
            "can",
            "gone-1",
            CommandKind::DisconnectSource {
                source_id: "ghost".into(),
            },
        );
        {
            let conn = s::open(&db).unwrap();
            s::save_chat_receipt(
                &conn,
                &s::ChatReceiptRow {
                    client_id: "can".into(),
                    request_id: "gone-1".into(),
                    digest: String::new(),
                    digest_v: DIGEST_V2,
                    status: "expired".into(),
                    intent_id: None,
                    error: None,
                    provider: String::new(),
                    channel: String::new(),
                    updated_utc: "2026-09-16T00:00:00Z".into(),
                },
            )
            .unwrap();
            let recovered = ServerState::recover_receipts(s::load_chat_receipts(&conn).unwrap());
            let guard = st.lock().await;
            *guard.receipts.lock().unwrap() = recovered;
        }
        let r = ServerState::handle(&st, &can, gone_cmd).await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("issue a new id"));
    }

    #[tokio::test]
    async fn terminal_persist_failure_is_surfaced_not_swallowed() {
        let (st, can, _, tmp) = test_state().await;
        // Point settlement at a directory: every durable write fails.
        let bad = tmp.path().to_string_lossy().into_owned();
        let receipt = Receipt {
            request_id: "x1".into(),
            status: ReceiptStatus::Accepted,
            intent_id: None,
            error: None,
        };
        let out = ServerState::settle_terminal(&st, &bad, &can.id, "x1", "ab12", receipt).await;
        assert_eq!(out.status, ReceiptStatus::Accepted);
        let note = out.error.expect("durability loss must be reported");
        assert!(note.contains("durable outcome write failed"), "{note}");
    }

    #[tokio::test]
    async fn accounts_are_managed_through_commands() {
        let (st, can, dejana, _tmp) = test_state().await;
        // Non-admin refused.
        let r = ServerState::handle(
            &st,
            &dejana,
            cmd(
                "dejana",
                "acc-0",
                CommandKind::UpsertAccount {
                    account: AccountBinding {
                        id: "tw".into(),
                        provider: "twitch".into(),
                        channel_id: "111".into(),
                        channel_name: "alice".into(),
                    },
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        // Admin upsert persists to the database.
        let r = ServerState::handle(
            &st,
            &can,
            cmd(
                "can",
                "acc-1",
                CommandKind::UpsertAccount {
                    account: AccountBinding {
                        id: "tw".into(),
                        provider: "twitch".into(),
                        channel_id: "111".into(),
                        channel_name: "alice".into(),
                    },
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Accepted);
        // Removal refused while a source binds the account.
        {
            let mut guard = st.lock().await;
            guard.coord.upsert_source(ChatSource {
                id: "s1".into(),
                key: SourceKey {
                    provider: Provider::Twitch,
                    room: "alice".into(),
                },
                account_id: Some("tw".into()),
                channel: "alice".into(),
                broadcast_id: None,
                binding_ids: vec![],
            });
        }
        let r = ServerState::handle(
            &st,
            &can,
            cmd(
                "can",
                "acc-2",
                CommandKind::RemoveAccount {
                    account_id: "tw".into(),
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
        assert!(r.error.unwrap().contains("bound sources"));
        // Unknown provider refused.
        let r = ServerState::handle(
            &st,
            &can,
            cmd(
                "can",
                "acc-3",
                CommandKind::UpsertAccount {
                    account: AccountBinding {
                        id: "xx".into(),
                        provider: "myspace".into(),
                        channel_id: "1".into(),
                        channel_name: "x".into(),
                    },
                },
            ),
        )
        .await;
        assert_eq!(r.status, ReceiptStatus::Rejected);
    }

    #[test]
    fn caps_derive_from_granted_scopes() {
        assert_eq!(
            caps_for(Provider::Twitch, &["user:read:chat".to_string()]),
            (false, false)
        );
        assert_eq!(
            caps_for(
                Provider::Twitch,
                &["user:read:chat".to_string(), "user:write:chat".to_string()]
            ),
            (true, false)
        );
        assert_eq!(
            caps_for(
                Provider::Twitch,
                &[
                    "user:read:chat".to_string(),
                    "user:write:chat".to_string(),
                    "moderator:manage:chat_messages".to_string(),
                    "moderator:manage:banned_users".to_string(),
                ]
            ),
            (true, true)
        );
    }
}
