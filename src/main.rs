//! stream-manager desktop UI (iced).
//!
//! The persisted [`store`] database is the source of truth: the editor
//! loads the selected plan, saves navigate-away/explicitly, and the
//! review tab renders only from an immutable snapshot of the current
//! revision. Demo identities never leak into non-demo reviews.

use iced::widget::canvas::{Frame, Geometry, Path, Stroke, Text};
use iced::widget::{
    button, canvas, column, container, row, scrollable, text, text_editor, text_input, toggler,
};
use iced::{clipboard, Color, Element, Font, Length, Point, Size, Task};
use std::collections::HashMap;

mod ui;
use stream_manager_core::demo::{self, DEMO_BANNER};
use stream_manager_core::domain::{DestinationPlan, FieldOverride, Provider, Role};
use stream_manager_core::providers;
use stream_manager_core::store;
use stream_manager_core::video::{
    AudioBehavior, CtaBanner, FullSlate, Placement, RegionMask, VideoKind, VideoProfile,
};
use ui::chat::{AccountView, ChatConfig, ChatCtx, ChatMsg, ChatPanel, Proposal};

#[derive(Debug, Clone)]
struct AppConfig {
    db_path: String,
    demo_default: bool,
    status_cmd: Vec<String>,
    token_parent: String,
    chat: ChatConfig,
}

fn parse_bool(v: &str) -> bool {
    matches!(v.trim(), "1" | "true" | "yes")
}

fn parse_usize(v: &str, fallback: usize) -> usize {
    v.trim().parse().unwrap_or(fallback)
}

/// Non-secret runtime configuration. Precedence: environment, then
/// `~/.config/stream-manager/config.toml` (written by the Home Manager
/// module), then built-in defaults. Never carries secret material.
fn app_config() -> AppConfig {
    let mut cfg = AppConfig {
        db_path: shellexpand("~/.local/share/stream-manager/app.db"),
        demo_default: true,
        status_cmd: vec![
            "canix".into(),
            "--output".into(),
            "json".into(),
            "secret".into(),
            "instantclone".into(),
            "status".into(),
        ],
        token_parent: shellexpand("~/.config/stream-manager"),
        chat: ChatConfig::default(),
    };
    let path = shellexpand("~/.config/stream-manager/config.toml");
    if let Ok(text) = std::fs::read_to_string(&path) {
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            let v = v.trim().trim_matches('"').to_string();
            match k.trim() {
                "database_path" => cfg.db_path = shellexpand(&v),
                "demo_mode" => cfg.demo_default = parse_bool(&v),
                "instantclone_status_command" => {
                    let parts: Vec<String> = v.split_whitespace().map(str::to_string).collect();
                    if !parts.is_empty() {
                        cfg.status_cmd = parts;
                    }
                }
                "token_store_path" => {
                    let expanded = shellexpand(&v);
                    cfg.chat.token_store_path = expanded.clone();
                    if let Some(parent) = std::path::Path::new(&expanded).parent() {
                        cfg.token_parent = parent.to_string_lossy().into_owned();
                    }
                }
                "chat_enabled" => cfg.chat.enabled = parse_bool(&v),
                "chat_auto_connect" => cfg.chat.auto_connect = parse_bool(&v),
                "chat_buffer_messages" => {
                    cfg.chat.max_messages = parse_usize(&v, 5_000).clamp(100, 50_000);
                }
                "chat_buffer_bytes" => {
                    cfg.chat.max_bytes = parse_usize(&v, 8 * 1024 * 1024).clamp(1 << 20, 1 << 30);
                }
                "kick_relay_url" => cfg.chat.relay_url = v,
                "kick_relay_bearer_path" => cfg.chat.relay_bearer_path = shellexpand(&v),
                "rumble_api_url_path" => cfg.chat.rumble_api_path = shellexpand(&v),
                "token_key_path" => cfg.chat.token_key_path = shellexpand(&v),
                "twitch_client_id" => cfg.chat.twitch_client_id = v,
                "youtube_client_id" => cfg.chat.youtube_client_id = v,
                "kick_client_id" => cfg.chat.kick_client_id = v,
                "kick_client_secret_path" => cfg.chat.kick_secret_path = shellexpand(&v),
                "oauth_redirect_uri" => cfg.chat.redirect_uri = v,
                "chat_server_url" => {
                    cfg.chat.server_url = v.trim().trim_end_matches('/').to_owned()
                }
                "chat_server_token_path" => cfg.chat.server_token_path = shellexpand(&v),
                _ => {}
            }
        }
    }
    if let Ok(p) = std::env::var("STREAM_MANAGER_DB") {
        if !p.trim().is_empty() {
            cfg.db_path = shellexpand(p.trim());
        }
    }
    if let Ok(d) = std::env::var("STREAM_MANAGER_DEMO") {
        cfg.demo_default = parse_bool(&d);
    }
    if let Ok(c) = std::env::var("STREAM_MANAGER_STATUS_CMD") {
        let parts: Vec<String> = c.split_whitespace().map(str::to_string).collect();
        if !parts.is_empty() {
            cfg.status_cmd = parts;
        }
    }
    // Focused chat overrides (the file remains the primary surface).
    if let Ok(v) = std::env::var("STREAM_MANAGER_CHAT") {
        cfg.chat.enabled = parse_bool(&v);
    }
    if let Ok(v) = std::env::var("STREAM_MANAGER_CHAT_AUTO_CONNECT") {
        cfg.chat.auto_connect = parse_bool(&v);
    }
    if let Ok(v) = std::env::var("STREAM_MANAGER_KICK_RELAY_URL") {
        if !v.trim().is_empty() {
            cfg.chat.relay_url = v.trim().to_owned();
        }
    }
    if let Ok(v) = std::env::var("STREAM_MANAGER_RUMBLE_API_PATH") {
        if !v.trim().is_empty() {
            cfg.chat.rumble_api_path = shellexpand(v.trim());
        }
    }
    if let Ok(v) = std::env::var("STREAM_MANAGER_TOKEN_KEY") {
        if !v.trim().is_empty() {
            cfg.chat.token_key_path = shellexpand(v.trim());
        }
    }
    cfg
}

/// Best-effort creation of the reserved token-store directory (0700).
/// The encrypted backend itself is deferred; the directory existing with
/// safe permissions is the precondition it will require.
fn ensure_token_parent() {
    let cfg = app_config();
    let path = std::path::Path::new(&cfg.token_parent);
    if path.as_os_str().is_empty() {
        return;
    }
    if std::fs::create_dir_all(path).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Streams,
    Destinations,
    Review,
    History,
    Video,
    Chat,
}

impl Tab {
    const ALL: [Tab; 6] = [
        Tab::Streams,
        Tab::Destinations,
        Tab::Review,
        Tab::History,
        Tab::Video,
        Tab::Chat,
    ];
    fn label(&self) -> &'static str {
        match self {
            Tab::Streams => "Streams",
            Tab::Destinations => "Destinations",
            Tab::Review => "Review",
            Tab::History => "History",
            Tab::Video => "Video",
            Tab::Chat => "Chat",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriMode {
    Inherit,
    Replace,
    Clear,
}

impl TriMode {
    fn label(&self) -> &'static str {
        match self {
            TriMode::Inherit => "inherit",
            TriMode::Replace => "override",
            TriMode::Clear => "clear",
        }
    }
    fn cycle(&self) -> TriMode {
        match self {
            TriMode::Inherit => TriMode::Replace,
            TriMode::Replace => TriMode::Clear,
            TriMode::Clear => TriMode::Inherit,
        }
    }
}

#[derive(Debug, Clone)]
struct EditedDest {
    binding_id: String,
    provider: Provider,
    account_id: Option<String>,
    role: Role,
    funnel_target: String,
    desc_mode: TriMode,
    desc_text: String,
    title_mode: TriMode,
    title_text: String,
    remote_id: Option<String>,
    remote_url: Option<String>,
    video_profile: Option<String>,
}

#[derive(Debug)]
struct EditedPlan {
    id: String,
    title: String,
    desc: text_editor::Content,
    start_local: String,
    end_local: String,
    timezone: String,
    language: String,
    category_intent: String,
    tags: String,
    status: String,
    revision: u64,
    primary_id: String,
    dests: Vec<EditedDest>,
}

/// Plain-data copy for background tasks (`text_editor::Content` is not
/// `Clone`, so plans are snapshotted field-by-field at dispatch).
#[derive(Debug, Clone)]
struct PlanSnapshotData {
    id: String,
    title: String,
    desc_text: String,
    start_local: String,
    end_local: String,
    timezone: String,
    language: String,
    category_intent: String,
    tags: String,
    status: String,
    revision: u64,
    primary_id: String,
    dests: Vec<EditedDest>,
}

impl EditedPlan {
    fn snapshot(&self) -> PlanSnapshotData {
        PlanSnapshotData {
            id: self.id.clone(),
            title: self.title.clone(),
            desc_text: self.desc.text(),
            start_local: self.start_local.clone(),
            end_local: self.end_local.clone(),
            timezone: self.timezone.clone(),
            language: self.language.clone(),
            category_intent: self.category_intent.clone(),
            tags: self.tags.clone(),
            status: self.status.clone(),
            revision: self.revision,
            primary_id: self.primary_id.clone(),
            dests: self.dests.clone(),
        }
    }

    fn restore(snap: PlanSnapshotData) -> EditedPlan {
        EditedPlan {
            id: snap.id,
            title: snap.title,
            desc: text_editor::Content::with_text(&snap.desc_text),
            start_local: snap.start_local,
            end_local: snap.end_local,
            timezone: snap.timezone,
            language: snap.language,
            category_intent: snap.category_intent,
            tags: snap.tags,
            status: snap.status,
            revision: snap.revision,
            primary_id: snap.primary_id,
            dests: snap.dests,
        }
    }
}

#[derive(Debug, Clone)]
struct EditedProfile {
    id: String,
    name: String,
    kind: ProfileKindEdit,
    audio: AudioBehavior,
}

#[derive(Debug, Clone)]
enum ProfileKindEdit {
    Passthrough,
    Banner {
        text: String,
        link: String,
        qr: bool,
        bottom: bool,
        size: String,
        opacity: String,
    },
    Mask {
        x: String,
        y: String,
        w: String,
        h: String,
        opacity: String,
    },
    Slate {
        headline: String,
        body: String,
        link: String,
    },
}

/// A review under display, with the exact input it was built from so
/// execution can revalidate rather than trust UI memory.
#[derive(Debug, Clone)]
struct ActiveReview {
    review: stream_manager_core::review::ReviewSnapshot,
    input_json: String,
    /// Edit generation and database epoch the review was built from.
    /// Apply requires both to still match: unsaved edits or a database
    /// switch invalidate the approval, not just the button state.
    gen: u64,
    epoch: u64,
}

impl ActiveReview {
    fn fresh(&self, gen: u64, epoch: u64, dirty: bool) -> bool {
        !dirty && self.gen == gen && self.epoch == epoch
    }
}

#[derive(Debug, Clone, Default)]
struct ManualState {
    op_id: String,
    dest_id: String,
    rendered: String,
    url: String,
    note: String,
    error: String,
    copied: bool,
}

#[derive(Debug, Clone)]
enum PendingNav {
    Select(String),
    Archive,
    Reopen,
}

/// Creation queued behind a converging save (snapshotted at execution).
#[derive(Debug, Clone, Copy)]
enum PendingCreate {
    New,
    Duplicate,
}

#[derive(Debug, Clone)]
enum Message {
    Startup,
    PlansLoaded(Result<Vec<store::PlanRow>, String>),
    CurrentLoaded(Result<Box<LoadedPlan>, LoadError>),
    Saved(Result<SaveReceipt, String>),
    TabSelected(Tab),
    TitleChanged(String),
    DescAction(text_editor::Action),
    StartChanged(String),
    EndChanged(String),
    TagsChanged(String),
    ToggleDemo(bool),
    LoadDemo,
    CreatePlan,
    DuplicatePlan,
    SelectPlan(String),
    ArchivePlan,
    ReopenPlan,
    SaveNow,
    RoleClicked(String),
    PrimaryClicked(String),
    FunnelTargetChanged(String, String),
    DescModeClicked(String),
    DescTextChanged(String, String),
    AccountClicked(String),
    RefreshTransport,
    TransportLoaded(Result<String, String>),
    AddBinding(Provider),
    BindingAdded(Result<String, String>),
    BindingsLoaded(Result<BindingsData, String>),
    OpsLoaded(Result<Vec<store::OperationRow>, String>),
    ActionsLoaded(Result<(String, Vec<store::ActionRow>), String>),
    SelectOperation(String),
    BuildReview,
    ApplyAll,
    OpRecorded(Result<String, String>),
    RetryAction(String, String),
    ActionUpdated(Result<(String, String), String>),
    ReconcileOpen(String, String),
    ReconcileConfirm(String, String, bool),
    ManualOpen(String, String),
    ManualLoaded(Result<ManualLoaded, String>),
    ManualUrlChanged(String),
    ManualNoteChanged(String),
    ManualCopy,
    ManualCopied,
    ManualOpenDashboard,
    DashboardOpened(Result<String, String>),
    ManualRecord,
    ManualRecorded(Result<(String, String), String>),
    ProfileSelected(String),
    ProfileFieldChanged(String, String),
    ProfileSaveRequested,
    ProfileSaved(Result<String, String>),
    ProfileAssigned(String),
    ProfilesLoaded(Result<Vec<EditedProfile>, String>),
    PlanCreated(Result<String, String>),
    Seeded(Result<String, String>),
    StatusChanged(Result<String, String>),
    Chat(ChatMsg),
    DiscardAndReload,
}

/// A completed save, identified so stale responses cannot touch a
/// different plan or a newer edit generation.
#[derive(Debug, Clone)]
struct SaveReceipt {
    plan_id: String,
    gen: u64,
    revision: u64,
}

#[derive(Debug, Clone)]
struct ManualLoaded {
    op_id: String,
    dest_id: String,
    rendered: String,
}

#[derive(Debug, Clone)]
struct LoadedPlan {
    plan: PlanSnapshotData,
    bindings: Vec<store::BindingRow>,
    accounts: Vec<store::AccountRow>,
    /// Request tag: loads that arrive after a demo switch or a newer
    /// navigation are ignored, failures alike.
    req_epoch: u64,
    req_id: String,
}

/// A failed load carries the same request tag, so only the currently
/// wanted plan's error reaches the status line.
#[derive(Debug, Clone)]
struct LoadError {
    epoch: u64,
    id: String,
    error: String,
}

#[derive(Debug, Clone)]
struct BindingsData {
    bindings: Vec<store::BindingRow>,
    accounts: Vec<store::AccountRow>,
}

fn db_error(e: impl std::fmt::Display) -> String {
    format!("{e}")
}

/// Owned chat context: accounts, plan-destination proposals, and the CTA
/// preset gated by the current review. Chat never mutates metadata.
fn chat_proposals(current: Option<&EditedPlan>, accounts: &[store::AccountRow]) -> Vec<Proposal> {
    let Some(cur) = current else {
        return vec![];
    };
    cur.dests
        .iter()
        .map(|d| {
            let channel = d
                .account_id
                .as_deref()
                .and_then(|a| accounts.iter().find(|x| x.id == a))
                .map(|a| a.channel_name.clone())
                .unwrap_or_default();
            Proposal {
                binding_id: d.binding_id.clone(),
                provider: d.provider,
                account_id: d.account_id.clone(),
                channel,
                remote_id: d.remote_id.clone(),
                selected: false,
            }
        })
        .collect()
}

/// Database paths: the demo uses a separate file so fixture identities
/// can never become production bindings by toggling a UI flag.
fn db_paths() -> (String, String) {
    let cfg = app_config();
    let demo = std::env::var("STREAM_MANAGER_DEMO_DB")
        .map(|p| shellexpand(p.trim()))
        .unwrap_or_else(|_| {
            let dir = std::path::Path::new(&cfg.db_path)
                .parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| shellexpand("~/.local/share/stream-manager"));
            format!("{dir}/app-demo.db")
        });
    (cfg.db_path, demo)
}

struct App {
    tab: Tab,
    demo_mode: bool,
    db_path: String,
    main_db_path: String,
    demo_db_path: String,
    plans: Vec<store::PlanRow>,
    current: Option<EditedPlan>,
    /// Edit generation: incremented on every mutating edit. Save receipts
    /// carry the generation they persisted, so stale responses can neither
    /// clear a newer dirty flag nor stamp the wrong revision.
    gen: u64,
    dirty: bool,
    saving: bool,
    /// A save requested while one was in flight. The receipt handler
    /// issues at most one follow-up instead of stacking concurrent
    /// writers on the same revision.
    save_queued: bool,
    pending_nav: Option<PendingNav>,
    pending_create: Option<PendingCreate>,
    pending_demo: Option<bool>,
    bindings: Vec<store::BindingRow>,
    accounts: Vec<store::AccountRow>,
    profiles: Vec<EditedProfile>,
    selected_profile: Option<String>,
    operations: Vec<store::OperationRow>,
    selected_op: Option<String>,
    actions: Vec<store::ActionRow>,
    review: Option<ActiveReview>,
    manual: ManualState,
    reconcile: Option<(String, String)>,
    transport: String,
    probing: bool,
    status: String,
    validation: String,
    chat: ChatPanel,
    /// Database/session epoch: bumped on every demo switch. Async loads
    /// carry the epoch they were dispatched with; stale ones are ignored.
    db_epoch: u64,
    /// Latest requested plan load; older in-flight loads are dropped.
    loading_plan: Option<(u64, String)>,
    /// A save hit a newer revision: edits are kept, reload is explicit.
    conflict: bool,
    chat_plan_id: Option<String>,
}

impl App {
    fn new() -> Self {
        ensure_token_parent();
        let cfg = app_config();
        let (main_db, demo_db) = db_paths();
        let demo_mode = cfg.demo_default;
        Self {
            tab: Tab::Streams,
            demo_mode,
            db_path: if demo_mode {
                demo_db.clone()
            } else {
                main_db.clone()
            },
            main_db_path: main_db,
            demo_db_path: demo_db,
            plans: vec![],
            current: None,
            gen: 0,
            dirty: false,
            saving: false,
            save_queued: false,
            pending_nav: None,
            pending_create: None,
            pending_demo: None,
            bindings: vec![],
            accounts: vec![],
            profiles: default_profiles(),
            selected_profile: Some("cta-banner".into()),
            operations: vec![],
            selected_op: None,
            actions: vec![],
            review: None,
            manual: ManualState::default(),
            reconcile: None,
            transport: "Transport status: unknown — press Refresh.".into(),
            probing: false,
            status: "Loading…".into(),
            validation: String::new(),
            chat: ChatPanel::new(cfg.chat.clone()),
            db_epoch: 0,
            loading_plan: None,
            conflict: false,
            chat_plan_id: None,
        }
    }

    fn title(&self) -> String {
        "stream-manager".into()
    }

    /// Record a mutating edit: bumps the generation, marks dirty, and
    /// revalidates (which implicitly retires any displayed review via the
    /// revision/generation checks at render time).
    fn edited(&mut self) {
        self.gen += 1;
        self.dirty = true;
        self.status = "Unsaved changes.".into();
        self.revalidate();
    }

    /// Owned snapshot of everything the Chat workspace may read.
    fn chat_ctx(&self) -> ChatCtx {
        let accounts = self
            .accounts
            .iter()
            .map(|a| AccountView {
                id: a.id.clone(),
                provider: provider_of(&a.provider),
                channel_id: a.channel_id.clone(),
                channel_name: a.channel_name.clone(),
            })
            .collect();
        let proposals = chat_proposals(self.current.as_ref(), &self.accounts);
        // CTA presets consume the approved review artifact: only a fresh,
        // error-free review yields insertable text. Anything else explains
        // why instead of reconstructing copy from the live editor.
        let (cta_text, cta_blocked) = match self.current.as_ref() {
            Some(cur) => {
                let fresh = self
                    .review
                    .as_ref()
                    .is_some_and(|r| r.fresh(self.gen, self.db_epoch, self.dirty));
                let errors = self
                    .review
                    .as_ref()
                    .map(|r| r.review.errors.join("; "))
                    .unwrap_or_default();
                if !fresh {
                    (None, Some("no fresh review — build one first".to_string()))
                } else if !errors.is_empty() {
                    (None, Some(errors))
                } else {
                    let url = cur.dests.iter().find_map(|d| d.remote_url.clone());
                    let text = match (cur.title.trim().is_empty(), url) {
                        (false, Some(u)) => Some(format!("{} — {u}", cur.title.trim())),
                        (false, None) => Some(cur.title.trim().to_owned()),
                        _ => None,
                    };
                    (text, None)
                }
            }
            None => (None, None),
        };
        ChatCtx {
            demo_mode: self.demo_mode,
            db_path: self.db_path.clone(),
            accounts,
            proposals,
            cta_text,
            cta_blocked,
        }
    }

    /// Keep the panel's proposal list in step with the open plan,
    /// preserving per-binding selection.
    fn sync_chat_proposals(&mut self) {
        let plan_id = self.current.as_ref().map(|c| c.id.clone());
        if plan_id == self.chat_plan_id {
            return;
        }
        self.chat_plan_id = plan_id;
        let ctx = self.chat_ctx();
        let _ = self.chat.update(ChatMsg::ProposalsRefreshed, &ctx);
    }

    // ---------- persistence tasks (off the UI thread) ----------

    fn load_plans_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<Vec<store::PlanRow>, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    store::list_plans(&conn).map_err(db_error)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::PlansLoaded,
        )
    }

    fn load_current_task(&mut self, id: String) -> Task<Message> {
        let path = self.db_path.clone();
        let epoch = self.db_epoch;
        let req_id = id.clone();
        self.loading_plan = Some((epoch, id.clone()));
        Task::perform(
            async move {
                let req_inner = req_id.clone();
                tokio::task::spawn_blocking(move || -> Result<LoadedPlan, LoadError> {
                    let tag = |error: String| LoadError {
                        epoch,
                        id: req_inner.clone(),
                        error,
                    };
                    let conn = store::open(&path).map_err(|e| tag(db_error(e)))?;
                    let full = store::get_plan(&conn, &id)
                        .map_err(|e| tag(db_error(e)))?
                        .ok_or_else(|| tag("plan not found".to_string()))?;
                    let dests =
                        store::load_destinations(&conn, &id).map_err(|e| tag(db_error(e)))?;
                    let bindings = store::list_bindings(&conn).map_err(|e| tag(db_error(e)))?;
                    let accounts = store::list_accounts(&conn).map_err(|e| tag(db_error(e)))?;
                    let known: HashMap<&str, &store::BindingRow> =
                        bindings.iter().map(|b| (b.id.as_str(), b)).collect();
                    let plan = PlanSnapshotData {
                        id: full.id,
                        title: full.title,
                        desc_text: full.description,
                        start_local: utc_to_local_input(&full.start_utc, &full.timezone),
                        end_local: full
                            .end_utc
                            .as_deref()
                            .map(|u| utc_to_local_input(u, &full.timezone))
                            .unwrap_or_default(),
                        timezone: full.timezone,
                        language: full.language,
                        category_intent: full.category_intent,
                        tags: full.tags_json,
                        status: full.status,
                        revision: full.revision,
                        primary_id: full.primary_id.unwrap_or_default(),
                        dests: dests
                            .into_iter()
                            .map(|d| {
                                let b = known.get(d.destination_id.as_str());
                                EditedDest {
                                    provider: b
                                        .map(|x| provider_of(&x.provider))
                                        .unwrap_or(Provider::Youtube),
                                    account_id: b.and_then(|x| x.account_id.clone()),
                                    binding_id: d.destination_id.clone(),
                                    role: d.role,
                                    funnel_target: d.funnel_target.unwrap_or_default(),
                                    desc_mode: tri_of(&d.description),
                                    desc_text: tri_text(&d.description),
                                    title_mode: tri_of(&d.title),
                                    title_text: tri_text(&d.title),
                                    remote_id: d.remote_broadcast_id,
                                    remote_url: d.remote_watch_url,
                                    video_profile: d.video_profile_id,
                                }
                            })
                            .collect(),
                    };
                    Ok(LoadedPlan {
                        plan,
                        bindings,
                        accounts,
                        req_epoch: epoch,
                        req_id: req_inner.clone(),
                    })
                })
                .await
                .map_err(|e| LoadError {
                    epoch,
                    id: req_id.clone(),
                    error: format!("join: {e}"),
                })?
                .map(Box::new)
            },
            Message::CurrentLoaded,
        )
    }

    /// Switch between the demo database and the real one. Fixture
    /// identities live only in the demo file and can never leak across.
    fn switch_demo(&mut self, demo: bool) -> Task<Message> {
        self.demo_mode = demo;
        self.db_path = if demo {
            self.demo_db_path.clone()
        } else {
            self.main_db_path.clone()
        };
        self.current = None;
        self.plans.clear();
        self.review = None;
        self.operations.clear();
        self.actions.clear();
        self.selected_op = None;
        self.bindings.clear();
        self.accounts.clear();
        self.pending_nav = None;
        self.pending_create = None;
        // An in-flight save belongs to the other database; its receipt
        // lands as stale and only refreshes. Reset the writer state so a
        // stuck flag can never block the new file.
        self.saving = false;
        self.save_queued = false;
        self.dirty = false;
        self.gen += 1;
        // New database/session epoch: in-flight loads for the other file
        // are ignored on arrival, and chat resets its live session.
        self.db_epoch += 1;
        self.loading_plan = None;
        self.conflict = false;
        self.chat_plan_id = None;
        self.status = "Loading…".into();
        self.revalidate();
        let chat_ctx = self.chat_ctx();
        let chat_reset = self.chat.update(ChatMsg::SessionReset, &chat_ctx);
        Task::batch([
            self.load_plans_task(),
            self.load_bindings_task(),
            self.load_profiles_task(),
            chat_reset,
        ])
    }

    /// Run a queued navigation step once the editor is clean.
    fn run_pending_nav(&mut self, nav: PendingNav) -> Task<Message> {
        match nav {
            PendingNav::Select(id) => self.load_current_task(id),
            PendingNav::Archive => self.set_status_task("archived"),
            PendingNav::Reopen => self.set_status_task("draft"),
        }
    }

    fn set_status_task(&self, status: &'static str) -> Task<Message> {
        let Some(id) = self.current.as_ref().map(|c| c.id.clone()) else {
            return Task::none();
        };
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    store::set_plan_status(&conn, &id, status).map_err(db_error)?;
                    Ok(id)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::StatusChanged,
        )
    }

    fn create_plan_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        let n = self.plans.len() + 1;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    let id = format!("plan-{n}");
                    store::create_plan(&conn, &id, "Untitled stream", "").map_err(db_error)?;
                    Ok(id)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::PlanCreated,
        )
    }

    fn duplicate_plan_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        let src = self.current.as_ref().map(EditedPlan::snapshot);
        let n = self.plans.len() + 1;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    let Some(s) = src else {
                        return Err("no plan selected".into());
                    };
                    let id = format!("plan-{n}");
                    let src_full = store::get_plan(&conn, &s.id)
                        .map_err(db_error)?
                        .ok_or_else(|| "source plan vanished".to_string())?;
                    store::create_plan(&conn, &id, &s.title, "").map_err(db_error)?;
                    store::update_plan_content(
                        &conn,
                        &id,
                        &format!("{} (copy)", s.title),
                        &s.desc_text,
                        &src_full.start_utc,
                        src_full.end_utc.as_deref(),
                        &s.timezone,
                        &s.language,
                        &s.category_intent,
                        &s.tags,
                        None,
                    )
                    .map_err(db_error)?;
                    for d in &s.dests {
                        store::save_destination(
                            &conn,
                            &id,
                            &DestinationPlan {
                                destination_id: d.binding_id.clone(),
                                role: d.role,
                                title: tri_from(d.title_mode, &d.title_text),
                                description: tri_from(d.desc_mode, &d.desc_text),
                                tags: FieldOverride::Inherit,
                                funnel_target: None,
                                remote_broadcast_id: None,
                                remote_watch_url: None,
                                video_profile_id: d.video_profile.clone(),
                            },
                        )
                        .map_err(db_error)?;
                    }
                    Ok(id)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::PlanCreated,
        )
    }

    fn run_pending_create(&self, kind: PendingCreate) -> Task<Message> {
        match kind {
            PendingCreate::New => self.create_plan_task(),
            PendingCreate::Duplicate => self.duplicate_plan_task(),
        }
    }

    fn save_current_task(&mut self) -> Task<Message> {
        let Some(cur) = self.current.as_ref().map(EditedPlan::snapshot) else {
            return Task::none();
        };
        // Single writer: never stack a second save on the same revision.
        // Callers racing an in-flight save set the flag; the receipt
        // handler issues at most one follow-up.
        if self.saving {
            self.save_queued = true;
            return Task::none();
        }
        let path = self.db_path.clone();
        let gen = self.gen;
        self.saving = true;
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<SaveReceipt, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    let (start_utc, end_utc) =
                        local_inputs_to_storage(&cur.start_local, &cur.end_local, &cur.timezone)
                            .map_err(|e| e.to_string())?;
                    // Atomic plan-plus-destinations write guarded by the
                    // editor's revision: a concurrent writer wins loudly
                    // (stale revision) instead of interleaving rows.
                    let dests: Vec<DestinationPlan> = cur
                        .dests
                        .iter()
                        .map(|d| DestinationPlan {
                            destination_id: d.binding_id.clone(),
                            role: d.role,
                            title: tri_from(d.title_mode, &d.title_text),
                            description: tri_from(d.desc_mode, &d.desc_text),
                            tags: FieldOverride::Inherit,
                            funnel_target: if d.role == Role::Funnel && !d.funnel_target.is_empty()
                            {
                                Some(d.funnel_target.clone())
                            } else {
                                None
                            },
                            remote_broadcast_id: d.remote_id.clone(),
                            remote_watch_url: d.remote_url.clone(),
                            video_profile_id: d.video_profile.clone(),
                        })
                        .collect();
                    let rev = store::save_plan_atomic(
                        &conn,
                        &cur.id,
                        &cur.title,
                        &cur.desc_text,
                        &start_utc,
                        end_utc.as_deref(),
                        &cur.timezone,
                        &cur.language,
                        &cur.category_intent,
                        &cur.tags,
                        if cur.primary_id.is_empty() {
                            None
                        } else {
                            Some(cur.primary_id.as_str())
                        },
                        cur.revision,
                        &dests,
                    )
                    .map_err(db_error)?;
                    Ok(SaveReceipt {
                        plan_id: cur.id.clone(),
                        gen,
                        revision: rev,
                    })
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::Saved,
        )
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Startup => {
                let chat_ctx = self.chat_ctx();
                return Task::batch([
                    self.load_plans_task(),
                    self.load_bindings_task(),
                    self.load_profiles_task(),
                    self.chat.update(ChatMsg::StartupLoad, &chat_ctx),
                ]);
            }
            Message::Chat(m) => {
                // Popping out shares the one application view (iced
                // `application` renders the same tree per window), so the
                // main tab follows to Chat to make the pop-out useful.
                if matches!(m, ChatMsg::OpenDetached) {
                    self.tab = Tab::Chat;
                }
                let ctx = self.chat_ctx();
                let task = self.chat.update(m, &ctx);
                self.sync_chat_proposals();
                return task;
            }
            Message::PlansLoaded(Ok(plans)) => {
                let first_empty = plans.is_empty();
                self.plans = plans;
                // Selection is preserved across refreshes: only auto-load
                // when nothing is open (initial startup).
                if self.current.is_none() {
                    if first_empty && self.demo_mode {
                        return self.seed_demo_task();
                    }
                    if let Some(first) = self.plans.first().cloned() {
                        return self.load_current_task(first.id);
                    }
                    self.status = "No plans yet — create a draft or load the demo.".into();
                } else if !self.dirty {
                    self.status = "Loaded.".into();
                }
            }
            Message::PlansLoaded(Err(e)) => {
                self.status = format!("Database error: {e}");
            }
            Message::CurrentLoaded(Ok(loaded)) => {
                // Ignore loads overtaken by a demo switch or a newer
                // navigation — results and failures alike.
                let fresh =
                    self.loading_plan.as_ref().is_some_and(|(epoch, id)| {
                        *epoch == loaded.req_epoch && *id == loaded.req_id
                    }) && loaded.req_epoch == self.db_epoch;
                if !fresh {
                    return Task::none();
                }
                self.loading_plan = None;
                self.bindings = loaded.bindings;
                self.accounts = loaded.accounts;
                self.current = Some(EditedPlan::restore(loaded.plan));
                self.review = None;
                self.conflict = false;
                self.status = "Loaded.".into();
                self.revalidate();
                self.sync_chat_proposals();
            }
            Message::CurrentLoaded(Err(e)) => {
                // Only the currently wanted plan's failure surfaces; an
                // older request failing after navigation/switch stays silent.
                let wanted = self
                    .loading_plan
                    .as_ref()
                    .is_some_and(|(epoch, id)| *epoch == e.epoch && *id == e.id)
                    && e.epoch == self.db_epoch;
                if wanted {
                    self.loading_plan = None;
                    self.status = format!("Load error: {}", e.error);
                }
            }
            Message::Saved(Ok(receipt)) => {
                self.saving = false;
                let current_id = self.current.as_ref().map(|c| c.id.clone());
                if current_id.as_deref() == Some(receipt.plan_id.as_str()) {
                    if receipt.gen == self.gen {
                        // This receipt covers the latest edits.
                        self.dirty = false;
                        if let Some(cur) = &mut self.current {
                            cur.revision = receipt.revision;
                        }
                        for p in &mut self.plans {
                            if p.id == receipt.plan_id {
                                p.revision = receipt.revision;
                            }
                        }
                        self.status = format!("Saved rev {}.", receipt.revision);
                        if let Some(kind) = self.pending_create.take() {
                            let refresh = self.load_plans_refresh_task();
                            return Task::batch([refresh, self.run_pending_create(kind)]);
                        }
                        if self.save_queued {
                            self.save_queued = false;
                            if self.dirty {
                                self.status = "Saving…".into();
                                let followup = self.save_current_task();
                                return Task::batch([followup, self.load_plans_refresh_task()]);
                            }
                        }
                    } else {
                        // Newer edits landed while this save was in flight:
                        // adopt the persisted revision so the follow-up
                        // save's CAS targets it instead of failing stale,
                        // and converge instead of reporting a revision the
                        // editor has moved past. The queued flag is
                        // consumed here: this follow-up covers it.
                        self.save_queued = false;
                        if let Some(cur) = &mut self.current {
                            cur.revision = cur.revision.max(receipt.revision);
                        }
                        self.status = "Saving…".into();
                        let followup = self.save_current_task();
                        let refresh = self.load_plans_refresh_task();
                        // Continue to pending navigation only once clean;
                        // keep it queued meanwhile.
                        return Task::batch([followup, refresh]);
                    }
                }
                // Stale receipts (e.g. for a plan since navigated away
                // from) only refresh the list, never touch the editor.
                let refresh = self.load_plans_refresh_task();
                if let Some(demo) = self.pending_demo.take() {
                    if !self.dirty {
                        return self.switch_demo(demo);
                    }
                    self.pending_demo = Some(demo);
                    return refresh;
                }
                if let Some(nav) = self.pending_nav.take() {
                    if !self.dirty {
                        return Task::batch([refresh, self.run_pending_nav(nav)]);
                    }
                    // Still dirty (a follow-up save is converging); keep
                    // the navigation queued behind it.
                    self.pending_nav = Some(nav);
                    return refresh;
                }
                return refresh;
            }
            Message::Saved(Err(e)) => {
                self.saving = false;
                // A queued follow-up is dropped on error: retrying a
                // deterministically failing save would loop forever. The
                // operator (or queued navigation) retries explicitly.
                self.save_queued = false;
                // Failed saves stop navigation: the pending target stays
                // queued so the operator can retry instead of losing edits.
                // A queued database switch is dropped — switching files
                // with unsaved edits would strand them.
                self.pending_demo = None;
                if e.contains("stale revision") {
                    // Another writer moved the plan: keep the edits, say
                    // so, and offer an explicit reload. Never silently
                    // overwrite or discard.
                    self.conflict = true;
                    self.status = format!("Save conflict ({e}). Edits kept — reload to rebase.");
                } else {
                    self.status = format!("Save error: {e}");
                }
                if self.pending_nav.is_none() {
                    return self.load_plans_refresh_task();
                }
            }
            Message::DiscardAndReload => {
                self.conflict = false;
                self.dirty = false;
                if let Some(id) = self.current.as_ref().map(|c| c.id.clone()) {
                    return self.load_current_task(id);
                }
            }
            Message::TabSelected(t) => {
                self.tab = t;
                // Tab switches keep the editor: the save carries its own
                // snapshot and receipts only touch a matching plan+generation.
                let save = self.save_current_task();
                self.revalidate();
                self.sync_chat_proposals();
                if t == Tab::History {
                    return Task::batch([save, self.load_ops_task()]);
                }
                if t == Tab::Destinations {
                    return Task::batch([save, self.load_bindings_task()]);
                }
                return save;
            }
            Message::TitleChanged(v) => {
                if let Some(cur) = &mut self.current {
                    cur.title = v;
                }
                self.edited();
            }
            Message::DescAction(a) => {
                if let Some(cur) = &mut self.current {
                    cur.desc.perform(a);
                }
                self.edited();
            }
            Message::StartChanged(v) => {
                if let Some(cur) = &mut self.current {
                    cur.start_local = v;
                }
                self.edited();
            }
            Message::EndChanged(v) => {
                if let Some(cur) = &mut self.current {
                    cur.end_local = v;
                }
                self.edited();
            }
            Message::TagsChanged(v) => {
                if let Some(cur) = &mut self.current {
                    cur.tags = v;
                }
                self.edited();
            }
            Message::ToggleDemo(d) => {
                if self.dirty {
                    // Never strand edits: flush into the current database
                    // before switching files. A queued creation belongs to
                    // this database and is dropped with a note.
                    self.pending_nav = None;
                    if self.pending_create.take().is_some() {
                        self.status = "Saving… (queued creation dropped by database switch)".into();
                    } else {
                        self.status = "Saving…".into();
                    }
                    let save = self.save_current_task();
                    // The mode switch itself happens after the save lands;
                    // stash the target in the status line is wrong, so use
                    // a dedicated queued switch.
                    self.pending_demo = Some(d);
                    return save;
                }
                return self.switch_demo(d);
            }
            Message::LoadDemo => {
                if !self.demo_mode {
                    if self.dirty {
                        self.pending_create = None;
                        self.pending_demo = Some(true);
                        self.status = "Saving…".into();
                        return self.save_current_task();
                    }
                    return self.switch_demo(true);
                }
                return self.seed_demo_task();
            }
            Message::CreatePlan => {
                // Serialize behind any in-flight save: creation runs once
                // the editor is clean, snapshotting then-current state.
                if self.dirty || self.saving {
                    self.pending_create = Some(PendingCreate::New);
                    self.status = "Saving…".into();
                    return self.save_current_task();
                }
                return self.create_plan_task();
            }
            Message::DuplicatePlan => {
                if self.dirty || self.saving {
                    self.pending_create = Some(PendingCreate::Duplicate);
                    self.status = "Saving…".into();
                    return self.save_current_task();
                }
                return self.duplicate_plan_task();
            }
            Message::PlanCreated(Ok(id)) => {
                self.status = format!("Created {id}.");
                return Task::batch([self.load_plans_task(), self.load_current_task(id)]);
            }
            Message::PlanCreated(Err(e)) => {
                self.status = format!("Create error: {e}");
            }
            Message::Seeded(Ok(id)) => {
                self.status = "Offline demo loaded.".into();
                return Task::batch([self.load_plans_task(), self.load_current_task(id)]);
            }
            Message::Seeded(Err(e)) => {
                self.status = format!("Demo seed error: {e}");
            }
            Message::SelectPlan(id) => {
                if self.current.as_ref().is_some_and(|c| c.id == id) {
                    return Task::none();
                }
                if self.dirty {
                    // Queue behind the converging save; Saved either loads
                    // the target or keeps it queued on failure.
                    self.pending_nav = Some(PendingNav::Select(id));
                    self.status = "Saving…".into();
                    return self.save_current_task();
                }
                return self.load_current_task(id);
            }
            Message::ArchivePlan => {
                if self.dirty {
                    self.pending_nav = Some(PendingNav::Archive);
                    self.status = "Saving…".into();
                    return self.save_current_task();
                }
                return self.set_status_task("archived");
            }
            Message::ReopenPlan => {
                if self.dirty {
                    self.pending_nav = Some(PendingNav::Reopen);
                    self.status = "Saving…".into();
                    return self.save_current_task();
                }
                return self.set_status_task("draft");
            }
            Message::StatusChanged(Ok(id)) => {
                self.status = format!("Updated {id}.");
                return Task::batch([self.load_plans_task(), self.load_current_task(id)]);
            }
            Message::StatusChanged(Err(e)) => {
                self.status = format!("Status error: {e}");
            }
            Message::SaveNow => {
                self.status = "Saving…".into();
                return self.save_current_task();
            }
            Message::RoleClicked(id) => {
                if let Some(cur) = &mut self.current {
                    for d in &mut cur.dests {
                        if d.binding_id == id {
                            d.role = match d.role {
                                Role::Primary => Role::Mirror,
                                Role::Mirror => Role::Funnel,
                                Role::Funnel => Role::Mirror,
                            };
                            if d.role == Role::Funnel && d.funnel_target.is_empty() {
                                d.funnel_target = cur.primary_id.clone();
                            }
                        }
                    }
                }
                self.edited();
            }
            Message::PrimaryClicked(id) => {
                if let Some(cur) = &mut self.current {
                    cur.primary_id = id;
                }
                self.edited();
            }
            Message::FunnelTargetChanged(id, v) => {
                if let Some(cur) = &mut self.current {
                    for d in &mut cur.dests {
                        if d.binding_id == id {
                            d.funnel_target = v.clone();
                        }
                    }
                }
                self.edited();
            }
            Message::DescModeClicked(id) => {
                if let Some(cur) = &mut self.current {
                    for d in &mut cur.dests {
                        if d.binding_id == id {
                            d.desc_mode = d.desc_mode.cycle();
                        }
                    }
                }
                self.edited();
            }
            Message::DescTextChanged(id, v) => {
                if let Some(cur) = &mut self.current {
                    for d in &mut cur.dests {
                        if d.binding_id == id {
                            d.desc_text = v.clone();
                        }
                    }
                }
                self.edited();
            }
            Message::AccountClicked(id) => {
                // Cycle through accounts of the same provider (None included).
                if let Some(cur) = &mut self.current {
                    let prov = cur
                        .dests
                        .iter()
                        .find(|d| d.binding_id == id)
                        .map(|d| d.provider);
                    if let Some(prov) = prov {
                        let mut opts: Vec<Option<String>> = vec![None];
                        for a in &self.accounts {
                            if a.provider == prov.slug() {
                                opts.push(Some(a.id.clone()));
                            }
                        }
                        for d in &mut cur.dests {
                            if d.binding_id == id {
                                let pos = opts.iter().position(|o| o == &d.account_id).unwrap_or(0);
                                d.account_id = opts[(pos + 1) % opts.len()].clone();
                            }
                        }
                    }
                }
                self.edited();
            }
            Message::AddBinding(prov) => {
                let path = self.db_path.clone();
                let plan_id = self.current.as_ref().map(|c| c.id.clone());
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<String, String> {
                            let conn = store::open(&path).map_err(db_error)?;
                            let existing = store::list_bindings(&conn).map_err(db_error)?;
                            let mut n = existing.len() + 1;
                            let mut id = format!("{}-{n}", prov.slug());
                            while existing.iter().any(|b| b.id == id) {
                                n += 1;
                                id = format!("{}-{n}", prov.slug());
                            }
                            store::save_binding(
                                &conn,
                                &store::BindingRow {
                                    id: id.clone(),
                                    provider: prov.slug().into(),
                                    account_id: None,
                                    instantclone_name: None,
                                },
                            )
                            .map_err(db_error)?;
                            if let Some(pid) = plan_id {
                                let d = DestinationPlan::new(&id, Role::Mirror);
                                store::save_destination(&conn, &pid, &d).map_err(db_error)?;
                            }
                            Ok(id)
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::BindingAdded,
                );
            }
            Message::BindingAdded(Ok(id)) => {
                self.status = format!("Added destination {id}.");
                let reload = self.load_bindings_task();
                if let Some(cur) = self.current.as_ref().map(|c| c.id.clone()) {
                    return Task::batch([reload, self.load_current_task(cur)]);
                }
                return reload;
            }
            Message::BindingAdded(Err(e)) => {
                self.status = format!("Add destination error: {e}");
            }
            Message::RefreshTransport => {
                if self.probing {
                    return Task::none();
                }
                self.probing = true;
                self.transport = "Probing InstantClone status…".into();
                return Task::perform(probe_instantclone(), Message::TransportLoaded);
            }
            Message::TransportLoaded(r) => {
                self.probing = false;
                match r {
                    Ok(s) => self.transport = s,
                    Err(e) => self.transport = format!("Transport status unknown: {e}"),
                }
            }
            Message::BindingsLoaded(Ok(b)) => {
                self.bindings = b.bindings;
                self.accounts = b.accounts;
            }
            Message::BindingsLoaded(Err(e)) => {
                self.status = format!("Bindings error: {e}");
            }
            Message::OpsLoaded(Ok(ops)) => {
                self.operations = ops;
                if let Some(sel) = self.selected_op.clone() {
                    return self.load_actions_task(sel);
                }
            }
            Message::OpsLoaded(Err(e)) => {
                self.status = format!("History error: {e}");
            }
            Message::ActionsLoaded(Ok((op, actions))) => {
                if self.selected_op.as_deref() == Some(op.as_str()) {
                    self.actions = actions;
                }
            }
            Message::ActionsLoaded(Err(e)) => {
                self.status = format!("Actions error: {e}");
            }
            Message::SelectOperation(id) => {
                self.selected_op = Some(id.clone());
                self.reconcile = None;
                return self.load_actions_task(id);
            }
            Message::BuildReview => match self.build_review() {
                Some((review, input_json)) => {
                    let gen = self.gen;
                    let epoch = self.db_epoch;
                    self.review = Some(ActiveReview {
                        review,
                        input_json,
                        gen,
                        epoch,
                    });
                    self.status = "Review built.".into();
                }
                None => {
                    self.review = None;
                    self.status = "Nothing to review — no plan loaded.".into();
                }
            },
            Message::ApplyAll => {
                if let Some(r) = self.review.clone() {
                    // Approval covers the complete reviewed input at this
                    // generation: unsaved edits or a database switch since
                    // the build refuse here, and the persisted revision is
                    // rechecked inside the task.
                    if !r.fresh(self.gen, self.db_epoch, self.dirty) {
                        self.status =
                            "Review is stale (edits since build) — rebuild before applying.".into();
                        return Task::none();
                    }
                    return self.apply_task(r.input_json, r.review.demo);
                }
            }
            Message::OpRecorded(Ok(op)) => {
                self.selected_op = Some(op.clone());
                self.status = format!("Recorded operation {op}.");
                return Task::batch([self.load_ops_task(), self.load_actions_task(op)]);
            }
            Message::OpRecorded(Err(e)) => {
                self.status = format!("Publish error: {e}");
            }
            Message::RetryAction(op, dest) => {
                // Re-attempt a single failed/blocked action. Without
                // credentials this honestly lands back at blocked; the
                // retry path itself (no replay of successes) is exercised.
                let path = self.db_path.clone();
                let demo = self.demo_mode;
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<(String, String), String> {
                            let conn = store::open(&path).map_err(db_error)?;
                            let why = if demo {
                                "demo mode: no authorized account"
                            } else {
                                "not connected: no authorized account"
                            };
                            store::update_action_state(
                                &conn,
                                &op,
                                &dest,
                                "blocked",
                                Some(why),
                                None,
                            )
                            .map_err(db_error)?;
                            Ok((op, dest))
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::ActionUpdated,
                );
            }
            Message::ActionUpdated(Ok((op, _))) => {
                if let Some(sel) = self.selected_op.clone() {
                    if sel == op {
                        return self.load_actions_task(op);
                    }
                }
            }
            Message::ActionUpdated(Err(e)) => {
                self.status = format!("Retry error: {e}");
            }
            Message::ReconcileOpen(op, dest) => {
                self.reconcile = Some((op, dest));
            }
            Message::ReconcileConfirm(op, dest, remote_applied) => {
                self.reconcile = None;
                let path = self.db_path.clone();
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<(String, String), String> {
                            let conn = store::open(&path).map_err(db_error)?;
                            // Operator-verified outcome only: never auto-assumed.
                            let (state, err) = if remote_applied {
                                (
                                    "succeeded_unverified",
                                    Some("operator confirmed remote effect; no API read-back"),
                                )
                            } else {
                                (
                                    "pending",
                                    Some("operator confirmed no remote effect; safe to retry"),
                                )
                            };
                            store::update_action_state(&conn, &op, &dest, state, err, None)
                                .map_err(db_error)?;
                            Ok((op, dest))
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::ActionUpdated,
                );
            }
            Message::ManualOpen(op, dest) => {
                // Open manual work by immutable operation/action identity.
                // The copy text comes from the stored action payload, never
                // from the live editor, so later edits cannot contaminate
                // an old operation.
                let path = self.db_path.clone();
                self.tab = Tab::Review;
                self.manual = ManualState {
                    op_id: op.clone(),
                    dest_id: dest.clone(),
                    ..ManualState::default()
                };
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<ManualLoaded, String> {
                            let conn = store::open(&path).map_err(db_error)?;
                            let action = store::get_action(&conn, &op, &dest)
                                .map_err(db_error)?
                                .ok_or_else(|| "manual action not found".to_string())?;
                            if action.kind != "manual" {
                                return Err("action is not a manual action".into());
                            }
                            let rendered: Option<String> =
                                serde_json::from_str::<serde_json::Value>(&action.after_json)
                                    .ok()
                                    .and_then(|v| {
                                        v.get("rendered")
                                            .and_then(|r| r.as_str().map(str::to_string))
                                    });
                            Ok(ManualLoaded {
                                op_id: op,
                                dest_id: dest,
                                rendered: rendered.unwrap_or_default(),
                            })
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::ManualLoaded,
                );
            }
            Message::ManualLoaded(Ok(loaded)) => {
                if self.manual.op_id == loaded.op_id && self.manual.dest_id == loaded.dest_id {
                    self.manual.rendered = loaded.rendered;
                    self.manual.error = String::new();
                }
            }
            Message::ManualLoaded(Err(e)) => {
                self.manual.error = e;
            }
            Message::ManualUrlChanged(v) => {
                self.manual.url = v;
            }
            Message::ManualNoteChanged(v) => {
                self.manual.note = v;
            }
            Message::ManualCopy => {
                return clipboard::write(self.manual.rendered.clone())
                    .map(|()| Message::ManualCopied);
            }
            Message::ManualCopied => {
                self.manual.copied = true;
            }
            Message::ManualOpenDashboard => {
                return Task::perform(open_dashboard(), Message::DashboardOpened);
            }
            Message::DashboardOpened(Ok(_)) => {
                self.manual.error = "Dashboard opened in browser.".into();
            }
            Message::DashboardOpened(Err(e)) => {
                self.manual.error = format!("Could not open browser: {e}");
            }
            Message::ManualRecord => {
                let m = self.manual.clone();
                let path = self.db_path.clone();
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<(String, String), String> {
                            let completion =
                                stream_manager_core::providers::rumble::ManualCompletion {
                                    destination_id: m.dest_id.clone(),
                                    public_url: m.url.trim().to_string(),
                                    completed_utc: chrono::Utc::now().to_rfc3339(),
                                    operator_note: m.note.clone(),
                                };
                            completion.validate().map_err(|e| e.to_string())?;
                            let conn = store::open(&path).map_err(db_error)?;
                            // Eligibility (exists, manual, awaiting work)
                            // is enforced inside the store call.
                            store::record_manual_completion(
                                &conn,
                                &m.op_id,
                                &m.dest_id,
                                &completion.public_url,
                                &completion.operator_note,
                                &completion.completed_utc,
                            )
                            .map_err(db_error)?;
                            Ok((m.op_id.clone(), m.dest_id.clone()))
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::ManualRecorded,
                );
            }
            Message::ManualRecorded(Ok((op, _))) => {
                self.manual.error = "Recorded as manually completed.".into();
                if let Some(sel) = self.selected_op.clone() {
                    if sel == op {
                        return self.load_actions_task(op);
                    }
                }
            }
            Message::ManualRecorded(Err(e)) => {
                self.manual.error = e;
            }
            Message::ProfileSelected(id) => {
                self.selected_profile = Some(id);
            }
            Message::ProfileFieldChanged(field, v) => {
                if let Some(p) = self
                    .profiles
                    .iter_mut()
                    .find(|p| Some(p.id.clone()) == self.selected_profile)
                {
                    p.set_field(&field, &v);
                }
                self.status = "Unsaved profile changes.".into();
            }
            Message::ProfileSaveRequested => {
                let path = self.db_path.clone();
                let profiles = self.profiles.clone();
                return Task::perform(
                    async move {
                        tokio::task::spawn_blocking(move || -> Result<String, String> {
                            let conn = store::open(&path).map_err(db_error)?;
                            for p in &profiles {
                                let Some(vp) = p.to_profile() else {
                                    return Err(format!(
                                        "profile '{}' has invalid geometry",
                                        p.name
                                    ));
                                };
                                store::save_video_profile(
                                    &conn,
                                    &vp.id,
                                    &vp.name,
                                    vp.version,
                                    &serde_json::to_string(&vp).map_err(|e| e.to_string())?,
                                )
                                .map_err(db_error)?;
                            }
                            Ok(format!("{} profiles", profiles.len()))
                        })
                        .await
                        .map_err(|e| format!("join: {e}"))?
                    },
                    Message::ProfileSaved,
                );
            }
            Message::ProfileSaved(Ok(n)) => {
                self.status = format!("Saved {n}.");
            }
            Message::ProfileSaved(Err(e)) => {
                self.status = format!("Profile error: {e}");
            }
            Message::ProfilesLoaded(Ok(profiles)) => {
                // Saved profiles win over built-in defaults so restart
                // restores visible edits instead of overwriting them.
                if !profiles.is_empty() {
                    if self
                        .selected_profile
                        .as_ref()
                        .is_none_or(|s| !profiles.iter().any(|p| &p.id == s))
                    {
                        self.selected_profile = profiles.first().map(|p| p.id.clone());
                    }
                    self.profiles = profiles;
                    self.revalidate();
                }
            }
            Message::ProfilesLoaded(Err(e)) => {
                self.status = format!("Profiles error: {e}");
            }
            Message::ProfileAssigned(dest) => {
                if let Some(sel) = self.selected_profile.clone() {
                    if let Some(cur) = &mut self.current {
                        for d in &mut cur.dests {
                            if d.binding_id == dest {
                                d.video_profile = if d.video_profile.as_deref() == Some(&sel) {
                                    None
                                } else {
                                    Some(sel.clone())
                                };
                            }
                        }
                    }
                    self.edited();
                }
            }
        }
        Task::none()
    }

    // ---------- helpers ----------

    fn load_profiles_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<Vec<EditedProfile>, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    let rows = store::list_video_profiles(&conn).map_err(db_error)?;
                    let mut out = Vec::new();
                    for (_, _, json) in rows {
                        if let Ok(vp) = serde_json::from_str::<VideoProfile>(&json) {
                            out.push(EditedProfile::from_profile(&vp));
                        }
                    }
                    Ok(out)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::ProfilesLoaded,
        )
    }

    fn load_bindings_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<BindingsData, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    Ok(BindingsData {
                        bindings: store::list_bindings(&conn).map_err(db_error)?,
                        accounts: store::list_accounts(&conn).map_err(db_error)?,
                    })
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::BindingsLoaded,
        )
    }

    fn load_plans_refresh_task(&self) -> Task<Message> {
        self.load_plans_task()
    }

    fn load_ops_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<Vec<store::OperationRow>, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    store::list_operations(&conn).map_err(db_error)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::OpsLoaded,
        )
    }

    fn load_actions_task(&self, op: String) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(
                    move || -> Result<(String, Vec<store::ActionRow>), String> {
                        let conn = store::open(&path).map_err(db_error)?;
                        let actions = store::list_actions(&conn, &op).map_err(db_error)?;
                        Ok((op, actions))
                    },
                )
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::ActionsLoaded,
        )
    }

    fn seed_demo_task(&self) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    let conn = store::open(&path).map_err(db_error)?;
                    if !store::list_plans(&conn).map_err(db_error)?.is_empty() {
                        let first = store::list_plans(&conn).map_err(db_error)?;
                        return Ok(first[0].id.clone());
                    }
                    let (plan, dests, providers) = demo::demo_plan();
                    let start = plan.start_utc.to_rfc3339();
                    store::create_plan(&conn, &plan.id, &plan.title, &start).map_err(db_error)?;
                    store::update_plan_content(
                        &conn,
                        &plan.id,
                        &plan.title,
                        &plan.description,
                        &start,
                        None,
                        &plan.timezone,
                        &plan.language,
                        &plan.category_intent,
                        &serde_json::to_string(&plan.tags).unwrap_or("[]".into()),
                        plan.primary_id.as_deref(),
                    )
                    .map_err(db_error)?;
                    for d in &dests {
                        store::save_destination(&conn, &plan.id, d).map_err(db_error)?;
                    }
                    for (id, prov) in &providers {
                        let slug = prov.slug().to_string();
                        let _ = store::save_binding(
                            &conn,
                            &store::BindingRow {
                                id: id.clone(),
                                provider: slug,
                                account_id: None,
                                instantclone_name: Some(id.clone()),
                            },
                        );
                    }
                    for p in default_video_profiles() {
                        let _ = store::save_video_profile(
                            &conn,
                            &p.id,
                            &p.name,
                            p.version,
                            &serde_json::to_string(&p).unwrap_or("{}".into()),
                        );
                    }
                    Ok(plan.id)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::Seeded,
        )
    }

    /// Snapshot input shared by live validation and review building.
    fn snapshot_input(&self) -> Option<stream_manager_core::review::SnapshotInput> {
        use stream_manager_core::review::{SnapshotDest, SnapshotInput, SnapshotPlan};
        let cur = self.current.as_ref()?;
        let plan = SnapshotPlan {
            id: cur.id.clone(),
            title: cur.title.clone(),
            description: cur.desc.text(),
            start_local: cur.start_local.clone(),
            revision: cur.revision,
            primary_id: if cur.primary_id.is_empty() {
                None
            } else {
                Some(cur.primary_id.clone())
            },
        };
        let mut dests: Vec<SnapshotDest> = cur
            .dests
            .iter()
            .map(|d| SnapshotDest {
                dest_id: d.binding_id.clone(),
                provider: d.provider,
                role: if d.binding_id == cur.primary_id {
                    Role::Primary
                } else {
                    d.role
                },
                title: tri_from(d.title_mode, &d.title_text),
                description: tri_from(d.desc_mode, &d.desc_text),
                funnel_target: if d.role == Role::Funnel && !d.funnel_target.is_empty() {
                    Some(d.funnel_target.clone())
                } else {
                    None
                },
                remote_url: d.remote_url.clone(),
                video_profile: d.video_profile.clone(),
            })
            .collect();
        // Primary fixing matches validation: exactly the selected primary.
        for d in &mut dests {
            if Some(d.dest_id.as_str()) == plan.primary_id.as_deref() {
                d.role = Role::Primary;
                d.funnel_target = None;
            } else if d.role == Role::Primary {
                d.role = Role::Mirror;
            }
        }
        Some(SnapshotInput { plan, dests })
    }

    /// Build a review through the central gate, returning the review
    /// plus the exact input JSON so execution can revalidate.
    fn build_review(&self) -> Option<(stream_manager_core::review::ReviewSnapshot, String)> {
        use stream_manager_core::review::{SnapshotDest, SnapshotInput, SnapshotPlan};
        let cur = self.current.as_ref()?;
        let plan = SnapshotPlan {
            id: cur.id.clone(),
            title: cur.title.clone(),
            description: cur.desc.text(),
            start_local: cur.start_local.clone(),
            revision: cur.revision,
            primary_id: if cur.primary_id.is_empty() {
                None
            } else {
                Some(cur.primary_id.clone())
            },
        };
        let mut dests: Vec<SnapshotDest> = cur
            .dests
            .iter()
            .map(|d| SnapshotDest {
                dest_id: d.binding_id.clone(),
                provider: d.provider,
                role: if d.binding_id == cur.primary_id {
                    Role::Primary
                } else {
                    d.role
                },
                title: tri_from(d.title_mode, &d.title_text),
                description: tri_from(d.desc_mode, &d.desc_text),
                funnel_target: if d.role == Role::Funnel && !d.funnel_target.is_empty() {
                    Some(d.funnel_target.clone())
                } else {
                    None
                },
                remote_url: d.remote_url.clone(),
                video_profile: d.video_profile.clone(),
            })
            .collect();
        for d in &mut dests {
            if Some(d.dest_id.as_str()) == plan.primary_id.as_deref() {
                d.role = Role::Primary;
                d.funnel_target = None;
            } else if d.role == Role::Primary {
                d.role = Role::Mirror;
            }
        }
        let providers: HashMap<String, Provider> = cur
            .dests
            .iter()
            .map(|d| (d.binding_id.clone(), d.provider))
            .collect();
        let profiles: HashMap<String, VideoProfile> = self
            .profiles
            .iter()
            .filter_map(|p| p.to_profile().map(|v| (p.id.clone(), v)))
            .collect();
        let review = stream_manager_core::review::build_review(
            &plan,
            &dests,
            &providers,
            &profiles,
            self.demo_mode,
        );
        let input_json = serde_json::to_string(&SnapshotInput { plan, dests }).ok()?;
        Some((review, input_json))
    }

    fn apply_task(&self, input_json: String, demo: bool) -> Task<Message> {
        let path = self.db_path.clone();
        Task::perform(
            async move {
                tokio::task::spawn_blocking(move || -> Result<String, String> {
                    use stream_manager_core::review::{can_apply, SnapshotInput};
                    let conn = store::open(&path).map_err(db_error)?;
                    // Revalidate from the persisted input: a stale or
                    // hand-driven apply is refused here, not just at the
                    // button.
                    let input: SnapshotInput = serde_json::from_str(&input_json)
                        .map_err(|e| format!("bad snapshot: {e}"))?;
                    // Execution admission: the reviewed revision must
                    // still be the persisted one. A disabled button is
                    // not enforcement.
                    let persisted =
                        store::plan_revision(&conn, &input.plan.id).map_err(db_error)?;
                    if persisted != Some(input.plan.revision) {
                        return Err(format!(
                            "stale review: plan is at rev {:?}, review built on {}",
                            persisted, input.plan.revision
                        ));
                    }
                    let providers: HashMap<String, Provider> = store::list_bindings(&conn)
                        .map_err(db_error)?
                        .into_iter()
                        .map(|b| (b.id.clone(), provider_of(&b.provider)))
                        .collect();
                    let profiles: HashMap<String, VideoProfile> = store::list_video_profiles(&conn)
                        .map_err(db_error)?
                        .into_iter()
                        .filter_map(|(_, _, json)| serde_json::from_str::<VideoProfile>(&json).ok())
                        .map(|v| (v.id.clone(), v))
                        .collect();
                    let review = stream_manager_core::review::build_review(
                        &input.plan,
                        &input.dests,
                        &providers,
                        &profiles,
                        demo,
                    );
                    if !can_apply(&review) {
                        return Err(format!("review refused: {}", review.errors.join("; ")));
                    }
                    let op_id = format!("op-{}", chrono::Utc::now().format("%Y%m%dT%H%M%S"));
                    let actions: Vec<stream_manager_core::publish::PublishActionIntent> = review
                        .dests
                        .iter()
                        .map(|d| {
                            let kind = if d.provider == Provider::Rumble {
                                "manual".to_string()
                            } else if d.role == Role::Primary {
                                "create".to_string()
                            } else {
                                "update".to_string()
                            };
                            stream_manager_core::publish::PublishActionIntent {
                                destination_id: d.dest_id.clone(),
                                account_id: String::new(),
                                kind,
                                before: serde_json::json!({}),
                                after: serde_json::json!({
                                    "rendered": d.rendered.as_ref().ok().and_then(|o| o.clone()),
                                    "blocked": d.blocked,
                                }),
                                remote_id: None,
                                state: stream_manager_core::publish::ActionState::Pending,
                                error: None,
                            }
                        })
                        .collect();
                    store::record_operation(
                        &conn,
                        &op_id,
                        &review.plan_id,
                        review.revision,
                        &input_json,
                        &actions,
                    )
                    .map_err(db_error)?;
                    // Terminal honesty: without authorized accounts nothing
                    // leaves the machine. Manual work queues as manual;
                    // API work blocks until an account connects.
                    for d in &review.dests {
                        let (state, err) = if d.blocked.is_some() {
                            ("blocked", d.blocked.clone())
                        } else if d.provider == Provider::Rumble {
                            ("manual_required", Some("use the manual panel".to_string()))
                        } else if demo {
                            (
                                "blocked",
                                Some("demo mode: no authorized account".to_string()),
                            )
                        } else {
                            (
                                "blocked",
                                Some("not connected: no authorized account".to_string()),
                            )
                        };
                        store::update_action_state(
                            &conn,
                            &op_id,
                            &d.dest_id,
                            state,
                            err.as_deref(),
                            None,
                        )
                        .map_err(db_error)?;
                    }
                    Ok(op_id)
                })
                .await
                .map_err(|e| format!("join: {e}"))?
            },
            Message::OpRecorded,
        )
    }

    /// Live editor validation through the same central gate used by
    /// review and execution. Every edit refreshes this; the displayed
    /// review additionally carries its own staleness banner.
    fn revalidate(&mut self) {
        if self
            .current
            .as_ref()
            .is_some_and(|c| c.start_local.trim().is_empty())
        {
            self.validation = "Schedule problem: set a valid local start time.".into();
            return;
        }
        let Some(input) = self.snapshot_input() else {
            if self.current.is_some() {
                self.validation = "Schedule problem: set a valid local start time.".into();
            } else {
                self.validation.clear();
            }
            return;
        };
        let providers: HashMap<String, Provider> = self
            .current
            .as_ref()
            .map(|cur| {
                cur.dests
                    .iter()
                    .map(|d| (d.binding_id.clone(), d.provider))
                    .collect()
            })
            .unwrap_or_default();
        let profiles: HashMap<String, VideoProfile> = self
            .profiles
            .iter()
            .filter_map(|p| p.to_profile().map(|v| (p.id.clone(), v)))
            .collect();
        let review = stream_manager_core::review::build_review(
            &input.plan,
            &input.dests,
            &providers,
            &profiles,
            self.demo_mode,
        );
        if review.errors.is_empty() {
            self.validation.clear();
        } else {
            self.validation = review.errors.join(" | ");
        }
    }

    // ---------- views ----------

    fn view(&self) -> Element<'_, Message> {
        let tabs = row(Tab::ALL.iter().map(|t| {
            let b = button(text(t.label())).on_press(Message::TabSelected(*t));
            let b = if *t == self.tab {
                b.style(button::primary)
            } else {
                b.style(button::secondary)
            };
            b.into()
        }))
        .spacing(8);
        let banner = if self.demo_mode {
            text(DEMO_BANNER).size(14)
        } else {
            text("Live mode: credentials required; publishing is explicit review/apply.").size(14)
        };
        let body: Element<'_, Message> = match self.tab {
            Tab::Streams => self.view_streams(),
            Tab::Destinations => self.view_destinations(),
            Tab::Review => self.view_review(),
            Tab::History => self.view_history(),
            Tab::Video => self.view_video(),
            Tab::Chat => self.chat.view(&self.chat_ctx()),
        };
        column![tabs, banner, body].spacing(12).padding(16).into()
    }

    fn view_streams(&self) -> Element<'_, Message> {
        let mut list = column![text("Streams").size(15)];
        for p in &self.plans {
            let b = button(text(format!("{} [{}]", p.title, p.status)))
                .on_press(Message::SelectPlan(p.id.clone()));
            let b = if self.current.as_ref().is_some_and(|c| c.id == p.id) {
                b.style(button::primary)
            } else {
                b.style(button::secondary)
            };
            list = list.push(b);
        }
        list = list.push(button(text("+ New draft")).on_press(Message::CreatePlan));
        list = list.push(button(text("Load offline demo")).on_press(Message::LoadDemo));
        if self.conflict {
            list = list.push(
                button(text("Reload newer revision (discards unsaved edits)"))
                    .on_press(Message::DiscardAndReload),
            );
        }
        let list = list.spacing(4);
        let Some(cur) = &self.current else {
            // First-run empty state keeps creation controls visible.
            return row![
                container(list).width(280.0),
                container(
                    column![
                        text("No plan open.").size(14),
                        text("Create a draft, pick a stream, or load the offline demo.").size(13),
                        text(&self.status).size(13),
                    ]
                    .spacing(8)
                )
                .padding(16)
            ]
            .spacing(16)
            .into();
        };
        let mut editor = column![
            text(format!(
                "{} · rev {} · {}",
                cur.id, cur.revision, cur.status
            ))
            .size(13),
            text("Title"),
            text_input("Stream title", &cur.title).on_input(Message::TitleChanged),
            text("Description (multiline)"),
            text_editor(&cur.desc)
                .on_action(Message::DescAction)
                .height(140.0),
            text("Start (local, YYYY-MM-DD HH:MM)"),
            text_input("2026-10-02 20:00", &cur.start_local).on_input(Message::StartChanged),
            text("End (local, optional)"),
            text_input("", &cur.end_local).on_input(Message::EndChanged),
            text(format!(
                "Timezone: {} · Language: {}",
                cur.timezone, cur.language
            ))
            .size(13),
            text("Tags (JSON array)"),
            text_input("[\"live\"]", &cur.tags).on_input(Message::TagsChanged),
            text(format!("Primary: {}", cur.primary_id)).size(13),
            row![
                button(text("Save")).on_press(Message::SaveNow),
                button(text("New draft")).on_press(Message::CreatePlan),
                button(text("Duplicate")).on_press(Message::DuplicatePlan),
                button(text("Archive")).on_press(Message::ArchivePlan),
                button(text("Reopen")).on_press(Message::ReopenPlan),
                button(text("Load offline demo")).on_press(Message::LoadDemo),
            ]
            .spacing(8),
            toggler(self.demo_mode)
                .label("Offline demo")
                .on_toggle(Message::ToggleDemo),
            text(&self.status).size(13),
            text(&self.validation).size(13),
        ]
        .spacing(8);
        for d in &cur.dests {
            let is_primary = d.binding_id == cur.primary_id;
            editor = editor.push(container(column![
                text(format!(
                    "{} — {} [{:?}]{}",
                    d.binding_id,
                    d.provider.display(),
                    if is_primary { Role::Primary } else { d.role },
                    match &d.account_id {
                        Some(a) => format!(" · account {a}"),
                        None => " · no account".into(),
                    }
                ))
                .size(13),
                row![
                    button(text(format!("role: {:?}", d.role)))
                        .on_press(Message::RoleClicked(d.binding_id.clone())),
                    button(text("make primary"))
                        .on_press(Message::PrimaryClicked(d.binding_id.clone())),
                    button(text(format!("desc: {}", d.desc_mode.label())))
                        .on_press(Message::DescModeClicked(d.binding_id.clone())),
                    button(text("account")).on_press(Message::AccountClicked(d.binding_id.clone())),
                ]
                .spacing(6),
                text_input("override text…", &d.desc_text).on_input({
                    let id = d.binding_id.clone();
                    move |v| Message::DescTextChanged(id.clone(), v)
                }),
                text_input("funnel target…", &d.funnel_target).on_input({
                    let id = d.binding_id.clone();
                    move |v| Message::FunnelTargetChanged(id.clone(), v)
                }),
            ]));
        }
        // Third pane: live destination preview from the current edits.
        // All strings are owned: the temporary review must not be
        // borrowed by the returned element tree.
        let mut preview = column![text("Destination preview").size(14)].spacing(6);
        if let Some((review, _)) = self.build_review() {
            for d in &review.dests {
                let body = match &d.rendered {
                    Ok(Some(t)) => {
                        let mut s: String = t.chars().take(160).collect();
                        if t.chars().count() > 160 {
                            s.push('…');
                        }
                        s
                    }
                    Ok(None) => "(cleared)".into(),
                    Err(e) => format!("(template error: {e})"),
                };
                let head = format!("{} [{:?}]", d.dest_id, d.role);
                let state: String = d.blocked.clone().unwrap_or_else(|| "ready".into());
                preview = preview.push(container(column![
                    text(head).size(12),
                    text(body).size(12),
                    text(state).size(12),
                ]));
            }
            if !review.errors.is_empty() {
                preview = preview
                    .push(text(format!("{} error(s) — see Review", review.errors.len())).size(12));
            }
        } else {
            preview = preview.push(text("Set a start time to preview.").size(12));
        }
        row![
            container(list).width(280.0),
            container(scrollable(editor)).width(Length::FillPortion(3)),
            container(scrollable(preview)).width(Length::FillPortion(2)),
        ]
        .spacing(16)
        .into()
    }

    fn view_destinations(&self) -> Element<'_, Message> {
        let mut col = column![
            text(&self.transport).size(13),
            button(text(if self.probing {
                "Probing…"
            } else {
                "Refresh transport status"
            }))
            .on_press_maybe(if self.probing {
                None
            } else {
                Some(Message::RefreshTransport)
            }),
            text("Transport (InstantClone declaration) and metadata authorization are separate.")
                .size(13),
        ]
        .spacing(8);
        for b in &self.bindings {
            let prov = provider_of(&b.provider);
            let caps = adapter_caps(prov);
            let acct = b
                .account_id
                .as_deref()
                .and_then(|id| self.accounts.iter().find(|a| a.id == id));
            col = col.push(container(column![
                text(format!("{} — {}", b.id, prov.display())).size(14),
                text(format!(
                    "transport: {} | account: {} | auth: {}",
                    b.instantclone_name.as_deref().unwrap_or("not declared"),
                    acct.map(|a| format!("{} ({})", a.channel_name, a.channel_id))
                        .unwrap_or_else(|| "none".into()),
                    if acct.is_some_and(|a| !a.credential_ref.is_empty()) {
                        "credential configured (not verified)"
                    } else {
                        "not connected"
                    },
                ))
                .size(13),
                text(format!(
                    "event update: {:?} | channel-now: {:?} | draft-safe: {}",
                    caps.update_event,
                    caps.update_channel_now,
                    caps.draft_without_channel_side_effect,
                ))
                .size(13),
            ]));
        }
        if self.bindings.is_empty() {
            col = col.push(text(
                "No destination bindings stored yet — load the offline demo or add one below.",
            ));
        }
        let mut add = row![text("Add destination: ").size(13)].spacing(6);
        for prov in [
            Provider::Youtube,
            Provider::Twitch,
            Provider::Kick,
            Provider::Rumble,
        ] {
            add = add.push(
                button(text(format!("+ {}", prov.display()))).on_press(Message::AddBinding(prov)),
            );
        }
        col = col.push(add);
        scrollable(col.spacing(8)).into()
    }

    fn view_review(&self) -> Element<'_, Message> {
        let mut col =
            column![button(text("Build review from current plan")).on_press(Message::BuildReview)]
                .spacing(8);
        let Some(active) = &self.review else {
            col = col.push(text("No review built yet.").size(13));
            col = col.push(self.view_manual());
            return scrollable(col).into();
        };
        let r = &active.review;
        let stale = self
            .current
            .as_ref()
            .is_some_and(|c| c.id != r.plan_id || c.revision != r.revision);
        if stale {
            col = col.push(
                text("STALE: the plan changed since this review — rebuild before applying.")
                    .size(13),
            );
        }
        if r.demo {
            col = col.push(
                text("Demo review: separate demo database, fixture identities (DEMO_*).").size(13),
            );
        }
        col = col.push(
            text(format!(
                "Reviewing immutable revision {} of “{}”",
                r.revision, r.title
            ))
            .size(13),
        );
        // Admission comes from the review's own errors, not the live
        // editor: the button and execution share one gate.
        for e in &r.errors {
            col = col.push(text(format!("Blocks apply: {e}")).size(13));
        }
        for d in &r.dests {
            let body = match &d.rendered {
                Ok(Some(t)) => t.clone(),
                Ok(None) => "(cleared)".into(),
                Err(e) => format!("(template error: {e})"),
            };
            col = col.push(container(column![
                text(format!("{} [{:?}] → {}", d.dest_id, d.role, d.action)).size(14),
                text(body).size(13),
                text(d.blocked.as_deref().unwrap_or("ready")).size(13),
            ]));
        }
        for v in &r.policy {
            col = col.push(text(format!("Policy {}: {}", v.rule_id, v.message)).size(13));
        }
        let admittable = stream_manager_core::review::can_apply(r) && !stale;
        col = col.push(
            button(text("Record publish operation (review → apply)")).on_press_maybe(
                if admittable {
                    Some(Message::ApplyAll)
                } else {
                    None
                },
            ),
        );
        if !admittable && !stale {
            col = col.push(
                text("Resolve the listed errors to enable apply. Manual Rumble actions are recorded from History after apply.").size(13),
            );
        }
        col = col.push(self.view_manual());
        scrollable(col).into()
    }

    fn view_manual(&self) -> Element<'_, Message> {
        if self.manual.op_id.is_empty() {
            return column![text(
                "Rumble manual workflow: apply a review, then open the manual action from History."
            )
            .size(13)]
            .into();
        }
        column![
            text(format!(
                "Manual: {} in {} (stored action payload)",
                self.manual.dest_id, self.manual.op_id
            ))
            .size(14),
            text(&self.manual.rendered).size(13),
            row![
                button(text(if self.manual.copied {
                    "Copied ✓"
                } else {
                    "Copy text"
                }))
                .on_press(Message::ManualCopy),
                button(text("Open Rumble dashboard")).on_press(Message::ManualOpenDashboard),
            ]
            .spacing(8),
            text("Public URL (paste back from dashboard)"),
            text_input("https://rumble.com/…", &self.manual.url)
                .on_input(Message::ManualUrlChanged),
            text("Operator note"),
            text_input("", &self.manual.note).on_input(Message::ManualNoteChanged),
            button(text("Record manual completion")).on_press(Message::ManualRecord),
            text(&self.manual.error).size(13),
            text("Manual completion is recorded operator testimony, not API verification.")
                .size(12),
        ]
        .spacing(8)
        .into()
    }

    fn view_history(&self) -> Element<'_, Message> {
        let ops = column(self.operations.iter().map(|o| {
            button(text(format!(
                "{} · {} rev {} [{}]",
                o.id, o.plan_id, o.plan_revision, o.state
            )))
            .on_press(Message::SelectOperation(o.id.clone()))
            .into()
        }))
        .spacing(4);
        let mut detail = column![text("Select an operation for per-action results.").size(13)];
        for a in &self.actions {
            let mut row_ = column![
                text(format!("{} [{}] ({})", a.destination_id, a.state, a.kind)).size(13),
                text(
                    a.error
                        .clone()
                        .unwrap_or_else(|| a.remote_id.clone().unwrap_or_default()),
                )
                .size(12),
            ];
            let mut btns = row![].spacing(6);
            if ["pending", "failed", "blocked"].contains(&a.state.as_str()) {
                btns = btns.push(button(text("Retry (this action only)")).on_press(
                    Message::RetryAction(a.operation_id.clone(), a.destination_id.clone()),
                ));
            }
            if a.state == "outcome_unknown" {
                btns = btns.push(button(text("Reconcile…")).on_press(Message::ReconcileOpen(
                    a.operation_id.clone(),
                    a.destination_id.clone(),
                )));
            }
            if a.kind == "manual"
                && ["manual_required", "pending", "blocked"].contains(&a.state.as_str())
            {
                btns = btns.push(button(text("Manual panel")).on_press(Message::ManualOpen(
                    a.operation_id.clone(),
                    a.destination_id.clone(),
                )));
            }
            row_ = row_.push(btns);
            detail = detail.push(container(row_));
        }
        if let Some((op, dest)) = self.reconcile.clone() {
            detail = detail.push(container(column![
                text(format!(
                    "Reconcile {dest} in {op}: did the remote effect happen?"
                ))
                .size(13),
                text("Check the provider dashboard, then record the verified outcome.").size(12),
                row![
                    button(text("Remote applied")).on_press(Message::ReconcileConfirm(
                        op.clone(),
                        dest.clone(),
                        true
                    )),
                    button(text("No remote effect"))
                        .on_press(Message::ReconcileConfirm(op, dest, false)),
                ]
                .spacing(8),
            ]));
        }
        row![
            container(scrollable(ops)).width(340.0),
            container(scrollable(detail.spacing(8))).padding(8)
        ]
        .spacing(12)
        .into()
    }

    fn view_video(&self) -> Element<'_, Message> {
        let list = column(self.profiles.iter().map(|p| {
            button(text(format!("{} (v{})", p.name, p.version())))
                .on_press(Message::ProfileSelected(p.id.clone()))
                .into()
        }))
        .spacing(4);
        let Some(sel) = self
            .profiles
            .iter()
            .find(|p| Some(p.id.as_str()) == self.selected_profile.as_deref())
        else {
            return text("No profile selected.").into();
        };
        let mut editor = column![
            text("Stored profiles + native preview. Preview only — outgoing video is NOT transformed.")
                .size(13),
            text(&sel.name).size(15),
        ]
        .spacing(8);
        editor = editor.push(self.view_profile_fields(sel));
        if let Some(vp) = sel.to_profile() {
            let link = sel.primary_link();
            editor = editor.push(
                canvas(MaskPreview { profile: vp, link })
                    .width(Length::Fill)
                    .height(Length::Fixed(300.0)),
            );
            editor = editor.push(text("Blur/pixelation is not offered; opaque masks are not described as guaranteed redaction.").size(12));
        } else {
            editor =
                editor.push(text("Profile has invalid geometry — fix the fields above.").size(13));
        }
        editor = editor.push(button(text("Save profiles")).on_press(Message::ProfileSaveRequested));
        let mut assign = column![text("Assign selected profile to destination:").size(13)];
        if let Some(cur) = &self.current {
            for d in &cur.dests {
                assign = assign.push(
                    button(text(format!(
                        "{} {}",
                        d.binding_id,
                        if d.video_profile.as_deref() == Some(&sel.id) {
                            "✓"
                        } else {
                            ""
                        }
                    )))
                    .on_press(Message::ProfileAssigned(d.binding_id.clone())),
                );
            }
        }
        row![
            container(list).width(260.0),
            container(scrollable(editor.spacing(8))).width(Length::Fill),
            container(scrollable(assign.spacing(4))).width(260.0),
        ]
        .spacing(12)
        .into()
    }

    fn view_profile_fields<'a>(&self, sel: &'a EditedProfile) -> Element<'a, Message> {
        let id = sel.id.clone();
        fn field<'a>(id: String, name: &'a str, value: &'a str) -> Element<'a, Message> {
            column![
                text(name).size(12),
                text_input(name, value)
                    .on_input(move |v| { Message::ProfileFieldChanged(format!("{id}:{name}"), v) }),
            ]
            .spacing(2)
            .into()
        }
        match &sel.kind {
            ProfileKindEdit::Passthrough => {
                column![text("Clean output for primary/mirror.").size(13)].into()
            }
            ProfileKindEdit::Banner {
                text: t,
                link,
                size,
                opacity,
                ..
            } => column![
                field(id.clone(), "Banner text", t),
                field(id.clone(), "Link (reviewed-revision URL)", link),
                field(id.clone(), "Size (0.05–0.40 of frame height)", size),
                field(id.clone(), "Opacity (0–1)", opacity),
            ]
            .spacing(6)
            .into(),
            ProfileKindEdit::Mask {
                x,
                y,
                w,
                h,
                opacity,
            } => column![
                field(id.clone(), "X (0–1)", x),
                field(id.clone(), "Y (0–1)", y),
                field(id.clone(), "W (0–1)", w),
                field(id.clone(), "H (0–1)", h),
                field(id.clone(), "Opacity (0–1)", opacity),
                text("2% safe margins enforced.").size(12),
            ]
            .spacing(6)
            .into(),
            ProfileKindEdit::Slate {
                headline,
                body,
                link,
            } => column![
                field(id.clone(), "Headline", headline),
                field(id.clone(), "Body", body),
                field(id.clone(), "Link", link),
            ]
            .spacing(6)
            .into(),
        }
    }
}

// ---------- free helpers ----------

fn provider_of(slug: &str) -> Provider {
    match slug {
        "youtube" => Provider::Youtube,
        "twitch" => Provider::Twitch,
        "kick" => Provider::Kick,
        _ => Provider::Rumble,
    }
}

fn adapter_caps(p: Provider) -> providers::AdapterCaps {
    match p {
        Provider::Youtube => providers::youtube::caps(),
        Provider::Twitch => providers::twitch::caps(),
        Provider::Kick => providers::kick::caps(),
        Provider::Rumble => providers::rumble::caps(),
    }
}

fn tri_of<T: Clone>(f: &FieldOverride<T>) -> TriMode {
    match f {
        FieldOverride::Inherit => TriMode::Inherit,
        FieldOverride::Replace(_) => TriMode::Replace,
        FieldOverride::Clear => TriMode::Clear,
    }
}

fn tri_text(f: &FieldOverride<String>) -> String {
    match f {
        FieldOverride::Replace(v) => v.clone(),
        _ => String::new(),
    }
}

fn tri_from(mode: TriMode, text: &str) -> FieldOverride<String> {
    match mode {
        TriMode::Inherit => FieldOverride::Inherit,
        TriMode::Replace => FieldOverride::Replace(text.to_string()),
        TriMode::Clear => FieldOverride::Clear,
    }
}

fn utc_to_local_input(utc: &str, tz: &str) -> String {
    stream_manager_core::schedule::utc_to_local_input(utc, tz)
}

fn local_inputs_to_utc(
    start: &str,
    end: &str,
    tz: &str,
) -> Result<
    (
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    ),
    stream_manager_core::schedule::ScheduleError,
> {
    use stream_manager_core::schedule::{local_to_utc, validate_range};
    if start.trim().is_empty() {
        // Fresh drafts without a schedule yet: callers persist empty and
        // review stays unavailable. Never invent a timestamp.
        return Err(stream_manager_core::schedule::ScheduleError::Nonexistent);
    }
    let s = local_to_utc(start.trim(), tz)?;
    let e = if end.trim().is_empty() {
        None
    } else {
        Some(local_to_utc(end.trim(), tz)?)
    };
    validate_range(&s, &e)?;
    Ok((s, e))
}

fn local_inputs_to_storage(
    start: &str,
    end: &str,
    tz: &str,
) -> Result<(String, Option<String>), stream_manager_core::schedule::ScheduleError> {
    if start.trim().is_empty() {
        return Ok((String::new(), None));
    }
    let (s, e) = local_inputs_to_utc(start, end, tz)?;
    Ok((s.to_rfc3339(), e.map(|v| v.to_rfc3339())))
}

// ---------- video preview ----------

fn default_profiles() -> Vec<EditedProfile> {
    vec![
        EditedProfile {
            id: "passthrough".into(),
            name: "Passthrough (clean)".into(),
            kind: ProfileKindEdit::Passthrough,
            audio: AudioBehavior::Keep,
        },
        EditedProfile {
            id: "cta-banner".into(),
            name: "CTA banner + QR".into(),
            kind: ProfileKindEdit::Banner {
                text: "Watch the full show live".into(),
                link: "{{primary_watch_url}}".into(),
                qr: true,
                bottom: true,
                size: "0.12".into(),
                opacity: "0.95".into(),
            },
            audio: AudioBehavior::Keep,
        },
        EditedProfile {
            id: "region-mask".into(),
            name: "Opaque region mask".into(),
            kind: ProfileKindEdit::Mask {
                x: "0.05".into(),
                y: "0.70".into(),
                w: "0.90".into(),
                h: "0.22".into(),
                opacity: "1.00".into(),
            },
            audio: AudioBehavior::Keep,
        },
        EditedProfile {
            id: "full-slate".into(),
            name: "Full-frame slate".into(),
            kind: ProfileKindEdit::Slate {
                headline: "Preview stream".into(),
                body: "The full livestream is on {{primary_platform}}: {{primary_watch_url}}"
                    .into(),
                link: "{{primary_watch_url}}".into(),
            },
            audio: AudioBehavior::Duck,
        },
    ]
}

fn default_video_profiles() -> Vec<VideoProfile> {
    default_profiles()
        .into_iter()
        .filter_map(|p| p.to_profile())
        .collect()
}

impl EditedProfile {
    fn version(&self) -> u32 {
        1
    }

    /// Restore editor state from a stored profile. Numeric fields render
    /// back to strings; unknown shapes fall back to passthrough rather
    /// than failing the whole list.
    fn from_profile(vp: &VideoProfile) -> EditedProfile {
        let num = |v: f32| {
            let s = format!("{v:.2}");
            s
        };
        let kind = match &vp.kind {
            VideoKind::Passthrough => ProfileKindEdit::Passthrough,
            VideoKind::CtaBanner(b) => ProfileKindEdit::Banner {
                text: b.text.clone(),
                link: b.link_url.clone().unwrap_or_default(),
                qr: b.with_qr,
                bottom: matches!(b.placement, Placement::Bottom),
                size: num(b.size),
                opacity: num(b.opacity),
            },
            VideoKind::RegionMask(m) => ProfileKindEdit::Mask {
                x: num(m.x),
                y: num(m.y),
                w: num(m.w),
                h: num(m.h),
                opacity: num(m.opacity),
            },
            VideoKind::FullSlate(s) => ProfileKindEdit::Slate {
                headline: s.headline.clone(),
                body: s.body.clone(),
                link: s.link_url.clone().unwrap_or_default(),
            },
        };
        EditedProfile {
            id: vp.id.clone(),
            name: vp.name.clone(),
            kind,
            audio: vp.audio,
        }
    }
    fn parse(key: &str, v: &str) -> Option<f32> {
        let _ = key;
        v.trim().parse().ok()
    }
    fn set_field(&mut self, field: &str, v: &str) {
        let Some(name) = field.split(':').nth(1) else {
            return;
        };
        match &mut self.kind {
            ProfileKindEdit::Banner {
                text,
                link,
                size,
                opacity,
                ..
            } => match name {
                "Banner text" => *text = v.into(),
                "Link (reviewed-revision URL)" => *link = v.into(),
                "Size (0.05–0.40 of frame height)" => *size = v.into(),
                "Opacity (0–1)" => *opacity = v.into(),
                _ => {}
            },
            ProfileKindEdit::Mask {
                x,
                y,
                w,
                h,
                opacity,
            } => match name {
                "X (0–1)" => *x = v.into(),
                "Y (0–1)" => *y = v.into(),
                "W (0–1)" => *w = v.into(),
                "H (0–1)" => *h = v.into(),
                "Opacity (0–1)" => *opacity = v.into(),
                _ => {}
            },
            ProfileKindEdit::Slate {
                headline,
                body,
                link,
            } => match name {
                "Headline" => *headline = v.into(),
                "Body" => *body = v.into(),
                "Link" => *link = v.into(),
                _ => {}
            },
            ProfileKindEdit::Passthrough => {}
        }
    }
    fn primary_link(&self) -> String {
        match &self.kind {
            ProfileKindEdit::Banner { link, .. } => link.clone(),
            ProfileKindEdit::Slate { link, .. } => link.clone(),
            _ => String::new(),
        }
    }
    fn to_profile(&self) -> Option<VideoProfile> {
        let kind = match &self.kind {
            ProfileKindEdit::Passthrough => VideoKind::Passthrough,
            ProfileKindEdit::Banner {
                text,
                link,
                qr,
                bottom,
                size,
                opacity,
            } => {
                let b = CtaBanner {
                    text: text.clone(),
                    link_url: if link.is_empty() {
                        None
                    } else {
                        Some(link.clone())
                    },
                    with_qr: *qr,
                    placement: if *bottom {
                        Placement::Bottom
                    } else {
                        Placement::Top
                    },
                    size: Self::parse("size", size)?,
                    opacity: Self::parse("opacity", opacity)?,
                };
                b.validate().ok()?;
                VideoKind::CtaBanner(b)
            }
            ProfileKindEdit::Mask {
                x,
                y,
                w,
                h,
                opacity,
            } => {
                let m = RegionMask {
                    x: Self::parse("x", x)?,
                    y: Self::parse("y", y)?,
                    w: Self::parse("w", w)?,
                    h: Self::parse("h", h)?,
                    opacity: Self::parse("opacity", opacity)?,
                };
                m.validate().ok()?;
                VideoKind::RegionMask(m)
            }
            ProfileKindEdit::Slate {
                headline,
                body,
                link,
            } => VideoKind::FullSlate(FullSlate {
                headline: headline.clone(),
                body: body.clone(),
                link_url: if link.is_empty() {
                    None
                } else {
                    Some(link.clone())
                },
            }),
        };
        Some(VideoProfile {
            id: self.id.clone(),
            name: self.name.clone(),
            version: 1,
            kind,
            audio: self.audio,
            show_from_secs: None,
            show_until_secs: None,
        })
    }
}

struct MaskPreview {
    profile: VideoProfile,
    link: String,
}

impl canvas::Program<Message> for MaskPreview {
    type State = ();

    fn draw(
        &self,
        _state: &(),
        renderer: &iced::Renderer,
        _theme: &iced::Theme,
        bounds: iced::Rectangle,
        _cursor: iced::mouse::Cursor,
    ) -> Vec<Geometry> {
        let mut frame = Frame::new(renderer, bounds.size());
        // Constrain the preview to a 16:9 viewport, letterboxed: geometry
        // in the editor is normalized, so the preview must be too.
        let k = (bounds.width / 16.0).min(bounds.height / 9.0).max(1.0);
        let (w, h) = (16.0 * k, 9.0 * k);
        let (x0, y0) = ((bounds.width - w) / 2.0, (bounds.height - h) / 2.0);
        frame.fill_rectangle(
            Point::ORIGIN,
            Size::new(bounds.width, bounds.height),
            Color::BLACK,
        );
        frame.fill_rectangle(
            Point::new(x0, y0),
            Size::new(w, h),
            Color::from_rgb8(0x0B, 0x0D, 0x10),
        );
        // Synthetic program picture: scanline bands so transforms read.
        for (i, y) in [0.15, 0.35, 0.55].iter().enumerate() {
            let c = [0x2A, 0x33, 0x3D][i];
            frame.fill_rectangle(
                Point::new(x0, y0 + h * y),
                Size::new(w, h * 0.08),
                Color::from_rgb8(c, c + 8, c + 16),
            );
        }
        match &self.profile.kind {
            VideoKind::Passthrough => {}
            VideoKind::CtaBanner(b) => {
                let bh = h * b.size.clamp(0.05, 0.4);
                let y = match b.placement {
                    Placement::Bottom => y0 + h - bh,
                    Placement::Top => y0,
                };
                frame.fill_rectangle(
                    Point::new(x0, y),
                    Size::new(w, bh),
                    Color {
                        r: 0.06,
                        g: 0.16,
                        b: 0.32,
                        a: b.opacity.clamp(0.0, 1.0),
                    },
                );
                frame.fill_text(Text {
                    content: b.text.clone(),
                    position: Point::new(x0 + 12.0, y + bh / 2.0 - 8.0),
                    size: iced::Pixels(15.0),
                    color: Color::WHITE,
                    font: Font::default(),
                    ..Text::default()
                });
                if b.with_qr {
                    draw_qr(
                        &mut frame,
                        &self.link,
                        Point::new(x0 + w - bh - 8.0, y + 4.0),
                        bh - 8.0,
                    );
                }
            }
            VideoKind::RegionMask(m) => {
                frame.fill_rectangle(
                    Point::new(x0 + w * m.x, y0 + h * m.y),
                    Size::new(w * m.w, h * m.h),
                    Color {
                        r: 0.0,
                        g: 0.0,
                        b: 0.0,
                        a: m.opacity.clamp(0.0, 1.0),
                    },
                );
            }
            VideoKind::FullSlate(s) => {
                frame.fill_rectangle(
                    Point::new(x0, y0),
                    Size::new(w, h),
                    Color::from_rgb8(0x1D, 0x21, 0x26),
                );
                frame.fill_text(Text {
                    content: s.headline.clone(),
                    position: Point::new(x0 + w / 2.0 - 80.0, y0 + h / 2.0 - 30.0),
                    size: iced::Pixels(22.0),
                    color: Color::WHITE,
                    font: Font::default(),
                    ..Text::default()
                });
                // Word-wrap the full body instead of truncating it.
                for (i, line) in wrap_text(&s.body, 44).iter().take(4).enumerate() {
                    frame.fill_text(Text {
                        content: line.clone(),
                        position: Point::new(
                            x0 + w / 2.0 - 80.0,
                            y0 + h / 2.0 + 4.0 + i as f32 * 17.0,
                        ),
                        size: iced::Pixels(13.0),
                        color: Color::from_rgb8(0x9A, 0xA3, 0xAD),
                        font: Font::default(),
                        ..Text::default()
                    });
                }
            }
        }
        // Viewport border.
        frame.stroke(
            &Path::rectangle(Point::new(x0, y0), Size::new(w - 1.0, h - 1.0)),
            Stroke::default()
                .with_color(Color::from_rgb8(0x2C, 0x33, 0x3B))
                .with_width(1.0),
        );
        vec![frame.into_geometry()]
    }
}

/// Greedy word wrap for canvas text (no truncation).
fn wrap_text(body: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in body.split_whitespace() {
        if !line.is_empty() && line.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn draw_qr(frame: &mut Frame, content: &str, at: Point, size: f32) {
    use qrcode::QrCode;
    let code = QrCode::new(content.as_bytes()).unwrap_or_else(|_| QrCode::new("preview").unwrap());
    let n = code.width() as f32;
    let cell = size / n;
    frame.fill_rectangle(
        Point::new(at.x - 3.0, at.y - 3.0),
        Size::new(size + 6.0, size + 6.0),
        Color::WHITE,
    );
    for y in 0..code.width() {
        for x in 0..code.width() {
            if code[(x, y)] == qrcode::Color::Dark {
                frame.fill_rectangle(
                    Point::new(at.x + x as f32 * cell, at.y + y as f32 * cell),
                    Size::new(cell + 0.5, cell + 0.5),
                    Color::BLACK,
                );
            }
        }
    }
}

// ---------- subprocesses (bounded) ----------

/// Probe the existing non-secret Canix status interface off the UI thread
/// with a bounded timeout. Single-flight (see `probing`), kills the child
/// on timeout/cancel, truncates output, and never reads secret-bearing
/// runtime config.
async fn probe_instantclone() -> Result<String, String> {
    let cfg = app_config();
    let (prog, args) = cfg
        .status_cmd
        .split_first()
        .map(|(p, a)| (p.clone(), a.to_vec()))
        .unwrap_or_else(|| ("canix".into(), vec![]));
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(args).kill_on_drop(true);
    let out = tokio::time::timeout(std::time::Duration::from_secs(10), cmd.output())
        .await
        .map_err(|_| "status probe timed out".to_string())?
        .map_err(|e| format!("probe failed: {e}"))?;
    if !out.status.success() {
        let tail = String::from_utf8_lossy(&out.stderr);
        let tail: String = tail.chars().take(500).collect();
        return Err(format!("canix status failed: {tail}"));
    }
    let stdout: Vec<u8> = out.stdout.into_iter().take(32 * 1024).collect();
    let v: serde_json::Value =
        serde_json::from_slice(&stdout).map_err(|e| format!("bad status JSON: {e}"))?;
    let empty = vec![];
    let dests = v
        .get("destinations")
        .and_then(|d| d.as_array())
        .unwrap_or(&empty);
    let mut lines = vec![format!("Declared destinations: {}", dests.len())];
    for d in dests.iter().take(12) {
        lines.push(format!(
            "{}: platform={} enabled={} source={}",
            d.get("name").and_then(|x| x.as_str()).unwrap_or("?"),
            d.get("platform").and_then(|x| x.as_str()).unwrap_or("?"),
            d.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false),
            d.get("encryptedFileReadable")
                .and_then(|x| x.as_bool())
                .unwrap_or(false),
        ));
    }
    lines.push("Runtime/decryption/provider connectivity: unknown (not probed).".into());
    Ok(lines.join("\n"))
}

async fn open_dashboard() -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("xdg-open");
    cmd.arg(stream_manager_core::providers::rumble::RUMBLE_DASHBOARD_URL)
        .kill_on_drop(true);
    let out = tokio::time::timeout(std::time::Duration::from_secs(10), cmd.output())
        .await
        .map_err(|_| "browser launch timed out".to_string())?
        .map_err(|e| format!("browser launch failed: {e}"))?;
    if out.status.success() {
        Ok("opened".into())
    } else {
        Err("browser exited non-zero".into())
    }
}

fn main() -> iced::Result {
    iced::application(App::title, App::update, App::view)
        .subscription(App::subscription)
        .run_with(|| (App::new(), Task::done(Message::Startup)))
}

impl App {
    /// Passive subscriptions: the single chat event bus, detached-window
    /// close events, and chat keyboard navigation (Chat tab only).
    fn subscription(&self) -> iced::Subscription<Message> {
        let bus = iced::Subscription::run(ui::chat::bus_worker);
        let closed =
            iced::window::close_events().map(|id| Message::Chat(ChatMsg::DetachedClosed(id)));
        if self.tab != Tab::Chat {
            return iced::Subscription::batch([bus, closed]);
        }
        let keys = iced::keyboard::on_key_press(|key, _mods| match key {
            iced::keyboard::Key::Named(iced::keyboard::key::Named::ArrowUp) => {
                Some(Message::Chat(ChatMsg::SelectPrev))
            }
            iced::keyboard::Key::Named(iced::keyboard::key::Named::ArrowDown) => {
                Some(Message::Chat(ChatMsg::SelectNext))
            }
            _ => None,
        });
        iced::Subscription::batch([bus, closed, keys])
    }
}

#[cfg(test)]
mod ui_logic_tests {
    use super::*;

    fn demo_app() -> App {
        // Pure state machine checks: no display, no network, no database.
        let mut app = App::new();
        let (plan, dests, _) = demo::demo_plan();
        app.current = Some(EditedPlan {
            id: plan.id.clone(),
            title: plan.title.clone(),
            desc: text_editor::Content::with_text(&plan.description),
            start_local: "2026-10-02 20:00".into(),
            end_local: String::new(),
            timezone: plan.timezone.clone(),
            language: plan.language.clone(),
            category_intent: plan.category_intent.clone(),
            tags: "[]".into(),
            status: "draft".into(),
            revision: 1,
            primary_id: "youtube-main".into(),
            dests: dests
                .into_iter()
                .map(|d| EditedDest {
                    provider: match d.destination_id.as_str() {
                        "youtube-main" => Provider::Youtube,
                        "rumble-main" => Provider::Rumble,
                        "kick-main" => Provider::Kick,
                        _ => Provider::Twitch,
                    },
                    account_id: None,
                    binding_id: d.destination_id.clone(),
                    role: d.role,
                    funnel_target: d.funnel_target.unwrap_or_default(),
                    desc_mode: tri_of(&d.description),
                    desc_text: tri_text(&d.description),
                    title_mode: TriMode::Inherit,
                    title_text: String::new(),
                    remote_id: d.remote_broadcast_id,
                    remote_url: d.remote_watch_url,
                    video_profile: None,
                })
                .collect(),
        });
        app
    }

    #[test]
    fn review_uses_current_edits_not_fixture() {
        let mut app = demo_app();
        app.current.as_mut().unwrap().title = "Edited title".into();
        let (r, input_json) = app.build_review().expect("review builds");
        assert_eq!(r.title, "Edited title");
        assert_eq!(r.revision, 1);
        // The persisted input round-trips and rebuilds identically.
        let input: stream_manager_core::review::SnapshotInput =
            serde_json::from_str(&input_json).unwrap();
        assert_eq!(input.plan.title, "Edited title");
    }

    #[test]
    fn funnel_without_primary_url_blocks_dependents() {
        let mut app = demo_app();
        // Drop the primary URL: the Rumble funnel must block, not render a
        // placeholder or invented link.
        for d in &mut app.current.as_mut().unwrap().dests {
            d.remote_url = None;
        }
        let (r, _) = app.build_review().expect("review builds");
        let funnel = r
            .dests
            .iter()
            .find(|d| d.dest_id == "rumble-main")
            .expect("rumble present");
        assert!(funnel.blocked.is_some());
        assert!(!stream_manager_core::review::can_apply(&r));
    }

    #[test]
    fn invalid_profile_geometry_rejects_preview() {
        let mut p = EditedProfile {
            id: "m".into(),
            name: "M".into(),
            kind: ProfileKindEdit::Mask {
                x: "0.0".into(),
                y: "0.0".into(),
                w: "0.5".into(),
                h: "0.5".into(),
                opacity: "1.0".into(),
            },
            audio: AudioBehavior::Keep,
        };
        // x=0 violates the 2% safe margin.
        assert!(p.to_profile().is_none());
        p.set_field("m:X (0–1)", "0.05");
        p.set_field("m:Y (0–1)", "0.05");
        assert!(p.to_profile().is_some());
    }

    #[test]
    fn save_receipt_for_other_plan_leaves_editor_alone() {
        let mut app = demo_app();
        app.gen = 3;
        app.dirty = true;
        let rev_before = app.current.as_ref().unwrap().revision;
        let _ = app.update(Message::Saved(Ok(SaveReceipt {
            plan_id: "other-plan".into(),
            gen: 3,
            revision: 99,
        })));
        let cur = app.current.as_ref().unwrap();
        assert_eq!(cur.revision, rev_before);
        assert!(app.dirty, "foreign receipts must not clear dirty");
    }

    #[test]
    fn stale_save_receipt_converges_without_false_saved() {
        let mut app = demo_app();
        // Edits at gen 2 were saved; another keystroke landed at gen 3.
        app.gen = 3;
        app.dirty = true;
        let rev_before = app.current.as_ref().unwrap().revision;
        let _ = app.update(Message::Saved(Ok(SaveReceipt {
            plan_id: "demo-plan".into(),
            gen: 2,
            revision: rev_before + 1,
        })));
        // Must not report the stale revision as saved, must not clear
        // dirty — but must adopt the persisted revision so the follow-up
        // save's CAS targets it instead of failing stale.
        assert!(app.dirty);
        assert_eq!(app.current.as_ref().unwrap().revision, rev_before + 1);
        assert_eq!(app.status, "Saving…");
        assert!(app.saving);
    }

    #[test]
    fn stale_load_error_is_ignored_after_navigation() {
        let mut app = demo_app();
        app.db_epoch = 1;
        app.loading_plan = Some((1, "other".into()));
        let before = app.status.clone();
        let _ = app.update(Message::CurrentLoaded(Err(LoadError {
            epoch: 0,
            id: "demo-plan".into(),
            error: "disk gone".into(),
        })));
        assert_eq!(app.status, before, "stale failures stay silent");
    }

    #[test]
    fn concurrent_saves_serialize_behind_one_writer() {
        let mut app = demo_app();
        app.gen = 1;
        app.dirty = true;
        let first = app.update(Message::SaveNow);
        assert!(app.saving);
        drop(first);
        // A second save while one is in flight queues instead of racing.
        let second = app.update(Message::SaveNow);
        drop(second);
        assert!(app.save_queued);
        // Receipt for the in-flight save with no new edits: clean, the
        // queued flag consumed, no follow-up dispatched.
        let _ = app.update(Message::Saved(Ok(SaveReceipt {
            plan_id: "demo-plan".into(),
            gen: 1,
            revision: 2,
        })));
        assert!(!app.dirty);
        assert!(!app.saving);
        assert!(!app.save_queued);
    }

    #[test]
    fn create_queues_behind_dirty_save() {
        let mut app = demo_app();
        app.gen = 1;
        app.dirty = true;
        let _ = app.update(Message::CreatePlan);
        assert!(matches!(app.pending_create, Some(PendingCreate::New)));
        assert!(app.saving, "flush dispatched first");
        // The matching save runs the queued creation once clean.
        let _ = app.update(Message::Saved(Ok(SaveReceipt {
            plan_id: "demo-plan".into(),
            gen: 1,
            revision: 2,
        })));
        assert!(app.pending_create.is_none());
        assert!(!app.dirty);
    }

    #[test]
    fn apply_refuses_stale_review_after_edits() {
        let mut app = demo_app();
        app.gen = 2;
        app.dirty = false;
        app.review = Some(ActiveReview {
            review: stream_manager_core::review::ReviewSnapshot {
                plan_id: "demo-plan".into(),
                revision: 1,
                title: String::new(),
                demo: true,
                dests: vec![],
                policy: vec![],
                errors: vec![],
            },
            input_json: "{}".into(),
            gen: 1,
            epoch: 0,
        });
        app.edited();
        let _ = app.update(Message::ApplyAll);
        assert!(app.status.contains("stale"), "{}", app.status);
    }

    #[test]
    fn select_plan_queues_behind_dirty_save() {
        let mut app = demo_app();
        app.gen = 1;
        app.dirty = true;
        let _ = app.update(Message::SelectPlan("other".into()));
        assert!(matches!(
            app.pending_nav,
            Some(PendingNav::Select(ref id)) if id == "other"
        ));
        assert_eq!(app.current.as_ref().unwrap().id, "demo-plan");
        // Matching receipt clears dirty and dequeues the navigation.
        let _ = app.update(Message::Saved(Ok(SaveReceipt {
            plan_id: "demo-plan".into(),
            gen: 1,
            revision: 2,
        })));
        assert!(app.pending_nav.is_none());
        assert!(!app.dirty);
        assert_eq!(app.status, "Saved rev 2.");
    }

    #[test]
    fn failed_save_stops_navigation() {
        let mut app = demo_app();
        app.gen = 1;
        app.dirty = true;
        let _ = app.update(Message::SelectPlan("other".into()));
        let _ = app.update(Message::Saved(Err("disk gone".into())));
        assert!(app.pending_nav.is_some(), "target stays queued for retry");
        assert_eq!(app.current.as_ref().unwrap().id, "demo-plan");
        assert!(app.status.contains("Save error"));
    }

    #[test]
    fn tri_mode_cycles_without_losing_text() {
        let m = TriMode::Inherit;
        assert_eq!(m.cycle(), TriMode::Replace);
        assert_eq!(m.cycle().cycle(), TriMode::Clear);
        assert!(matches!(
            tri_from(TriMode::Replace, "t"),
            FieldOverride::Replace(_)
        ));
        assert!(matches!(
            tri_from(TriMode::Clear, "t"),
            FieldOverride::Clear
        ));
    }
}
