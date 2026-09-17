//! Local SQLite storage with versioned migrations.
//!
//! Holds drafts, destination/resource bindings and publish history.
//! Never stores secret material (see [`crate::credentials`]).

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

pub const SCHEMA_VERSION: u32 = 8;

const MIGRATION_001: &str = r#"
CREATE TABLE IF NOT EXISTS stream_plans (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  start_utc TEXT NOT NULL,
  end_utc TEXT,
  timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
  language TEXT NOT NULL DEFAULT 'en',
  category_intent TEXT NOT NULL DEFAULT '',
  category_id TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  destination_ids_json TEXT NOT NULL DEFAULT '[]',
  primary_id TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS destination_plans (
  plan_id TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'mirror',
  title_json TEXT NOT NULL DEFAULT '{"inherit":null}',
  description_json TEXT NOT NULL DEFAULT '{"inherit":null}',
  funnel_target TEXT,
  remote_broadcast_id TEXT,
  remote_watch_url TEXT,
  video_profile_id TEXT,
  PRIMARY KEY (plan_id, destination_id)
);
CREATE TABLE IF NOT EXISTS publish_operations (
  id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_revision INTEGER NOT NULL,
  created_utc TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS publish_actions (
  operation_id TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  account_id TEXT,
  kind TEXT NOT NULL,
  before_json TEXT NOT NULL DEFAULT '{}',
  after_json TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL DEFAULT 'pending',
  remote_id TEXT,
  error TEXT,
  PRIMARY KEY (operation_id, destination_id)
);
"#;

const MIGRATION_002: &str = r#"
CREATE TABLE IF NOT EXISTS platform_accounts (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  channel_name TEXT NOT NULL DEFAULT '',
  credential_ref TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_provider_channel
  ON platform_accounts (provider, channel_id);
CREATE TABLE IF NOT EXISTS destination_bindings (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  account_id TEXT REFERENCES platform_accounts (id),
  instantclone_name TEXT
);
CREATE TABLE IF NOT EXISTS video_profiles (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  profile_json TEXT NOT NULL DEFAULT '{}'
);
-- Added in v2 for databases created by the original v1 schema, whose
-- destination_plans table has no tags_json column.
ALTER TABLE destination_plans
  ADD COLUMN tags_json TEXT NOT NULL DEFAULT '{"inherit":null}';
"#;

const MIGRATION_003: &str = r#"
-- Manual-completion evidence, kept with the action it proves.
ALTER TABLE publish_actions ADD COLUMN completed_note TEXT;
ALTER TABLE publish_actions ADD COLUMN completed_utc TEXT;
"#;

const MIGRATION_004: &str = r#"
-- Chat workspace: explicit source selection and non-secret preferences.
-- Sources reuse destination bindings and platform accounts by id; no
-- secret material, tokens, or message bodies live here.
CREATE TABLE IF NOT EXISTS chat_sources (
  id TEXT PRIMARY KEY,
  binding_id TEXT NOT NULL,
  account_id TEXT,
  channel TEXT NOT NULL DEFAULT '',
  room_id TEXT,
  enabled INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS chat_prefs (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);
"#;

const MIGRATION_005: &str = r#"
-- Durable action receipts for the central server: dispatch markers plus
-- terminal outcomes. A marker without a terminal outcome means the
-- process died mid-action: recovery reports outcome-unknown, never a
-- silent retry. No secret material, only request ids and digests.
-- Superseded by MIGRATION_006 (client-scoped keys, stable digests);
-- existing rows migrate forward there.
CREATE TABLE IF NOT EXISTS chat_action_receipts (
  request_id TEXT PRIMARY KEY,
  digest INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'dispatched',
  intent_id TEXT,
  error TEXT,
  updated_utc TEXT NOT NULL DEFAULT ''
);
"#;

const MIGRATION_006: &str = r#"
-- Client-scoped receipt identity with stable digests. Request ids are
-- only unique per operator, so the key becomes (client_id,
-- request_id). Digests become SHA-256 hex over the canonical payload
-- (digest_v = 2); pre-existing rows keep their legacy u64 digest with
-- digest_v = 1 and retain map-hit duplicate semantics. Tombstone rows
-- (status = 'expired') record evicted terminal ids so their reuse is
-- rejected instead of silently treated as new.
CREATE TABLE IF NOT EXISTS chat_action_receipts_v2 (
  client_id TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL,
  digest TEXT NOT NULL DEFAULT '',
  digest_v INTEGER NOT NULL DEFAULT 2,
  status TEXT NOT NULL DEFAULT 'dispatched',
  intent_id TEXT,
  error TEXT,
  updated_utc TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (client_id, request_id)
);
-- Legacy rows have no trustworthy owner: the v5 key was the bare
-- request id, so no authenticated namespace can claim them. They all
-- become tombstones — reuse is rejected, never executed, never
-- reported as the old outcome.
INSERT OR IGNORE INTO chat_action_receipts_v2
  (client_id, request_id, digest, digest_v, status, intent_id, error, updated_utc)
  SELECT '', request_id, CAST(digest AS TEXT), 1, 'expired', NULL, NULL, updated_utc
  FROM chat_action_receipts;
DROP TABLE chat_action_receipts;
"#;

const MIGRATION_007: &str = r#"
-- Repair: databases migrated by the original v6 (which preserved legacy
-- statuses) may still hold ownerless client_id = '' rows outside
-- 'expired'. Every such row becomes a tombstone: no namespace can claim
-- it, so reuse is rejected, never executed.
UPDATE chat_action_receipts_v2
SET status = 'expired', intent_id = NULL, error = NULL, updated_utc = datetime('now')
WHERE client_id = '' AND status != 'expired';
"#;

const MIGRATION_008: &str = r#"
-- Restart-safe reconciliation context: every receipt remembers the
-- provider and channel the action was admitted against, so an
-- outcome-unknown row stays authorizable after the in-memory
-- coordinator is gone. Runs after MIGRATION_006 on fresh databases
-- too, so the columns always exist.
ALTER TABLE chat_action_receipts_v2 ADD COLUMN provider TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_action_receipts_v2 ADD COLUMN channel TEXT NOT NULL DEFAULT '';
"#;

/// Open (creating parents) and migrate the database.
pub fn open(path: &str) -> Result<Connection> {
    let expanded = shellexpand(path);
    if let Some(parent) = std::path::Path::new(&expanded).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).context("create db parent dir")?;
        }
    }
    let conn = Connection::open(&expanded).context("open sqlite")?;
    // Background tasks open their own connections; writers serialize in
    // SQLite instead of failing with SQLITE_BUSY.
    conn.busy_timeout(std::time::Duration::from_secs(30))
        .context("set busy timeout")?;
    migrate(&conn)?;
    Ok(conn)
}

/// Exclusive single-owner lease for the service database, held for the
/// whole process lifetime. Uses flock(LOCK_EX|LOCK_NB) on a persistent
/// anchor next to the database that is never unlinked: the kernel
/// releases the lock when the last descriptor closes, so a crash cannot
/// leave a stale lock behind. Acquire before migration, bootstrap, or
/// serving; drop only at shutdown.
pub struct ServiceLock {
    _file: std::fs::File,
    path: std::path::PathBuf,
}

impl ServiceLock {
    /// Anchor path for a database, for diagnostics.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

pub fn acquire_service_lock(db_path: &str) -> Result<ServiceLock> {
    use std::os::unix::io::AsRawFd as _;
    let expanded = shellexpand(db_path);
    let db = std::path::Path::new(&expanded);
    if let Some(parent) = db.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).context("create db parent dir")?;
        }
    }
    let mut anchor = db.to_path_buf();
    anchor.set_extension("lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&anchor)
        .with_context(|| format!("open lock anchor {}", anchor.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&anchor, std::fs::Permissions::from_mode(0o600));
    }
    // SAFETY: flock on a live descriptor we own; no memory unsafety.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::bail!(
                "database {} is locked by another live service instance",
                db.display()
            );
        }
        return Err(err).with_context(|| format!("lock {}", anchor.display()));
    }
    Ok(ServiceLock {
        _file: file,
        path: anchor,
    })
}

fn shellexpand(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_string()
}

pub fn migrate(conn: &Connection) -> Result<()> {
    // sqlite_master always exists, so this probes table presence without
    // swallowing corruption, permission or locking failures.
    let has_version_table: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .context("probe schema_version table")?;
    let current: u32 = if has_version_table {
        conn.query_row(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .context("read schema version")?
    } else {
        0
    };
    if current > SCHEMA_VERSION {
        anyhow::bail!(
            "database schema v{current} is newer than supported v{}",
            SCHEMA_VERSION
        );
    }
    let tx = conn.unchecked_transaction()?;
    if current < 1 {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);",
        )?;
        tx.execute_batch(MIGRATION_001)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (1)", [])?;
    }
    if current < 2 {
        tx.execute_batch(MIGRATION_002)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (2)", [])?;
    }
    if current < 3 {
        tx.execute_batch(MIGRATION_003)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (3)", [])?;
    }
    if current < 4 {
        tx.execute_batch(MIGRATION_004)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (4)", [])?;
    }
    if current < 5 {
        tx.execute_batch(MIGRATION_005)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (5)", [])?;
    }
    if current < 6 {
        tx.execute_batch(MIGRATION_006)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (6)", [])?;
    }
    if current < 7 {
        tx.execute_batch(MIGRATION_007)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (7)", [])?;
    }
    if current < 8 {
        tx.execute_batch(MIGRATION_008)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (8)", [])?;
    }
    tx.commit()?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PlanRow {
    pub id: String,
    pub title: String,
    pub status: String,
    pub revision: u64,
}

pub fn create_plan(conn: &Connection, id: &str, title: &str, start_utc: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO stream_plans (id, title, start_utc) VALUES (?1, ?2, ?3)",
        params![id, title, start_utc],
    )
    .context("insert plan")?;
    Ok(())
}

pub fn list_plans(conn: &Connection) -> Result<Vec<PlanRow>> {
    let mut stmt =
        conn.prepare("SELECT id, title, status, revision FROM stream_plans ORDER BY rowid DESC")?;
    let rows = stmt
        .query_map([], |r| {
            Ok(PlanRow {
                id: r.get(0)?,
                title: r.get(1)?,
                status: r.get(2)?,
                revision: r.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn set_plan_status(conn: &Connection, id: &str, status: &str) -> Result<()> {
    conn.execute(
        "UPDATE stream_plans SET status = ?1, revision = revision + 1 WHERE id = ?2",
        params![status, id],
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PlanFull {
    pub id: String,
    pub title: String,
    pub description: String,
    pub start_utc: String,
    pub end_utc: Option<String>,
    pub timezone: String,
    pub language: String,
    pub category_intent: String,
    pub category_id: Option<String>,
    pub tags_json: String,
    pub primary_id: Option<String>,
    pub status: String,
    pub revision: u64,
}

pub fn get_plan(conn: &Connection, id: &str) -> Result<Option<PlanFull>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, description, start_utc, end_utc, timezone, language,
                category_intent, category_id, tags_json, primary_id, status, revision
         FROM stream_plans WHERE id = ?1",
    )?;
    let mut rows = stmt.query_map(params![id], |r| {
        Ok(PlanFull {
            id: r.get(0)?,
            title: r.get(1)?,
            description: r.get(2)?,
            start_utc: r.get(3)?,
            end_utc: r.get(4)?,
            timezone: r.get(5)?,
            language: r.get(6)?,
            category_intent: r.get(7)?,
            category_id: r.get(8)?,
            tags_json: r.get(9)?,
            primary_id: r.get(10)?,
            status: r.get(11)?,
            revision: r.get::<_, i64>(12)? as u64,
        })
    })?;
    Ok(rows.next().transpose()?)
}

#[allow(clippy::too_many_arguments)]
pub fn update_plan_content(
    conn: &Connection,
    id: &str,
    title: &str,
    description: &str,
    start_utc: &str,
    end_utc: Option<&str>,
    timezone: &str,
    language: &str,
    category_intent: &str,
    tags_json: &str,
    primary_id: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE stream_plans SET title=?1, description=?2, start_utc=?3, end_utc=?4,
                timezone=?5, language=?6, category_intent=?7, tags_json=?8, primary_id=?9,
                revision = revision + 1 WHERE id = ?10",
        params![
            title,
            description,
            start_utc,
            end_utc,
            timezone,
            language,
            category_intent,
            tags_json,
            primary_id,
            id
        ],
    )?;
    Ok(())
}

/// Atomic plan save: content plus the complete destination set commit
/// together, guarded by an expected-revision compare-and-swap. A concurrent
/// writer advancing the revision first makes this save fail with a
/// `stale revision` error instead of silently overwriting it. Replacing the
/// full destination set also drops destinations the editor removed.
#[allow(clippy::too_many_arguments)]
pub fn save_plan_atomic(
    conn: &Connection,
    id: &str,
    title: &str,
    description: &str,
    start_utc: &str,
    end_utc: Option<&str>,
    timezone: &str,
    language: &str,
    category_intent: &str,
    tags_json: &str,
    primary_id: Option<&str>,
    expected_revision: u64,
    dests: &[crate::domain::DestinationPlan],
) -> Result<u64> {
    let tx = conn.unchecked_transaction()?;
    let changed = tx.execute(
        "UPDATE stream_plans SET title=?1, description=?2, start_utc=?3, end_utc=?4,
                timezone=?5, language=?6, category_intent=?7, tags_json=?8, primary_id=?9,
                revision = revision + 1 WHERE id = ?10 AND revision = ?11",
        params![
            title,
            description,
            start_utc,
            end_utc,
            timezone,
            language,
            category_intent,
            tags_json,
            primary_id,
            id,
            expected_revision as i64
        ],
    )?;
    if changed == 0 {
        anyhow::bail!("stale revision: plan {id} moved past revision {expected_revision}");
    }
    tx.execute(
        "DELETE FROM destination_plans WHERE plan_id = ?1",
        params![id],
    )?;
    for dest in dests {
        tx.execute(
            "INSERT INTO destination_plans
               (plan_id, destination_id, role, title_json, description_json, tags_json,
                funnel_target, remote_broadcast_id, remote_watch_url, video_profile_id)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                id,
                dest.destination_id,
                format!("{:?}", dest.role).to_lowercase(),
                serde_json::to_string(&dest.title)?,
                serde_json::to_string(&dest.description)?,
                serde_json::to_string(&dest.tags)?,
                dest.funnel_target,
                dest.remote_broadcast_id,
                dest.remote_watch_url,
                dest.video_profile_id,
            ],
        )?;
    }
    let revision: i64 = tx.query_row(
        "SELECT revision FROM stream_plans WHERE id = ?1",
        params![id],
        |r| r.get(0),
    )?;
    tx.commit()?;
    Ok(revision as u64)
}

/// Persisted revision of a plan, for execution admission: an apply must
/// prove the reviewed revision is still current before recording anything.
pub fn plan_revision(conn: &Connection, id: &str) -> Result<Option<u64>> {
    let mut stmt = conn.prepare("SELECT revision FROM stream_plans WHERE id = ?1")?;
    let mut rows = stmt.query_map(params![id], |r| r.get::<_, i64>(0))?;
    Ok(rows.next().transpose()?.map(|r| r as u64))
}

pub fn save_destination(
    conn: &Connection,
    plan_id: &str,
    dest: &crate::domain::DestinationPlan,
) -> Result<()> {
    conn.execute(
        "INSERT INTO destination_plans
           (plan_id, destination_id, role, title_json, description_json, tags_json,
            funnel_target, remote_broadcast_id, remote_watch_url, video_profile_id)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT (plan_id, destination_id) DO UPDATE SET
           role=excluded.role, title_json=excluded.title_json,
           description_json=excluded.description_json, tags_json=excluded.tags_json,
           funnel_target=excluded.funnel_target,
           remote_broadcast_id=excluded.remote_broadcast_id,
           remote_watch_url=excluded.remote_watch_url,
           video_profile_id=excluded.video_profile_id",
        params![
            plan_id,
            dest.destination_id,
            format!("{:?}", dest.role).to_lowercase(),
            serde_json::to_string(&dest.title)?,
            serde_json::to_string(&dest.description)?,
            serde_json::to_string(&dest.tags)?,
            dest.funnel_target,
            dest.remote_broadcast_id,
            dest.remote_watch_url,
            dest.video_profile_id,
        ],
    )?;
    Ok(())
}

pub fn load_destinations(
    conn: &Connection,
    plan_id: &str,
) -> Result<Vec<crate::domain::DestinationPlan>> {
    let mut stmt = conn.prepare(
        "SELECT destination_id, role, title_json, description_json, tags_json,
                funnel_target, remote_broadcast_id, remote_watch_url, video_profile_id
         FROM destination_plans WHERE plan_id = ?1 ORDER BY destination_id",
    )?;
    let rows = stmt.query_map(params![plan_id], |r| {
        let role_str: String = r.get(1)?;
        let role = match role_str.as_str() {
            "primary" => crate::domain::Role::Primary,
            "funnel" => crate::domain::Role::Funnel,
            _ => crate::domain::Role::Mirror,
        };
        let title: crate::domain::FieldOverride<String> =
            serde_json::from_str(&r.get::<_, String>(2)?).unwrap_or_default();
        let description: crate::domain::FieldOverride<String> =
            serde_json::from_str(&r.get::<_, String>(3)?).unwrap_or_default();
        let tags: crate::domain::FieldOverride<Vec<String>> =
            serde_json::from_str(&r.get::<_, String>(4)?).unwrap_or_default();
        Ok(crate::domain::DestinationPlan {
            destination_id: r.get(0)?,
            role,
            title,
            description,
            tags,
            funnel_target: r.get(5)?,
            remote_broadcast_id: r.get(6)?,
            remote_watch_url: r.get(7)?,
            video_profile_id: r.get(8)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("load destinations")
}

#[derive(Debug, Clone)]
pub struct AccountRow {
    pub id: String,
    pub provider: String,
    pub channel_id: String,
    pub channel_name: String,
    pub credential_ref: String,
}

pub fn save_account(conn: &Connection, a: &AccountRow) -> Result<()> {
    conn.execute(
        "INSERT INTO platform_accounts (id, provider, channel_id, channel_name, credential_ref)
         VALUES (?1,?2,?3,?4,?5)
         ON CONFLICT (id) DO UPDATE SET provider=excluded.provider,
           channel_id=excluded.channel_id, channel_name=excluded.channel_name,
           credential_ref=excluded.credential_ref",
        params![
            a.id,
            a.provider,
            a.channel_id,
            a.channel_name,
            a.credential_ref
        ],
    )?;
    Ok(())
}

pub fn list_accounts(conn: &Connection) -> Result<Vec<AccountRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, provider, channel_id, channel_name, credential_ref
         FROM platform_accounts ORDER BY provider, channel_id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(AccountRow {
            id: r.get(0)?,
            provider: r.get(1)?,
            channel_id: r.get(2)?,
            channel_name: r.get(3)?,
            credential_ref: r.get(4)?,
        })
    })?;
    let out: Vec<AccountRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct BindingRow {
    pub id: String,
    pub provider: String,
    pub account_id: Option<String>,
    pub instantclone_name: Option<String>,
}

pub fn save_binding(conn: &Connection, b: &BindingRow) -> Result<()> {
    conn.execute(
        "INSERT INTO destination_bindings (id, provider, account_id, instantclone_name)
         VALUES (?1,?2,?3,?4)
         ON CONFLICT (id) DO UPDATE SET provider=excluded.provider,
           account_id=excluded.account_id, instantclone_name=excluded.instantclone_name",
        params![b.id, b.provider, b.account_id, b.instantclone_name],
    )?;
    Ok(())
}

pub fn list_bindings(conn: &Connection) -> Result<Vec<BindingRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, provider, account_id, instantclone_name
         FROM destination_bindings ORDER BY id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(BindingRow {
            id: r.get(0)?,
            provider: r.get(1)?,
            account_id: r.get(2)?,
            instantclone_name: r.get(3)?,
        })
    })?;
    let out: Vec<BindingRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

pub fn save_video_profile(
    conn: &Connection,
    id: &str,
    name: &str,
    version: u32,
    profile_json: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO video_profiles (id, name, version, profile_json)
         VALUES (?1,?2,?3,?4)
         ON CONFLICT (id) DO UPDATE SET name=excluded.name,
           version=excluded.version, profile_json=excluded.profile_json",
        params![id, name, version as i64, profile_json],
    )?;
    Ok(())
}

pub fn list_video_profiles(conn: &Connection) -> Result<Vec<(String, String, String)>> {
    let mut stmt = conn.prepare("SELECT id, name, profile_json FROM video_profiles ORDER BY id")?;
    let rows = stmt.query_map([], |r| {
        let id: String = r.get(0)?;
        let name: String = r.get(1)?;
        let json: String = r.get(2)?;
        Ok((id, name, json))
    })?;
    let out: Vec<(String, String, String)> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

/// Explicit chat source row: resolves to one chat subscription. Several
/// destinations may share a source; dedupe happens in the chat coordinator.
#[derive(Debug, Clone)]
pub struct ChatSourceRow {
    pub id: String,
    pub binding_id: String,
    pub account_id: Option<String>,
    pub channel: String,
    pub room_id: Option<String>,
    pub enabled: bool,
}

pub fn save_chat_source(conn: &Connection, s: &ChatSourceRow) -> Result<()> {
    conn.execute(
        "INSERT INTO chat_sources (id, binding_id, account_id, channel, room_id, enabled)
         VALUES (?1,?2,?3,?4,?5,?6)
         ON CONFLICT (id) DO UPDATE SET binding_id=excluded.binding_id,
           account_id=excluded.account_id, channel=excluded.channel,
           room_id=excluded.room_id, enabled=excluded.enabled",
        params![
            s.id,
            s.binding_id,
            s.account_id,
            s.channel,
            s.room_id,
            i64::from(s.enabled)
        ],
    )?;
    Ok(())
}

pub fn list_chat_sources(conn: &Connection) -> Result<Vec<ChatSourceRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, binding_id, account_id, channel, room_id, enabled
         FROM chat_sources ORDER BY id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(ChatSourceRow {
            id: r.get(0)?,
            binding_id: r.get(1)?,
            account_id: r.get(2)?,
            channel: r.get(3)?,
            room_id: r.get(4)?,
            enabled: r.get::<_, i64>(5)? != 0,
        })
    })?;
    let out: Vec<ChatSourceRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

pub fn delete_account(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM platform_accounts WHERE id = ?1", params![id])?;
    Ok(())
}

/// Durable chat action receipt. `status` is one of `dispatched`
/// (reserved, no verdict yet), `accepted`, `rejected`, `unknown`, or
/// `expired` (evicted terminal id whose reuse must be rejected, never
/// treated as new). `digest_v` is 2 for SHA-256 hex digests, 1 for
/// legacy u64 digests carried forward by the v6 migration.
/// `provider`/`channel` record what the action was admitted against so
/// reconciliation stays authorizable after a restart (v8 columns).
#[derive(Debug, Clone)]
pub struct ChatReceiptRow {
    pub client_id: String,
    pub request_id: String,
    pub digest: String,
    pub digest_v: i64,
    pub status: String,
    pub intent_id: Option<String>,
    pub error: Option<String>,
    pub provider: String,
    pub channel: String,
    pub updated_utc: String,
}

fn row_from_mapped(r: &rusqlite::Row<'_>) -> rusqlite::Result<ChatReceiptRow> {
    Ok(ChatReceiptRow {
        client_id: r.get(0)?,
        request_id: r.get(1)?,
        digest: r.get(2)?,
        digest_v: r.get(3)?,
        status: r.get(4)?,
        intent_id: r.get(5)?,
        error: r.get(6)?,
        provider: r.get(7)?,
        channel: r.get(8)?,
        updated_utc: r.get(9)?,
    })
}

const RECEIPT_COLUMNS: &str = "client_id, request_id, digest, digest_v, status, intent_id, error, provider, channel, updated_utc";

pub fn save_chat_receipt(conn: &Connection, r: &ChatReceiptRow) -> Result<()> {
    conn.execute(
        "INSERT INTO chat_action_receipts_v2 (client_id, request_id, digest, digest_v, status, intent_id, error, provider, channel, updated_utc)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT (client_id, request_id) DO UPDATE SET digest=excluded.digest,
           digest_v=excluded.digest_v, status=excluded.status, intent_id=excluded.intent_id,
           error=excluded.error, provider=excluded.provider, channel=excluded.channel,
           updated_utc=excluded.updated_utc",
        params![
            r.client_id,
            r.request_id,
            r.digest,
            r.digest_v,
            r.status,
            r.intent_id,
            r.error,
            r.provider,
            r.channel,
            r.updated_utc
        ],
    )?;
    Ok(())
}

pub fn load_chat_receipt(
    conn: &Connection,
    client_id: &str,
    request_id: &str,
) -> Result<Option<ChatReceiptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {RECEIPT_COLUMNS}
         FROM chat_action_receipts_v2 WHERE client_id = ?1 AND request_id = ?2"
    ))?;
    let mut rows = stmt.query_map(params![client_id, request_id], row_from_mapped)?;
    Ok(rows.next().transpose()?)
}

/// Atomically attest an outcome-unknown action: the provider outcome
/// was checked out of band and is stored as evidence. One IMMEDIATE
/// transaction moves a still-`unknown` (or pre-restart `dispatched`)
/// row to its terminal verdict and returns it; decided or missing rows
/// refuse with their reason, so concurrent attestations serialize and
/// reconciliation can never rewrite a decided verdict. The database
/// moves first — callers publish the returned row to memory after.
pub enum ReconcileClaim {
    // Boxed: the row is wide and claims cross await points.
    Moved(Box<ChatReceiptRow>),
    Decided(Box<ChatReceiptRow>),
    Missing,
}

pub fn claim_reconcile(
    conn: &Connection,
    client_id: &str,
    request_id: &str,
    status: &str,
    note: &str,
    now_utc: &str,
) -> Result<ReconcileClaim> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<ReconcileClaim> {
        let mut stmt = conn.prepare(&format!(
            "SELECT {RECEIPT_COLUMNS} FROM chat_action_receipts_v2
             WHERE client_id = ?1 AND request_id = ?2"
        ))?;
        let mut rows = stmt.query_map(params![client_id, request_id], row_from_mapped)?;
        let Some(row) = rows.next().transpose()? else {
            return Ok(ReconcileClaim::Missing);
        };
        if row.status != "unknown" && row.status != "dispatched" {
            return Ok(ReconcileClaim::Decided(Box::new(row)));
        }
        conn.execute(
            "UPDATE chat_action_receipts_v2
                SET status = ?3, error = ?4, updated_utc = ?5
              WHERE client_id = ?1 AND request_id = ?2
                AND status IN ('unknown','dispatched')",
            params![client_id, request_id, status, note, now_utc],
        )?;
        let mut moved = row;
        moved.status = status.to_owned();
        moved.error = Some(note.to_owned());
        moved.updated_utc = now_utc.to_owned();
        Ok(ReconcileClaim::Moved(Box::new(moved)))
    })();
    match result {
        Ok(outcome) => {
            conn.execute_batch("COMMIT")?;
            Ok(outcome)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Claim one idempotent slot atomically: BEGIN IMMEDIATE takes the
/// write lock first, so two processes racing the same id serialize and
/// exactly one observes absence. Returns the pre-existing row, if any.
pub enum ReceiptClaim {
    Fresh,
    // Boxed: the row is wide and this enum crosses await points.
    Existing(Box<ChatReceiptRow>),
}

pub fn claim_chat_receipt(
    conn: &Connection,
    client_id: &str,
    request_id: &str,
    digest: &str,
    now_utc: &str,
) -> Result<ReceiptClaim> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<ReceiptClaim> {
        // Ownerless legacy rows match no authenticated namespace: any hit
        // here is non-executable regardless of its stored status.
        if load_chat_receipt(conn, "", request_id)?.is_some() {
            return Ok(ReceiptClaim::Existing(Box::new(ChatReceiptRow {
                client_id: String::new(),
                request_id: request_id.to_owned(),
                digest: String::new(),
                digest_v: 1,
                status: "expired".into(),
                intent_id: None,
                error: None,
                provider: String::new(),
                channel: String::new(),
                updated_utc: now_utc.to_owned(),
            })));
        }
        let existing = load_chat_receipt(conn, client_id, request_id)?;
        if let Some(row) = existing {
            return Ok(ReceiptClaim::Existing(Box::new(row)));
        }
        conn.execute(
            "INSERT INTO chat_action_receipts_v2
               (client_id, request_id, digest, digest_v, status, intent_id, error, provider, channel, updated_utc)
             VALUES (?1,?2,?3,2,'dispatched',NULL,NULL,'','',?4)",
            params![client_id, request_id, digest, now_utc],
        )?;
        Ok(ReceiptClaim::Fresh)
    })();
    match result {
        Ok(outcome) => {
            conn.execute_batch("COMMIT")?;
            Ok(outcome)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Rotate the receipts table in one transaction: terminal rows beyond
/// `keep_terminal` become tombstones (their reuse is rejected, never
/// treated as new). Tombstones are retained without a count cap — each
/// is one small row, and deleting one would turn its id executable
/// again. Request ids must be UUIDs so the table grows only with
/// genuinely new actions. Returns the tombstoned (client, request) ids
/// for the memory mirror.
pub fn rotate_chat_receipts(
    conn: &Connection,
    keep_terminal: u64,
) -> Result<Vec<(String, String)>> {
    let tx = conn.unchecked_transaction()?;
    // Only terminal verdicts rotate: `unknown` rows stay until explicit
    // reconciliation, and `dispatched` markers stay until they resolve.
    let mut victims_stmt = tx.prepare(
        "SELECT client_id, request_id FROM chat_action_receipts_v2
         WHERE status IN ('accepted','rejected')
         ORDER BY updated_utc DESC LIMIT -1 OFFSET ?1",
    )?;
    let victims: Vec<(String, String)> = victims_stmt
        .query_map(params![keep_terminal as i64], |r| {
            let client_id: String = r.get(0)?;
            let request_id: String = r.get(1)?;
            Ok((client_id, request_id))
        })
        .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
        .context("select rotation victims")?;
    drop(victims_stmt);
    for (client_id, request_id) in &victims {
        tx.execute(
            "INSERT INTO chat_action_receipts_v2
               (client_id, request_id, digest, digest_v, status, intent_id, error, provider, channel, updated_utc)
             VALUES (?1,?2,'',2,'expired',NULL,NULL,'','',datetime('now'))
             ON CONFLICT (client_id, request_id) DO UPDATE SET
               status='expired', intent_id=NULL, error=NULL, updated_utc=datetime('now')",
            params![client_id, request_id],
        )?;
    }
    tx.commit()?;
    Ok(victims)
}

pub fn load_chat_receipts(conn: &Connection) -> Result<Vec<ChatReceiptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {RECEIPT_COLUMNS} FROM chat_action_receipts_v2 ORDER BY updated_utc"
    ))?;
    let rows = stmt.query_map([], row_from_mapped)?;
    let out: Vec<ChatReceiptRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

/// Nonsecret key/value preferences (bootstrap markers, UI state).
pub fn get_pref(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT value FROM chat_prefs WHERE key = ?1")?;
    let mut rows = stmt.query_map(params![key], |r| r.get::<_, String>(0))?;
    Ok(rows.next().transpose()?)
}

pub fn set_pref(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO chat_prefs (key, value) VALUES (?1,?2)
         ON CONFLICT (key) DO UPDATE SET value=excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Bootstrap marker: set once the seed accounts have been written, so an
/// intentionally emptied account table is never reseeded on restart.
pub const ACCOUNTS_BOOTSTRAPPED: &str = "accounts_bootstrapped";

/// Write seed accounts and the bootstrap marker in one transaction.
/// Returns true when seeding happened. A second process racing startup
/// either wins (we then see the marker) or loses its inserts harmlessly
/// to the marker check — either way the table is seeded at most once
/// with operator-supplied rows.
pub fn seed_accounts_once(conn: &Connection, rows: &[AccountRow]) -> Result<bool> {
    let tx = conn.unchecked_transaction()?;
    let marked: bool = tx
        .query_row(
            "SELECT COUNT(*) FROM chat_prefs WHERE key = ?1",
            params![ACCOUNTS_BOOTSTRAPPED],
            |r| r.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if marked {
        return Ok(false);
    }
    let existing: i64 = tx.query_row("SELECT COUNT(*) FROM platform_accounts", [], |r| r.get(0))?;
    if existing == 0 {
        for a in rows {
            tx.execute(
                "INSERT INTO platform_accounts (id, provider, channel_id, channel_name, credential_ref)
                 VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT (id) DO NOTHING",
                params![a.id, a.provider, a.channel_id, a.channel_name, a.credential_ref],
            )?;
        }
    }
    tx.execute(
        "INSERT INTO chat_prefs (key, value) VALUES (?1,'1')
         ON CONFLICT (key) DO UPDATE SET value='1'",
        params![ACCOUNTS_BOOTSTRAPPED],
    )?;
    tx.commit()?;
    Ok(true)
}

/// Persist operation intent + actions BEFORE any network mutation.
pub fn record_operation(
    conn: &Connection,
    op_id: &str,
    plan_id: &str,
    plan_revision: u64,
    snapshot_json: &str,
    actions: &[crate::publish::PublishActionIntent],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "INSERT INTO publish_operations (id, plan_id, plan_revision, created_utc, snapshot_json, state)
         VALUES (?1,?2,?3,?4,?5,'pending')",
        params![
            op_id,
            plan_id,
            plan_revision as i64,
            chrono::Utc::now().to_rfc3339(),
            snapshot_json
        ],
    )?;
    for a in actions {
        tx.execute(
            "INSERT INTO publish_actions
               (operation_id, destination_id, account_id, kind, before_json, after_json, state)
             VALUES (?1,?2,?3,?4,?5,?6,'pending')",
            params![
                op_id,
                a.destination_id,
                a.account_id,
                a.kind,
                a.before.to_string(),
                a.after.to_string()
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn update_action_state(
    conn: &Connection,
    operation_id: &str,
    destination_id: &str,
    state: &str,
    error: Option<&str>,
    remote_id: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE publish_actions SET state=?1, error=?2, remote_id=?3
         WHERE operation_id=?4 AND destination_id=?5",
        params![state, error, remote_id, operation_id, destination_id],
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ActionRow {
    pub operation_id: String,
    pub destination_id: String,
    pub account_id: Option<String>,
    pub kind: String,
    pub before_json: String,
    pub after_json: String,
    pub state: String,
    pub remote_id: Option<String>,
    pub error: Option<String>,
    pub completed_note: Option<String>,
    pub completed_utc: Option<String>,
}

pub fn list_actions(conn: &Connection, operation_id: &str) -> Result<Vec<ActionRow>> {
    let mut stmt = conn.prepare(
        "SELECT operation_id, destination_id, account_id, kind, before_json,
                after_json, state, remote_id, error, completed_note, completed_utc
         FROM publish_actions WHERE operation_id = ?1 ORDER BY destination_id",
    )?;
    let rows = stmt.query_map(params![operation_id], |r| {
        Ok(ActionRow {
            operation_id: r.get(0)?,
            destination_id: r.get(1)?,
            account_id: r.get(2)?,
            kind: r.get(3)?,
            before_json: r.get(4)?,
            after_json: r.get(5)?,
            state: r.get(6)?,
            remote_id: r.get(7)?,
            error: r.get(8)?,
            completed_note: r.get(9)?,
            completed_utc: r.get(10)?,
        })
    })?;
    let out: Vec<ActionRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

pub fn get_action(
    conn: &Connection,
    operation_id: &str,
    destination_id: &str,
) -> Result<Option<ActionRow>> {
    let mut stmt = conn.prepare(
        "SELECT operation_id, destination_id, account_id, kind, before_json,
                after_json, state, remote_id, error, completed_note, completed_utc
         FROM publish_actions WHERE operation_id = ?1 AND destination_id = ?2",
    )?;
    let mut rows = stmt.query_map(params![operation_id, destination_id], |r| {
        Ok(ActionRow {
            operation_id: r.get(0)?,
            destination_id: r.get(1)?,
            account_id: r.get(2)?,
            kind: r.get(3)?,
            before_json: r.get(4)?,
            after_json: r.get(5)?,
            state: r.get(6)?,
            remote_id: r.get(7)?,
            error: r.get(8)?,
            completed_note: r.get(9)?,
            completed_utc: r.get(10)?,
        })
    })?;
    Ok(rows.next().transpose()?)
}

pub fn get_operation(conn: &Connection, operation_id: &str) -> Result<Option<OperationRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, plan_revision, state FROM publish_operations WHERE id = ?1",
    )?;
    let mut rows = stmt.query_map(params![operation_id], |r| {
        Ok(OperationRow {
            id: r.get(0)?,
            plan_id: r.get(1)?,
            plan_revision: r.get::<_, i64>(2)? as u64,
            state: r.get(3)?,
        })
    })?;
    Ok(rows.next().transpose()?)
}

/// Record operator completion of a manual action, with evidence.
///
/// Eligibility is enforced here, not in the UI: the action must exist,
/// be a manual action, and currently require manual work. The note and
/// timestamp persist with the action they prove.
pub fn record_manual_completion(
    conn: &Connection,
    operation_id: &str,
    destination_id: &str,
    public_url: &str,
    note: &str,
    completed_utc: &str,
) -> Result<()> {
    let action = get_action(conn, operation_id, destination_id)?
        .ok_or_else(|| anyhow::anyhow!("no such action"))?;
    if action.kind != "manual" {
        anyhow::bail!("action is not a manual action");
    }
    if action.state != "manual_required" {
        anyhow::bail!(
            "action is not awaiting manual work (state: {})",
            action.state
        );
    }
    conn.execute(
        "UPDATE publish_actions
         SET state='manually_completed', error=NULL, remote_id=?1,
             completed_note=?2, completed_utc=?3
         WHERE operation_id=?4 AND destination_id=?5",
        params![
            public_url,
            note,
            completed_utc,
            operation_id,
            destination_id
        ],
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct OperationRow {
    pub id: String,
    pub plan_id: String,
    pub plan_revision: u64,
    pub state: String,
}

pub fn list_operations(conn: &Connection) -> Result<Vec<OperationRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, plan_revision, state FROM publish_operations ORDER BY rowid DESC",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(OperationRow {
            id: r.get(0)?,
            plan_id: r.get(1)?,
            plan_revision: r.get::<_, i64>(2)? as u64,
            state: r.get(3)?,
        })
    })?;
    let out: Vec<OperationRow> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrates_and_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        let plans = list_plans(&conn).unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].title, "Show");
        set_plan_status(&conn, "p1", "archived").unwrap();
        let plans = list_plans(&conn).unwrap();
        assert_eq!(plans[0].status, "archived");
        assert_eq!(plans[0].revision, 2);
    }

    #[test]
    fn rejects_duplicate_plan_id() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        assert!(create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").is_err());
    }

    #[test]
    fn roundtrips_full_plan_and_destinations() {
        use crate::domain::{DestinationPlan, FieldOverride, Role};
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        update_plan_content(
            &conn,
            "p1",
            "Show!",
            "Desc",
            "2026-01-01T18:00:00Z",
            None,
            "Europe/Berlin",
            "en",
            "Tech",
            "[\"live\"]",
            Some("yt"),
        )
        .unwrap();
        let full = get_plan(&conn, "p1").unwrap().unwrap();
        assert_eq!(full.title, "Show!");
        assert_eq!(full.revision, 2);
        assert_eq!(full.primary_id.as_deref(), Some("yt"));
        let mut d = DestinationPlan::new("rumble", Role::Funnel);
        d.funnel_target = Some("yt".into());
        d.description = FieldOverride::Replace("Watch on {{primary_platform}}".into());
        save_destination(&conn, "p1", &d).unwrap();
        let loaded = load_destinations(&conn, "p1").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].role, Role::Funnel);
        assert!(matches!(loaded[0].description, FieldOverride::Replace(_)));
    }

    #[test]
    fn stores_accounts_bindings_and_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        save_account(
            &conn,
            &AccountRow {
                id: "a1".into(),
                provider: "youtube".into(),
                channel_id: "UC1".into(),
                channel_name: "Dev".into(),
                credential_ref: "/run/agenix/yt".into(),
            },
        )
        .unwrap();
        save_account(
            &conn,
            &AccountRow {
                id: "a2".into(),
                provider: "youtube".into(),
                channel_id: "UC2".into(),
                channel_name: "Live".into(),
                credential_ref: "".into(),
            },
        )
        .unwrap();
        assert_eq!(list_accounts(&conn).unwrap().len(), 2);
        save_binding(
            &conn,
            &BindingRow {
                id: "youtube-main".into(),
                provider: "youtube".into(),
                account_id: Some("a1".into()),
                instantclone_name: Some("youtube-main".into()),
            },
        )
        .unwrap();
        assert_eq!(list_bindings(&conn).unwrap().len(), 1);
        save_video_profile(&conn, "cta", "CTA", 1, "{}").unwrap();
        assert_eq!(list_video_profiles(&conn).unwrap().len(), 1);
    }

    #[test]
    fn rejects_newer_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        conn.execute("INSERT INTO schema_version (version) VALUES (999)", [])
            .unwrap();
        assert!(migrate(&conn).is_err());
    }

    /// A database created by the original v1 schema (no tags_json, no v2
    /// tables) must upgrade to the current schema with data intact.
    #[test]
    fn upgrades_original_v1_database() {
        use rusqlite::Connection as RawConn;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        {
            let raw = RawConn::open(&db).unwrap();
            raw.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version (version) VALUES (1);
                 CREATE TABLE stream_plans (
                   id TEXT PRIMARY KEY, title TEXT NOT NULL,
                   description TEXT NOT NULL DEFAULT '', start_utc TEXT NOT NULL,
                   end_utc TEXT, timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
                   language TEXT NOT NULL DEFAULT 'en',
                   category_intent TEXT NOT NULL DEFAULT '',
                   category_id TEXT, tags_json TEXT NOT NULL DEFAULT '[]',
                   destination_ids_json TEXT NOT NULL DEFAULT '[]',
                   primary_id TEXT, status TEXT NOT NULL DEFAULT 'draft',
                   revision INTEGER NOT NULL DEFAULT 1);
                 INSERT INTO stream_plans (id, title, start_utc)
                   VALUES ('old', 'Old show', '2026-01-01T18:00:00Z');
                 CREATE TABLE destination_plans (
                   plan_id TEXT NOT NULL, destination_id TEXT NOT NULL,
                   role TEXT NOT NULL DEFAULT 'mirror',
                   title_json TEXT NOT NULL DEFAULT 'x',
                   description_json TEXT NOT NULL DEFAULT 'x',
                   funnel_target TEXT, remote_broadcast_id TEXT,
                   remote_watch_url TEXT, video_profile_id TEXT,
                   PRIMARY KEY (plan_id, destination_id));
                 INSERT INTO destination_plans (plan_id, destination_id, role)
                   VALUES ('old', 'yt', 'primary');
                 CREATE TABLE publish_operations (
                   id TEXT PRIMARY KEY, plan_id TEXT NOT NULL,
                   plan_revision INTEGER NOT NULL, created_utc TEXT NOT NULL,
                   snapshot_json TEXT NOT NULL,
                   state TEXT NOT NULL DEFAULT 'pending');
                 CREATE TABLE publish_actions (
                   operation_id TEXT NOT NULL, destination_id TEXT NOT NULL,
                   account_id TEXT, kind TEXT NOT NULL,
                   before_json TEXT NOT NULL DEFAULT '{}',
                   after_json TEXT NOT NULL DEFAULT '{}',
                   state TEXT NOT NULL DEFAULT 'pending',
                   remote_id TEXT, error TEXT,
                   PRIMARY KEY (operation_id, destination_id));",
            )
            .unwrap();
        }
        let conn = open(db.to_str().unwrap()).unwrap();
        // Old plan and destination survive the upgrade.
        let plans = list_plans(&conn).unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].title, "Old show");
        let dests = load_destinations(&conn, "old").unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].role, crate::domain::Role::Primary);
        // v2 tables exist and accept writes.
        save_binding(
            &conn,
            &BindingRow {
                id: "yt".into(),
                provider: "youtube".into(),
                account_id: None,
                instantclone_name: None,
            },
        )
        .unwrap();
        // v3 evidence columns exist.
        conn.execute(
            "UPDATE publish_actions SET completed_note='x' WHERE 1=0",
            [],
        )
        .unwrap();
    }

    #[test]
    fn concurrent_writers_serialize_without_busy_errors() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db").to_string_lossy().into_owned();
        {
            let conn = open(&db).unwrap();
            create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        }
        let errors: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let dbc = db.clone();
                    s.spawn(move || {
                        let conn = open(&dbc).unwrap();
                        for _ in 0..25 {
                            set_plan_status(&conn, "p1", &format!("s{i}"))?;
                        }
                        anyhow::Ok(())
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().ok()?.err())
                .map(|e| format!("{e:#}"))
                .collect()
        });
        assert!(errors.is_empty(), "{errors:?}");
    }

    #[test]
    fn manual_completion_requires_eligible_action() {
        use crate::publish::PublishActionIntent;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let conn = open(db.to_str().unwrap()).unwrap();
        let mk = |dest: &str, kind: &str| PublishActionIntent {
            destination_id: dest.into(),
            account_id: String::new(),
            kind: kind.into(),
            before: serde_json::json!({}),
            after: serde_json::json!({}),
            remote_id: None,
            state: crate::publish::ActionState::Pending,
            error: None,
        };
        record_operation(
            &conn,
            "op1",
            "p1",
            1,
            "{}",
            &[mk("rb", "manual"), mk("yt", "update")],
        )
        .unwrap();
        // Non-manual action can never complete manually.
        update_action_state(&conn, "op1", "yt", "manual_required", None, None).unwrap();
        assert!(record_manual_completion(
            &conn,
            "op1",
            "yt",
            "https://rumble.com/v-x",
            "note",
            "2026-01-01T00:00:00Z",
        )
        .is_err());
        // Manual action in the wrong state is refused.
        assert!(record_manual_completion(
            &conn,
            "op1",
            "rb",
            "https://rumble.com/v-x",
            "note",
            "2026-01-01T00:00:00Z",
        )
        .is_err());
        update_action_state(&conn, "op1", "rb", "manual_required", None, None).unwrap();
        record_manual_completion(
            &conn,
            "op1",
            "rb",
            "https://rumble.com/v-x",
            "pasted from dashboard",
            "2026-01-01T00:00:00Z",
        )
        .unwrap();
        let a = get_action(&conn, "op1", "rb").unwrap().unwrap();
        assert_eq!(a.state, "manually_completed");
        assert_eq!(a.remote_id.as_deref(), Some("https://rumble.com/v-x"));
        assert_eq!(a.completed_note.as_deref(), Some("pasted from dashboard"));
        assert_eq!(a.completed_utc.as_deref(), Some("2026-01-01T00:00:00Z"));
        // No double completion.
        assert!(record_manual_completion(
            &conn,
            "op1",
            "rb",
            "https://rumble.com/v-y",
            "again",
            "2026-01-01T00:00:01Z",
        )
        .is_err());
    }

    /// Fixed save arguments for the atomic-save tests. The tuple shape is
    /// test-only plumbing; the lint below is a deliberate local exception.
    #[allow(clippy::type_complexity)]
    fn atomic_save_args(
        rev: u64,
    ) -> (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        Option<&'static str>,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        Option<&'static str>,
        u64,
    ) {
        (
            "p1",
            "Show",
            "desc",
            "2026-01-01T18:00:00Z",
            None,
            "Europe/Berlin",
            "en",
            "",
            "[]",
            None,
            rev,
        )
    }

    #[test]
    fn atomic_save_bumps_once_and_replaces_destinations() {
        use crate::domain::{DestinationPlan, Role};
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        let (a, b, c, d, e, f, g, h, i, j, rev) = atomic_save_args(1);
        let rev2 = save_plan_atomic(
            &conn,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i,
            j,
            rev,
            &[DestinationPlan::new("yt", Role::Primary)],
        )
        .unwrap();
        assert_eq!(rev2, 2);
        assert_eq!(load_destinations(&conn, "p1").unwrap().len(), 1);
        // Second save replaces the set instead of accumulating orphans.
        let (a, b, c, d, e, f, g, h, i, j, _) = atomic_save_args(0);
        let rev3 = save_plan_atomic(
            &conn,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i,
            j,
            rev2,
            &[DestinationPlan::new("tw", Role::Mirror)],
        )
        .unwrap();
        assert_eq!(rev3, 3);
        let dests = load_destinations(&conn, "p1").unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].destination_id, "tw");
    }

    #[test]
    fn atomic_save_rejects_stale_revision() {
        use crate::domain::{DestinationPlan, Role};
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        create_plan(&conn, "p1", "Show", "2026-01-01T18:00:00Z").unwrap();
        let (a, b, c, d, e, f, g, h, i, j, _) = atomic_save_args(0);
        // Revision 1 expected, but the row is still at 1 only once;
        // first save moves to 2, the replayed save with rev 1 must fail.
        save_plan_atomic(&conn, a, b, c, d, e, f, g, h, i, j, 1, &[]).unwrap();
        let err = save_plan_atomic(
            &conn,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i,
            j,
            1,
            &[DestinationPlan::new("yt", Role::Primary)],
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("stale revision"));
        // Failed CAS left destinations untouched.
        assert!(load_destinations(&conn, "p1").unwrap().is_empty());
        assert_eq!(plan_revision(&conn, "p1").unwrap(), Some(2));
    }

    /// Build a database with the *original* v1 shape (no tags_json on
    /// destination_plans, no v2/v3 tables) and prove open() migrates it
    /// forward with data intact. Old migrations are never edited; the
    /// current code must tolerate real history.
    fn legacy_db(dir: &tempfile::TempDir, version: u32, with_tags: bool) -> String {
        let path = dir.path().join("legacy.db");
        let s = path.to_str().unwrap().to_owned();
        {
            let conn = rusqlite::Connection::open(&s).unwrap();
            conn.execute_batch("CREATE TABLE schema_version (version INTEGER PRIMARY KEY);")
                .unwrap();
            let tags_col = if with_tags {
                ", tags_json TEXT NOT NULL DEFAULT '{\"inherit\":null}'"
            } else {
                ""
            };
            conn.execute_batch(&format!(
                "CREATE TABLE stream_plans (
                   id TEXT PRIMARY KEY, title TEXT NOT NULL,
                   description TEXT NOT NULL DEFAULT '', start_utc TEXT NOT NULL,
                   end_utc TEXT, timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
                   language TEXT NOT NULL DEFAULT 'en',
                   category_intent TEXT NOT NULL DEFAULT '', category_id TEXT,
                   tags_json TEXT NOT NULL DEFAULT '[]',
                   destination_ids_json TEXT NOT NULL DEFAULT '[]', primary_id TEXT,
                   status TEXT NOT NULL DEFAULT 'draft',
                   revision INTEGER NOT NULL DEFAULT 1);
                 CREATE TABLE destination_plans (
                   plan_id TEXT NOT NULL, destination_id TEXT NOT NULL,
                   role TEXT NOT NULL DEFAULT 'mirror',
                   title_json TEXT NOT NULL DEFAULT '{{\"inherit\":null}}',
                   description_json TEXT NOT NULL DEFAULT '{{\"inherit\":null}}'
                   {tags_col},
                   funnel_target TEXT, remote_broadcast_id TEXT,
                   remote_watch_url TEXT, video_profile_id TEXT,
                   PRIMARY KEY (plan_id, destination_id));
                 CREATE TABLE publish_operations (
                   id TEXT PRIMARY KEY, plan_id TEXT NOT NULL,
                   plan_revision INTEGER NOT NULL, created_utc TEXT NOT NULL,
                   snapshot_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending');
                 CREATE TABLE publish_actions (
                   operation_id TEXT NOT NULL, destination_id TEXT NOT NULL,
                   account_id TEXT, kind TEXT NOT NULL,
                   before_json TEXT NOT NULL DEFAULT '{{}}',
                   after_json TEXT NOT NULL DEFAULT '{{}}',
                   state TEXT NOT NULL DEFAULT 'pending', remote_id TEXT, error TEXT,
                   PRIMARY KEY (operation_id, destination_id));"
            ))
            .unwrap();
            if version >= 2 {
                conn.execute_batch(
                    "CREATE TABLE platform_accounts (
                       id TEXT PRIMARY KEY, provider TEXT NOT NULL,
                       channel_id TEXT NOT NULL,
                       channel_name TEXT NOT NULL DEFAULT '',
                       credential_ref TEXT NOT NULL DEFAULT '');
                     CREATE TABLE destination_bindings (
                       id TEXT PRIMARY KEY, provider TEXT NOT NULL,
                       account_id TEXT, instantclone_name TEXT);
                     CREATE TABLE video_profiles (
                       id TEXT PRIMARY KEY, name TEXT NOT NULL,
                       version INTEGER NOT NULL DEFAULT 1,
                       profile_json TEXT NOT NULL DEFAULT '{}');",
                )
                .unwrap();
            }
            if version == 3 {
                conn.execute_batch(
                    "ALTER TABLE publish_actions ADD COLUMN completed_note TEXT;
                     ALTER TABLE publish_actions ADD COLUMN completed_utc TEXT;",
                )
                .unwrap();
            }
            for v in 1..=version {
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    rusqlite::params![v],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO stream_plans (id, title, start_utc, revision)
                 VALUES ('p1', 'Legacy', '2026-01-01T18:00:00Z', 7)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO destination_plans (plan_id, destination_id, role)
                 VALUES ('p1', 'yt', 'primary')",
                [],
            )
            .unwrap();
        }
        s
    }

    #[test]
    fn legacy_variants_migrate_forward_with_data_intact() {
        for (version, with_tags) in [(1, false), (2, true), (3, true)] {
            let dir = tempfile::tempdir().unwrap();
            let s = legacy_db(&dir, version, with_tags);
            let conn = open(&s).unwrap();
            let plan = get_plan(&conn, "p1").unwrap().unwrap();
            assert_eq!(plan.title, "Legacy");
            assert_eq!(plan.revision, 7, "v{version} history must survive");
            assert_eq!(load_destinations(&conn, "p1").unwrap().len(), 1);
            // v4 tables exist and chat sources round-trip.
            save_chat_source(
                &conn,
                &ChatSourceRow {
                    id: "cs1".into(),
                    binding_id: "yt".into(),
                    account_id: None,
                    channel: "chan".into(),
                    room_id: None,
                    enabled: true,
                },
            )
            .unwrap();
            assert_eq!(list_chat_sources(&conn).unwrap().len(), 1);
        }
    }

    #[test]
    fn chat_receipts_roundtrip_and_prune() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        let row = |client: &str, id: &str, status: &str| ChatReceiptRow {
            client_id: client.into(),
            request_id: id.into(),
            digest: "abc123".into(),
            digest_v: 2,
            status: status.into(),
            intent_id: None,
            error: None,
            provider: String::new(),
            channel: String::new(),
            updated_utc: format!("2026-09-16T00:00:{id}Z"),
        };
        save_chat_receipt(&conn, &row("can", "a", "dispatched")).unwrap();
        save_chat_receipt(&conn, &row("can", "b", "accepted")).unwrap();
        // Same request id under another client is a distinct row.
        save_chat_receipt(&conn, &row("dejana", "a", "accepted")).unwrap();
        // Upsert overwrites the same (client, request) id.
        save_chat_receipt(&conn, &row("can", "a", "accepted")).unwrap();
        let rows = load_chat_receipts(&conn).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|r| r.client_id == "can"
            && r.request_id == "a"
            && r.status == "accepted"
            && r.digest_v == 2));
        // Rotation tombstones evicted terminal rows instead of deleting
        // them: the ids stay rejected, never executable again.
        let victims = rotate_chat_receipts(&conn, 2).unwrap();
        assert_eq!(victims.len(), 1);
        let rows = load_chat_receipts(&conn).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|r| r.status == "expired"));
    }

    #[test]
    fn legacy_receipts_migrate_to_v6() {
        // A v5 database carries the old single-key table; opening it must
        // migrate rows forward as non-executable tombstones: the bare ids
        // have no trustworthy owner, so reuse is rejected, never replayed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db").to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version (version) VALUES (5);
                 CREATE TABLE chat_action_receipts (
                   request_id TEXT PRIMARY KEY,
                   digest INTEGER NOT NULL DEFAULT 0,
                   status TEXT NOT NULL DEFAULT 'dispatched',
                   intent_id TEXT, error TEXT,
                   updated_utc TEXT NOT NULL DEFAULT ''
                 );
                 CREATE TABLE chat_prefs (
                   key TEXT PRIMARY KEY,
                   value TEXT NOT NULL DEFAULT ''
                 );
                 INSERT INTO chat_action_receipts
                   (request_id, digest, status, updated_utc)
                 VALUES ('old-1', 12345, 'accepted', '2026-09-16T00:00:00Z');",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let rows = load_chat_receipts(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].request_id, "old-1");
        assert_eq!(rows[0].client_id, "");
        assert_eq!(rows[0].status, "expired");
        assert!(get_pref(&conn, ACCOUNTS_BOOTSTRAPPED).unwrap().is_none());
        set_pref(&conn, ACCOUNTS_BOOTSTRAPPED, "1").unwrap();
        assert_eq!(
            get_pref(&conn, ACCOUNTS_BOOTSTRAPPED).unwrap().as_deref(),
            Some("1")
        );
    }

    #[test]
    fn concurrent_claims_serialize_to_one_fresh() {
        use std::sync::{Arc, Barrier};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db").to_string_lossy().into_owned();
        open(&path).unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let fresh = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                let fresh = fresh.clone();
                std::thread::spawn(move || {
                    // Separate connections racing one id, as independent
                    // processes would: BEGIN IMMEDIATE serializes them.
                    let conn = open(&path).unwrap();
                    barrier.wait();
                    if let ReceiptClaim::Fresh =
                        claim_chat_receipt(&conn, "can", "race-1", "dd", "2026-09-16T00:00:00Z")
                            .unwrap()
                    {
                        fresh.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(fresh.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn claim_is_atomic_and_rotation_tombstones() {
        use std::collections::HashSet;
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        // First claim wins; the second observes the marker.
        assert!(matches!(
            claim_chat_receipt(&conn, "can", "r1", "aa", "2026-09-16T00:00:00Z").unwrap(),
            ReceiptClaim::Fresh
        ));
        assert!(matches!(
            claim_chat_receipt(&conn, "can", "r1", "aa", "2026-09-16T00:00:01Z").unwrap(),
            ReceiptClaim::Existing(_)
        ));
        // Rotation converts old terminal rows to tombstones and reports
        // them; tombstones survive rotation.
        for i in 0..5 {
            let id = format!("old-{i}");
            save_chat_receipt(
                &conn,
                &ChatReceiptRow {
                    client_id: "can".into(),
                    request_id: id,
                    digest: "bb".into(),
                    digest_v: 2,
                    status: "accepted".into(),
                    intent_id: None,
                    error: None,
                    provider: String::new(),
                    channel: String::new(),
                    updated_utc: format!("2026-09-01T00:00:0{i}Z"),
                },
            )
            .unwrap();
        }
        let victims = rotate_chat_receipts(&conn, 2).unwrap();
        assert_eq!(victims.len(), 3);
        let victims_set: HashSet<(String, String)> = victims.into_iter().collect();
        assert!(victims_set.contains(&("can".to_string(), "old-0".to_string())));
        // The tombstoned rows refuse in the claim path.
        assert!(matches!(
            claim_chat_receipt(&conn, "can", "old-0", "bb", "2026-09-16T00:00:02Z").unwrap(),
            ReceiptClaim::Existing(_)
        ));
        let rows = load_chat_receipts(&conn).unwrap();
        assert!(rows
            .iter()
            .any(|r| r.request_id == "old-0" && r.status == "expired"));
    }

    #[test]
    fn v8_adds_receipt_context_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v7.db").to_string_lossy().into_owned();
        // A v7 database: receipts table without the context columns.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version (version) VALUES (7);
                 CREATE TABLE chat_action_receipts_v2 (
                   client_id TEXT NOT NULL DEFAULT '',
                   request_id TEXT NOT NULL,
                   digest TEXT NOT NULL DEFAULT '',
                   digest_v INTEGER NOT NULL DEFAULT 2,
                   status TEXT NOT NULL DEFAULT 'dispatched',
                   intent_id TEXT,
                   error TEXT,
                   updated_utc TEXT NOT NULL DEFAULT '',
                   PRIMARY KEY (client_id, request_id)
                 );
                 INSERT INTO chat_action_receipts_v2
                   (client_id, request_id, digest, status, updated_utc)
                 VALUES ('can', 'u1', 'bb', 'unknown', '2026-09-16T00:00:00Z');",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        // Migration adds the columns; old rows read back with empty
        // context (reconcile then falls back to the live coordinator).
        let row = load_chat_receipt(&conn, "can", "u1").unwrap().unwrap();
        assert_eq!(row.status, "unknown");
        assert!(row.provider.is_empty());
        assert!(row.channel.is_empty());
        // And the migrated row still reconciles.
        assert!(matches!(
            claim_reconcile(
                &conn,
                "can",
                "u1",
                "accepted",
                "seen live",
                "2026-09-16T00:01:00Z"
            )
            .unwrap(),
            ReconcileClaim::Moved(_)
        ));
    }

    #[test]
    fn reconcile_moves_only_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        for (id, status) in [("u1", "unknown"), ("d1", "dispatched"), ("a1", "accepted")] {
            save_chat_receipt(
                &conn,
                &ChatReceiptRow {
                    client_id: "can".into(),
                    request_id: id.into(),
                    digest: "bb".into(),
                    digest_v: 2,
                    status: status.into(),
                    intent_id: Some("i1".into()),
                    error: Some("transport: boom".into()),
                    provider: "twitch".into(),
                    channel: "alice".into(),
                    updated_utc: "2026-09-16T00:00:00Z".into(),
                },
            )
            .unwrap();
        }
        // Unknown and pre-restart dispatched markers move with evidence,
        // returning the committed row for memory publication.
        let moved = claim_reconcile(
            &conn,
            "can",
            "u1",
            "accepted",
            "dashboard shows sent",
            "2026-09-16T00:01:00Z",
        )
        .unwrap();
        let ReconcileClaim::Moved(row) = moved else {
            panic!("unknown row must move");
        };
        assert_eq!(row.status, "accepted");
        assert_eq!(row.provider, "twitch");
        assert_eq!(row.channel, "alice");
        assert!(matches!(
            claim_reconcile(
                &conn,
                "can",
                "d1",
                "rejected",
                "never sent",
                "2026-09-16T00:01:00Z"
            )
            .unwrap(),
            ReconcileClaim::Moved(_)
        ));
        // Decided verdicts never rewrite (carrying the deciding row);
        // missing rows report Missing.
        let decided = claim_reconcile(
            &conn,
            "can",
            "a1",
            "rejected",
            "changed my mind",
            "2026-09-16T00:01:00Z",
        )
        .unwrap();
        let ReconcileClaim::Decided(row) = decided else {
            panic!("decided row must refuse");
        };
        assert_eq!(row.status, "accepted");
        assert!(matches!(
            claim_reconcile(
                &conn,
                "can",
                "missing",
                "accepted",
                "x",
                "2026-09-16T00:01:00Z"
            )
            .unwrap(),
            ReconcileClaim::Missing
        ));
        let row = load_chat_receipt(&conn, "can", "u1").unwrap().unwrap();
        assert_eq!(row.status, "accepted");
        assert_eq!(row.error.as_deref(), Some("dashboard shows sent"));
        assert_eq!(row.intent_id.as_deref(), Some("i1"));
        // Reconciled rows repeat identically through the claim path.
        match claim_chat_receipt(&conn, "can", "u1", "bb", "2026-09-16T00:02:00Z").unwrap() {
            ReceiptClaim::Existing(r) => assert_eq!(r.status, "accepted"),
            ReceiptClaim::Fresh => panic!("reconciled row must not reopen"),
        }
    }

    #[test]
    fn rotation_preserves_unknown_until_reconciled() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        for (id, status) in [("u1", "unknown"), ("u2", "unknown"), ("a1", "accepted")] {
            save_chat_receipt(
                &conn,
                &ChatReceiptRow {
                    client_id: "can".into(),
                    request_id: id.into(),
                    digest: "bb".into(),
                    digest_v: 2,
                    status: status.into(),
                    intent_id: None,
                    error: None,
                    provider: String::new(),
                    channel: String::new(),
                    updated_utc: "2026-09-16T00:00:00Z".to_string(),
                },
            )
            .unwrap();
        }
        // Only the accepted row rotates; unknowns stay for reconciliation.
        let victims = rotate_chat_receipts(&conn, 0).unwrap();
        assert_eq!(victims.len(), 1);
        let rows = load_chat_receipts(&conn).unwrap();
        assert!(rows
            .iter()
            .any(|r| r.request_id == "u1" && r.status == "unknown"));
        assert!(rows
            .iter()
            .any(|r| r.request_id == "u2" && r.status == "unknown"));
    }

    #[test]
    fn seed_accounts_once_is_sticky() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        let row = AccountRow {
            id: "a1".into(),
            provider: "twitch".into(),
            channel_id: "111".into(),
            channel_name: "alice".into(),
            credential_ref: String::new(),
        };
        assert!(seed_accounts_once(&conn, std::slice::from_ref(&row)).unwrap());
        assert_eq!(list_accounts(&conn).unwrap().len(), 1);
        // Second call with different rows seeds nothing and keeps the table.
        let other = AccountRow {
            id: "a2".into(),
            ..row
        };
        assert!(!seed_accounts_once(&conn, &[other]).unwrap());
        assert_eq!(list_accounts(&conn).unwrap().len(), 1);
        // An intentionally emptied table is never reseeded.
        delete_account(&conn, "a1").unwrap();
        assert!(!seed_accounts_once(&conn, &[]).unwrap());
        assert!(list_accounts(&conn).unwrap().is_empty());
    }

    #[test]
    fn service_lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db").to_string_lossy().into_owned();
        // First holder wins; the anchor persists next to the database.
        let first = acquire_service_lock(&db).unwrap();
        assert_eq!(
            first.path().extension().and_then(|s| s.to_str()),
            Some("lock")
        );
        assert!(first.path().exists());
        // A second live holder — same process, separate descriptor — is
        // refused, proving the lock is kernel-held, not a name check.
        assert!(acquire_service_lock(&db).is_err());
        drop(first);
        // Release is by close: no stale file to clean, the next holder
        // proceeds immediately.
        let second = acquire_service_lock(&db).unwrap();
        assert!(second.path().exists());
    }

    #[test]
    fn v6_repair_tombstones_ownerless_rows() {
        // A database migrated by the original v6 (which preserved legacy
        // statuses) still holds an ownerless accepted row. Opening it
        // must repair that row into a tombstone via the v7 migration.
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("repaired.db")
            .to_string_lossy()
            .into_owned();
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                 INSERT INTO schema_version (version) VALUES (6);
                 CREATE TABLE chat_action_receipts_v2 (
                   client_id TEXT NOT NULL DEFAULT '',
                   request_id TEXT NOT NULL,
                   digest TEXT NOT NULL DEFAULT '',
                   digest_v INTEGER NOT NULL DEFAULT 2,
                   status TEXT NOT NULL DEFAULT 'dispatched',
                   intent_id TEXT, error TEXT,
                   updated_utc TEXT NOT NULL DEFAULT '',
                   PRIMARY KEY (client_id, request_id)
                 );
                 CREATE TABLE chat_prefs (
                   key TEXT PRIMARY KEY,
                   value TEXT NOT NULL DEFAULT ''
                 );
                 INSERT INTO chat_action_receipts_v2
                   (client_id, request_id, digest, digest_v, status, updated_utc)
                 VALUES ('', 'old-9', '12345', 1, 'accepted', '2026-09-16T00:00:00Z');",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        let rows = load_chat_receipts(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "expired");
        // And the claim path treats the ownerless id as non-executable
        // even if a repair were ever skipped.
        assert!(matches!(
            claim_chat_receipt(&conn, "can", "old-9", "whatever", "2026-09-16T00:00:01Z").unwrap(),
            ReceiptClaim::Existing(_)
        ));
    }

    #[test]
    fn accounts_delete_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(dir.path().join("app.db").to_str().unwrap()).unwrap();
        save_account(
            &conn,
            &AccountRow {
                id: "a1".into(),
                provider: "twitch".into(),
                channel_id: "111".into(),
                channel_name: "alice".into(),
                credential_ref: String::new(),
            },
        )
        .unwrap();
        assert_eq!(list_accounts(&conn).unwrap().len(), 1);
        delete_account(&conn, "a1").unwrap();
        assert!(list_accounts(&conn).unwrap().is_empty());
        // Missing rows are success (goal is absence).
        delete_account(&conn, "a1").unwrap();
    }
}
