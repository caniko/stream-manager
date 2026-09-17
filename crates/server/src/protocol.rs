//! Versioned client protocol (v2).
//!
//! Commands carry an immutable client-generated `request_id` plus actor;
//! source-targeting mutations additionally carry the `expected_boot`
//! (server boot the operator approved against) and `expected_room`
//! generation, and the server rejects the command when the server
//! restarted or the source moved since. The server answers with a
//! receipt and applies each request id at most once. Completed repeats
//! return the original receipt unchanged; only in-flight repeats report
//! `Duplicate`. State flows back as a cursor-ordered event stream with
//! bounded resume: expired, unknown, or future cursors get a fresh
//! snapshot plus a visible gap, never a silent replay or a stall.

pub const PROTOCOL_VERSION: u32 = 2;

use serde::{Deserialize, Serialize};
use stream_manager_chat::model::{ChatCaps, ChatSource, ConnState, TimelineEvent};

use crate::state::AccountBinding;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub v: u32,
    pub request_id: String,
    pub actor: String,
    pub kind: CommandKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandKind {
    ConnectSource {
        source: ChatSource,
    },
    /// One-time admin bootstrap: verify the refresh token against the
    /// provider, bind it to the account identity, then store it.
    ImportToken {
        account_id: String,
        refresh_token: String,
    },
    DisconnectSource {
        source_id: String,
    },
    SendMessage {
        source_id: String,
        /// Server boot from the snapshot the operator approved against;
        /// rejects when the server restarted since (approvals do not
        /// survive a restart: post-crash outcomes are unknown).
        expected_boot: u64,
        /// Room generation from the snapshot the operator approved
        /// against; rejects when the source moved since.
        expected_room: u64,
        text: String,
        reply_to: Option<String>,
    },
    DeleteMessage {
        source_id: String,
        expected_boot: u64,
        expected_room: u64,
        remote_id: String,
    },
    ModerateUser {
        source_id: String,
        expected_boot: u64,
        expected_room: u64,
        user: String,
        duration_secs: Option<u64>,
    },
    /// Operator attestation for an outcome-unknown receipt: records what
    /// the provider actually did (checked out of band) as evidence. Never
    /// replays the action. Only rows still `unknown` (or pre-restart
    /// `dispatched` markers) can be reconciled.
    ReconcileUnknown {
        target_request_id: String,
        outcome: ReconciledOutcome,
        /// Required human evidence, e.g. "provider dashboard shows the
        /// message as sent (m9)". Length-capped.
        evidence: String,
    },
    CancelIntent {
        intent_id: String,
    },
    /// Admin-only: drop the in-memory session and the stored refresh for
    /// an account, cancelling its sources and queued sends.
    LogoutAccount {
        account_id: String,
    },
    /// Admin-only: create or update a canonical account in the database.
    UpsertAccount {
        account: AccountBinding,
    },
    /// Admin-only: remove a canonical account. Refused while any source
    /// still binds to it — disconnect those first, explicitly.
    RemoveAccount {
        account_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub request_id: String,
    pub status: ReceiptStatus,
    pub intent_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptStatus {
    Accepted,
    /// Same request id seen before: the original receipt, no new action.
    Duplicate,
    Rejected,
    /// Sent but the provider gave no definitive verdict: never auto-retry.
    Unknown,
}

/// Attested terminal outcome for [`CommandKind::ReconcileUnknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconciledOutcome {
    Confirmed,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerEvent {
    pub cursor: u64,
    pub kind: EventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    Snapshot {
        snapshot: Snapshot,
    },
    Gap {
        reason: String,
    },
    Sources {
        sources: Vec<SourceView>,
    },
    Timeline {
        events: Vec<TimelineEvent>,
    },
    Intent {
        intent_id: String,
        state: String,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceView {
    pub source: ChatSource,
    pub conn: ConnState,
    pub caps: ChatCaps,
    /// Current room generation: clients echo it back as `expected_room`
    /// so mutations bind to the selection the operator approved.
    pub room_gen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub instance: String,
    /// Server boot id (random per process): mutations echo it back as
    /// `expected_boot` so approvals never cross a restart.
    pub boot: u64,
    pub session: u64,
    pub sources: Vec<SourceView>,
    pub recent: Vec<TimelineEvent>,
    pub backlog_start: u64,
}
