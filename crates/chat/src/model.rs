//! Normalized chat model, separate from metadata publishing.
//!
//! Identity rules:
//! - A chat source is (provider, account/credential reference, channel,
//!   optional broadcast/room id). Destination *names* never imply a source;
//!   an RTMP declaration is not chat authorization.
//! - Authors are provider-scoped: equal display names on different
//!   providers are different identities, always.
//! - Dedupe key is (provider, room, remote id) where the provider supplies
//!   one — never message text. ID-less providers reconcile by occurrence.

use serde::{Deserialize, Serialize};
pub use stream_manager_core::domain::Provider;
pub use stream_manager_core::providers::Support;

/// One chat subscription target. Several destinations may resolve to the
/// same key; the coordinator deduplicates subscriptions while keeping the
/// destination associations on [`ChatSource::binding_ids`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceKey {
    pub provider: Provider,
    pub room: String,
}

impl SourceKey {
    pub fn label(&self) -> String {
        format!("{}:{}", self.provider.display(), self.room)
    }
}

/// Operator-configured source. Reuses the core account/binding identities;
/// there is no second account registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSource {
    pub id: String,
    pub key: SourceKey,
    pub account_id: Option<String>,
    pub channel: String,
    pub broadcast_id: Option<String>,
    pub binding_ids: Vec<String>,
}

impl ChatSource {
    pub fn badge(&self) -> &'static str {
        match self.key.provider {
            Provider::Youtube => "YT",
            Provider::Twitch => "TW",
            Provider::Kick => "KK",
            Provider::Rumble => "RU",
        }
    }
}

/// Connection health per source. One failed provider degrades only itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnState {
    Offline {
        reason: String,
    },
    Connecting,
    Live {
        since_utc: String,
    },
    Reconnecting {
        attempt: u32,
    },
    RateLimited {
        retry_after_secs: u64,
    },
    Ended {
        reason: String,
    },
    /// Kick without configured relay access: other providers still work.
    RelayRequired,
    /// Stream ended or chat disabled; history may still render.
    PossibleGap {
        reason: String,
    },
}

impl ConnState {
    pub fn short(&self) -> &'static str {
        match self {
            ConnState::Offline { .. } => "offline",
            ConnState::Connecting => "connecting",
            ConnState::Live { .. } => "live",
            ConnState::Reconnecting { .. } => "reconnecting",
            ConnState::RateLimited { .. } => "rate-limited",
            ConnState::Ended { .. } => "ended",
            ConnState::RelayRequired => "relay required",
            ConnState::PossibleGap { .. } => "possible gap",
        }
    }

    pub fn is_receiving(&self) -> bool {
        matches!(self, ConnState::Live { .. } | ConnState::PossibleGap { .. })
    }
}

/// One capability dimension. Documented support, implementation status,
/// current authorization, and live verification are distinct: the UI
/// combines them, this struct keeps them separate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub documented: Support,
    pub implemented: bool,
}

impl Capability {
    pub fn api() -> Self {
        Self {
            documented: Support::Api,
            implemented: true,
        }
    }
    pub fn manual() -> Self {
        Self {
            documented: Support::Manual,
            implemented: true,
        }
    }
    pub fn unsupported() -> Self {
        Self {
            documented: Support::Unsupported,
            implemented: false,
        }
    }
    pub fn planned() -> Self {
        Self {
            documented: Support::Api,
            implemented: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCaps {
    pub receive: Capability,
    pub send: Capability,
    pub native_reply: Capability,
    pub delete: Capability,
    pub timeout: Capability,
    pub ban: Capability,
    pub moderation_events: Capability,
    pub history_resume: Capability,
}

/// Chat capability matrix per provider (implementation status, reviewed
/// 2026-09-15; authorization/verification are runtime state, see docs).
pub fn caps_of(provider: Provider) -> ChatCaps {
    match provider {
        Provider::Twitch => ChatCaps {
            receive: Capability::api(),
            send: Capability::api(),
            native_reply: Capability::api(),
            delete: Capability::api(),
            timeout: Capability::api(),
            ban: Capability::api(),
            moderation_events: Capability::api(),
            history_resume: Capability::unsupported(),
        },
        Provider::Youtube => ChatCaps {
            receive: Capability::api(),
            send: Capability::api(),
            native_reply: Capability::unsupported(),
            delete: Capability::api(),
            timeout: Capability::unsupported(),
            // Documented endpoint, not yet wired: ban stays planned
            // until a live-verified implementation exists.
            ban: Capability::planned(),
            moderation_events: Capability::api(),
            history_resume: Capability::api(),
        },
        Provider::Kick => ChatCaps {
            receive: Capability::api(),
            send: Capability::api(),
            native_reply: Capability::api(),
            delete: Capability::api(),
            timeout: Capability::planned(),
            ban: Capability::planned(),
            moderation_events: Capability::planned(),
            history_resume: Capability::unsupported(),
        },
        Provider::Rumble => ChatCaps {
            receive: Capability::api(),
            send: Capability::unsupported(),
            native_reply: Capability::unsupported(),
            delete: Capability::unsupported(),
            timeout: Capability::unsupported(),
            ban: Capability::unsupported(),
            moderation_events: Capability::unsupported(),
            history_resume: Capability::unsupported(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextFragment {
    pub text: String,
    pub emote_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyRef {
    pub remote_id: String,
    pub author_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaidInfo {
    SuperChat { amount: String },
    SuperSticker { amount: String },
    Rant { amount_cents: u64 },
    GiftedSub { count: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    Text,
    Paid(PaidInfo),
    Membership,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModState {
    Visible,
    /// Operator hid it locally; the remote message still exists.
    HiddenLocal,
    /// Provider confirmed remote deletion.
    DeletedRemote,
}

/// One normalized inbound message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Local arrival sequence: the merged timeline order. Provider
    /// timestamps are preserved separately and never imply global order.
    pub seq: u64,
    pub source: SourceKey,
    pub remote_id: Option<String>,
    pub author_id: Option<String>,
    pub author_name: String,
    pub provider_ts: Option<String>,
    pub text: String,
    pub fragments: Vec<TextFragment>,
    pub badges: Vec<String>,
    pub reply_to: Option<ReplyRef>,
    pub kind: MessageKind,
    pub mod_state: ModState,
}

impl ChatMessage {
    pub fn is_paid(&self) -> bool {
        matches!(self.kind, MessageKind::Paid(_))
    }

    pub fn mentions(&self, needle: &str) -> bool {
        !needle.is_empty() && self.text.to_lowercase().contains(&needle.to_lowercase())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimelineEvent {
    Message(Box<ChatMessage>),
    Deletion {
        seq: u64,
        source: SourceKey,
        remote_id: String,
    },
    Clear {
        seq: u64,
        source: SourceKey,
    },
    ConnChange {
        seq: u64,
        source: SourceKey,
        state: ConnState,
    },
    Gap {
        seq: u64,
        source: SourceKey,
        reason: String,
    },
}

impl TimelineEvent {
    pub fn seq(&self) -> u64 {
        match self {
            TimelineEvent::Message(m) => m.seq,
            TimelineEvent::Deletion { seq, .. }
            | TimelineEvent::Clear { seq, .. }
            | TimelineEvent::ConnChange { seq, .. }
            | TimelineEvent::Gap { seq, .. } => *seq,
        }
    }
}

/// Immutable outbound intent. Execution rechecks session validity,
/// permissions, limits, and policy at send time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundIntent {
    pub id: String,
    pub source_id: String,
    pub source: SourceKey,
    pub identity: String,
    pub room_gen: u64,
    pub text: String,
    pub reply_to: Option<ReplyRef>,
    pub targets: Vec<String>,
    pub state: IntentState,
    pub remote_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentState {
    Pending,
    Confirmed,
    Rejected,
    /// Sent but the provider gave no definitive verdict: never auto-retry.
    OutcomeUnknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_keys_are_provider_scoped() {
        let a = SourceKey {
            provider: Provider::Twitch,
            room: "alice".into(),
        };
        let b = SourceKey {
            provider: Provider::Kick,
            room: "alice".into(),
        };
        assert_ne!(a, b, "same name on two platforms is not one identity");
    }

    #[test]
    fn rumble_is_read_only_by_matrix() {
        let caps = caps_of(Provider::Rumble);
        assert!(caps.receive.implemented);
        assert!(!caps.send.implemented);
        assert_eq!(caps.send.documented, Support::Unsupported);
    }

    #[test]
    fn conn_state_labels_cover_degraded_modes() {
        assert_eq!(ConnState::RelayRequired.short(), "relay required");
        assert!(ConnState::Live {
            since_utc: String::new()
        }
        .is_receiving());
        assert!(!ConnState::RelayRequired.is_receiving());
    }
}
