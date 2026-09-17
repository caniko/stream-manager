use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Supported platforms. TikTok is out of scope by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Youtube,
    Twitch,
    Kick,
    Rumble,
}

impl Provider {
    pub fn slug(&self) -> &'static str {
        match self {
            Provider::Youtube => "youtube",
            Provider::Twitch => "twitch",
            Provider::Kick => "kick",
            Provider::Rumble => "rumble",
        }
    }

    pub fn display(&self) -> &'static str {
        match self {
            Provider::Youtube => "YouTube",
            Provider::Twitch => "Twitch",
            Provider::Kick => "Kick",
            Provider::Rumble => "Rumble",
        }
    }
}

/// Destination role within a stream plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Primary,
    Mirror,
    Funnel,
}

/// Tri-state field override: inherit shared value, replace it, or
/// deliberately clear it (only where the provider supports clearing).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldOverride<T> {
    #[default]
    Inherit,
    Replace(T),
    Clear,
}

impl<T: Clone> FieldOverride<T> {
    pub fn resolve<'a>(&'a self, shared: &'a T) -> Option<&'a T> {
        match self {
            FieldOverride::Inherit => Some(shared),
            FieldOverride::Replace(v) => Some(v),
            FieldOverride::Clear => None,
        }
    }

    pub fn is_override(&self) -> bool {
        !matches!(self, FieldOverride::Inherit)
    }
}

/// Authenticated platform account. Never carries secret material;
/// credential access goes through [`crate::credentials::SecretRef`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformAccount {
    pub id: String,
    pub provider: Provider,
    /// Provider-side channel/user identity (id + display name).
    pub channel_id: String,
    pub channel_name: String,
    /// Runtime secret reference (agenix path or token-store key id).
    pub credential_ref: String,
}

/// Stable destination identity. Multiple accounts per platform work
/// because identity is the destination id, never the platform alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationBinding {
    pub id: String,
    pub provider: Provider,
    pub account_id: Option<String>,
    /// Optional InstantClone destination name (transport only).
    pub instantclone_name: Option<String>,
}

/// Shared stream metadata + schedule + destination selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPlan {
    pub id: String,
    pub title: String,
    pub description: String,
    pub start_utc: DateTime<Utc>,
    pub end_utc: Option<DateTime<Utc>>,
    /// Selected IANA timezone for display/entry (e.g. Europe/Berlin).
    pub timezone: String,
    pub language: String,
    pub category_intent: String,
    /// Provider-specific category id, separate from shared intent.
    pub category_id: Option<String>,
    pub tags: Vec<String>,
    pub destination_ids: Vec<String>,
    pub primary_id: Option<String>,
    pub status: PlanStatus,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanStatus {
    Draft,
    Ready,
    Published,
    Archived,
}

/// Per-destination plan: role, overrides, remote binding, video profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationPlan {
    pub destination_id: String,
    pub role: Role,
    pub title: FieldOverride<String>,
    pub description: FieldOverride<String>,
    pub tags: FieldOverride<Vec<String>>,
    /// Funnel target destination id (required when role == Funnel).
    pub funnel_target: Option<String>,
    /// Remote resource binding (broadcast/event id, public watch URL).
    pub remote_broadcast_id: Option<String>,
    pub remote_watch_url: Option<String>,
    pub video_profile_id: Option<String>,
}

impl DestinationPlan {
    pub fn new(destination_id: &str, role: Role) -> Self {
        Self {
            destination_id: destination_id.to_string(),
            role,
            title: FieldOverride::Inherit,
            description: FieldOverride::Inherit,
            tags: FieldOverride::Inherit,
            funnel_target: None,
            remote_broadcast_id: None,
            remote_watch_url: None,
            video_profile_id: None,
        }
    }
}
