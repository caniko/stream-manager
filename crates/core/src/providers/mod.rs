//! Capability-aware provider adapters.
//!
//! Capabilities are modelled per operation AND per field — never a single
//! `supportsPublishing` boolean. Every adapter also verifies the
//! authenticated account matches the selected destination before writing.

pub mod http;
pub mod kick;
pub mod rumble;
pub mod twitch;
pub mod youtube;

use crate::domain::Provider;
use serde::{Deserialize, Serialize};

/// Per-operation/field support level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Support {
    /// Writable via documented API.
    Api,
    /// First-class manual workflow (rendered copy + dashboard + record).
    Manual,
    /// Documented as unsupported.
    Unsupported,
    /// Supported by the API but this account lacks authorization/scopes.
    AuthRequired,
    /// Not yet verified against official docs.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCaps {
    pub title: Support,
    pub description: Support,
    pub scheduling: Support,
    pub category: Support,
    pub tags: Support,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCaps {
    pub create_event: Support,
    pub update_event: Support,
    pub update_channel_now: Support,
    pub fields: FieldCaps,
    /// Whether scheduling a draft can avoid touching the live channel.
    pub draft_without_channel_side_effect: bool,
}

/// Metadata values staged for one destination write.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StagedMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub scheduled_start_utc: Option<String>,
    pub category_id: Option<String>,
    pub tags: Vec<String>,
}

pub trait ProviderAdapter: Send + Sync {
    fn provider(&self) -> Provider;
    fn capabilities(&self) -> AdapterCaps;
    fn provider_name(&self) -> &'static str;
}

/// Capability matrix by provider. Identity is (provider, channel), so
/// capability lookup never depends on destination naming.
pub fn caps_of(provider: Provider) -> AdapterCaps {
    match provider {
        Provider::Youtube => youtube::caps(),
        Provider::Twitch => twitch::caps(),
        Provider::Kick => kick::caps(),
        Provider::Rumble => rumble::caps(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_matrices_are_explicit() {
        for caps in [
            youtube::caps(),
            twitch::caps(),
            kick::caps(),
            rumble::caps(),
        ] {
            // Every adapter must take a position on event updates.
            assert_ne!(caps.update_event, Support::Unknown);
        }
        // Rumble must be manual-first until a documented write API exists.
        assert_eq!(rumble::caps().update_event, Support::Manual);
        // Kick description writes are unsupported (read-only field).
        assert_eq!(kick::caps().fields.description, Support::Unsupported);
        // Twitch has no generic per-stream description endpoint.
        assert_eq!(twitch::caps().fields.description, Support::Unsupported);
    }
}
