//! Rumble first-class manual adapter.
//!
//! No documented metadata-write API has been established, so this adapter
//! is explicit: render copy, open the provider dashboard, accept the
//! pasted-back public URL, and record operator completion. Manual
//! completion is NOT API verification. No undocumented endpoints, no
//! cookie extraction, no notification-API masquerade.

use super::{AdapterCaps, FieldCaps, ProviderAdapter, Support};
use crate::domain::Provider;

pub struct RumbleManualAdapter;

/// Dashboard URL opened for the operator (no credentials in the URL).
pub const RUMBLE_DASHBOARD_URL: &str = "https://rumble.com/account/live-streaming";

pub fn caps() -> AdapterCaps {
    AdapterCaps {
        create_event: Support::Manual,
        update_event: Support::Manual,
        update_channel_now: Support::Manual,
        fields: FieldCaps {
            title: Support::Manual,
            description: Support::Manual,
            scheduling: Support::Manual,
            category: Support::Manual,
            tags: Support::Manual,
        },
        draft_without_channel_side_effect: true,
    }
}

impl ProviderAdapter for RumbleManualAdapter {
    fn provider(&self) -> Provider {
        Provider::Rumble
    }
    fn capabilities(&self) -> AdapterCaps {
        caps()
    }
    fn provider_name(&self) -> &'static str {
        "Rumble (manual)"
    }
}

/// Manual completion record: operator asserts the dashboard matches the
/// reviewed copy. Stored in publish history as ManuallyCompleted.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ManualCompletion {
    pub destination_id: String,
    pub public_url: String,
    pub completed_utc: String,
    pub operator_note: String,
}

impl ManualCompletion {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.public_url.trim().is_empty() {
            return Err("public URL is required");
        }
        let url = url::Url::parse(&self.public_url).map_err(|_| "invalid URL")?;
        if url.scheme() != "https" {
            return Err("public URL must be https");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_completion_requires_https_url() {
        let ok = ManualCompletion {
            destination_id: "rumble-main".into(),
            public_url: "https://rumble.com/v-abc".into(),
            completed_utc: "2026-01-01T00:00:00Z".into(),
            operator_note: "pasted".into(),
        };
        assert!(ok.validate().is_ok());
        let bad = ManualCompletion {
            public_url: "rtmp://example.invalid/live".into(),
            ..ok.clone()
        };
        assert!(bad.validate().is_err());
    }
}
