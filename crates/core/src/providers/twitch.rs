//! Twitch Helix channel-metadata adapter.
//!
//! - Supported channel updates (title/category/tags where permitted).
//! - Schedule segments are separate resources gated by account eligibility.
//! - No description-to-bio mapping and no invented per-stream endpoint.
//! - Preparing a future stream must not silently retitle the live channel:
//!   "apply to channel now" is a separate explicit action.

use super::{AdapterCaps, FieldCaps, ProviderAdapter, Support};
use crate::domain::Provider;

pub struct TwitchAdapter;

pub fn caps() -> AdapterCaps {
    AdapterCaps {
        create_event: Support::Unsupported,
        update_event: Support::Api,
        update_channel_now: Support::Api,
        fields: FieldCaps {
            title: Support::Api,
            description: Support::Unsupported,
            scheduling: Support::Manual,
            category: Support::Api,
            tags: Support::Api,
        },
        draft_without_channel_side_effect: true,
    }
}

impl ProviderAdapter for TwitchAdapter {
    fn provider(&self) -> Provider {
        Provider::Twitch
    }
    fn capabilities(&self) -> AdapterCaps {
        caps()
    }
    fn provider_name(&self) -> &'static str {
        "Twitch"
    }
}
