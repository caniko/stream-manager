//! Kick channel-metadata adapter.
//!
//! - Documented channel title/category/tags updates.
//! - `channel_description` is readable but NOT writable: reported
//!   Unsupported, never written.

use super::{AdapterCaps, FieldCaps, ProviderAdapter, Support};
use crate::domain::Provider;

pub struct KickAdapter;

pub fn caps() -> AdapterCaps {
    AdapterCaps {
        create_event: Support::Unsupported,
        update_event: Support::Api,
        update_channel_now: Support::Api,
        fields: FieldCaps {
            title: Support::Api,
            description: Support::Unsupported,
            scheduling: Support::Unsupported,
            category: Support::Api,
            tags: Support::Api,
        },
        draft_without_channel_side_effect: true,
    }
}

impl ProviderAdapter for KickAdapter {
    fn provider(&self) -> Provider {
        Provider::Kick
    }
    fn capabilities(&self) -> AdapterCaps {
        caps()
    }
    fn provider_name(&self) -> &'static str {
        "Kick"
    }
}
