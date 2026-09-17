//! YouTube Data API v3 liveBroadcast/liveStream adapter.
//!
//! - Creates or binds scheduled `liveBroadcast` resources.
//! - Updates title/description/scheduling with `part=` semantics,
//!   preserving unrelated fields.
//! - Never touches the `liveStream` (ingest) resource and never rotates
//!   stream keys. New preparation events keep auto-start disabled.

use super::{AdapterCaps, FieldCaps, ProviderAdapter, Support};
use crate::domain::Provider;

pub struct YoutubeAdapter;

pub fn caps() -> AdapterCaps {
    AdapterCaps {
        create_event: Support::Api,
        update_event: Support::Api,
        update_channel_now: Support::Unsupported,
        fields: FieldCaps {
            title: Support::Api,
            description: Support::Api,
            scheduling: Support::Api,
            category: Support::Api,
            tags: Support::Api,
        },
        draft_without_channel_side_effect: true,
    }
}

impl ProviderAdapter for YoutubeAdapter {
    fn provider(&self) -> Provider {
        Provider::Youtube
    }
    fn capabilities(&self) -> AdapterCaps {
        caps()
    }
    fn provider_name(&self) -> &'static str {
        "YouTube"
    }
}

/// Build a `liveBroadcasts.update` `part=` value with preservation
/// semantics: only the parts being written are sent.
///
/// `scheduledStartTime`/`scheduledEndTime` live in `snippet`;
/// `status` (privacy, selfDeclaredMadeForKids) is only included when
/// scheduling intent explicitly requires it.
pub fn update_parts(title: bool, description: bool, scheduling: bool) -> String {
    let mut parts = vec!["id"];
    if title || description || scheduling {
        parts.push("snippet");
    }
    if scheduling {
        parts.push("status");
    }
    parts.join(",")
}

/// Merge staged values into a previously read remote `snippet`,
/// preserving every unrelated key. `None` means "leave remote as-is";
/// clearing is only possible where the API documents it.
pub fn merge_snippet(
    remote: &mut serde_json::Map<String, serde_json::Value>,
    title: Option<&str>,
    description: Option<&str>,
    scheduled_start: Option<&str>,
    scheduled_end: Option<&str>,
) {
    if let Some(t) = title {
        remote.insert("title".into(), serde_json::Value::String(t.into()));
    }
    if let Some(d) = description {
        remote.insert("description".into(), serde_json::Value::String(d.into()));
    }
    if let Some(s) = scheduled_start {
        remote.insert(
            "scheduledStartTime".into(),
            serde_json::Value::String(s.into()),
        );
    }
    if let Some(e) = scheduled_end {
        remote.insert(
            "scheduledEndTime".into(),
            serde_json::Value::String(e.into()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_preserve_unrelated_values() {
        assert_eq!(update_parts(true, false, false), "id,snippet");
        assert_eq!(update_parts(false, false, false), "id");
        assert_eq!(update_parts(false, false, true), "id,snippet,status");
    }

    #[test]
    fn merge_preserves_unrelated_snippet_keys() {
        let mut remote = serde_json::Map::from_iter([
            ("title".into(), serde_json::json!("Old")),
            ("description".into(), serde_json::json!("Keep me")),
            ("categoryId".into(), serde_json::json!("28")),
            ("isDefaultBroadcast".into(), serde_json::json!(false)),
        ]);
        merge_snippet(&mut remote, Some("New"), None, None, None);
        assert_eq!(remote["title"], serde_json::json!("New"));
        assert_eq!(remote["description"], serde_json::json!("Keep me"));
        assert_eq!(remote["categoryId"], serde_json::json!("28"));
        assert_eq!(remote["isDefaultBroadcast"], serde_json::json!(false));
    }
}
