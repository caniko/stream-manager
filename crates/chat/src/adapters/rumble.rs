//! Rumble: read-only polling of the official Live Stream API.
//!
//! The documented window is small (`recent_messages`, `recent_rants`) and
//! carries **no stable message ids**. Reconciliation is occurrence-aware
//! over overlapping snapshots: repeats get occurrence-suffixed keys and
//! the UI discloses ambiguity instead of claiming exactly-once delivery.
//! The API URL is a bearer secret (responses may contain a stream key):
//! only needed fields are parsed and full responses are never logged or
//! stored. Send/moderation stay unavailable; the UI offers an external
//! native-chat action instead.

use crate::coordinator::Incoming;
use crate::model::{MessageKind, PaidInfo, SourceKey, TextFragment};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct RumbleMsg {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub badges: Vec<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub created_on: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RumbleRant {
    #[serde(flatten)]
    pub msg: RumbleMsg,
    #[serde(default)]
    pub amount_cents: u64,
    #[serde(default)]
    pub amount_dollars: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatBlock {
    #[serde(default)]
    pub recent_messages: Vec<RumbleMsg>,
    #[serde(default)]
    pub recent_rants: Vec<RumbleRant>,
}

#[derive(Debug, Clone, Deserialize)]
struct StreamBlock {
    /// Livestream id used for explicit stream selection.
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub is_live: bool,
    #[serde(default)]
    pub chat: Option<ChatBlock>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiResponse {
    #[serde(default)]
    pub livestreams: Vec<StreamBlock>,
}

/// Narrow parse: only chat/rant liveness fields leave this function.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub is_live: bool,
    pub messages: Vec<RumbleMsg>,
    pub rants: Vec<RumbleRant>,
}

pub fn parse_snapshot(raw: &str, target: Option<&str>) -> Result<Snapshot, String> {
    let res: ApiResponse =
        serde_json::from_str(raw).map_err(|e| format!("rumble response unusable: {e}"))?;
    // An explicitly selected stream wins; when it is absent the parse
    // fails closed instead of silently attaching to another stream.
    // Without a selection: the live stream, else the first entry.
    if let Some(t) = target {
        let found = res.livestreams.iter().find(|s| s.id == t);
        let Some(found) = found else {
            return Err(format!("selected rumble stream {t} is not present"));
        };
        return Ok(Snapshot {
            is_live: found.is_live,
            messages: found
                .chat
                .clone()
                .map(|c| c.recent_messages)
                .unwrap_or_default(),
            rants: found
                .chat
                .clone()
                .map(|c| c.recent_rants)
                .unwrap_or_default(),
        });
    }
    let live = res
        .livestreams
        .iter()
        .find(|s| s.is_live)
        .or(res.livestreams.first());
    match live {
        None => Ok(Snapshot {
            is_live: false,
            messages: vec![],
            rants: vec![],
        }),
        Some(s) => Ok(Snapshot {
            is_live: s.is_live,
            messages: s
                .chat
                .clone()
                .map(|c| c.recent_messages)
                .unwrap_or_default(),
            rants: s.chat.clone().map(|c| c.recent_rants).unwrap_or_default(),
        }),
    }
}

fn occurrence_key(m: &RumbleMsg, occurrence: u64) -> String {
    format!("{}|{}|{}|occ{occurrence}", m.username, m.text, m.created_on)
}

/// Occurrence-aware overlap reconciliation. Counts of each
/// (user, text, timestamp) triple already emitted are tracked; only
/// surplus occurrences become new items. Rearranged windows whose counts
/// shrink are reported as possible loss rather than replayed.
pub struct Reconciler {
    counts: HashMap<String, u64>,
    cap: usize,
}

impl Reconciler {
    pub fn new(cap: usize) -> Self {
        Self {
            counts: HashMap::new(),
            cap: cap.max(64),
        }
    }

    pub fn reconcile(
        &mut self,
        session_gen: u64,
        room_gen: u64,
        source_id: &str,
        snap: &Snapshot,
    ) -> Reconciled {
        let mut fresh: HashMap<String, u64> = HashMap::new();
        let mut items = vec![];
        let mut ambiguous_repeats = 0u64;
        let mut all: Vec<&RumbleMsg> = snap.messages.iter().collect();
        all.extend(snap.rants.iter().map(|r| &r.msg));
        for m in all {
            let base = format!("{}|{}|{}", m.username, m.text, m.created_on);
            let n = fresh.entry(base.clone()).or_insert(0);
            *n += 1;
            let already = self.counts.get(&base).copied().unwrap_or(0);
            if *n > already {
                // A repeated identical triple is ambiguous whether it
                // appears twice in one window or across snapshots.
                if already > 0 || *n > 1 {
                    ambiguous_repeats += 1;
                }
                let rant = snap.rants.iter().find(|r| {
                    r.msg.username == m.username
                        && r.msg.text == m.text
                        && r.msg.created_on == m.created_on
                });
                items.push(self.to_incoming(session_gen, room_gen, source_id, m, rant, *n - 1));
            }
        }
        let mut lost = 0u64;
        for (base, old) in &self.counts {
            let now = fresh.get(base).copied().unwrap_or(0);
            if now < *old {
                lost += *old - now;
            }
        }
        self.counts = fresh;
        if self.counts.len() > self.cap {
            self.counts.clear();
            lost += 1;
        }
        Reconciled {
            items,
            ambiguous_repeats,
            possible_loss: lost,
            ended: !snap.is_live,
        }
    }

    fn to_incoming(
        &self,
        session_gen: u64,
        room_gen: u64,
        source_id: &str,
        m: &RumbleMsg,
        rant: Option<&RumbleRant>,
        occurrence: u64,
    ) -> Incoming {
        let kind = match rant {
            Some(r) => MessageKind::Paid(PaidInfo::Rant {
                amount_cents: r.amount_cents,
            }),
            None => MessageKind::Text,
        };
        Incoming {
            session_gen,
            room_gen,
            source_id: source_id.to_owned(),
            remote_id: None,
            idless_key: Some(occurrence_key(m, occurrence)),
            author_id: None,
            author_name: if m.username.is_empty() {
                "?".into()
            } else {
                m.username.clone()
            },
            provider_ts: Some(m.created_on.clone()),
            text: m.text.clone(),
            fragments: vec![TextFragment {
                text: m.text.clone(),
                emote_id: None,
            }],
            badges: m.badges.clone(),
            reply_to: None,
            kind,
        }
    }
}

pub struct Reconciled {
    pub items: Vec<Incoming>,
    /// Repeated identical messages admitted with occurrence suffixes:
    /// genuinely ambiguous, disclosed to the operator.
    pub ambiguous_repeats: u64,
    /// Older occurrences that fell out of the window: possible loss.
    pub possible_loss: u64,
    pub ended: bool,
}

/// External native-chat action (read-only provider): open the dashboard.
pub fn native_chat_url(channel: &str) -> String {
    format!("https://rumble.com/c/{}", channel.trim_start_matches('@'))
}

pub fn source_key(channel: &str) -> SourceKey {
    SourceKey {
        provider: crate::model::Provider::Rumble,
        room: channel.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAP1: &str = r#"{"livestreams":[{"id":"s1","is_live":true,
      "chat":{"recent_messages":[
        {"username":"amy","badges":["premium"],"text":"gg","created_on":"2026-09-15T10:00:00Z"},
        {"username":"amy","badges":["premium"],"text":"gg","created_on":"2026-09-15T10:00:00Z"}],
       "recent_rants":[
        {"username":"zed","badges":[],"text":"take it","created_on":"2026-09-15T10:00:01Z",
         "expires_on":"2026-09-15T10:02:01Z","amount_cents":100,"amount_dollars":1}]}}]}"#;
    const SNAP2: &str = r#"{"livestreams":[{"id":"s1","is_live":true,
      "chat":{"recent_messages":[
        {"username":"amy","badges":["premium"],"text":"gg","created_on":"2026-09-15T10:00:00Z"},
        {"username":"amy","badges":["premium"],"text":"gg","created_on":"2026-09-15T10:00:00Z"},
        {"username":"amy","badges":["premium"],"text":"gg","created_on":"2026-09-15T10:00:00Z"}],
       "recent_rants":[]}}]}"#;

    #[test]
    fn repeats_reconcile_by_occurrence() {
        let mut r = Reconciler::new(512);
        let s1 = parse_snapshot(SNAP1, None).unwrap();
        assert!(s1.is_live);
        let first = r.reconcile(1, 0, "ru", &s1);
        assert_eq!(first.items.len(), 3);
        assert_eq!(first.ambiguous_repeats, 1, "identical repeat is ambiguous");
        assert!(matches!(first.items[2].kind, MessageKind::Paid(_)));
        // Identical redelivery: nothing new.
        let redelivered = r.reconcile(1, 0, "ru", &parse_snapshot(SNAP1, None).unwrap());
        assert!(redelivered.items.is_empty());
        assert_eq!(redelivered.possible_loss, 0);
        // One more identical message: one surplus occurrence.
        let s2 = parse_snapshot(SNAP2, None).unwrap();
        let more = r.reconcile(1, 0, "ru", &s2);
        assert_eq!(more.items.len(), 1);
        assert_eq!(more.ambiguous_repeats, 1);
    }

    #[test]
    fn narrow_parse_ignores_stream_key_and_unknown() {
        let raw = r#"{"now":1,"livestreams":[{"id":"s","is_live":false,
          "stream_key":"SECRET-MUST-NOT-LEAK","watching_now":3,
          "chat":{"recent_messages":[],"recent_rants":[]}}]}"#;
        let s = parse_snapshot(raw, None).unwrap();
        assert!(!s.is_live);
        assert!(s.messages.is_empty());
    }

    #[test]
    fn explicit_stream_selection_wins() {
        let raw = r#"{"livestreams":[
          {"id":"other","is_live":true,"chat":{"recent_messages":[{"username":"x","text":"other","created_on":"t"}],"recent_rants":[]}},
          {"id":"s1","is_live":false,"chat":{"recent_messages":[{"username":"y","text":"mine","created_on":"t"}],"recent_rants":[]}}]}"#;
        let picked = parse_snapshot(raw, Some("s1")).unwrap();
        assert!(!picked.is_live);
        assert_eq!(picked.messages[0].text, "mine");
        let auto = parse_snapshot(raw, None).unwrap();
        assert!(auto.is_live);
        assert_eq!(auto.messages[0].text, "other");
        // An absent explicit selection fails closed, never attaches to
        // another stream.
        assert!(parse_snapshot(raw, Some("missing")).is_err());
    }
}
