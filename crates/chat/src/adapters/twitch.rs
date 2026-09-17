//! Twitch: EventSub WebSocket receive, Helix Chat API send.
//!
//! - Receive: `wss://eventsub.wss.twitch.tv/ws` — welcome (session id,
//!   keepalive), notification (message, notification, clear, delete),
//!   keepalive, ping, reconnect (resume at the given URL), revocation,
//!   close. Requested reconnects resume; connection loss does not replay.
//! - Send: `POST https://api.twitch.tv/helix/chat/messages` (never the
//!   EventSub socket), user token with `user:write:chat`.
//! - Shared Chat provenance (`source_broadcaster_user_*`) is preserved as
//!   a badge; sends stay addressed to the intended broadcaster id.

pub const EVENTSUB_WS_URL: &str = "wss://eventsub.wss.twitch.tv/ws";
pub const HELIX_CHAT_URL: &str = "https://api.twitch.tv/helix/chat/messages";
pub const HELIX_EVENTSUB_URL: &str = "https://api.twitch.tv/helix/eventsub/subscriptions";
pub const MAX_CHARS: usize = 500;

/// Chat subscriptions this app maintains per connected source.
pub const SUBSCRIPTION_TYPES: [&str; 4] = [
    "channel.chat.message",
    "channel.chat.notification",
    "channel.chat.message_delete",
    "channel.chat.clear",
];

use crate::coordinator::Incoming;
use crate::model::{MessageKind, PaidInfo, SourceKey, TextFragment};
use serde_json::Value;

/// One parsed WebSocket frame.
#[derive(Debug, Clone)]
pub enum WsFrame {
    Welcome {
        session_id: String,
        keepalive_secs: u64,
    },
    Keepalive,
    Ping,
    Notification(Notification),
    /// Requested reconnect: resume at the URL, do not promise replay.
    Reconnect {
        reconnect_url: Option<String>,
    },
    Revocation {
        subscription_type: String,
        status: String,
    },
    Close,
    Unknown {
        message_type: String,
    },
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub message_id: String,
    pub message_type: String,
    pub subscription_type: String,
    pub event: Value,
}

pub fn parse_envelope(raw: &str) -> anyhow::Result<WsFrame> {
    let v: Value = serde_json::from_str(raw)?;
    let meta = &v["metadata"];
    let message_type = meta["message_type"].as_str().unwrap_or("").to_owned();
    let payload = &v["payload"];
    match message_type.as_str() {
        "session_welcome" => Ok(WsFrame::Welcome {
            session_id: payload["session"]["id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            keepalive_secs: payload["session"]["keepalive_timeout_seconds"]
                .as_u64()
                .unwrap_or(10)
                .clamp(10, 600),
        }),
        "session_keepalive" => Ok(WsFrame::Keepalive),
        "ping" => Ok(WsFrame::Ping),
        "notification" => Ok(WsFrame::Notification(Notification {
            message_id: meta["message_id"].as_str().unwrap_or_default().to_owned(),
            message_type,
            subscription_type: payload["subscription"]["type"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            event: payload["event"].clone(),
        })),
        "session_reconnect" => Ok(WsFrame::Reconnect {
            reconnect_url: payload["session"]["reconnect_url"]
                .as_str()
                .map(str::to_owned),
        }),
        "revocation" => Ok(WsFrame::Revocation {
            subscription_type: payload["subscription"]["type"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            status: payload["subscription"]["status"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        }),
        "close" => Ok(WsFrame::Close),
        other => Ok(WsFrame::Unknown {
            message_type: other.to_owned(),
        }),
    }
}

/// Subscription creation payload for one chat subscription on a session.
/// Subscriptions are created through the Helix EventSub API — never over
/// the socket itself.
pub fn subscribe_payload(
    session_id: &str,
    subscription_type: &str,
    broadcaster_id: &str,
    user_id: &str,
) -> Value {
    serde_json::json!({
        "type": subscription_type,
        "version": "1",
        "condition": { "broadcaster_user_id": broadcaster_id, "user_id": user_id },
        "transport": { "method": "websocket", "session_id": session_id },
    })
}

/// Create the chat subscriptions for one session via Helix. A 409
/// (already subscribed) counts as success for idempotent reconnects;
/// 401/403 surfaces as unauthorized so the loop stops instead of
/// retrying a dead credential.
pub async fn subscribe_all(
    http: &reqwest::Client,
    eventsub_url: &str,
    client_id: &str,
    token: &str,
    session_id: &str,
    broadcaster_id: &str,
    user_id: &str,
) -> Result<(), String> {
    for kind in SUBSCRIPTION_TYPES {
        let res = http
            .post(eventsub_url)
            .header("Client-Id", client_id)
            .bearer_auth(token)
            .json(&subscribe_payload(
                session_id,
                kind,
                broadcaster_id,
                user_id,
            ))
            .send()
            .await
            .map_err(|e| format!("eventsub subscribe transport: {e}"))?;
        let status = res.status();
        if status.is_success() || status.as_u16() == 409 {
            continue;
        }
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(format!(
                "eventsub subscribe unauthorized ({status}) for {kind}"
            ));
        }
        return Err(format!("eventsub subscribe refused ({status}) for {kind}"));
    }
    Ok(())
}

/// Convert a `channel.chat.message` notification into a normalized
/// message. Returns `None` for subscription types handled elsewhere
/// (delete/clear route to [`DeleteAction`]).
pub fn message_notification(
    session_gen: u64,
    room_gen: u64,
    source_id: &str,
    _room: &str,
    n: &Notification,
) -> Option<Incoming> {
    if n.subscription_type != "channel.chat.message" {
        return None;
    }
    let e = &n.event;
    let remote_id = e["message_id"].as_str()?.to_owned();
    let text = e["message"]["text"].as_str().unwrap_or_default().to_owned();
    let mut fragments = vec![];
    for part in e["message"]["fragments"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        let ptype = part["type"].as_str().unwrap_or("text");
        fragments.push(TextFragment {
            text: part["text"].as_str().unwrap_or_default().to_owned(),
            emote_id: if ptype == "emote" {
                part["emote"]["id"].as_str().map(str::to_owned)
            } else {
                None
            },
        });
    }
    let mut badges: Vec<String> = e["badges"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|b| b["set_id"].as_str().map(str::to_owned))
        .collect();
    // Shared Chat provenance: keep which channel the message came from.
    if let Some(src) = e["source_broadcaster_user_login"].as_str() {
        badges.push(format!("shared:{src}"));
    }
    if e["cheer"].is_object() {
        badges.push("cheer".into());
    }
    Some(Incoming {
        session_gen,
        room_gen,
        source_id: source_id.to_owned(),
        remote_id: Some(remote_id),
        idless_key: None,
        author_id: e["chatter_user_id"].as_str().map(str::to_owned),
        author_name: e["chatter_user_name"].as_str().unwrap_or("?").to_owned(),
        provider_ts: e["message_timestamp"].as_str().map(str::to_owned),
        text,
        fragments,
        badges,
        reply_to: e["reply"]["parent_message_id"]
            .as_str()
            .map(|id| crate::model::ReplyRef {
                remote_id: id.to_owned(),
                author_name: e["reply"]["parent_user_name"]
                    .as_str()
                    .unwrap_or("?")
                    .to_owned(),
            }),
        kind: MessageKind::Text,
    })
}

/// `channel.chat.notification` (subs, gifts, raids, announcements).
pub fn notice_notification(
    session_gen: u64,
    room_gen: u64,
    source_id: &str,
    _room: &str,
    n: &Notification,
) -> Option<Incoming> {
    if n.subscription_type != "channel.chat.notification" {
        return None;
    }
    let e = &n.event;
    let notice = e["notice_type"].as_str().unwrap_or("");
    let kind = match notice {
        "sub" | "resub" | "sub_gift" | "community_sub_gift" | "gift_paid_upgrade"
        | "prime_paid_upgrade" | "raid" | "unraid" | "pay_it_forward" | "announcement" => {
            MessageKind::Membership
        }
        "bits_badge_tier" => MessageKind::Paid(PaidInfo::GiftedSub { count: 0 }),
        _ => MessageKind::System,
    };
    let remote_id = e["message_id"].as_str()?.to_owned();
    Some(Incoming {
        session_gen,
        room_gen,
        source_id: source_id.to_owned(),
        remote_id: Some(remote_id),
        idless_key: None,
        author_id: e["chatter_user_id"].as_str().map(str::to_owned),
        author_name: e["chatter_user_name"].as_str().unwrap_or("—").to_owned(),
        provider_ts: e["message_timestamp"].as_str().map(str::to_owned),
        text: e["message"]["text"].as_str().unwrap_or_default().to_owned(),
        fragments: vec![],
        badges: vec![format!("notice:{notice}")],
        reply_to: None,
        kind,
    })
}

/// Moderation control actions from delete/clear subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteAction {
    DeleteMessage { remote_id: String },
    ClearRoom,
}

pub fn moderation_notification(n: &Notification) -> Option<DeleteAction> {
    match n.subscription_type.as_str() {
        "channel.chat.message_delete" => Some(DeleteAction::DeleteMessage {
            remote_id: n.event["target_message_id"].as_str()?.to_owned(),
        }),
        "channel.chat.clear" => Some(DeleteAction::ClearRoom),
        _ => None,
    }
}

/// Helix send payload. `reply_parent_id` uses the native reply field;
/// without native support the caller offers an explicit plain-message
/// fallback instead of cross-posting a reply elsewhere.
pub fn send_payload(
    broadcaster_id: &str,
    sender_id: &str,
    text: &str,
    reply_parent_id: Option<&str>,
) -> Result<Value, String> {
    if text.chars().count() > MAX_CHARS {
        return Err(format!("message exceeds {MAX_CHARS} characters"));
    }
    if text.trim().is_empty() {
        return Err("message is empty".into());
    }
    let mut body = serde_json::json!({
        "broadcaster_id": broadcaster_id,
        "sender_id": sender_id,
        "message": text,
    });
    if let Some(parent) = reply_parent_id {
        body["reply_parent_message_id"] = Value::String(parent.to_owned());
    }
    Ok(body)
}

/// Provider verdict: Helix answers 200 with `data[].is_sent`. HTTP
/// success with `is_sent=false` is a rejection, never a confirmation.
pub fn send_verdict(body: &Value) -> Result<Option<String>, String> {
    let data = body
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| "send response has no data".to_string())?;
    if data.get("is_sent").and_then(|v| v.as_bool()) == Some(true) {
        Ok(data
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned))
    } else {
        let reason = data
            .get("drop_reason")
            .and_then(|r| r.get("code"))
            .and_then(|v| v.as_str())
            .unwrap_or("rejected");
        Err(format!("provider refused send: {reason}"))
    }
}

pub fn source_key(room_login: &str) -> SourceKey {
    SourceKey {
        provider: crate::model::Provider::Twitch,
        room: room_login.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WELCOME: &str = r#"{"metadata":{"message_id":"w1","message_type":"session_welcome","message_timestamp":"2026-09-15T10:00:00Z"},"payload":{"session":{"id":"sess-1","status":"connected","keepalive_timeout_seconds":10,"reconnect_url":null,"connected_at":"2026-09-15T10:00:00Z"}}}"#;
    const NOTIF: &str = r#"{"metadata":{"message_id":"dup-1","message_type":"notification","message_timestamp":"2026-09-15T10:01:00Z"},"payload":{"subscription":{"id":"sub-1","type":"channel.chat.message","version":"1"},"event":{"broadcaster_user_id":"111","broadcaster_user_login":"alice","chatter_user_id":"222","chatter_user_name":"bob","message_id":"m-1","message":{"text":"hello","fragments":[{"type":"text","text":"hello"}]},"message_type":"text","badges":[{"set_id":"subscriber","id":"3","info":""}],"message_timestamp":"2026-09-15T10:01:00Z","reply":null}}}"#;
    const DELETE: &str = r#"{"metadata":{"message_id":"d1","message_type":"notification","message_timestamp":"2026-09-15T10:02:00Z"},"payload":{"subscription":{"id":"sub-2","type":"channel.chat.message_delete","version":"1"},"event":{"broadcaster_user_id":"111","target_user_id":"222","target_message_id":"m-1","message_timestamp":"2026-09-15T10:02:00Z"}}}"#;

    #[test]
    fn lifecycle_frames_parse() {
        assert!(
            matches!(parse_envelope(WELCOME).unwrap(), WsFrame::Welcome { session_id, keepalive_secs } if session_id == "sess-1" && keepalive_secs == 10)
        );
        assert!(matches!(
            parse_envelope(r#"{"metadata":{"message_type":"session_reconnect"},"payload":{"session":{"reconnect_url":"wss://x"}}}"#).unwrap(),
            WsFrame::Reconnect { reconnect_url: Some(_) }
        ));
        assert!(matches!(
            parse_envelope(r#"{"metadata":{"message_type":"revocation"},"payload":{"subscription":{"type":"channel.chat.message","status":"authorization_revoked"}}}"#).unwrap(),
            WsFrame::Revocation { status, .. } if status == "authorization_revoked"
        ));
    }

    #[test]
    fn message_parses_with_dedupe_id() {
        let WsFrame::Notification(n) = parse_envelope(NOTIF).unwrap() else {
            panic!("expected notification");
        };
        // Duplicate deliveries carry the same envelope message_id.
        let WsFrame::Notification(n2) = parse_envelope(NOTIF).unwrap() else {
            panic!();
        };
        assert_eq!(n.message_id, n2.message_id);
        let inc = message_notification(1, 0, "tw", "alice", &n).unwrap();
        assert_eq!(inc.remote_id.as_deref(), Some("m-1"));
        assert_eq!(inc.author_name, "bob");
        assert_eq!(inc.author_id.as_deref(), Some("222"));
        assert!(inc.badges.contains(&"subscriber".to_string()));
    }

    #[test]
    fn delete_routes_to_control() {
        let WsFrame::Notification(n) = parse_envelope(DELETE).unwrap() else {
            panic!();
        };
        assert_eq!(
            moderation_notification(&n),
            Some(DeleteAction::DeleteMessage {
                remote_id: "m-1".into()
            })
        );
        assert!(message_notification(1, 0, "tw", "alice", &n).is_none());
    }

    #[test]
    fn subscribe_body_targets_broadcaster_session() {
        let body = subscribe_payload("sess-1", "channel.chat.message", "111", "222");
        assert_eq!(body["transport"]["session_id"], "sess-1");
        assert_eq!(body["condition"]["broadcaster_user_id"], "111");
        assert_eq!(body["condition"]["user_id"], "222");
    }

    #[tokio::test]
    async fn subscribe_all_posts_four_and_accepts_409() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(409))
            .expect(4)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        subscribe_all(&http, &server.uri(), "cid", "tok", "sess-1", "111", "222")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn subscribe_all_surfaces_unauthorized() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let err = subscribe_all(&http, &server.uri(), "cid", "tok", "sess-1", "111", "222")
            .await
            .unwrap_err();
        assert!(err.contains("unauthorized"), "{err}");
    }

    #[test]
    fn send_validates_limits_and_verdict() {
        assert!(send_payload("1", "2", "", None).is_err());
        assert!(send_payload("1", "2", &"x".repeat(501), None).is_err());
        let ok = send_verdict(&serde_json::json!({"data":[{"message_id":"m","is_sent":true}]}));
        assert_eq!(ok, Ok(Some("m".into())));
        assert!(send_verdict(
            &serde_json::json!({"data":[{"is_sent":false,"drop_reason":{"code":"banned"}}]})
        )
        .is_err());
    }
}
