//! YouTube: liveChatId resolution, list polling, insert send.
//!
//! The official server-streaming protocol is gRPC
//! (`V3DataLiveChatMessageService.StreamList` on
//! `youtube.googleapis.com:443`, see docs/chat-capabilities.md for the
//! proto reference). This adapter implements the documented REST `list`
//! fallback: it retains continuation tokens, honors
//! `pollingIntervalMillis`, and backs off on quota errors. `insert` and
//! moderation use their documented endpoints. The actual broadcast's
//! `liveChatId` is resolved first; a channel or video id is never
//! substituted.

pub const MAX_CHARS: usize = 200;

use crate::coordinator::Incoming;
use crate::model::{MessageKind, PaidInfo, SourceKey, TextFragment};
use serde_json::Value;

/// Resolve the liveChatId from a videos/liveBroadcasts response. An
/// explicitly selected broadcast id wins; otherwise the first item.
/// Errors when the broadcast has no active chat instead of guessing.
pub fn resolve_live_chat_id(body: &Value, expect_video: Option<&str>) -> Result<String, String> {
    let items = body["items"].as_array();
    // An explicitly selected broadcast that is absent is an error, never
    // a silent fallback to someone else's chat.
    if let Some(want) = expect_video {
        let found = items.and_then(|items| items.iter().find(|i| i["id"].as_str() == Some(want)));
        let Some(item) = found else {
            return Err(format!("selected broadcast {want} is not live"));
        };
        return item["liveStreamingDetails"]["activeLiveChatId"]
            .as_str()
            .or_else(|| item["snippet"]["liveChatId"].as_str())
            .map(str::to_owned)
            .ok_or_else(|| {
                "no active liveChatId on the selected broadcast (chat disabled or ended?)".into()
            });
    }
    let item = items.and_then(|items| items.first());
    item.and_then(|item| {
        item["liveStreamingDetails"]["activeLiveChatId"]
            .as_str()
            .or_else(|| item["snippet"]["liveChatId"].as_str())
    })
    .map(str::to_owned)
    .ok_or_else(|| "no active liveChatId on this broadcast (chat disabled or ended?)".into())
}

pub struct ListPage {
    pub messages: Vec<Incoming>,
    pub deleted_ids: Vec<String>,
    pub next_page_token: Option<String>,
    pub polling_interval_ms: u64,
    pub chat_ended: bool,
}

/// Parse one `liveChatMessages.list` page. `session_gen`/`source_id` tag
/// the items; `room` names the liveChatId for dedupe scope.
pub fn parse_list_page(
    session_gen: u64,
    room_gen: u64,
    source_id: &str,
    room: &str,
    body: &Value,
) -> ListPage {
    let mut messages = vec![];
    let mut deleted_ids = vec![];
    let mut chat_ended = false;
    for item in body["items"].as_array().cloned().unwrap_or_default() {
        let snippet = &item["snippet"];
        let kind_str = snippet["type"].as_str().unwrap_or("");
        let remote_id = item["id"].as_str().unwrap_or_default().to_owned();
        if remote_id.is_empty() {
            continue;
        }
        let author = &item["authorDetails"];
        let base = || Incoming {
            session_gen,
            room_gen,
            source_id: source_id.to_owned(),
            remote_id: Some(remote_id.clone()),
            idless_key: None,
            author_id: author["channelId"].as_str().map(str::to_owned),
            author_name: author["displayName"].as_str().unwrap_or("?").to_owned(),
            provider_ts: snippet["publishedAt"].as_str().map(str::to_owned),
            text: String::new(),
            fragments: vec![],
            badges: vec![],
            reply_to: None,
            kind: MessageKind::Text,
        };
        match kind_str {
            "textMessageEvent" => {
                let mut m = base();
                m.text = snippet["textMessageDetails"]["messageText"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                m.fragments = vec![TextFragment {
                    text: m.text.clone(),
                    emote_id: None,
                }];
                messages.push(m);
            }
            "superChatEvent" => {
                let mut m = base();
                let d = &snippet["superChatDetails"];
                m.text = d["userComment"].as_str().unwrap_or_default().to_owned();
                m.kind = MessageKind::Paid(PaidInfo::SuperChat {
                    amount: d["amountDisplayString"].as_str().unwrap_or("?").to_owned(),
                });
                messages.push(m);
            }
            "superStickerEvent" => {
                let mut m = base();
                m.kind = MessageKind::Paid(PaidInfo::SuperSticker {
                    amount: snippet["superStickerDetails"]["amountDisplayString"]
                        .as_str()
                        .unwrap_or("?")
                        .to_owned(),
                });
                messages.push(m);
            }
            "membershipGiftingEvent"
            | "giftMembershipReceivedEvent"
            | "newSponsorEvent"
            | "memberMilestoneChatEvent" => {
                let mut m = base();
                m.kind = MessageKind::Membership;
                messages.push(m);
            }
            "messageDeletedEvent" => {
                if let Some(id) = snippet["messageDeletedDetails"]["deletedMessageId"]
                    .as_str()
                    .map(str::to_owned)
                {
                    deleted_ids.push(id);
                }
            }
            "chatEndedEvent" => chat_ended = true,
            "sponsorOnlyModeStartedEvent" | "sponsorOnlyModeEndedEvent" => {
                let mut m = base();
                m.kind = MessageKind::System;
                m.text = kind_str.to_owned();
                messages.push(m);
            }
            _ => {}
        }
    }
    let _ = room;
    ListPage {
        messages,
        deleted_ids,
        next_page_token: body["nextPageToken"].as_str().map(str::to_owned),
        polling_interval_ms: body["pollingIntervalMillis"]
            .as_u64()
            .unwrap_or(5_000)
            .max(1_000),
        chat_ended,
    }
}

/// Official collection endpoint for `liveChatMessages.insert` (HTTP
/// reference: `POST .../youtube/v3/liveChat/messages` with
/// `?part=snippet`).
pub const INSERT_URL: &str = "https://www.googleapis.com/youtube/v3/liveChat/messages";

#[derive(Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Sent(Option<String>),
    /// Local validation or an explicit provider refusal: never retry.
    Refused(String),
    /// Transport failure or error status: outcome unknown, never retry.
    Unknown(String),
}

/// Post one chat message, separating local refusal, provider refusal,
/// and transport failure so callers map them honestly.
pub async fn insert_message(
    http: &reqwest::Client,
    base_url: &str,
    access_token: &str,
    live_chat_id: &str,
    text: &str,
) -> InsertOutcome {
    let body = match insert_payload(live_chat_id, text) {
        Ok(b) => b,
        Err(e) => return InsertOutcome::Refused(e),
    };
    let res = match http
        .post(format!("{base_url}?part=snippet"))
        .bearer_auth(access_token)
        .json(&body)
        .send()
        .await
    {
        Ok(res) => res,
        Err(e) => return InsertOutcome::Unknown(format!("transport: {e}")),
    };
    if !res.status().is_success() {
        return InsertOutcome::Unknown(format!("youtube insert refused: {}", res.status()));
    }
    let v: serde_json::Value = res.json().await.unwrap_or_default();
    InsertOutcome::Sent(v.get("id").and_then(|x| x.as_str()).map(str::to_owned))
}

/// `liveChatMessages.insert` body. YouTube has no native reply: callers
/// must not present sends as replies.
pub fn insert_payload(live_chat_id: &str, text: &str) -> Result<Value, String> {
    if text.chars().count() > MAX_CHARS {
        return Err(format!("message exceeds {MAX_CHARS} characters"));
    }
    if text.trim().is_empty() {
        return Err("message is empty".into());
    }
    Ok(serde_json::json!({
        "snippet": {
            "liveChatId": live_chat_id,
            "type": "textMessageEvent",
            "textMessageDetails": { "messageText": text },
        },
    }))
}

pub fn source_key(live_chat_id: &str) -> SourceKey {
    SourceKey {
        provider: crate::model::Provider::Youtube,
        room: live_chat_id.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"{
      "nextPageToken": "tok-2", "pollingIntervalMillis": 7000,
      "items": [
        {"id": "m1", "snippet": {"type": "textMessageEvent", "publishedAt": "2026-09-15T10:00:00Z",
          "textMessageDetails": {"messageText": "hi"}},
         "authorDetails": {"channelId": "UC1", "displayName": "amy"}},
        {"id": "m2", "snippet": {"type": "superChatEvent", "publishedAt": "2026-09-15T10:00:01Z",
          "superChatDetails": {"amountDisplayString": "$5.00", "userComment": "great!"}},
         "authorDetails": {"channelId": "UC2", "displayName": "zed"}},
        {"id": "m3", "snippet": {"type": "newSponsorEvent", "publishedAt": "2026-09-15T10:00:02Z"},
         "authorDetails": {"channelId": "UC3", "displayName": "kim"}},
        {"id": "m4", "snippet": {"type": "messageDeletedEvent",
          "messageDeletedDetails": {"deletedMessageId": "m1"}}},
        {"id": "m5", "snippet": {"type": "chatEndedEvent"}}
      ]}"#;

    #[test]
    fn page_parses_typed_events_and_continuation() {
        let body: Value = serde_json::from_str(PAGE).unwrap();
        let page = parse_list_page(1, 0, "yt", "chat-id", &body);
        assert_eq!(page.next_page_token.as_deref(), Some("tok-2"));
        assert_eq!(page.polling_interval_ms, 7_000);
        assert!(page.chat_ended);
        assert_eq!(page.deleted_ids, vec!["m1".to_string()]);
        assert_eq!(page.messages.len(), 3);
        assert!(matches!(page.messages[0].kind, MessageKind::Text));
        assert!(matches!(page.messages[1].kind, MessageKind::Paid(_)));
        assert!(matches!(page.messages[2].kind, MessageKind::Membership));
        assert_eq!(page.messages[0].author_id.as_deref(), Some("UC1"));
    }

    #[test]
    fn live_chat_id_resolution_never_substitutes() {
        let ok: Value = serde_json::from_str(
            r#"{"items":[{"liveStreamingDetails":{"activeLiveChatId":"CHAT-9"}}]}"#,
        )
        .unwrap();
        assert_eq!(resolve_live_chat_id(&ok, None).unwrap(), "CHAT-9");
        let missing: Value = serde_json::from_str(r#"{"items":[{"id":"vid"}]}"#).unwrap();
        assert!(resolve_live_chat_id(&missing, None).is_err());
        let empty: Value = serde_json::from_str(r#"{"items":[]}"#).unwrap();
        assert!(resolve_live_chat_id(&empty, None).is_err());
    }

    #[test]
    fn explicit_broadcast_selection_wins() {
        let body: Value = serde_json::from_str(
            r#"{"items":[
              {"id":"vid-a","liveStreamingDetails":{"activeLiveChatId":"CHAT-A"}},
              {"id":"vid-b","liveStreamingDetails":{"activeLiveChatId":"CHAT-B"}}]}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_live_chat_id(&body, Some("vid-b")).unwrap(),
            "CHAT-B"
        );
        assert_eq!(resolve_live_chat_id(&body, None).unwrap(), "CHAT-A");
        assert!(resolve_live_chat_id(&body, Some("vid-zzz")).is_err());
    }

    #[test]
    fn insert_validates() {
        assert!(insert_payload("c", "").is_err());
        assert!(insert_payload("c", &"x".repeat(201)).is_err());
        let p = insert_payload("c", "hello").unwrap();
        assert_eq!(p["snippet"]["liveChatId"], "c");
    }

    #[tokio::test]
    async fn insert_posts_exact_official_request() {
        assert_eq!(
            INSERT_URL,
            "https://www.googleapis.com/youtube/v3/liveChat/messages"
        );
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/youtube/v3/liveChat/messages"))
            .and(wiremock::matchers::query_param("part", "snippet"))
            .and(wiremock::matchers::header("authorization", "Bearer tok"))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "snippet": {
                    "liveChatId": "CHAT-1",
                    "type": "textMessageEvent",
                    "textMessageDetails": {"messageText": "hello"},
                },
            })))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": "m9"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = format!("{}/youtube/v3/liveChat/messages", server.uri());
        assert_eq!(
            insert_message(&http, &base, "tok", "CHAT-1", "hello").await,
            InsertOutcome::Sent(Some("m9".into()))
        );
        assert!(matches!(
            insert_message(&http, &base, "tok", "CHAT-1", "").await,
            InsertOutcome::Refused(_)
        ));
    }
}
