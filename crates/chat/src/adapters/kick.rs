//! Kick: webhook receive via an operator relay, documented send endpoints.
//!
//! Provider webhooks cannot reach a desktop directly, so Kick delivery is:
//! public HTTPS ingress (operator reverse proxy) -> opt-in relay binary ->
//! authenticated outbound stream -> this desktop app. Without configured
//! relay access the source shows `Relay required`; other providers still
//! work. Nothing here opens a listener: ingress lives in `crates/relay`.
//!
//! Signature: RSA PKCS#1 v1.5 + SHA-256 over
//! `messageId.timestamp.rawBody` (base64), verified against Kick's public
//! key — never a guessed HMAC.

pub const MAX_CHARS: usize = 500;
/// Reject webhook timestamps older than this (replay protection).
pub const MAX_SKEW_SECS: i64 = 300;

use crate::coordinator::Incoming;
use crate::model::{MessageKind, SourceKey, TextFragment};
use serde_json::Value;
use std::collections::{HashSet, VecDeque};

/// Verify a Kick webhook signature. All inputs are the exact header/body
/// bytes; any parse or crypto failure is a rejection, never a bypass.
pub fn verify_signature(
    public_key_pem: &str,
    message_id: &str,
    timestamp: &str,
    raw_body: &[u8],
    signature_b64: &str,
) -> Result<(), String> {
    use base64::Engine as _;
    use rsa::pkcs1v15::VerifyingKey;
    use rsa::pkcs8::DecodePublicKey;
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;
    let key = rsa::RsaPublicKey::from_public_key_pem(public_key_pem)
        .map_err(|e| format!("kick public key unusable: {e}"))?;
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_b64.trim())
        .map_err(|e| format!("kick signature not base64: {e}"))?;
    let mut signed = Vec::with_capacity(message_id.len() + timestamp.len() + raw_body.len() + 2);
    signed.extend_from_slice(message_id.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(timestamp.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(raw_body);
    let vk = VerifyingKey::<Sha256>::new(key);
    let sig = rsa::pkcs1v15::Signature::try_from(sig_bytes.as_slice())
        .map_err(|_| "kick signature has wrong length".to_string())?;
    vk.verify(&signed, &sig)
        .map_err(|_| "kick signature mismatch".to_string())
}

/// Bounded replay/dedupe window for webhook ingress.
pub struct ReplayWindow {
    seen: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl ReplayWindow {
    pub fn new(cap: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            cap: cap.max(16),
        }
    }

    /// Returns false when the message id was already processed.
    pub fn check(&mut self, message_id: &str) -> bool {
        if !self.seen.insert(message_id.to_owned()) {
            return false;
        }
        self.order.push_back(message_id.to_owned());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

/// Ingress decision before parsing: timestamp freshness, channel
/// authorization, then dedupe. Visible gap reporting is the caller's job.
#[derive(Debug, PartialEq, Eq)]
pub enum Ingress {
    Accept,
    StaleTimestamp,
    ForbiddenChannel,
    Duplicate,
}

pub fn ingress_check(
    window: &mut ReplayWindow,
    now_unix: i64,
    timestamp_rfc3339: &str,
    channel: &str,
    allowed_channels: &[String],
    message_id: &str,
) -> Ingress {
    let ts = chrono::DateTime::parse_from_rfc3339(timestamp_rfc3339)
        .map(|t| t.timestamp())
        .unwrap_or(i64::MIN);
    if (now_unix - ts).abs() > MAX_SKEW_SECS {
        return Ingress::StaleTimestamp;
    }
    if !allowed_channels.iter().any(|c| c == channel) {
        return Ingress::ForbiddenChannel;
    }
    if !window.check(message_id) {
        return Ingress::Duplicate;
    }
    Ingress::Accept
}

#[derive(Debug, Clone)]
pub struct KickChatEvent {
    pub event_type: String,
    pub message_id: String,
    pub broadcaster: String,
    pub broadcaster_id: String,
    pub body: Value,
}

fn user_id_string(v: &Value) -> Option<String> {
    v.as_str()
        .map(str::to_owned)
        .or_else(|| v.as_i64().map(|n| n.to_string()))
        .or_else(|| v.as_u64().map(|n| n.to_string()))
}

/// Parse a verified `chat.message.sent` body following the official
/// webhook payload shape. The event type arrives in the
/// `Kick-Event-Type` header (preserved by the relay envelope), not in
/// the body. Numeric user ids are stringified; a missing message id is a
/// hard error — never an empty-string dedupe key.
pub fn parse_chat_message(event_type: &str, body: &Value) -> Result<KickChatEvent, String> {
    if event_type != "chat.message.sent" {
        return Err(format!("unsupported kick event: {event_type}"));
    }
    let message_id = body["message_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "kick message has no id".to_string())?
        .to_owned();
    let broadcaster = body["broadcaster"]["username"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "kick message has no broadcaster".to_string())?
        .to_owned();
    Ok(KickChatEvent {
        event_type: event_type.to_owned(),
        message_id,
        broadcaster,
        broadcaster_id: user_id_string(&body["broadcaster"]["user_id"]).unwrap_or_default(),
        body: body.clone(),
    })
}

/// Relay envelope: the relay verifies the provider signature, then
/// forwards the event type/version/id plus the untouched original body.
/// Desktops never re-verify, but they never guess the type either.
pub fn envelope_line(
    event_type: &str,
    version: &str,
    message_id: &str,
    timestamp: &str,
    body: &Value,
) -> Vec<u8> {
    let mut line = serde_json::to_vec(&serde_json::json!({
        "kick_event_type": event_type,
        "kick_event_version": version,
        "kick_event_message_id": message_id,
        "kick_event_timestamp": timestamp,
        "body": body,
    }))
    .unwrap_or_default();
    line.push(b'\n');
    line
}

pub fn parse_envelope_line(line: &[u8]) -> Result<(String, Value), String> {
    let v: Value = serde_json::from_slice(line).map_err(|e| format!("relay line not JSON: {e}"))?;
    let event_type = v["kick_event_type"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "relay line missing event type".to_string())?
        .to_owned();
    let body = v.get("body").cloned().unwrap_or(Value::Null);
    Ok((event_type, body))
}

pub fn message_event(
    session_gen: u64,
    room_gen: u64,
    source_id: &str,
    ev: &KickChatEvent,
) -> Incoming {
    let sender = &ev.body["sender"];
    let content = ev.body["content"].as_str().unwrap_or_default().to_owned();
    let mut badges = vec![];
    for b in sender["identity"]["badges"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        if let Some(t) = b["type"].as_str() {
            badges.push(t.to_owned());
        }
    }
    // Text fallback is always the full content; emote positions are not
    // rendered (documented limitation), ids are preserved for inspection.
    let fragments = vec![TextFragment {
        text: content.clone(),
        emote_id: None,
    }];
    Incoming {
        session_gen,
        room_gen,
        source_id: source_id.to_owned(),
        remote_id: Some(ev.message_id.clone()),
        idless_key: None,
        author_id: user_id_string(&sender["user_id"]),
        author_name: sender["username"].as_str().unwrap_or("?").to_owned(),
        provider_ts: ev.body["created_at"].as_str().map(str::to_owned),
        text: content,
        fragments,
        badges,
        reply_to: ev.body["replies_to"]["message_id"]
            .as_str()
            .map(|id| crate::model::ReplyRef {
                remote_id: id.to_owned(),
                author_name: ev.body["replies_to"]["sender"]["username"]
                    .as_str()
                    .unwrap_or("?")
                    .to_owned(),
            }),
        kind: MessageKind::Text,
    }
}

/// Documented send body (`POST /public/v1/chat`). `broadcaster_user_id`
/// vs `user_id` targeting is the caller's explicit choice, honored here.
pub fn send_payload(
    broadcaster_user_id: &str,
    content: &str,
    reply_to: Option<&str>,
) -> Result<Value, String> {
    if content.trim().is_empty() {
        return Err("message is empty".into());
    }
    if content.chars().count() > MAX_CHARS {
        return Err(format!("message exceeds {MAX_CHARS} characters"));
    }
    if content.len() > 2048 {
        return Err("message exceeds 2048 UTF-8 bytes".into());
    }
    let broadcaster: i64 = broadcaster_user_id
        .parse()
        .map_err(|_| "kick broadcaster id must be numeric — reconnect the source".to_string())?;
    let mut body = serde_json::json!({
        "broadcaster_user_id": broadcaster,
        "content": content,
        "type": "user",
    });
    if let Some(parent) = reply_to {
        body["reply_to_message_id"] = Value::String(parent.to_owned());
    }
    Ok(body)
}

/// Provider verdict: `data.is_sent` decides. HTTP success with
/// `is_sent=false` is a rejection, never a confirmation.
pub fn send_verdict(body: &Value) -> Result<Option<String>, String> {
    let data = body
        .get("data")
        .ok_or_else(|| "kick send: no data".to_string())?;
    if data.get("is_sent").and_then(|v| v.as_bool()) == Some(true) {
        Ok(data
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned))
    } else {
        Err("kick refused the send (is_sent=false)".into())
    }
}

/// Documented delete (`DELETE /public/v1/chat/{message_id}`, scope
/// `moderation:chat_message:manage`).
pub fn delete_path(message_id: &str) -> String {
    format!("https://api.kick.com/public/v1/chat/{message_id}")
}

pub fn source_key(channel: &str) -> SourceKey {
    SourceKey {
        provider: crate::model::Provider::Kick,
        room: channel.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> (String, rsa::RsaPrivateKey) {
        use rsa::pkcs8::EncodePublicKey;
        let mut rng = rand::thread_rng();
        let privkey = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pubkey = rsa::RsaPublicKey::from(&privkey);
        let pem = pubkey
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();
        (pem, privkey)
    }

    fn sign(privkey: &rsa::RsaPrivateKey, message_id: &str, ts: &str, body: &[u8]) -> String {
        use base64::Engine as _;
        use rsa::pkcs1v15::SigningKey;
        use rsa::sha2::Sha256;
        use rsa::signature::{SignatureEncoding as _, Signer as _};
        let mut signed = vec![];
        signed.extend_from_slice(message_id.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(ts.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(body);
        let sk = SigningKey::<Sha256>::new(privkey.clone());
        let sig = sk.sign(&signed);
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
    }

    /// Official `chat.message.sent` shape (numeric ids, `replies_to`,
    /// sender identity badges, emote positions).
    const BODY: &[u8] = br#"{"message_id":"m1","replies_to":{"message_id":"m0","content":"parent","sender":{"user_id":9,"username":"zed"}},"broadcaster":{"user_id":123456789,"username":"alice"},"sender":{"user_id":987654321,"username":"bob","identity":{"badges":[{"text":"Subscriber","type":"subscriber","count":3}]}},"content":"hi [emote:4148074:HYPERCLAP]","emotes":[{"emote_id":"4148074","positions":[{"s":3,"e":27}]}],"created_at":"2026-09-15T10:00:00Z"}"#;

    #[test]
    fn signature_roundtrip_and_tamper_reject() {
        let (pem, privkey) = test_key();
        let ts = "2026-09-15T10:00:01Z";
        let sig = sign(&privkey, "m1", ts, BODY);
        assert!(verify_signature(&pem, "m1", ts, BODY, &sig).is_ok());
        assert!(verify_signature(&pem, "m1", ts, b"tampered", &sig).is_err());
        assert!(verify_signature(&pem, "other", ts, BODY, &sig).is_err());
        assert!(verify_signature("not-a-key", "m1", ts, BODY, &sig).is_err());
    }

    #[test]
    fn ingress_guards_replay_channel_and_dedupe() {
        let mut w = ReplayWindow::new(64);
        let allowed = vec!["alice".to_string()];
        let now = chrono::DateTime::parse_from_rfc3339("2026-09-15T10:00:00Z")
            .unwrap()
            .timestamp();
        assert_eq!(
            ingress_check(&mut w, now, "2026-09-15T10:00:00Z", "alice", &allowed, "m1"),
            Ingress::Accept
        );
        assert_eq!(
            ingress_check(&mut w, now, "2026-09-15T10:00:00Z", "alice", &allowed, "m1"),
            Ingress::Duplicate
        );
        assert_eq!(
            ingress_check(&mut w, now, "2026-09-15T09:40:00Z", "alice", &allowed, "m2"),
            Ingress::StaleTimestamp
        );
        assert_eq!(
            ingress_check(
                &mut w,
                now,
                "2026-09-15T10:00:00Z",
                "mallory",
                &allowed,
                "m3"
            ),
            Ingress::ForbiddenChannel
        );
    }

    #[test]
    fn official_payload_parses_with_numeric_ids() {
        let body: Value = serde_json::from_slice(BODY).unwrap();
        let ev = parse_chat_message("chat.message.sent", &body).unwrap();
        assert_eq!(ev.broadcaster, "alice");
        assert_eq!(ev.broadcaster_id, "123456789");
        assert_eq!(ev.message_id, "m1");
        let inc = message_event(1, 0, "kk", &ev);
        assert_eq!(inc.author_name, "bob");
        assert_eq!(inc.author_id.as_deref(), Some("987654321"));
        assert_eq!(inc.remote_id.as_deref(), Some("m1"));
        assert!(inc.badges.contains(&"subscriber".to_string()));
        let reply = inc.reply_to.expect("replies_to maps to a reply");
        assert_eq!(reply.remote_id, "m0");
        // Missing ids and wrong types are hard errors, never empty keys.
        assert!(parse_chat_message("moderation.banned", &body).is_err());
        assert!(parse_chat_message("chat.message.sent", &serde_json::json!({})).is_err());
    }

    #[test]
    fn envelope_roundtrips_type_and_body() {
        let body: Value = serde_json::from_slice(BODY).unwrap();
        let line = envelope_line(
            "chat.message.sent",
            "1",
            "m1",
            "2026-09-15T10:00:01Z",
            &body,
        );
        let (event_type, back) = parse_envelope_line(&line).unwrap();
        assert_eq!(event_type, "chat.message.sent");
        assert_eq!(back["message_id"], "m1");
        assert!(parse_envelope_line(b"not json\n").is_err());
    }

    #[test]
    fn send_payload_matches_documented_shape() {
        let p = send_payload("7", "hi", Some("parent-uuid")).unwrap();
        assert_eq!(p["broadcaster_user_id"], 7);
        assert_eq!(p["type"], "user");
        assert_eq!(p["reply_to_message_id"], "parent-uuid");
        assert!(send_payload("not-a-number", "hi", None).is_err());
        assert!(send_payload("7", "", None).is_err());
        assert!(send_payload("7", &"x".repeat(501), None).is_err());
        assert!(
            send_payload("7", &"ä".repeat(1025), None).is_err(),
            "byte ceiling"
        );
    }

    #[test]
    fn send_verdict_honors_is_sent() {
        let ok: Value =
            serde_json::from_str(r#"{"data":{"is_sent":true,"message_id":"uuid-1"}}"#).unwrap();
        assert_eq!(send_verdict(&ok), Ok(Some("uuid-1".into())));
        let no: Value = serde_json::from_str(r#"{"data":{"is_sent":false}}"#).unwrap();
        assert!(send_verdict(&no).is_err());
    }
}
