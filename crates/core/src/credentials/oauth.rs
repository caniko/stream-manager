//! Operator-owned OAuth flows per provider.
//!
//! The flows differ per provider and are modelled explicitly:
//! - Twitch: authorization code + PKCE optional, token at id.twitch.tv.
//! - YouTube: authorization code + PKCE, token at oauth2.googleapis.com.
//! - Kick: authorization code + PKCE **and** client secret at id.kick.com.
//!   The secret is operator-provisioned at runtime (agenix bootstrap file),
//!   never embedded in the binary.
//! - Rumble: no documented OAuth chat scope; read-only API URL instead.
//!
//! Refresh tokens persist only through [`crate::credentials::TokenStore`].
//! `state` is always generated and validated; redirect targets are the
//! operator's loopback/desktop handler, never a third party.

use crate::credentials::Secret;
use oauth2::{CsrfToken, PkceCodeChallenge, PkceCodeVerifier, Scope};

/// Provider OAuth endpoints and scope requirements (reviewed 2026-09-15;
/// see docs/chat-capabilities.md).
#[derive(Debug, Clone)]
pub struct OAuthProvider {
    pub authorize_url: &'static str,
    pub token_url: &'static str,
    pub revoke_url: Option<&'static str>,
    /// Scopes needed to *read* chat (never request more for reading).
    pub chat_read_scopes: &'static [&'static str],
    /// Additional scopes needed to *send* chat.
    pub chat_write_scopes: &'static [&'static str],
    /// Additional scopes needed for moderation endpoints.
    pub moderation_scopes: &'static [&'static str],
    /// True when the token exchange also requires the client secret.
    pub requires_client_secret: bool,
}

pub const TWITCH: OAuthProvider = OAuthProvider {
    authorize_url: "https://id.twitch.tv/oauth2/authorize",
    token_url: "https://id.twitch.tv/oauth2/token",
    revoke_url: Some("https://id.twitch.tv/oauth2/revoke"),
    chat_read_scopes: &["user:read:chat"],
    chat_write_scopes: &["user:write:chat"],
    moderation_scopes: &[
        "moderator:manage:chat_messages",
        "moderator:manage:banned_users",
    ],
    requires_client_secret: false,
};

pub const YOUTUBE: OAuthProvider = OAuthProvider {
    authorize_url: "https://accounts.google.com/o/oauth2/v2/auth",
    token_url: "https://oauth2.googleapis.com/token",
    revoke_url: Some("https://oauth2.googleapis.com/revoke"),
    chat_read_scopes: &["https://www.googleapis.com/auth/youtube.readonly"],
    chat_write_scopes: &["https://www.googleapis.com/auth/youtube.force-ssl"],
    moderation_scopes: &["https://www.googleapis.com/auth/youtube.force-ssl"],
    requires_client_secret: false,
};

pub const KICK: OAuthProvider = OAuthProvider {
    authorize_url: "https://id.kick.com/oauth/authorize",
    token_url: "https://id.kick.com/oauth/token",
    revoke_url: None,
    chat_read_scopes: &["events:subscribe"],
    chat_write_scopes: &["chat:write"],
    moderation_scopes: &["moderation:chat_message:manage", "moderation:ban"],
    requires_client_secret: true,
};

/// Pending authorization request: PKCE verifier plus the `state` the
/// callback must echo. Serialize the verifier alongside the state while
/// the operator completes the browser step; both are single-use.
pub struct AuthRequest {
    pub authorize_url: String,
    pub state: String,
    verifier: PkceCodeVerifier,
}

impl AuthRequest {
    pub fn begin(
        provider: &OAuthProvider,
        client_id: &str,
        redirect_uri: &str,
        with_write: bool,
        with_moderate: bool,
    ) -> Self {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let state = CsrfToken::new_random();
        let mut scopes: Vec<&str> = provider.chat_read_scopes.to_vec();
        if with_write {
            scopes.extend(provider.chat_write_scopes.iter().copied());
        }
        if with_moderate {
            scopes.extend(provider.moderation_scopes.iter().copied());
        }
        // One space-delimited scope parameter per RFC 6749: repeated
        // `scope=` keys have inconsistent provider support.
        let scope_param = urlencoding(&scopes.join(" "));
        let url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&state={}&code_challenge={}&code_challenge_method=S256&scope={}",
            provider.authorize_url,
            urlencoding(client_id),
            urlencoding(redirect_uri),
            state.secret(),
            challenge.as_str(),
            scope_param,
        );
        Self {
            authorize_url: url,
            state: state.secret().clone(),
            verifier,
        }
    }

    /// Validate the echoed state, consuming this request. A mismatch is a
    /// hard error: never exchange the code.
    pub fn accept_callback(self, state: &str, _code: &str) -> Result<CodeExchange, String> {
        if subtle_eq(&self.state, state) {
            Ok(CodeExchange {
                verifier: self.verifier.secret().clone(),
            })
        } else {
            Err("OAuth state mismatch: refusing code exchange".into())
        }
    }
}

fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

fn subtle_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Validated exchange material: the code was accepted with matching state.
pub struct CodeExchange {
    verifier: String,
}

impl CodeExchange {
    pub fn code_verifier(&self) -> &str {
        &self.verifier
    }

    pub fn _scope_hint(_s: Scope) -> bool {
        true
    }
}

/// Allowlisted provider error classification: arbitrary response bodies
/// must not reach UI errors or logs (they can echo tokens, secrets, or
/// account data). Only the HTTP status plus a short, known-safe `error`
/// code is reported; everything else becomes "request refused".
fn redacted(status: reqwest::StatusCode, body: &serde_json::Value) -> String {
    const LIMIT: usize = 64;
    let code = body
        .get("error")
        .and_then(|e| e.as_str())
        .or_else(|| body.get("code").and_then(|e| e.as_str()))
        .filter(|s| {
            // Codes only: short ASCII tokens, never free-form messages.
            s.len() <= LIMIT && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
        .unwrap_or("request refused");
    format!("{status}: {code}")
}

/// Best-effort provider revocation for logout. Failures are returned so
/// the caller can report them, but logout itself never depends on the
/// network: local state is always cleared first.
pub async fn revoke_access_token(
    revoke_url: &str,
    client_id: &str,
    access_token: &Secret,
    http: &reqwest::Client,
) -> anyhow::Result<()> {
    let res = http
        .post(revoke_url)
        .form(&[("token", access_token.expose()), ("client_id", client_id)])
        .send()
        .await?;
    if res.status().is_success() {
        Ok(())
    } else {
        anyhow::bail!(
            "revoke refused: {}",
            redacted(res.status(), &serde_json::Value::Null)
        )
    }
}

/// Refresh an access token (serialized per account by the caller via
/// [`crate::credentials::RefreshLocks`]). Returns the new access token;
/// rotation persistence stays with the caller so refresh + store are one
/// critical section.
pub async fn refresh_access_token(
    token_url: &str,
    client_id: &str,
    client_secret: Option<&Secret>,
    refresh_token: &Secret,
    http: &reqwest::Client,
) -> anyhow::Result<Refreshed> {
    let mut form = vec![
        ("grant_type", "refresh_token".to_string()),
        ("client_id", client_id.to_string()),
        ("refresh_token", refresh_token.expose().to_owned()),
    ];
    if let Some(s) = client_secret {
        form.push(("client_secret", s.expose().to_owned()));
    }
    let res = http.post(token_url).form(&form).send().await?;
    let status = res.status();
    let body: serde_json::Value = res.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!("token refresh refused: {}", redacted(status, &body));
    }
    let access = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("refresh response missing access_token"))?
        .to_owned();
    Ok(Refreshed {
        access_token: Secret::new(access),
        new_refresh: body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .map(|s| Secret::new(s.to_owned())),
        expires_in: body
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600),
        scope: body
            .get("scope")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned(),
    })
}

pub struct Refreshed {
    pub access_token: Secret,
    pub new_refresh: Option<Secret>,
    pub expires_in: u64,
    /// Granted scope string: the caller must verify required scopes are
    /// present before declaring the account connected.
    pub scope: String,
}

/// Exchange parameters: Kick requires the client secret here as well as
/// PKCE; it arrives as a runtime secret, never from the binary.
#[derive(Debug, Clone, Copy)]
pub struct ExchangeParams<'a> {
    pub token_url: &'a str,
    pub requires_client_secret: bool,
    pub client_id: &'a str,
    pub client_secret: Option<&'a Secret>,
    pub redirect_uri: &'a str,
    pub code: &'a str,
    pub code_verifier: &'a str,
}

/// Exchange an authorization code after state validation.
pub async fn exchange_code(
    p: &ExchangeParams<'_>,
    http: &reqwest::Client,
) -> anyhow::Result<Refreshed> {
    if p.requires_client_secret && p.client_secret.is_none() {
        anyhow::bail!("kick token exchange needs the operator client secret");
    }
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", p.code.to_string()),
        ("redirect_uri", p.redirect_uri.to_string()),
        ("client_id", p.client_id.to_string()),
        ("code_verifier", p.code_verifier.to_string()),
    ];
    if let Some(s) = p.client_secret {
        form.push(("client_secret", s.expose().to_owned()));
    }
    let res = http.post(p.token_url).form(&form).send().await?;
    let status = res.status();
    let body: serde_json::Value = res.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        anyhow::bail!("code exchange refused: {}", redacted(status, &body));
    }
    let access = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("exchange response missing access_token"))?
        .to_owned();
    let scope = match body.get("scope") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    };
    Ok(Refreshed {
        access_token: Secret::new(access),
        new_refresh: body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .map(|s| Secret::new(s.to_owned())),
        expires_in: body
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600),
        scope,
    })
}

/// Verified identity + granted scopes from a provider introspection
/// response body. Pure parsing so fixtures pin the shapes.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedIdentity {
    pub user_id: String,
    pub login: String,
    pub scopes: Vec<String>,
}

pub fn parse_twitch_validate(body: &serde_json::Value) -> Result<VerifiedIdentity, String> {
    Ok(VerifiedIdentity {
        user_id: body["user_id"]
            .as_str()
            .ok_or("twitch validate: no user_id")?
            .to_owned(),
        login: body["login"]
            .as_str()
            .ok_or("twitch validate: no login")?
            .to_owned(),
        scopes: body["scopes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
    })
}

pub fn parse_youtube_tokeninfo(body: &serde_json::Value) -> Result<Vec<String>, String> {
    body["scope"]
        .as_str()
        .map(|s| s.split_whitespace().map(str::to_owned).collect())
        .ok_or_else(|| "youtube tokeninfo: no scope".into())
}

pub fn parse_kick_users(body: &serde_json::Value) -> Result<VerifiedIdentity, String> {
    let first = body["data"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or("kick users: empty data")?;
    Ok(VerifiedIdentity {
        user_id: first["user_id"]
            .as_str()
            .or_else(|| first["id"].as_str())
            .ok_or("kick users: no user id")?
            .to_owned(),
        login: first["name"]
            .as_str()
            .or_else(|| first["slug"].as_str())
            .ok_or("kick users: no name")?
            .to_owned(),
        scopes: vec![],
    })
}

/// Verify granted scopes cover the required set before declaring an
/// account connected. Reading must not require write/moderation scopes.
pub fn scopes_cover(granted_scope: &str, required: &[&str]) -> bool {
    let granted: Vec<&str> = granted_scope.split_whitespace().collect();
    required.iter().all(|r| granted.contains(r))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_mismatch_refuses_exchange() {
        let req = AuthRequest::begin(&TWITCH, "cid", "http://127.0.0.1:8912/cb", false, false);
        assert!(
            req.authorize_url.contains("user%3Aread%3Achat")
                || req.authorize_url.contains("user:read:chat")
        );
        assert!(
            !req.authorize_url.contains("user%3Awrite%3Achat")
                && !req.authorize_url.contains("user:write:chat")
        );
        // Exactly one scope parameter, space-delimited.
        assert_eq!(req.authorize_url.matches("scope=").count(), 1);
        assert!(req.accept_callback("wrong-state", "code").is_err());
        let req = AuthRequest::begin(&TWITCH, "cid", "http://127.0.0.1:8912/cb", false, false);
        let state2 = req.state.clone();
        assert!(req.accept_callback(&state2, "code").is_ok());
    }

    #[test]
    fn scope_tiers_are_additive() {
        let read = AuthRequest::begin(&TWITCH, "cid", "http://127.0.0.1:8912/cb", false, false);
        let write = AuthRequest::begin(&TWITCH, "cid", "http://127.0.0.1:8912/cb", true, false);
        let full = AuthRequest::begin(&TWITCH, "cid", "http://127.0.0.1:8912/cb", true, true);
        assert!(!read.authorize_url.contains("write"));
        assert!(write.authorize_url.contains("user%3Awrite%3Achat"));
        assert!(!write.authorize_url.contains("moderator"));
        assert!(full
            .authorize_url
            .contains("moderator%3Amanage%3Abanned_users"));
        assert_eq!(full.authorize_url.matches("scope=").count(), 1);
    }

    #[test]
    fn read_only_scope_separation() {
        assert!(TWITCH.chat_read_scopes.iter().all(|s| !s.contains("write")));
        for (provider, secret) in [(KICK, true), (TWITCH, false), (YOUTUBE, false)] {
            assert_eq!(provider.requires_client_secret, secret);
        }
    }

    #[test]
    fn redaction_reports_codes_never_bodies() {
        let status = reqwest::StatusCode::BAD_REQUEST;
        assert_eq!(
            redacted(status, &serde_json::json!({"error": "invalid_grant"})),
            "400 Bad Request: invalid_grant"
        );
        // Secrets and prose never pass through, however short.
        assert_eq!(
            redacted(status, &serde_json::json!({"access_token": "abc"})),
            "400 Bad Request: request refused"
        );
        assert_eq!(
            redacted(status, &serde_json::json!({"error": "oops happened"})),
            "400 Bad Request: request refused"
        );
        // Multibyte truncation cannot panic: classification only.
        assert_eq!(
            redacted(status, &serde_json::json!({"error": "ä".repeat(500)})),
            "400 Bad Request: request refused"
        );
    }

    #[test]
    fn scope_check_is_exact() {
        assert!(scopes_cover(
            "user:read:chat user:write:chat",
            &["user:read:chat"]
        ));
        assert!(!scopes_cover("user:read:chat", &["user:write:chat"]));
    }

    #[tokio::test]
    async fn refresh_posts_form_and_parses() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 100,
                    "scope": "user:read:chat",
                })),
            )
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let out = refresh_access_token(
            &server.uri(),
            "cid",
            None,
            &Secret::new("old-refresh".into()),
            &http,
        )
        .await
        .unwrap();
        assert_eq!(out.access_token.expose(), "new-access");
        assert!(scopes_cover(&out.scope, &["user:read:chat"]));
    }

    #[test]
    fn identity_parsers_pin_provider_shapes() {
        let tw: serde_json::Value = serde_json::from_str(
            r#"{"client_id":"c","login":"alice","scopes":["user:read:chat"],"user_id":"111","expires_in":500}"#,
        )
        .unwrap();
        let id = parse_twitch_validate(&tw).unwrap();
        assert_eq!((id.login.as_str(), id.user_id.as_str()), ("alice", "111"));
        assert!(scopes_cover(&id.scopes.join(" "), &["user:read:chat"]));
        let yt: serde_json::Value = serde_json::from_str(
            r#"{"scope": "https://www.googleapis.com/auth/youtube.readonly"}"#,
        )
        .unwrap();
        assert_eq!(parse_youtube_tokeninfo(&yt).unwrap().len(), 1);
        let kk: serde_json::Value =
            serde_json::from_str(r#"{"data":[{"user_id":7,"name":"bob"}]}"#).unwrap();
        assert!(
            parse_kick_users(&kk).is_err(),
            "numeric ids must not parse as strings"
        );
        let kk2: serde_json::Value =
            serde_json::from_str(r#"{"data":[{"user_id":"7","name":"bob"}]}"#).unwrap();
        assert_eq!(parse_kick_users(&kk2).unwrap().login, "bob");
    }

    #[tokio::test]
    async fn exchange_posts_code_verifier_and_secret() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::body_string_contains("code_verifier=v"))
            .and(wiremock::matchers::body_string_contains("client_secret=s"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "a",
                    "refresh_token": "r",
                    "expires_in": 10,
                    "scope": ["user:read:chat"],
                })),
            )
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let secret = Secret::new("s".into());
        let params = ExchangeParams {
            token_url: &server.uri(),
            requires_client_secret: true,
            client_id: "cid",
            client_secret: Some(&secret),
            redirect_uri: "http://127.0.0.1:8912/callback",
            code: "code-1",
            code_verifier: "v",
        };
        let out = exchange_code(&params, &http).await.unwrap();
        assert_eq!(out.access_token.expose(), "a");
        assert!(scopes_cover(&out.scope, &["user:read:chat"]));
        // Kick-shaped flow without its required secret is refused locally.
        let no_secret = ExchangeParams {
            client_secret: None,
            ..params
        };
        assert!(exchange_code(&no_secret, &http).await.is_err());
    }

    #[tokio::test]
    async fn refresh_refusal_is_an_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"error": "invalid_grant"})),
            )
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        assert!(refresh_access_token(
            &server.uri(),
            "cid",
            None,
            &Secret::new("dead".into()),
            &http
        )
        .await
        .is_err());
    }
}
