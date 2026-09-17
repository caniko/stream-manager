//! Minimal documented HTTP layer for provider adapters.
//!
//! Every write is preceded by an account-match check: the authenticated
//! channel must equal the selected destination's channel before any
//! request leaves the machine. Transient mapping (401/429/5xx) is
//! explicit so publish can retry, refresh, or block accordingly.

use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("account mismatch: authed={authed} destination={destination}")]
    AccountMismatch { authed: String, destination: String },
    #[error("authorization required — reconnect the account")]
    AuthRequired,
    #[error("quota exhausted — do not retry until quota resets")]
    QuotaExceeded,
    #[error("rate limited — retry after {retry_after_secs:?}s")]
    RateLimited { retry_after_secs: Option<u64> },
    #[error("provider error: {0}")]
    Provider(StatusCode),
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
}

/// Refuse to build a request when the authenticated account does not
/// match the selected destination's channel identity.
pub fn check_account(authed_channel: &str, destination_channel: &str) -> Result<(), HttpError> {
    if authed_channel != destination_channel {
        return Err(HttpError::AccountMismatch {
            authed: "[redacted]".into(),
            destination: "[redacted]".into(),
        });
    }
    Ok(())
}

pub async fn map_response(res: reqwest::Response) -> Result<(), HttpError> {
    let status = res.status();
    if status.is_success() {
        return Ok(());
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after_secs = res
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        return Err(HttpError::RateLimited { retry_after_secs });
    }
    if status == StatusCode::FORBIDDEN {
        // 403 carries both auth and quota failures; the body reason
        // decides, since recovery differs (reconnect vs wait).
        if let Ok(body) = res.json::<serde_json::Value>().await {
            if quota_exceeded_in_body(&body) {
                return Err(HttpError::QuotaExceeded);
            }
        }
        return Err(HttpError::AuthRequired);
    }
    match status {
        StatusCode::UNAUTHORIZED => Err(HttpError::AuthRequired),
        s => Err(HttpError::Provider(s)),
    }
}

pub fn quota_exceeded_in_body(body: &serde_json::Value) -> bool {
    body.pointer("/error/errors")
        .and_then(|e| e.as_array())
        .is_some_and(|errs| {
            errs.iter()
                .any(|e| e.get("reason").and_then(|r| r.as_str()) == Some("quotaExceeded"))
        })
}

/// YouTube `liveBroadcasts.update` with explicit `part=` semantics.
/// Only the parts being written are sent, preserving unrelated values.
///
/// The account check runs first: no request leaves the machine when the
/// authenticated channel does not own the destination.
#[allow(clippy::too_many_arguments)]
pub async fn youtube_update(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    authed_channel: &str,
    destination_channel: &str,
    broadcast_id: &str,
    parts: &str,
    body: &serde_json::Value,
) -> Result<(), HttpError> {
    check_account(authed_channel, destination_channel)?;
    let res = client
        .put(format!("{base_url}/youtube/v3/liveBroadcasts"))
        .query(&[("part", parts), ("id", broadcast_id)])
        .bearer_auth(token)
        .json(body)
        .send()
        .await?;
    map_response(res).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{matchers, Mock, MockServer, ResponseTemplate};

    #[test]
    fn blocks_account_mismatch_without_network() {
        let err = check_account("chan-a", "chan-b").unwrap_err();
        assert!(matches!(err, HttpError::AccountMismatch { .. }));
        // Channel ids never appear in the error (redacted at the boundary).
        assert!(!err.to_string().contains("chan-a"));
    }

    #[tokio::test]
    async fn youtube_update_sends_only_requested_parts() {
        let server = MockServer::start().await;
        Mock::given(matchers::method("PUT"))
            .and(matchers::path("/youtube/v3/liveBroadcasts"))
            .and(matchers::query_param("part", "id,snippet"))
            .and(matchers::query_param("id", "b1"))
            .and(matchers::body_json(
                serde_json::json!({"id": "b1", "snippet": {"title": "New"}}),
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        // Body carries only snippet; status/contentDetails untouched remotely.
        let body = serde_json::json!({"id": "b1", "snippet": {"title": "New"}});
        youtube_update(
            &client,
            &server.uri(),
            "tok",
            "chan-a",
            "chan-a",
            "b1",
            "id,snippet",
            &body,
        )
        .await
        .unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn write_path_enforces_account_match() {
        let server = MockServer::start().await;
        Mock::given(matchers::method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let err = youtube_update(
            &client,
            &server.uri(),
            "tok",
            "chan-a",
            "chan-b",
            "b1",
            "id",
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, HttpError::AccountMismatch { .. }));
        server.verify().await;
    }

    #[tokio::test]
    async fn maps_401_to_auth_required() {
        let server = MockServer::start().await;
        Mock::given(matchers::method("PUT"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let err = youtube_update(
            &client,
            &server.uri(),
            "stale",
            "a",
            "a",
            "b1",
            "id",
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, HttpError::AuthRequired));
    }

    #[tokio::test]
    async fn maps_429_with_retry_after() {
        let server = MockServer::start().await;
        Mock::given(matchers::method("PUT"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let err = youtube_update(
            &client,
            &server.uri(),
            "tok",
            "a",
            "a",
            "b1",
            "id",
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            HttpError::RateLimited {
                retry_after_secs: Some(60)
            }
        ));
    }

    #[tokio::test]
    async fn maps_403_quota_separately_from_auth() {
        let server = MockServer::start().await;
        Mock::given(matchers::method("PUT"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {"errors": [{"reason": "quotaExceeded"}], "code": 403}
            })))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let err = youtube_update(
            &client,
            &server.uri(),
            "tok",
            "a",
            "a",
            "b1",
            "id",
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, HttpError::QuotaExceeded));
    }
}
