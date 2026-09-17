//! Labelled offline demo fixture. Exercises the real editor and
//! planner with no credentials and no external mutations.
//!
//! - YouTube: primary, normal full description.
//! - Rumble: funnel to YouTube with promotional description.
//! - Kick: mirror.
//! - Twitch: mirror, without an off-platform viewing request.

use crate::domain::{DestinationPlan, FieldOverride, Provider, Role, StreamPlan};
use std::collections::HashMap;

pub const DEMO_BANNER: &str = "DEMO — offline fixtures only. No credentials, no external calls.";

pub fn demo_destinations() -> Vec<(String, Provider)> {
    vec![
        ("youtube-main".into(), Provider::Youtube),
        ("rumble-main".into(), Provider::Rumble),
        ("kick-main".into(), Provider::Kick),
        ("twitch-main".into(), Provider::Twitch),
    ]
}

pub fn demo_plan() -> (StreamPlan, Vec<DestinationPlan>, HashMap<String, Provider>) {
    use chrono::TimeZone;
    let start = chrono::Utc.with_ymd_and_hms(2026, 10, 2, 18, 0, 0).unwrap();
    let plan = StreamPlan {
        id: "demo-plan".into(),
        title: "Friday Build Stream".into(),
        description: "Live build session with Q&A.".into(),
        start_utc: start,
        end_utc: None,
        timezone: "Europe/Berlin".into(),
        language: "en".into(),
        category_intent: "Science & Technology".into(),
        category_id: None,
        tags: vec!["live".into(), "build".into()],
        destination_ids: vec![
            "youtube-main".into(),
            "rumble-main".into(),
            "kick-main".into(),
            "twitch-main".into(),
        ],
        primary_id: Some("youtube-main".into()),
        status: crate::domain::PlanStatus::Draft,
        revision: 1,
    };
    let mut yt = DestinationPlan::new("youtube-main", Role::Primary);
    yt.remote_watch_url = Some("https://www.youtube.com/watch?v=DEMO_PRIMARY".into());
    yt.remote_broadcast_id = Some("DEMO_PRIMARY".into());

    let mut rumble = DestinationPlan::new("rumble-main", Role::Funnel);
    rumble.funnel_target = Some("youtube-main".into());
    rumble.description = FieldOverride::Replace(
        "This is the preview stream. Watch the full livestream on {{primary_platform}}: {{primary_watch_url}}".into(),
    );

    let kick = DestinationPlan::new("kick-main", Role::Mirror);
    let twitch = DestinationPlan::new("twitch-main", Role::Mirror);

    let providers = HashMap::from([
        ("youtube-main".to_string(), Provider::Youtube),
        ("rumble-main".to_string(), Provider::Rumble),
        ("kick-main".to_string(), Provider::Kick),
        ("twitch-main".to_string(), Provider::Twitch),
    ]);
    (plan, vec![yt, rumble, kick, twitch], providers)
}

/// Resolve one destination description for review, following
/// shared -> role template -> explicit override.
pub fn resolve_description(
    plan: &StreamPlan,
    dest: &DestinationPlan,
    primary_platform: &str,
    primary_watch_url: &str,
    destination_platform: &str,
) -> Result<Option<String>, crate::template::TemplateError> {
    let base: Option<String> = match &dest.description {
        FieldOverride::Inherit => Some(plan.description.clone()),
        FieldOverride::Replace(v) => Some(v.clone()),
        FieldOverride::Clear => None,
    };
    let Some(text) = base else { return Ok(None) };
    let vars = HashMap::from([
        ("title".to_string(), plan.title.clone()),
        ("description".to_string(), plan.description.clone()),
        ("primary_platform".to_string(), primary_platform.to_string()),
        (
            "primary_watch_url".to_string(),
            primary_watch_url.to_string(),
        ),
        (
            "destination_platform".to_string(),
            destination_platform.to_string(),
        ),
        ("start_local".to_string(), plan.start_utc.to_rfc3339()),
    ]);
    crate::template::render_template(&text, &vars).map(Some)
}
