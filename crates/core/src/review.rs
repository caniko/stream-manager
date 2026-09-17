//! Central review validation: the single gate for both displaying a
//! review and executing it.
//!
//! A [`ReviewSnapshot`] is built from plain serializable data (never UI
//! memory), carries every validation error, and [`can_apply`] decides
//! both button eligibility and execution admission. Execution rebuilds
//! the review from the persisted snapshot, so a stale or hand-driven
//! apply cannot bypass validation.

use crate::domain::{DestinationPlan, FieldOverride, Provider, Role};
use crate::funnel::validate_funnel;
use crate::policy::{self, PolicyViolation};
use crate::template::render_template;
use crate::video::VideoProfile;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPlan {
    pub id: String,
    pub title: String,
    pub description: String,
    pub start_local: String,
    pub revision: u64,
    pub primary_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDest {
    pub dest_id: String,
    pub provider: Provider,
    pub role: Role,
    pub title: FieldOverride<String>,
    pub description: FieldOverride<String>,
    pub funnel_target: Option<String>,
    pub remote_url: Option<String>,
    pub video_profile: Option<String>,
}

impl SnapshotDest {
    pub fn to_destination_plan(&self) -> DestinationPlan {
        DestinationPlan {
            destination_id: self.dest_id.clone(),
            role: self.role,
            title: self.title.clone(),
            description: self.description.clone(),
            tags: FieldOverride::Inherit,
            funnel_target: self.funnel_target.clone(),
            remote_broadcast_id: None,
            remote_watch_url: self.remote_url.clone(),
            video_profile_id: self.video_profile.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestReview {
    pub dest_id: String,
    pub provider: Provider,
    pub role: Role,
    pub action: String,
    pub rendered: Result<Option<String>, String>,
    pub blocked: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewSnapshot {
    pub plan_id: String,
    pub revision: u64,
    pub title: String,
    pub demo: bool,
    pub dests: Vec<DestReview>,
    pub policy: Vec<PolicyViolation>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInput {
    pub plan: SnapshotPlan,
    pub dests: Vec<SnapshotDest>,
}

pub fn planned_action(role: Role, provider: Provider) -> String {
    let name = provider.display();
    let caps = crate::providers::caps_of(provider);
    match role {
        Role::Primary => format!("{name}: create/bind event (autoStart off), then update"),
        Role::Mirror => format!("{name}: update per caps ({:?})", caps.update_event),
        Role::Funnel => format!(
            "{name}: dependent funnel after primary URL resolves ({:?})",
            caps.update_event
        ),
    }
}

/// Build a review, collecting every validation error:
/// funnel routing, policy violations, template failures, missing primary
/// URLs, and invalid/missing video profiles.
pub fn build_review(
    plan: &SnapshotPlan,
    dests: &[SnapshotDest],
    providers: &HashMap<String, Provider>,
    profiles: &HashMap<String, VideoProfile>,
    demo: bool,
) -> ReviewSnapshot {
    let mut errors: Vec<String> = Vec::new();

    let resolved: Vec<DestinationPlan> = dests.iter().map(|d| d.to_destination_plan()).collect();
    if let Err(e) = validate_funnel(&resolved) {
        errors.push(format!("funnel: {e}"));
    }

    let policy = policy::check(&resolved, providers, profiles);
    for v in &policy {
        errors.push(format!("policy {}: {}", v.rule_id, v.message));
    }

    let primary = resolved.iter().find(|d| d.role == Role::Primary);
    let primary_url = primary.and_then(|p| p.remote_watch_url.clone());
    let primary_platform = primary
        .map(|p| {
            providers
                .get(&p.destination_id)
                .copied()
                .unwrap_or(Provider::Youtube)
                .display()
        })
        .unwrap_or("?");

    // Profile assignments must resolve to valid profiles.
    for d in dests {
        if let Some(pid) = &d.video_profile {
            match profiles.get(pid) {
                Some(_) => {}
                None => errors.push(format!(
                    "destination {}: unknown video profile '{pid}'",
                    d.dest_id
                )),
            }
        }
    }

    let mut out = Vec::new();
    for d in dests {
        let provider = providers.get(&d.dest_id).copied().unwrap_or(d.provider);
        let base: Option<String> = match &d.description {
            FieldOverride::Inherit => Some(plan.description.clone()),
            FieldOverride::Replace(v) => Some(v.clone()),
            FieldOverride::Clear => None,
        };
        let rendered: Result<Option<String>, String> = match base {
            None => Ok(None),
            Some(text) => {
                let vars = HashMap::from([
                    ("title".to_string(), plan.title.clone()),
                    ("description".to_string(), plan.description.clone()),
                    ("primary_platform".to_string(), primary_platform.to_string()),
                    (
                        "primary_watch_url".to_string(),
                        primary_url.clone().unwrap_or_default(),
                    ),
                    (
                        "destination_platform".to_string(),
                        provider.display().to_string(),
                    ),
                    ("start_local".to_string(), plan.start_local.clone()),
                ]);
                render_template(&text, &vars)
                    .map(Some)
                    .map_err(|e| e.to_string())
            }
        };
        if let Err(e) = &rendered {
            errors.push(format!("destination {}: template: {e}", d.dest_id));
        }
        let leftover = matches!(rendered, Ok(Some(ref t)) if t.contains("{{"));
        if leftover {
            errors.push(format!(
                "destination {}: unresolved template placeholder in output",
                d.dest_id
            ));
        }
        let blocked = if d.role == Role::Funnel && primary_url.is_none() {
            Some(
                "blocked: primary watch URL unresolved — create the primary event first"
                    .to_string(),
            )
        } else {
            None
        };
        out.push(DestReview {
            dest_id: d.dest_id.clone(),
            provider,
            role: d.role,
            action: planned_action(d.role, provider),
            rendered,
            blocked,
        });
    }

    ReviewSnapshot {
        plan_id: plan.id.clone(),
        revision: plan.revision,
        title: plan.title.clone(),
        demo,
        dests: out,
        policy,
        errors,
    }
}

/// Admission for both the Apply button and execution.
pub fn can_apply(review: &ReviewSnapshot) -> bool {
    review.errors.is_empty() && review.dests.iter().all(|d| d.blocked.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::FieldOverride;

    fn plan() -> SnapshotPlan {
        SnapshotPlan {
            id: "p".into(),
            title: "Show".into(),
            description: "Desc".into(),
            start_local: "2026-10-02 20:00".into(),
            revision: 1,
            primary_id: Some("yt".into()),
        }
    }

    fn dest(id: &str, role: Role, target: Option<&str>) -> SnapshotDest {
        SnapshotDest {
            dest_id: id.into(),
            provider: if id == "yt" {
                Provider::Youtube
            } else {
                Provider::Rumble
            },
            role,
            title: FieldOverride::Inherit,
            description: FieldOverride::Inherit,
            funnel_target: target.map(str::to_string),
            remote_url: if id == "yt" {
                Some("https://www.youtube.com/watch?v=x".into())
            } else {
                None
            },
            video_profile: None,
        }
    }

    fn providers() -> HashMap<String, Provider> {
        HashMap::from([
            ("yt".to_string(), Provider::Youtube),
            ("rb".to_string(), Provider::Rumble),
        ])
    }

    #[test]
    fn valid_primary_funnel_applies() {
        let r = build_review(
            &plan(),
            &[
                dest("yt", Role::Primary, None),
                dest("rb", Role::Funnel, Some("yt")),
            ],
            &providers(),
            &HashMap::new(),
            true,
        );
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(can_apply(&r));
    }

    #[test]
    fn unknown_template_variable_blocks_apply() {
        let mut d = dest("yt", Role::Primary, None);
        d.description = FieldOverride::Replace("hi {{evil}}".into());
        let r = build_review(&plan(), &[d], &providers(), &HashMap::new(), true);
        assert!(!r.errors.is_empty());
        assert!(!can_apply(&r));
    }

    #[test]
    fn twitch_funnel_violation_blocks_apply() {
        let twitch = SnapshotDest {
            dest_id: "tw".into(),
            provider: Provider::Twitch,
            role: Role::Funnel,
            title: FieldOverride::Inherit,
            description: FieldOverride::Inherit,
            funnel_target: Some("yt".into()),
            remote_url: None,
            video_profile: None,
        };
        let prov = HashMap::from([
            ("yt".to_string(), Provider::Youtube),
            ("tw".to_string(), Provider::Twitch),
        ]);
        let r = build_review(
            &plan(),
            &[dest("yt", Role::Primary, None), twitch],
            &prov,
            &HashMap::new(),
            false,
        );
        assert!(r.errors.iter().any(|e| e.contains("twitch-no-funnel-out")));
        assert!(!can_apply(&r));
    }

    #[test]
    fn missing_primary_url_blocks_funnel_dest() {
        let mut yt = dest("yt", Role::Primary, None);
        yt.remote_url = None;
        let r = build_review(
            &plan(),
            &[yt, dest("rb", Role::Funnel, Some("yt"))],
            &providers(),
            &HashMap::new(),
            true,
        );
        assert!(!can_apply(&r));
        assert!(r.dests.iter().any(|d| d.blocked.is_some()));
    }

    #[test]
    fn unknown_profile_blocks_apply() {
        let mut d = dest("rb", Role::Mirror, None);
        d.video_profile = Some("ghost".into());
        let r = build_review(&plan(), &[d], &providers(), &HashMap::new(), false);
        assert!(r.errors.iter().any(|e| e.contains("unknown video profile")));
        assert!(!can_apply(&r));
    }

    #[test]
    fn snapshot_roundtrips_through_json() {
        let r = build_review(
            &plan(),
            &[dest("yt", Role::Primary, None)],
            &providers(),
            &HashMap::new(),
            false,
        );
        let input = SnapshotInput {
            plan: plan(),
            dests: vec![dest("yt", Role::Primary, None)],
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: SnapshotInput = serde_json::from_str(&json).unwrap();
        assert_eq!(back.plan.id, "p");
        assert_eq!(r.plan_id, "p");
    }
}
