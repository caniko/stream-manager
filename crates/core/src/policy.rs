//! Destination policy checks. Funnel intent is validated as data,
//! not by keyword detection. Each rule carries source + review date.

use crate::domain::{DestinationPlan, Provider, Role};
use crate::video::{VideoKind, VideoProfile};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PolicyRule {
    pub id: &'static str,
    pub source: &'static str,
    pub reviewed: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PolicyViolation {
    pub rule_id: String,
    pub destination_id: String,
    pub message: String,
}

/// Current rule set. Twitch simulcasting restrictions are enforced;
/// other platforms are unknown (not blanket-approved).
pub fn rules() -> Vec<PolicyRule> {
    vec![
        PolicyRule {
            id: "twitch-no-funnel-out",
            source: "Twitch Simulcasting Guidelines (official)",
            reviewed: "2026-09-15",
        },
        PolicyRule {
            id: "twitch-no-degrading-mask",
            source: "Twitch Simulcasting Guidelines (official)",
            reviewed: "2026-09-15",
        },
    ]
}

/// Check generated metadata + proposed video profiles.
/// `providers` maps destination id -> provider.
pub fn check(
    destinations: &[DestinationPlan],
    providers: &HashMap<String, Provider>,
    profiles: &HashMap<String, VideoProfile>,
) -> Vec<PolicyViolation> {
    let mut out = Vec::new();
    for d in destinations {
        if providers.get(&d.destination_id) != Some(&Provider::Twitch) {
            continue;
        }
        // Twitch must not be a funnel directing viewers elsewhere.
        if d.role == Role::Funnel {
            out.push(PolicyViolation {
                rule_id: "twitch-no-funnel-out".into(),
                destination_id: d.destination_id.clone(),
                message: "Twitch must not be used as a funnel to another concurrent livestream."
                    .into(),
            });
        }
        // No degrading funnel mask on the Twitch simulcast output.
        if let Some(pid) = &d.video_profile_id {
            if let Some(p) = profiles.get(pid) {
                let degrading = !matches!(p.kind, VideoKind::Passthrough);
                if degrading {
                    out.push(PolicyViolation {
                        rule_id: "twitch-no-degrading-mask".into(),
                        destination_id: d.destination_id.clone(),
                        message: format!(
                            "Twitch output must not carry degrading overlay '{}'.",
                            p.name
                        ),
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::FieldOverride;
    use crate::video::{AudioBehavior, Placement};

    fn d(id: &str, role: Role, target: Option<&str>, profile: Option<&str>) -> DestinationPlan {
        DestinationPlan {
            destination_id: id.into(),
            role,
            title: FieldOverride::Inherit,
            description: FieldOverride::Inherit,
            tags: FieldOverride::Inherit,
            funnel_target: target.map(str::to_string),
            remote_broadcast_id: None,
            remote_watch_url: None,
            video_profile_id: profile.map(str::to_string),
        }
    }

    #[test]
    fn blocks_twitch_funnel() {
        let ds = vec![
            d("yt", Role::Primary, None, None),
            d("tw", Role::Funnel, Some("yt"), None),
        ];
        let prov = HashMap::from([
            ("yt".to_string(), Provider::Youtube),
            ("tw".to_string(), Provider::Twitch),
        ]);
        let v = check(&ds, &prov, &HashMap::new());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].rule_id, "twitch-no-funnel-out");
    }

    #[test]
    fn blocks_degrading_mask_on_twitch() {
        let ds = vec![d("tw", Role::Mirror, None, Some("p1"))];
        let prov = HashMap::from([("tw".to_string(), Provider::Twitch)]);
        let prof = HashMap::from([(
            "p1".to_string(),
            VideoProfile {
                id: "p1".into(),
                name: "Funnel slate".into(),
                version: 1,
                kind: VideoKind::FullSlate(crate::video::FullSlate {
                    headline: "Watch elsewhere".into(),
                    body: "Full show on YouTube".into(),
                    link_url: None,
                }),
                audio: AudioBehavior::Keep,
                show_from_secs: None,
                show_until_secs: None,
            },
        )]);
        let _ = Placement::Bottom;
        let v = check(&ds, &prov, &prof);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].rule_id, "twitch-no-degrading-mask");
    }

    #[test]
    fn allows_twitch_mirror_passthrough() {
        let ds = vec![d("tw", Role::Mirror, None, None)];
        let prov = HashMap::from([("tw".to_string(), Provider::Twitch)]);
        assert!(check(&ds, &prov, &HashMap::new()).is_empty());
    }
}
