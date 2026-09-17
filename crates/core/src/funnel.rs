use crate::domain::{DestinationPlan, Role};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FunnelError {
    #[error("funnel destinations require exactly one primary, found {0}")]
    PrimaryCount(usize),
    #[error("destination '{0}' targets itself")]
    SelfTarget(String),
    #[error("destination '{0}' targets missing destination '{1}'")]
    MissingTarget(String, String),
    #[error("funnel routing cycle detected: {0}")]
    Cycle(String),
    #[error("destination '{0}' has role funnel but no target")]
    FunnelWithoutTarget(String),
}

/// Validate funnel routing: exactly one primary when funnels exist,
/// no self-targets, no missing targets, no cycles.
pub fn validate_funnel(destinations: &[DestinationPlan]) -> Result<(), FunnelError> {
    let ids: HashSet<&str> = destinations
        .iter()
        .map(|d| d.destination_id.as_str())
        .collect();
    let primaries = destinations
        .iter()
        .filter(|d| d.role == Role::Primary)
        .count();
    let funnels: Vec<&DestinationPlan> = destinations
        .iter()
        .filter(|d| d.role == Role::Funnel)
        .collect();

    if !funnels.is_empty() && primaries != 1 {
        return Err(FunnelError::PrimaryCount(primaries));
    }

    let targets: HashMap<&str, &str> = funnels
        .iter()
        .map(|d| {
            d.funnel_target
                .as_deref()
                .ok_or_else(|| FunnelError::FunnelWithoutTarget(d.destination_id.clone()))
                .map(|t| (d.destination_id.as_str(), t))
        })
        .collect::<Result<_, _>>()?;

    for (from, to) in &targets {
        if from == to {
            return Err(FunnelError::SelfTarget((*from).to_string()));
        }
        if !ids.contains(to) {
            return Err(FunnelError::MissingTarget(
                (*from).to_string(),
                (*to).to_string(),
            ));
        }
    }

    // Cycle detection over funnel edges.
    for start in targets.keys() {
        let mut seen = HashSet::new();
        let mut cur = *start;
        while let Some(next) = targets.get(cur) {
            if !seen.insert(cur) {
                return Err(FunnelError::Cycle((*start).to_string()));
            }
            cur = next;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::FieldOverride;

    fn plan(id: &str, role: Role, target: Option<&str>) -> DestinationPlan {
        DestinationPlan {
            destination_id: id.into(),
            role,
            title: FieldOverride::Inherit,
            description: FieldOverride::Inherit,
            tags: FieldOverride::Inherit,
            funnel_target: target.map(str::to_string),
            remote_broadcast_id: None,
            remote_watch_url: None,
            video_profile_id: None,
        }
    }

    #[test]
    fn accepts_primary_mirror_funnel() {
        let ds = vec![
            plan("yt", Role::Primary, None),
            plan("kick", Role::Mirror, None),
            plan("rumble", Role::Funnel, Some("yt")),
        ];
        assert!(validate_funnel(&ds).is_ok());
    }

    #[test]
    fn rejects_funnel_without_primary() {
        let ds = vec![plan("rumble", Role::Funnel, Some("yt"))];
        assert!(matches!(
            validate_funnel(&ds),
            Err(FunnelError::PrimaryCount(0))
        ));
    }

    #[test]
    fn rejects_self_target() {
        let ds = vec![
            plan("yt", Role::Primary, None),
            plan("r", Role::Funnel, Some("r")),
        ];
        assert!(matches!(
            validate_funnel(&ds),
            Err(FunnelError::SelfTarget(_))
        ));
    }

    #[test]
    fn rejects_missing_target() {
        let ds = vec![
            plan("yt", Role::Primary, None),
            plan("r", Role::Funnel, Some("ghost")),
        ];
        assert!(matches!(
            validate_funnel(&ds),
            Err(FunnelError::MissingTarget(_, _))
        ));
    }

    #[test]
    fn rejects_cycle() {
        let ds = vec![
            plan("yt", Role::Primary, None),
            plan("a", Role::Funnel, Some("b")),
            plan("b", Role::Funnel, Some("a")),
        ];
        assert!(matches!(validate_funnel(&ds), Err(FunnelError::Cycle(_))));
    }
}
