//! Publish operations: durable intent-first state machine.
//!
//! Intent rows are persisted BEFORE any network mutation so a crash,
//! timeout or cancel can reconcile instead of duplicating broadcasts.
//! Retries address failed/blocked actions only, never replay successes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Pending,
    InFlight,
    Succeeded,
    SucceededUnverified,
    Failed,
    Blocked,
    ManualRequired,
    ManuallyCompleted,
    /// Dispatched but outcome unknown (timeout/crash). Reconcile or ask.
    OutcomeUnknown,
}

impl ActionState {
    /// Directly safe to re-dispatch. `OutcomeUnknown` is deliberately
    /// excluded: it must reconcile first (see [`is_reconcilable`]).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ActionState::Pending | ActionState::Failed | ActionState::Blocked
        )
    }

    /// May enter reconciliation (query provider / ask operator), after
    /// which it can become retryable or terminal. Never retry directly.
    pub fn is_reconcilable(&self) -> bool {
        matches!(self, ActionState::OutcomeUnknown | ActionState::Failed)
    }

    pub fn is_terminal_success(&self) -> bool {
        matches!(
            self,
            ActionState::Succeeded
                | ActionState::SucceededUnverified
                | ActionState::ManuallyCompleted
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishActionIntent {
    pub destination_id: String,
    pub account_id: String,
    /// create | update | manual
    pub kind: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
    pub remote_id: Option<String>,
    pub state: ActionState,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileDecision {
    /// Safe to retry (no remote effect observed).
    Retry,
    /// Must query provider / ask operator before acting.
    ReconcileFirst,
    /// Already applied; do not re-issue.
    AlreadyApplied,
}

/// Decide what to do with an ambiguous (timeout/crash) action.
pub fn reconcile_ambiguous(had_remote_effect: Option<bool>) -> ReconcileDecision {
    match had_remote_effect {
        Some(true) => ReconcileDecision::AlreadyApplied,
        Some(false) => ReconcileDecision::Retry,
        None => ReconcileDecision::ReconcileFirst,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_scope() {
        assert!(ActionState::Failed.is_retryable());
        assert!(!ActionState::Succeeded.is_retryable());
        assert!(!ActionState::ManuallyCompleted.is_retryable());
        // Ambiguous outcomes must reconcile before any retry.
        assert!(!ActionState::OutcomeUnknown.is_retryable());
        assert!(ActionState::OutcomeUnknown.is_reconcilable());
        assert!(!ActionState::Succeeded.is_reconcilable());
    }

    #[test]
    fn ambiguous_requires_reconcile() {
        assert_eq!(reconcile_ambiguous(None), ReconcileDecision::ReconcileFirst);
        assert_eq!(reconcile_ambiguous(Some(false)), ReconcileDecision::Retry);
        assert_eq!(
            reconcile_ambiguous(Some(true)),
            ReconcileDecision::AlreadyApplied
        );
    }
}
