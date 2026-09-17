//! Central stream-manager service library.
//!
//! Nomad owns the canonical accounts, sources, chat session, provider
//! subscriptions, credentials, and action admission. Native clients on
//! Murph, Atlas, and Nomad share this state through the versioned
//! protocol in [`protocol`]; permissions in [`perms`] bind every action
//! to an operator (Can: administrator, Dejana: operator), never to a
//! machine name. [`state`] is the single writer behind one async mutex.

pub mod perms;
pub mod protocol;
pub mod state;
