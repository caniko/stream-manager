//! Documented provider chat adapters.
//!
//! Each adapter implements the provider's *documented* protocol only — no
//! cookie scraping, private APIs, browser automation, or aggregators.
//! Capability status per provider lives in `docs/chat-capabilities.md`,
//! which records URLs, review dates, scopes, transports, and limits.

pub mod kick;
pub mod rumble;
pub mod twitch;
pub mod youtube;
