//! Deterministic offline demo: the real coordinator fed by a fixed
//! script, with production networking disabled. Covers degraded states
//! (unauthorized, reconnecting, rate-limited, ended, relay-required,
//! read-only, possible gap) without credentials.

use crate::coordinator::{Coordinator, Incoming, IngestOutcome};
use crate::model::*;
use std::collections::VecDeque;

pub const DEMO_NOTICE: &str =
    "Private operator console. Demo feed: no credentials, no production networking.";

#[derive(Debug, Clone)]
pub enum DemoStep {
    Ingest {
        source: &'static str,
        remote_id: Option<&'static str>,
        idless: Option<&'static str>,
        user: &'static str,
        text: &'static str,
        kind: MessageKind,
    },
    Delete {
        source: &'static str,
        remote_id: &'static str,
    },
    Conn {
        source: &'static str,
        state: ConnState,
    },
    Gap {
        source: &'static str,
        reason: &'static str,
    },
}

/// Fixed script exercising every per-source state plus cross-platform
/// identity separation, duplicates, tombstones, and gaps.
pub fn demo_script() -> Vec<DemoStep> {
    use DemoStep::*;
    vec![
        Conn {
            source: "tw",
            state: ConnState::Connecting,
        },
        Conn {
            source: "yt",
            state: ConnState::Connecting,
        },
        Conn {
            source: "kk",
            state: ConnState::RelayRequired,
        },
        Conn {
            source: "ru",
            state: ConnState::Connecting,
        },
        Conn {
            source: "tw",
            state: ConnState::Live {
                since_utc: "2026-09-15T10:00:00Z".into(),
            },
        },
        Conn {
            source: "yt",
            state: ConnState::Live {
                since_utc: "2026-09-15T10:00:00Z".into(),
            },
        },
        Conn {
            source: "ru",
            state: ConnState::Live {
                since_utc: "2026-09-15T10:00:00Z".into(),
            },
        },
        // Same username on two platforms: two identities, always.
        Ingest {
            source: "tw",
            remote_id: Some("tw-1"),
            idless: None,
            user: "sam",
            text: "hello from twitch",
            kind: MessageKind::Text,
        },
        Ingest {
            source: "kk",
            remote_id: Some("kk-1"),
            idless: None,
            user: "sam",
            text: "hello from kick",
            kind: MessageKind::Text,
        },
        // Duplicate delivery suppressed.
        Ingest {
            source: "tw",
            remote_id: Some("tw-1"),
            idless: None,
            user: "sam",
            text: "hello from twitch",
            kind: MessageKind::Text,
        },
        // Paid + membership events.
        Ingest {
            source: "yt",
            remote_id: Some("yt-9"),
            idless: None,
            user: "patron",
            text: "take my $5",
            kind: MessageKind::Paid(PaidInfo::SuperChat {
                amount: "$5.00".into(),
            }),
        },
        Ingest {
            source: "tw",
            remote_id: Some("tw-2"),
            idless: None,
            user: "sub-anna",
            text: "3 months!",
            kind: MessageKind::Membership,
        },
        // ID-less Rumble-style repeats reconcile by occurrence.
        Ingest {
            source: "ru",
            remote_id: None,
            idless: Some("amy|gg|t0|occ0"),
            user: "amy",
            text: "gg",
            kind: MessageKind::Text,
        },
        Ingest {
            source: "ru",
            remote_id: None,
            idless: Some("amy|gg|t0|occ0"),
            user: "amy",
            text: "gg",
            kind: MessageKind::Text,
        },
        Ingest {
            source: "ru",
            remote_id: None,
            idless: Some("amy|gg|t0|occ1"),
            user: "amy",
            text: "gg",
            kind: MessageKind::Text,
        },
        // Deletion then replay: tombstone wins.
        Delete {
            source: "tw",
            remote_id: "tw-2",
        },
        Ingest {
            source: "tw",
            remote_id: Some("tw-2"),
            idless: None,
            user: "sub-anna",
            text: "3 months!",
            kind: MessageKind::Membership,
        },
        // Degraded states.
        Conn {
            source: "yt",
            state: ConnState::RateLimited {
                retry_after_secs: 30,
            },
        },
        Conn {
            source: "tw",
            state: ConnState::Reconnecting { attempt: 2 },
        },
        Gap {
            source: "tw",
            reason: "reconnect window elapsed; events during the outage are not replayed",
        },
        Conn {
            source: "tw",
            state: ConnState::Live {
                since_utc: "2026-09-15T10:05:00Z".into(),
            },
        },
        Conn {
            source: "yt",
            state: ConnState::Ended {
                reason: "broadcast ended".into(),
            },
        },
    ]
}

pub fn demo_sources() -> Vec<ChatSource> {
    vec![
        ChatSource {
            id: "tw".into(),
            key: SourceKey {
                provider: Provider::Twitch,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec!["twitch-main".into()],
        },
        ChatSource {
            id: "yt".into(),
            key: SourceKey {
                provider: Provider::Youtube,
                room: "CHAT-DEMO".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: Some("vid-demo".into()),
            binding_ids: vec!["youtube-main".into()],
        },
        ChatSource {
            id: "kk".into(),
            key: SourceKey {
                provider: Provider::Kick,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec!["kick-main".into()],
        },
        ChatSource {
            id: "ru".into(),
            key: SourceKey {
                provider: Provider::Rumble,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec!["rumble-main".into()],
        },
    ]
}

/// Apply the whole script to a coordinator; returns (accepted, duplicates,
/// tombstoned) for assertions and UI labels.
pub fn run_demo(c: &mut Coordinator) -> (usize, usize, usize) {
    for s in demo_sources() {
        c.upsert_source(s);
    }
    let gen = c.session_gen();
    let (mut ok, mut dup, mut tomb) = (0, 0, 0);
    for step in demo_script() {
        match step {
            DemoStep::Ingest {
                source,
                remote_id,
                idless,
                user,
                text,
                kind,
            } => {
                match c.ingest(Incoming {
                    session_gen: gen,
                    room_gen: c.room_gen_of(source).unwrap_or(0),
                    source_id: source.into(),
                    remote_id: remote_id.map(str::to_owned),
                    idless_key: idless.map(str::to_owned),
                    author_id: None,
                    author_name: user.into(),
                    provider_ts: Some("2026-09-15T10:00:00Z".into()),
                    text: text.into(),
                    fragments: vec![],
                    badges: vec![],
                    reply_to: None,
                    kind,
                }) {
                    IngestOutcome::Accepted(_) => ok += 1,
                    IngestOutcome::Duplicate => dup += 1,
                    IngestOutcome::Tombstoned => tomb += 1,
                    other => panic!("demo script must be deterministic, got {other:?}"),
                }
            }
            DemoStep::Delete { source, remote_id } => c.apply_delete(source, remote_id),
            DemoStep::Conn { source, state } => {
                c.set_conn(gen, source, state);
            }
            DemoStep::Gap { source, reason } => c.mark_gap(source, reason),
        }
    }
    (ok, dup, tomb)
}

/// Synthetic load probe: 100 events/s for 60 s with long bodies and
/// moderation events. Returns (ingested, elapsed_ms, evicted). The
/// coordinator must stay bounded; the caller reports the numbers.
pub fn load_probe(c: &mut Coordinator, source_id: &str) -> (usize, u128, u64) {
    let gen = c.session_gen();
    let body = "x".repeat(480);
    let start = std::time::Instant::now();
    let mut n = 0;
    for second in 0..60 {
        for i in 0..100 {
            let id = format!("load-{second}-{i}");
            if matches!(
                c.ingest(Incoming {
                    session_gen: gen,
                    room_gen: c.room_gen_of(source_id).unwrap_or(0),
                    source_id: source_id.into(),
                    remote_id: Some(id),
                    idless_key: None,
                    author_id: Some("u".into()),
                    author_name: "load".into(),
                    provider_ts: None,
                    text: body.clone(),
                    fragments: vec![],
                    badges: vec![],
                    reply_to: None,
                    kind: MessageKind::Text,
                }),
                IngestOutcome::Accepted(_)
            ) {
                n += 1;
            }
        }
        if second % 10 == 9 {
            c.apply_delete(source_id, &format!("load-{second}-0"));
        }
    }
    (n, start.elapsed().as_millis(), c.evicted())
}

pub fn timeline_snapshot(c: &Coordinator) -> VecDeque<TimelineEvent> {
    c.timeline().clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::BufferLimits;

    #[test]
    fn demo_script_is_deterministic_and_honest() {
        let mut c = Coordinator::new(BufferLimits::default());
        let (ok, dup, tomb) = run_demo(&mut c);
        assert_eq!((ok, dup, tomb), (6, 2, 1));
        // Rumble read-only source still receives; Kick stays relay-required.
        assert_eq!(c.conn_of("kk"), Some(&ConnState::RelayRequired));
        assert_eq!(
            c.conn_of("yt"),
            Some(&ConnState::Ended {
                reason: "broadcast ended".into()
            })
        );
        // Tombstoned membership stays deleted.
        let deleted = c
            .timeline()
            .iter()
            .filter_map(|ev| match ev {
                TimelineEvent::Message(m) if m.remote_id.as_deref() == Some("tw-2") => {
                    Some(m.mod_state)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(deleted, vec![ModState::DeletedRemote]);
    }
}
