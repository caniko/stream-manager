//! Single-threaded chat session coordinator.
//!
//! All chat state flows through here: sources, connection health, the
//! bounded merged timeline, dedupe/tombstones, drafts, reply bindings,
//! multi-target selection, and outbound intents. Transports push
//! [`Incoming`] items in; the UI reads snapshots out. Nothing here touches
//! the network, storage, or metadata reviews: chat activity can never
//! invalidate or rewrite a metadata review.

use crate::model::*;
use std::collections::{HashMap, HashSet, VecDeque};

/// Live-buffer bounds. Defaults: 5,000 messages plus a byte ceiling.
#[derive(Debug, Clone)]
pub struct BufferLimits {
    pub max_messages: usize,
    pub max_bytes: usize,
}

impl Default for BufferLimits {
    fn default() -> Self {
        Self {
            max_messages: 5_000,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// One normalized inbound item from a transport loop. Carries the session
/// and room generations the loop was started with so stale loops (after
/// account, plan, demo, disconnect, or rebind changes) are ignored with
/// their failures alike.
#[derive(Debug, Clone)]
pub struct Incoming {
    pub session_gen: u64,
    /// Room generation the emitting loop started with: messages from a
    /// loop predating a rebind are dropped, never merged into the new
    /// room's timeline.
    pub room_gen: u64,
    pub source_id: String,
    pub remote_id: Option<String>,
    /// Occurrence-aware key for ID-less providers (Rumble): supplied by
    /// the adapter, e.g. user + text + timestamp + snapshot occurrence.
    pub idless_key: Option<String>,
    pub author_id: Option<String>,
    pub author_name: String,
    pub provider_ts: Option<String>,
    pub text: String,
    pub fragments: Vec<TextFragment>,
    pub badges: Vec<String>,
    pub reply_to: Option<ReplyRef>,
    pub kind: MessageKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    Accepted(u64),
    Duplicate,
    Tombstoned,
    StaleSession,
    StaleRoom,
    UnknownSource,
}

#[derive(Debug, Clone)]
pub struct ReplyBinding {
    pub source_id: String,
    pub remote_id: String,
    pub author_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct MultiTarget {
    pub enabled: bool,
    pub selected: Vec<String>,
    pub text: String,
}

struct SourceRuntime {
    source: ChatSource,
    conn: ConnState,
    room_gen: u64,
}

/// Bounded insertion-ordered set for dedupe/tombstone guards: oldest
/// entries rotate out instead of the whole set clearing at once (which
/// would let replays resurrect deleted messages en masse).
struct OrderedSet {
    set: HashSet<(Provider, String, String)>,
    order: VecDeque<(Provider, String, String)>,
    cap: usize,
}

impl OrderedSet {
    fn new(cap: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap: cap.max(64),
        }
    }

    fn contains(&self, k: &(Provider, String, String)) -> bool {
        self.set.contains(k)
    }

    /// Insert; returns whether the key is newly present plus any rotated
    /// out oldest entry. Callers handling tombstones must treat a rotated
    /// entry as a lost replay guard, never silently.
    fn insert(
        &mut self,
        k: (Provider, String, String),
    ) -> (bool, Option<(Provider, String, String)>) {
        if !self.set.insert(k.clone()) {
            return (false, None);
        }
        self.order.push_back(k);
        let mut evicted = None;
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
                evicted = Some(old);
            }
        }
        (true, evicted)
    }

    fn clear(&mut self) {
        self.set.clear();
        self.order.clear();
    }
}

/// Accounted bytes for a buffered control event.
pub fn event_bytes(ev: &TimelineEvent) -> usize {
    match ev {
        TimelineEvent::Message(m) => message_bytes(m),
        TimelineEvent::Deletion {
            source, remote_id, ..
        } => source.room.len() + remote_id.len() + 64,
        TimelineEvent::Clear { source, .. } => source.room.len() + 64,
        TimelineEvent::ConnChange { source, state, .. } => {
            source.room.len() + format!("{state:?}").len() + 64
        }
        TimelineEvent::Gap { source, reason, .. } => source.room.len() + reason.len() + 64,
    }
}

/// Accounted bytes for one buffered message: every allocated field plus
/// a fixed overhead for the event wrapper itself.
pub fn message_bytes(m: &ChatMessage) -> usize {
    let mut n = m.text.len() + m.author_name.len() + 64;
    n += m.remote_id.as_ref().map(|s| s.len()).unwrap_or(0);
    n += m.author_id.as_ref().map(|s| s.len()).unwrap_or(0);
    n += m.provider_ts.as_ref().map(|s| s.len()).unwrap_or(0);
    n += m.badges.iter().map(|s| s.len()).sum::<usize>();
    n += m
        .fragments
        .iter()
        .map(|f| f.text.len() + f.emote_id.as_ref().map(|s| s.len()).unwrap_or(0))
        .sum::<usize>();
    n += m
        .reply_to
        .as_ref()
        .map(|r| r.remote_id.len() + r.author_name.len())
        .unwrap_or(0);
    n
}

pub struct Coordinator {
    session_gen: u64,
    sources: HashMap<String, SourceRuntime>,
    key_to_id: HashMap<SourceKey, String>,
    timeline: VecDeque<TimelineEvent>,
    seq: u64,
    seen: OrderedSet,
    tombstones: OrderedSet,
    bytes: usize,
    limits: BufferLimits,
    evicted: u64,
    evicted_controls: u64,
    drafts: HashMap<String, String>,
    reply: Option<ReplyBinding>,
    composer_source: Option<String>,
    multitarget: MultiTarget,
    intents: HashMap<String, OutboundIntent>,
    following: bool,
    unread: u64,
}

impl Coordinator {
    pub fn new(limits: BufferLimits) -> Self {
        Self {
            session_gen: 1,
            sources: HashMap::new(),
            key_to_id: HashMap::new(),
            timeline: VecDeque::new(),
            seq: 0,
            seen: OrderedSet::new(20_000),
            tombstones: OrderedSet::new(20_000),
            bytes: 0,
            limits,
            evicted: 0,
            evicted_controls: 0,
            drafts: HashMap::new(),
            reply: None,
            composer_source: None,
            multitarget: MultiTarget::default(),
            intents: HashMap::new(),
            following: true,
            unread: 0,
        }
    }

    pub fn session_gen(&self) -> u64 {
        self.session_gen
    }

    /// Account/plan/demo change: stale transport results are ignored after
    /// this. Intents keep their captured room generations individually.
    pub fn bump_session(&mut self) {
        self.session_gen += 1;
    }

    /// Full session reset (demo switch, logout-all): bump the generation,
    /// drop the live buffer with its replay guards, park every source
    /// offline. Drafts and the outbox survive; the reply binding does not.
    pub fn reset_for_new_session(&mut self) {
        self.session_gen += 1;
        self.timeline.clear();
        self.seen.clear();
        self.tombstones.clear();
        self.bytes = 0;
        self.evicted = 0;
        self.evicted_controls = 0;
        self.unread = 0;
        self.reply = None;
        for rt in self.sources.values_mut() {
            rt.conn = ConnState::Offline {
                reason: "session reset".into(),
            };
        }
    }

    pub fn limits(&self) -> &BufferLimits {
        &self.limits
    }

    pub fn set_limits(&mut self, limits: BufferLimits) {
        self.limits = limits;
        self.enforce_bounds();
    }

    // ---------- sources ----------

    /// Connect/Apply makes sources active. Browsing plans never calls this.
    pub fn upsert_source(&mut self, source: ChatSource) {
        let gen = self
            .sources
            .get(&source.id)
            .map(|s| s.room_gen)
            .unwrap_or(0);
        self.key_to_id.insert(source.key.clone(), source.id.clone());
        self.sources.insert(
            source.id.clone(),
            SourceRuntime {
                source,
                conn: ConnState::Offline {
                    reason: "not connected".into(),
                },
                room_gen: gen,
            },
        );
        if self.composer_source.is_none() {
            self.composer_source = self.sources.keys().next().cloned();
        }
    }

    pub fn remove_source(&mut self, id: &str) {
        if let Some(rt) = self.sources.remove(id) {
            self.key_to_id.remove(&rt.source.key);
        }
        self.drafts.remove(id);
        if self.reply.as_ref().is_some_and(|r| r.source_id == id) {
            self.reply = None;
        }
        if self.composer_source.as_deref() == Some(id) {
            self.composer_source = self.sources.keys().next().cloned();
        }
        self.multitarget.selected.retain(|s| s != id);
    }

    pub fn sources(&self) -> Vec<(&ChatSource, &ConnState)> {
        let mut out: Vec<_> = self
            .sources
            .values()
            .map(|rt| (&rt.source, &rt.conn))
            .collect();
        out.sort_by(|a, b| a.0.id.cmp(&b.0.id));
        out
    }

    pub fn conn_of(&self, id: &str) -> Option<&ConnState> {
        self.sources.get(id).map(|rt| &rt.conn)
    }

    pub fn room_gen_of(&self, id: &str) -> Option<u64> {
        self.sources.get(id).map(|rt| rt.room_gen)
    }

    /// Switching a live binding requires visible operator action: the room
    /// generation advances and drafts/reply/intents keep pointing at the
    /// generation they were captured with.
    pub fn rebind_room(&mut self, id: &str) {
        if let Some(rt) = self.sources.get_mut(id) {
            rt.room_gen += 1;
            rt.conn = ConnState::Offline {
                reason: "rebound — reconnect to resume".into(),
            };
        }
        if self.reply.as_ref().is_some_and(|r| r.source_id == id) {
            self.reply = None;
        }
    }

    pub fn set_conn(&mut self, session_gen: u64, id: &str, state: ConnState) -> bool {
        if session_gen != self.session_gen || !self.sources.contains_key(id) {
            return false;
        }
        if let Some(rt) = self.sources.get_mut(id) {
            rt.conn = state.clone();
        }
        let source = self.sources.get(id).map(|rt| rt.source.key.clone());
        if let Some(source) = source {
            let seq = self.next_seq();
            self.push(TimelineEvent::ConnChange { seq, source, state });
        }
        true
    }

    pub fn mark_gap(&mut self, id: &str, reason: &str) {
        let source = match self.sources.get(id) {
            Some(rt) => rt.source.key.clone(),
            None => return,
        };
        let seq = self.next_seq();
        self.push(TimelineEvent::Gap {
            seq,
            source,
            reason: reason.to_owned(),
        });
    }

    // ---------- ingest ----------

    pub fn ingest(&mut self, msg: Incoming) -> IngestOutcome {
        if msg.session_gen != self.session_gen {
            return IngestOutcome::StaleSession;
        }
        let rt = match self.sources.get(&msg.source_id) {
            Some(rt) => rt,
            None => return IngestOutcome::UnknownSource,
        };
        if msg.room_gen != rt.room_gen {
            return IngestOutcome::StaleRoom;
        }
        let key = rt.source.key.clone();
        let dedupe_id = msg
            .remote_id
            .clone()
            .or_else(|| msg.idless_key.clone())
            .unwrap_or_default();
        if !dedupe_id.is_empty() {
            let entry = (key.provider, key.room.clone(), dedupe_id.clone());
            if self.tombstones.contains(&entry) {
                return IngestOutcome::Tombstoned;
            }
            if !self.seen.insert(entry).0 {
                return IngestOutcome::Duplicate;
            }
        }
        let seq = self.next_seq();
        let stored = ChatMessage {
            seq,
            source: key,
            remote_id: msg.remote_id,
            author_id: msg.author_id,
            author_name: msg.author_name,
            provider_ts: msg.provider_ts,
            text: msg.text,
            fragments: msg.fragments,
            badges: msg.badges,
            reply_to: msg.reply_to,
            kind: msg.kind,
            mod_state: ModState::Visible,
        };
        self.push(TimelineEvent::Message(Box::new(stored)));
        if !self.following {
            self.unread += 1;
        }
        self.enforce_bounds();
        IngestOutcome::Accepted(seq)
    }

    /// Remote deletion targets the original provider/room/remote id.
    /// Deletions arriving before the message tombstone it so a later
    /// replay cannot resurrect it.
    pub fn apply_delete(&mut self, source_id: &str, remote_id: &str) {
        let key = match self.sources.get(source_id) {
            Some(rt) => rt.source.key.clone(),
            None => return,
        };
        let (_, rotated) =
            self.tombstones
                .insert((key.provider, key.room.clone(), remote_id.to_owned()));
        // A rotated-out deletion guard is a reliability loss the operator
        // must see: that old id could replay back to life.
        if let Some((provider, room, _)) = rotated {
            let id = self.key_to_id.get(&SourceKey { provider, room }).cloned();
            if let Some(id) = id {
                self.mark_gap(
                    &id,
                    "replay guard rotated under overload; old deletions may resurface",
                );
            }
        }
        for ev in self.timeline.iter_mut() {
            if let TimelineEvent::Message(m) = ev {
                if m.source == key && m.remote_id.as_deref() == Some(remote_id) {
                    m.mod_state = ModState::DeletedRemote;
                }
            }
        }
        let seq = self.next_seq();
        self.push(TimelineEvent::Deletion {
            seq,
            source: key,
            remote_id: remote_id.to_owned(),
        });
    }

    /// Remote clear: mark this source's visible messages deleted, keep the
    /// tombstones for individually deleted ids.
    pub fn apply_clear(&mut self, source_id: &str) {
        let key = match self.sources.get(source_id) {
            Some(rt) => rt.source.key.clone(),
            None => return,
        };
        for ev in self.timeline.iter_mut() {
            if let TimelineEvent::Message(m) = ev {
                if m.source == key && m.mod_state == ModState::Visible {
                    m.mod_state = ModState::DeletedRemote;
                }
            }
        }
        let seq = self.next_seq();
        self.push(TimelineEvent::Clear { seq, source: key });
    }

    /// Local hide is not a remote delete and creates no tombstone.
    pub fn hide_local(&mut self, seq: u64) -> bool {
        for ev in self.timeline.iter_mut() {
            if let TimelineEvent::Message(m) = ev {
                if m.seq == seq && m.mod_state == ModState::Visible {
                    m.mod_state = ModState::HiddenLocal;
                    return true;
                }
            }
        }
        false
    }

    // ---------- timeline views ----------

    pub fn timeline(&self) -> &VecDeque<TimelineEvent> {
        &self.timeline
    }

    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    pub fn evicted_controls(&self) -> u64 {
        self.evicted_controls
    }

    pub fn unread(&self) -> u64 {
        self.unread
    }

    pub fn set_following(&mut self, following: bool) {
        self.following = following;
        if following {
            self.unread = 0;
        }
    }

    pub fn is_following(&self) -> bool {
        self.following
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn push(&mut self, ev: TimelineEvent) {
        self.bytes += event_bytes(&ev);
        self.timeline.push_back(ev);
        self.enforce_bounds();
    }
    /// Bound the live buffer. Eviction is counted and exposed — never
    /// presented as lossless. Messages rotate out first; control events
    /// (deletions, clears, gaps, connection changes) survive unless the
    /// buffer holds nothing else, so a dropped final deletion cannot go
    /// silently missing while chatter continues.
    fn enforce_bounds(&mut self) {
        while self.timeline.len() > self.limits.max_messages || self.bytes > self.limits.max_bytes {
            let oldest_message = self
                .timeline
                .iter()
                .position(|ev| matches!(ev, TimelineEvent::Message(_)));
            match oldest_message {
                Some(i) => {
                    if let Some(TimelineEvent::Message(m)) = self.timeline.remove(i) {
                        self.bytes = self.bytes.saturating_sub(message_bytes(&m));
                        self.evicted += 1;
                    }
                }
                None => match self.timeline.pop_front() {
                    Some(ev) => {
                        self.bytes = self.bytes.saturating_sub(event_bytes(&ev));
                        self.evicted_controls += 1;
                    }
                    None => break,
                },
            }
        }
    }
    // ---------- composer: drafts, reply, multi-target ----------

    pub fn composer_source(&self) -> Option<&str> {
        self.composer_source.as_deref()
    }

    pub fn set_composer_source(&mut self, id: &str) -> bool {
        if self.sources.contains_key(id) {
            self.composer_source = Some(id.to_owned());
            true
        } else {
            false
        }
    }

    pub fn draft_of(&self, id: &str) -> &str {
        self.drafts.get(id).map(String::as_str).unwrap_or("")
    }

    pub fn set_draft(&mut self, id: &str, text: String) {
        if self.sources.contains_key(id) {
            if text.is_empty() {
                self.drafts.remove(id);
            } else {
                self.drafts.insert(id.to_owned(), text);
            }
        }
    }

    pub fn reply(&self) -> Option<&ReplyBinding> {
        self.reply.as_ref()
    }

    /// Selecting Reply binds that source and message. Filtering the feed
    /// never calls this, so filtering cannot retarget a draft.
    pub fn set_reply(&mut self, source_id: &str, remote_id: &str, author_name: &str) -> bool {
        if self.sources.contains_key(source_id) {
            self.reply = Some(ReplyBinding {
                source_id: source_id.to_owned(),
                remote_id: remote_id.to_owned(),
                author_name: author_name.to_owned(),
            });
            self.composer_source = Some(source_id.to_owned());
            true
        } else {
            false
        }
    }

    pub fn clear_reply(&mut self) {
        self.reply = None;
    }

    pub fn multitarget(&self) -> &MultiTarget {
        &self.multitarget
    }

    pub fn set_multitarget_enabled(&mut self, enabled: bool) {
        self.multitarget.enabled = enabled;
    }

    pub fn toggle_multitarget_recipient(&mut self, id: &str) {
        if !self.sources.contains_key(id) {
            return;
        }
        if self.multitarget.selected.iter().any(|s| s == id) {
            self.multitarget.selected.retain(|s| s != id);
        } else {
            self.multitarget.selected.push(id.to_owned());
        }
    }

    pub fn set_multitarget_text(&mut self, text: String) {
        self.multitarget.text = text;
    }

    // ---------- outbound intents ----------

    pub fn create_intent(
        &mut self,
        source_id: &str,
        identity: &str,
        text: String,
        reply_to: Option<ReplyRef>,
        targets: Vec<String>,
    ) -> Option<OutboundIntent> {
        let (key, room_gen) = {
            let rt = self.sources.get(source_id)?;
            (rt.source.key.clone(), rt.room_gen)
        };
        let seq = self.next_seq();
        let intent = OutboundIntent {
            id: format!("chat-{}-{seq}", self.session_gen),
            source_id: source_id.to_owned(),
            source: key,
            identity: identity.to_owned(),
            room_gen,
            text,
            reply_to,
            targets,
            state: IntentState::Pending,
            remote_id: None,
            error: None,
        };
        self.intents.insert(intent.id.clone(), intent.clone());
        Some(intent)
    }

    pub fn resolve_intent(
        &mut self,
        id: &str,
        state: IntentState,
        remote_id: Option<String>,
        error: Option<String>,
    ) -> bool {
        let Some(intent) = self.intents.get_mut(id) else {
            return false;
        };
        // Terminal states are final: a confirmed send is never retried.
        if intent.state != IntentState::Pending {
            return false;
        }
        intent.state = state;
        intent.remote_id = remote_id;
        intent.error = error;
        true
    }

    /// Operator cancel: only pending intents can be withdrawn.
    pub fn cancel_intent(&mut self, id: &str) -> bool {
        self.resolve_intent(
            id,
            IntentState::Rejected,
            None,
            Some("cancelled by operator".into()),
        )
    }

    pub fn intent(&self, id: &str) -> Option<&OutboundIntent> {
        self.intents.get(id)
    }

    pub fn intents(&self) -> Vec<&OutboundIntent> {
        let mut out: Vec<_> = self.intents.values().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

#[cfg(test)]
mod bounds_tests {
    use super::*;

    fn source(id: &str) -> ChatSource {
        ChatSource {
            id: id.into(),
            key: SourceKey {
                provider: Provider::Twitch,
                room: "alice".into(),
            },
            account_id: None,
            channel: "alice".into(),
            broadcast_id: None,
            binding_ids: vec![],
        }
    }

    fn msg(gen: u64, id: &str) -> Incoming {
        Incoming {
            session_gen: gen,
            room_gen: 0,
            source_id: "tw".into(),
            remote_id: Some(id.into()),
            idless_key: None,
            author_id: None,
            author_name: "bob".into(),
            provider_ts: None,
            text: "x".into(),
            fragments: vec![],
            badges: vec![],
            reply_to: None,
            kind: MessageKind::Text,
        }
    }

    #[test]
    fn controls_survive_message_overflow() {
        let mut c = Coordinator::new(BufferLimits {
            max_messages: 3,
            max_bytes: usize::MAX,
        });
        c.upsert_source(source("tw"));
        let gen = c.session_gen();
        c.set_conn(
            gen,
            "tw",
            ConnState::Live {
                since_utc: String::new(),
            },
        );
        for i in 0..5 {
            c.ingest(msg(gen, &format!("m{i}")));
        }
        assert_eq!(c.timeline().len(), 3);
        // The connection-change control is still there; only messages
        // rotated out.
        assert!(c
            .timeline()
            .iter()
            .any(|ev| matches!(ev, TimelineEvent::ConnChange { .. })));
        assert_eq!(c.evicted(), 3);
        assert_eq!(c.evicted_controls(), 0);
    }

    #[test]
    fn tombstone_rotation_marks_desync_gap() {
        let mut c = Coordinator::new(BufferLimits::default());
        c.upsert_source(source("tw"));
        // Overflow the 20k guard with distinct deletions.
        for i in 0..20_001u64 {
            c.apply_delete("tw", &format!("gone-{i}"));
        }
        let gaps = c
            .timeline()
            .iter()
            .filter(|ev| matches!(ev, TimelineEvent::Gap { .. }))
            .count();
        assert!(gaps >= 1, "rotated replay guards must surface a gap");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: &str, provider: Provider, room: &str) -> ChatSource {
        ChatSource {
            id: id.into(),
            key: SourceKey {
                provider,
                room: room.into(),
            },
            account_id: None,
            channel: room.into(),
            broadcast_id: None,
            binding_ids: vec![],
        }
    }

    fn incoming(gen: u64, src: &str, id: Option<&str>, user: &str, text: &str) -> Incoming {
        Incoming {
            session_gen: gen,
            room_gen: 0,
            source_id: src.into(),
            remote_id: id.map(str::to_owned),
            idless_key: None,
            author_id: None,
            author_name: user.into(),
            provider_ts: None,
            text: text.into(),
            fragments: vec![],
            badges: vec![],
            reply_to: None,
            kind: MessageKind::Text,
        }
    }

    fn coord() -> Coordinator {
        let mut c = Coordinator::new(BufferLimits::default());
        c.upsert_source(source("tw", Provider::Twitch, "alice"));
        c.upsert_source(source("kk", Provider::Kick, "alice"));
        c
    }

    #[test]
    fn dedupes_by_id_not_text() {
        let mut c = coord();
        let gen = c.session_gen();
        assert!(matches!(
            c.ingest(incoming(gen, "tw", Some("m1"), "bob", "hello")),
            IngestOutcome::Accepted(_)
        ));
        // Same id, different text: duplicate.
        assert_eq!(
            c.ingest(incoming(gen, "tw", Some("m1"), "bob", "changed")),
            IngestOutcome::Duplicate
        );
        // Same text, new id: accepted.
        assert!(matches!(
            c.ingest(incoming(gen, "tw", Some("m2"), "bob", "hello")),
            IngestOutcome::Accepted(_)
        ));
        // Same remote id on another provider: different identity.
        assert!(matches!(
            c.ingest(incoming(gen, "kk", Some("m1"), "bob", "hello")),
            IngestOutcome::Accepted(_)
        ));
    }

    #[test]
    fn stale_rooms_are_ignored() {
        let mut c = coord();
        let gen = c.session_gen();
        // A loop predating the current room never merges: its messages
        // drop instead of polluting the rebound timeline.
        let mut old = incoming(gen, "tw", Some("m1"), "bob", "stale room");
        old.room_gen = 999;
        assert_eq!(c.ingest(old), IngestOutcome::StaleRoom);
        // After a rebind only the new generation ingests.
        c.rebind_room("tw");
        let room = c.room_gen_of("tw").unwrap();
        assert!(room > 0);
        let mut late = incoming(gen, "tw", Some("m2"), "bob", "late");
        late.room_gen = 0;
        assert_eq!(c.ingest(late), IngestOutcome::StaleRoom);
        let mut fresh = incoming(gen, "tw", Some("m3"), "bob", "fresh");
        fresh.room_gen = room;
        assert!(matches!(c.ingest(fresh), IngestOutcome::Accepted(_)));
    }

    #[test]
    fn delete_before_replay_tombstones() {
        let mut c = coord();
        let gen = c.session_gen();
        c.apply_delete("tw", "m9");
        assert_eq!(
            c.ingest(incoming(gen, "tw", Some("m9"), "bob", "late replay")),
            IngestOutcome::Tombstoned
        );
    }

    #[test]
    fn idless_occurrence_keys_reconcile() {
        let mut c = coord();
        let gen = c.session_gen();
        let mut rumble = incoming(gen, "kk", None, "amy", "gg");
        rumble.idless_key = Some("amy|gg|t1|occ0".into());
        assert!(matches!(c.ingest(rumble), IngestOutcome::Accepted(_)));
        // Identical snapshot redelivery: same occurrence key, duplicate.
        let mut again = incoming(gen, "kk", None, "amy", "gg");
        again.idless_key = Some("amy|gg|t1|occ0".into());
        assert_eq!(c.ingest(again), IngestOutcome::Duplicate);
        // Repeated identical message: new occurrence, accepted with
        // disclosed ambiguity (the adapter marks the key).
        let mut occ = incoming(gen, "kk", None, "amy", "gg");
        occ.idless_key = Some("amy|gg|t1|occ1".into());
        assert!(matches!(c.ingest(occ), IngestOutcome::Accepted(_)));
    }

    #[test]
    fn stale_sessions_are_ignored() {
        let mut c = coord();
        let old = c.session_gen();
        c.bump_session();
        assert_eq!(
            c.ingest(incoming(old, "tw", Some("m1"), "bob", "stale")),
            IngestOutcome::StaleSession
        );
        assert!(!c.set_conn(
            old,
            "tw",
            ConnState::Live {
                since_utc: String::new()
            }
        ));
        assert!(c.timeline().is_empty());
    }

    #[test]
    fn bounds_evict_with_counts_not_losslessness() {
        let mut c = Coordinator::new(BufferLimits {
            max_messages: 10,
            max_bytes: usize::MAX,
        });
        c.upsert_source(source("tw", Provider::Twitch, "alice"));
        let gen = c.session_gen();
        for i in 0..25 {
            c.ingest(incoming(gen, "tw", Some(&format!("m{i}")), "bob", "x"));
        }
        assert_eq!(c.timeline().len(), 10);
        assert_eq!(c.evicted(), 15);
    }

    #[test]
    fn intents_are_single_terminal() {
        let mut c = coord();
        let i = c
            .create_intent("tw", "alice", "hi".into(), None, vec!["tw".into()])
            .unwrap();
        assert!(c.resolve_intent(&i.id, IntentState::Confirmed, Some("r1".into()), None));
        // No retry or overwrite of a confirmed send.
        assert!(!c.resolve_intent(&i.id, IntentState::Confirmed, Some("r2".into()), None));
        assert!(!c.cancel_intent(&i.id));
        assert_eq!(c.intent(&i.id).unwrap().remote_id.as_deref(), Some("r1"));
    }

    #[test]
    fn reply_binds_source_and_filter_never_untargets() {
        let mut c = coord();
        assert!(c.set_reply("kk", "m3", "bob"));
        assert_eq!(c.composer_source(), Some("kk"));
        assert_eq!(c.reply().unwrap().remote_id, "m3");
        // Drafts are per source and survive reply changes elsewhere.
        c.set_draft("tw", "twitch draft".into());
        c.clear_reply();
        assert_eq!(c.draft_of("tw"), "twitch draft");
    }

    #[test]
    fn rebind_invalidates_reply_but_keeps_draft() {
        let mut c = coord();
        c.set_draft("tw", "keep me".into());
        c.set_reply("tw", "m1", "bob");
        c.rebind_room("tw");
        assert!(c.reply().is_none());
        assert_eq!(c.draft_of("tw"), "keep me");
    }
}
