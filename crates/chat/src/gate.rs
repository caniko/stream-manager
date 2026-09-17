//! Pre/post-dispatch admission for outbound sends.
//!
//! One owner for send cancellation: the panel issues a ticket per intent
//! and keeps the gate's current session/room/auth generations in sync
//! with the coordinator. Executors check the ticket *before* any network
//! dispatch and re-check *after* the provider responds, so Cancel,
//! logout, disconnect, rebind, and session resets take effect even while
//! a send task is already spawned. A cancelled send never touches the
//! network; a send cancelled mid-flight is reported as potentially-sent,
//! never silently confirmed or retried.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct Ticket {
    source_id: String,
    session: u64,
    room: u64,
    auth: u64,
    cancelled: bool,
    dispatched: bool,
}

/// Post-execution settlement observed by the caller after the provider
/// responded. `dispatched` tickets were already in flight, so a cancel
/// recorded meanwhile annotates rather than prevents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    Clean,
    CancelledAfterDispatch,
    Moved,
}

#[derive(Debug, Clone, Default)]
struct Current {
    session: u64,
    rooms: HashMap<String, u64>,
    auth: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SendGate {
    tickets: Arc<Mutex<HashMap<String, Ticket>>>,
    current: Arc<Mutex<Current>>,
}

/// Cancel outcome for receipt wording: a ticket cancelled after
/// dispatch already ran, so its receipt must say the verdict stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelState {
    Missing,
    Queued,
    Dispatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    Cancelled,
    SessionMoved,
    RoomMoved,
    AuthChanged,
    UnknownIntent,
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reject::Cancelled => write!(f, "cancelled before dispatch"),
            Reject::SessionMoved => write!(f, "session moved before dispatch"),
            Reject::RoomMoved => write!(f, "room changed before dispatch"),
            Reject::AuthChanged => write!(f, "account session changed before dispatch"),
            Reject::UnknownIntent => write!(f, "no such send ticket"),
        }
    }
}

impl SendGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Capture the current generations into a new ticket.
    pub fn issue(&self, intent_id: &str, source_id: &str) {
        let cur = self
            .current
            .lock()
            .ok()
            .map(|c| c.clone())
            .unwrap_or_default();
        if let Ok(mut m) = self.tickets.lock() {
            m.insert(
                intent_id.to_owned(),
                Ticket {
                    source_id: source_id.to_owned(),
                    session: cur.session,
                    room: cur.rooms.get(source_id).copied().unwrap_or(u64::MAX),
                    auth: cur.auth,
                    cancelled: false,
                    dispatched: false,
                },
            );
        }
    }

    /// Operator cancel: marks the ticket. Returns false if already gone.
    pub fn cancel(&self, intent_id: &str) -> bool {
        !matches!(self.cancel_state(intent_id), CancelState::Missing)
    }

    /// Marks the ticket cancelled and reports whether the action was
    /// still queued or already dispatched.
    pub fn cancel_state(&self, intent_id: &str) -> CancelState {
        use CancelState::*;
        if let Ok(mut m) = self.tickets.lock() {
            if let Some(t) = m.get_mut(intent_id) {
                t.cancelled = true;
                return if t.dispatched { Dispatched } else { Queued };
            }
        }
        Missing
    }

    /// Cancel every ticket for one source (disconnect, rebind, logout).
    pub fn cancel_source(&self, source_id: &str) -> usize {
        let mut n = 0;
        if let Ok(mut m) = self.tickets.lock() {
            for t in m.values_mut().filter(|t| t.source_id == source_id) {
                t.cancelled = true;
                n += 1;
            }
        }
        n
    }

    pub fn clear(&self) {
        if let Ok(mut m) = self.tickets.lock() {
            m.clear();
        }
    }

    pub fn retire(&self, intent_id: &str) {
        if let Ok(mut m) = self.tickets.lock() {
            m.remove(intent_id);
        }
    }

    pub fn set_session(&self, session: u64) {
        if let Ok(mut c) = self.current.lock() {
            c.session = session;
        }
    }

    pub fn set_room(&self, source_id: &str, room: u64) {
        if let Ok(mut c) = self.current.lock() {
            c.rooms.insert(source_id.to_owned(), room);
        }
    }

    pub fn remove_room(&self, source_id: &str) {
        if let Ok(mut c) = self.current.lock() {
            c.rooms.remove(source_id);
        }
    }

    pub fn clear_rooms(&self) {
        if let Ok(mut c) = self.current.lock() {
            c.rooms.clear();
        }
    }

    pub fn set_auth(&self, auth: u64) {
        if let Ok(mut c) = self.current.lock() {
            c.auth = auth;
        }
    }

    /// Check immediately before network dispatch. The ticket and current
    /// generations are compared under both locks (fixed order
    /// tickets→current, the only nesting in this type), so a cancel
    /// landing mid-check cannot slip between the reads.
    pub fn admit(&self, intent_id: &str) -> Result<(), Reject> {
        let tickets = self.tickets.lock().map_err(|_| Reject::UnknownIntent)?;
        let t = tickets
            .get(intent_id)
            .cloned()
            .ok_or(Reject::UnknownIntent)?;
        let cur = self.current.lock().map_err(|_| Reject::UnknownIntent)?;
        if t.cancelled {
            return Err(Reject::Cancelled);
        }
        if t.session != cur.session {
            return Err(Reject::SessionMoved);
        }
        match cur.rooms.get(&t.source_id).copied() {
            Some(room) if room == t.room => {}
            _ => return Err(Reject::RoomMoved),
        }
        if t.auth != cur.auth {
            return Err(Reject::AuthChanged);
        }
        Ok(())
    }

    /// Re-check after the provider responded: true when anything moved
    /// mid-flight (the caller annotates instead of silently confirming).
    pub fn moved_since(&self, intent_id: &str) -> bool {
        self.admit(intent_id).is_err()
    }

    /// Atomic queued-to-dispatched transition: checks the ticket against
    /// current generations and marks it dispatched under one lock hold.
    /// A cancel landing before this call prevents execution; a cancel
    /// landing after it is recorded on the ticket and reported by
    /// [`Self::settle_status`], never silently dropped.
    pub fn dispatch(&self, intent_id: &str) -> Result<(), Reject> {
        let mut tickets = self.tickets.lock().map_err(|_| Reject::UnknownIntent)?;
        let t = tickets.get_mut(intent_id).ok_or(Reject::UnknownIntent)?;
        if t.cancelled {
            return Err(Reject::Cancelled);
        }
        if t.dispatched {
            return Err(Reject::UnknownIntent);
        }
        let cur = self.current.lock().map_err(|_| Reject::UnknownIntent)?;
        if t.session != cur.session {
            return Err(Reject::SessionMoved);
        }
        match cur.rooms.get(&t.source_id).copied() {
            Some(room) if room == t.room => {}
            _ => return Err(Reject::RoomMoved),
        }
        if t.auth != cur.auth {
            return Err(Reject::AuthChanged);
        }
        t.dispatched = true;
        Ok(())
    }

    /// Post-execution settlement. Must be called before
    /// [`Self::retire`]: a retired ticket always reads as moved.
    pub fn settle_status(&self, intent_id: &str) -> Settle {
        let Ok(tickets) = self.tickets.lock() else {
            return Settle::Moved;
        };
        let Some(t) = tickets.get(intent_id) else {
            return Settle::Moved;
        };
        let Ok(cur) = self.current.lock() else {
            return Settle::Moved;
        };
        let moved = t.session != cur.session
            || !matches!(cur.rooms.get(&t.source_id).copied(), Some(room) if room == t.room)
            || t.auth != cur.auth;
        match (t.cancelled && t.dispatched, moved) {
            (true, _) => Settle::CancelledAfterDispatch,
            (false, true) => Settle::Moved,
            (false, false) => Settle::Clean,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> SendGate {
        let g = SendGate::default();
        g.set_session(3);
        g.set_room("tw", 0);
        g.set_auth(7);
        g
    }

    #[test]
    fn cancel_blocks_dispatch() {
        let g = gate();
        g.issue("i1", "tw");
        assert!(g.admit("i1").is_ok());
        assert!(g.cancel("i1"));
        assert_eq!(g.admit("i1"), Err(Reject::Cancelled));
        assert!(g.moved_since("i1"));
    }

    #[test]
    fn generation_moves_reject() {
        let g = gate();
        g.issue("i1", "tw");
        g.set_session(4);
        assert_eq!(g.admit("i1"), Err(Reject::SessionMoved));
        g.set_session(3);
        g.set_room("tw", 1);
        assert_eq!(g.admit("i1"), Err(Reject::RoomMoved));
        g.set_room("tw", 0);
        g.set_auth(8);
        assert_eq!(g.admit("i1"), Err(Reject::AuthChanged));
        assert_eq!(g.admit("nope"), Err(Reject::UnknownIntent));
    }

    #[test]
    fn removed_room_counts_as_moved() {
        let g = gate();
        g.issue("i1", "tw");
        g.remove_room("tw");
        assert_eq!(g.admit("i1"), Err(Reject::RoomMoved));
    }

    #[test]
    fn settle_before_retire_reports_clean() {
        // Executor order: moved_since BEFORE retire. Retiring first
        // removes the ticket and every send would look moved.
        let g = gate();
        g.issue("i1", "tw");
        assert!(!g.moved_since("i1"));
        g.retire("i1");
        assert!(g.moved_since("i1"));
    }

    #[test]
    fn dispatch_marks_and_settles() {
        let g = gate();
        g.issue("i1", "tw");
        assert!(g.dispatch("i1").is_ok());
        // Double dispatch is refused: one ticket, one execution.
        assert_eq!(g.dispatch("i1"), Err(Reject::UnknownIntent));
        assert_eq!(g.settle_status("i1"), Settle::Clean);
    }

    #[test]
    fn cancel_before_dispatch_blocks_cancel_after_annotates() {
        let g = gate();
        g.issue("i1", "tw");
        assert!(g.cancel("i1"));
        assert_eq!(g.dispatch("i1"), Err(Reject::Cancelled));
        let g = gate();
        g.issue("i2", "tw");
        assert!(g.dispatch("i2").is_ok());
        assert!(g.cancel("i2"));
        assert_eq!(g.settle_status("i2"), Settle::CancelledAfterDispatch);
    }

    #[test]
    fn moved_room_settles_as_moved() {
        let g = gate();
        g.issue("i1", "tw");
        assert!(g.dispatch("i1").is_ok());
        g.set_room("tw", 9);
        assert_eq!(g.settle_status("i1"), Settle::Moved);
    }

    #[test]
    fn cancel_source_hits_only_that_source() {
        let g = gate();
        g.set_room("yt", 0);
        g.issue("i1", "tw");
        g.issue("i2", "yt");
        assert_eq!(g.cancel_source("tw"), 1);
        assert!(g.admit("i1").is_err());
        assert!(g.admit("i2").is_ok());
    }
}
