//! Private operator chat workspace.
//!
//! One merged operator feed over the selected streaming destinations.
//! This is not a viewer-facing bridge: viewer messages are never forwarded
//! between platforms, and nothing here renders into the video pipeline.
//!
//! Boundaries:
//! - [`model`] — normalized events, sources, capabilities, outbound intents.
//! - [`coordinator`] — single-threaded session: dedupe, tombstones, gaps,
//!   bounded buffers, drafts, reply bindings, multi-target selection.
//! - [`adapters`] — documented provider protocol handling (parse, verify,
//!   reconcile, request builders) with mock-contract tests.
//! - [`runtime`] — cancellable transport loops feeding a coordinator.
//! - [`demo`] — deterministic offline script; production networking off.

pub mod adapters;
pub mod coordinator;
pub mod demo;
pub mod gate;
pub mod model;
pub mod runtime;

pub use coordinator::{BufferLimits, Coordinator};
pub use gate::{Reject as GateReject, SendGate};
pub use model::{Capability, ChatCaps, ChatSource, ConnState, Support};
