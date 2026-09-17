# stream-manager

Native Rust/iced app for preparing upcoming livestreams and publishing their
metadata from one centralized UI. Metadata/UI MVP; live per-destination video
masking is a deferred, explicitly designed phase (see `docs/adr-live-masking.md`).

Not an OBS replacement. InstantClone keeps ingest/forwarding and stream keys;
Canix/Home Manager/agenix keep installation and credential provisioning.

UI design source: `design/stream-manager.op` (OpenPencil) plus
replayable `design/p*.js` scripts (p6 = Chat).

Chat is a private operator console: one merged feed over the selected
destinations with per-source replies, health, and capability-gated
moderation. No viewer-message relay, no chat overlay on video, no
masking-pipeline coupling. See `docs/chat-capabilities.md` (per-provider
matrix) and `docs/chat-handoff.md` (operator setup + blocked work).

## Launch

```sh
nix run .#stream-manager
STREAM_MANAGER_DEMO=1 nix run .#stream-manager  # labelled offline demo
```

Dev shell / checks:

```sh
nix develop
cargo test --workspace --locked
cargo clippy --workspace --all-targets -- --deny warnings
cargo fmt --all -- --check
nix flake check --no-update-lock-file
```

## Account onboarding (operator, no production action here)

1. Provision ingest via existing InstantClone flow (`canix secret instantclone …`).
   Transport declaration ≠ metadata authorization; the Destinations tab shows both.
2. Register one OAuth app per provider (Twitch, Google, Kick) with the
   loopback redirect (default `http://127.0.0.1:8912/callback`); put the
   public client ids in `services.stream-manager.chat.*ClientId`.
3. Provision the 32-byte runtime key file and reference it via
   `services.stream-manager.tokenKeyPath` (locked state until then — never
   plaintext fallback). Kick also needs its app secret in an
   agenix-provisioned file (`chat.kickClientSecretPath`).
4. Connect each account in the Chat tab (read-only first; add send scope
   only for sending). Rumble needs only its API-URL file
   (`chat.rumbleApiUrlPath`); Kick live chat needs the relay
   (`chat.kickRelayUrl`, or it shows Relay required).
5. Review → apply per destination. Retries touch failed actions only;
   ambiguous outcomes reconcile before any retry.
   Rumble is manual-first: copy rendered text, open dashboard, paste back the
   public URL, record completion.

Kick relay (opt-in, never auto-deployed):

```sh
kick-chat-relay --listen 127.0.0.1:8913 --public-key PATH --bearer PATH --channels PATH
```

Serve it behind your own TLS reverse proxy, register the public URL in the
Kick developer portal, and point `chat.kickRelayUrl` at the proxy. There is
a disabled-by-default NixOS module (`nixosModules.kick-chat-relay`).

## Status (verified 2026-09-16; review rounds 1–8, deployment slices 1–5)

- Implemented: persisted SQLite editing with atomic revision-guarded saves
  (plan + destinations commit together; stale writers get an explicit
  conflict, edits kept; follow-up saves converge on the persisted
  revision; creations serialize behind saves); epoch-tagged loads
  (results and failures ignored after demo switches/navigation);
  execution admission rechecks the persisted plan revision; approvals
  bound to edit generation + database epoch; immutable review snapshots
  through one central gate; CTA presets only from a fresh error-free
  review.
- Chat workspace (`crates/chat` + `src/ui/chat.rs`): normalized events,
  single coordinator (5,000-message + byte-bounded buffer with full
  allocation accounting, dedupe by provider/room/id behind
  insertion-ordered guards, tombstones, gaps, per-source drafts, reply
  binding, broadcaster-authored multi-target with preview), one send
  admission gate (cancel/logout/disconnect/rebind/session-reset cancel
  tickets; pre-dispatch checks plus mid-flight annotation; unified
  refresh with rotation persistence), documented adapters for all four
  providers (Twitch Helix-managed EventSub + keepalive + reconnect URL +
  Helix verdicts, YouTube liveChatId resolution with explicit selection +
  auth-aware polling + paid/member/deleted/ended handling, Kick official
  payload + envelope relay + per-source channel filter + typed
  send/delete, Rumble read-only occurrence-aware polling with stream
  selection), AES-256-GCM runtime token store (process-wide lock, unique
  temp files, fsync, validated keys) with locked state, native OAuth
  (PKCE, state validation, loopback callback, scope verification),
  capability-gated send/delete/ban-timeout, deterministic offline demo.
- Locally verified: 184 tests (incl. all of the above plus atomic
  claims across connections, rotation tombstoning with memory mirror,
  seed-once stickiness, lock exclusivity with release-on-drop,
  tombstone expiry rejection, persist-failure advisories on returned
  and cached receipts, legacy repair migration, unknown-preservation
  in rotation, mock-HTTP action lifecycle: confirmed/unknown repeats
  with exact request counts, stale-room/boot and cancel-before
  zero-request proofs, command-path cancel-after-dispatch verdict
  preservation, unified moderation delete lifecycle, canonical
  connect binding (unbound/foreign/mismatched rejections +
  server-resolved room), strict provider-id import rejection,
  leak-free ban abort, evidence-based reconcile flow, future-cursor
   resync, stale-room ingest drops, control backpressure, restart-safe
  DB-first reconcile with row-carried authorization, v7→v8 upgrade
  with context backfill, eviction-without-tombstones,
  topology-serialized lifecycle with race-abort connects,
  identity-change rebinds, server-resolved Rumble rooms, pre-dispatch
  delete preparation, room-bound settlement, panicking-worker
  observation with `Ended` preservation), Clippy `-D warnings`,
  rustfmt, Nix flake check (eval via explicit path ref — new files
  unstaged by policy, including the enabled server-module
  evaluation), all binaries link, relay + server loopback smokes
  (lock anchor, second-boot refusal while live, terminal
  persistence, auth, denials, concurrent duplicates → one action,
  payload-reuse rejection, account upsert/dup, resync),
  virtual-display launch without panic (software rendering), software
  sample encode.
- Central service (`crates/server` + `nixosModules.stream-manager-server`,
  disabled by default): versioned (v2) idempotent protocol keyed by
  (operator, request id) with SHA-256 payload digests, atomic
  reservation claims, per-operator roles (Can admin / Dejana
  operator), canonical SQLite accounts managed through admin commands
  (transactional one-time seed, no reseed; identity changes revoke
  live sessions), scope-derived session
  capabilities, durable dispatch markers with restart recovery to
  outcome-unknown, tombstone expiry for evicted ids (reuse rejected,
  never rerun; retained without count cap), queued→dispatched→settled
  gate lifecycle shared by sends and moderation through one shared
  prepare→dispatch→fire→settle path, expected-boot + expected-room
  generation binding on mutations, completed repeats returning the
  original receipt, evidence-based reconcile for unknowns (durable
  row moves first, restart-safe, never a replay), topology-serialized
  source/account lifecycle with revalidating installs, supervised
  workers, surfaced durability
  failures on returned and cached receipts, lifetime kernel lock on a
  persistent anchor (crash cannot stale it), server-owned admission
  reusing coordinator/gate/adapters, verified token import, logout
  with revoke + forget. Desktop client cutover is still open — see
  `docs/deployment.md` for the full rollout plan and blockers.
- Load honesty: the 6,000-event probe is an unpaced coordinator loop
  (no pacing, rendering, or RSS measurement) — a paced 60-second UI
  workload is still open (see `docs/chat-handoff.md`).
- Provider-verified: none — no credentials collected, no production calls.
  HTTP behavior is covered by mock-server contracts only.
- Read-only here: Rumble send/moderation (no verified endpoint).
- Deferred: live provider verification, YouTube gRPC streamList wiring,
  YouTube bans, Kick cross-channel sends and ban/timeout endpoints, live
  masking (ADR acceptance gates unmet), desktop screenshots (no tooling
  on this host), `.op` design update (p6 replay script only).
