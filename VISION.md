# stream-manager vision

`stream-manager` is a native Rust/iced desktop app where an operator prepares
an upcoming livestream, edits per-destination metadata, and publishes it to
several platforms from one UI. It is deliberately metadata-oriented: the MVP
handles titles, descriptions, tags, thumbnails and funnels per destination,
stores them in SQLite, and applies them through provider APIs. Video ingest,
forwarding and stream keys stay with InstantClone, and the app never replaces
OBS.

## Problem

Streaming to Twitch, YouTube, Kick and Rumble at once means the same metadata
entered four times, in four dashboards, with four sets of quirks, and no record
of what was actually published. The project gives that work one review screen:
each destination shows its own overrides, every field is bound to a provider
capability, and apply is per-destination so a failure on one platform does not
invalidate the others. The second half of the problem is chat: four separate
chat clients for one broadcast, with no shared read-only view and no safe way
to answer from one place.

## Principles

- **Metadata only, video untouched.** The README states the boundary directly:
  this is not an OBS replacement; live per-destination masking is a designed
  but deferred phase (ADR-001) and stays disabled until its acceptance tests
  pass.
- **Capabilities are declared, then enforced.** `docs/chat-capabilities.md`
  separates documented, implemented, authorized and live-verified support per
  provider, rather than assuming parity. Sends are gated on scopes; failures
  surface instead of being retried blindly.
- **Chat is a private operator console.** One merged feed over the selected
  destinations with per-source replies. Viewer messages are never relayed
  between platforms, never drawn into video, and never touch the masking
  pipeline.
- **Secrets never leak into arguments or logs.** Stream keys and provider
  tokens stay out of FFmpeg command lines, logs and the app DB; the token store
  is AES-256-GCM with an operator-provisioned runtime key and reports locked
  rather than falling back to plaintext.
- **Transport declaration is not authorization.** The Destinations tab shows
  both: a destination can be configured for forwarding while its metadata
  credentials are still missing, and the UI distinguishes them.
- **Optional infrastructure opts in explicitly.** The Kick relay and the
  central server module exist, but are never auto-deployed, register nothing
  by themselves, and open no ports by default.
- **Retries are scoped and reconciled.** Retry touches failed actions only, and
  ambiguous outcomes are reconciled before any retry.

## Scope

- Drafting and preparing stream metadata with reusable templates.
- Per-destination overrides and provider-specific validation, rendered from
  reviewed provider documentation.
- Apply/publish flow with per-destination results, history and reconciliation.
- Account onboarding: OAuth apps with loopback redirects, token store, account
  connect and scope upgrades.
- Chat workspace: merged feed, per-source reply, source health, and
  capability-gated moderation (delete, ban, timeout).
- A headless core crate plus an opt-in Kick webhook relay and an opt-in central
  state service, so native clients share canonical chat, account and action
  state.

## Non-goals

- Replacing OBS or owning video encoding, ingest and forwarding.
- Live per-destination video masking in the MVP; see ADR-001 for the accepted
  design and its acceptance gates.
- Relaying viewer messages between platforms, chat overlays on video, or any
  coupling between chat and the masking pipeline.
- Storing or provisioning stream keys and ingest credentials; InstantClone and
  canix's secret tooling own that.
- Automatic deployment of the relay or the central server.

## Direction

The repository's commit history is a single large feature landing plus
in-progress work on provider adapters (Twitch, Kick, Rumble), relay
protocol and server state. The chat handoff document lists what remains
externally blocked: OAuth app registration, runtime key and Kick secret
provisioning, and Rumble API URL setup — operator steps, not code. Recent
design scripts add p7/p8 chat layouts for medium and narrow widths. Live
masking remains the next designed phase, gated on its acceptance tests.

## Place in the fleet

Canix references the repository from atlas's Simit updater list, and the repo
ships `nix/home-manager.nix` for the client plus a `nixosModules` service for
the central server; `docs/deployment.md` describes Nomad as the owner of
canonical state with Murph, Atlas and Nomad running clients.
