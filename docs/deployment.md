# Deployment runbook: central service on Nomad, clients on Murph/Atlas/Nomad

Agreed topology: Nomad owns provider connections, tokens, shared chat
state, and action admission. Murph, Atlas, and Nomad run native
clients. Can administers (accounts, clients, permissions); Dejana
operates assigned channels (read/send/moderate, no account
administration). Full original scope ships before deployment; no
read-only pilot, no local-subscription fallback when Nomad is down.

## What exists in this checkout

- `stream-manager-server` binary + `stream-manager-server` library:
  versioned v1 protocol (idempotent commands, cursor-ordered events,
  snapshot+gap resync), per-operator bearer auth with channel-scoped
  roles, single-writer admission reusing the chat coordinator/gate,
  encrypted server-side token store, verified token import, logout
  with revoke + forget.
- `nixosModules.stream-manager-server` (disabled by default):
  DynamicUser, state directory, systemd credentials for every secret
  file, no firewall ports, no subscriptions registered by itself.
- `nixosModules.kick-chat-relay` with the same credential treatment.
- Home Manager client settings (`chat.serverUrl`, `chat.serverTokenPath`)
  consumed by the Chat tab's Check server action.
- `packages.stream-manager-server` output for Atlas builds.

## Slice 8 progress (durable ownership + lifecycle atomicity, this checkout)

- Receipts v8 carry provider/channel authorization context; `reconcile`
  moves the durable row first (transactional unknown→terminal) and
  publishes afterwards, so post-restart and concurrent attestations
  stay consistent. Memory eviction drops without tombstoning; the
  database stays authoritative for repeats.
- Topology lock (`acct-op → topo → key → state` everywhere): account
  refresh/import/logout/upsert/remove serialize per account; connects
  install under revalidation (room, binding, current session) with the
  lock held across install + worker registration — disconnects during
  lookups abort instead of resurrecting stale loops/credentials.
- Source-id replacement requires read rights on the old channel and
  advances the room; Rumble selectors cannot name another channel and
  rooms derive from the permitted channel.
- Deletes prepare fully before dispatch; confirmations apply only to
  the admitted room. Workers report clean exits and panics via
  `WorkerExited`, preserving terminal `Ended` states.
- Verified: restart reconcile, eviction behavior, race-abort connects,
  identity-change rebinds, mid-flight rebind settlement, panic/Ended
  worker supervision.

## Slice 7 progress (canonical admission + mutation boundary, earlier)

- Protocol v2: snapshots publish a per-boot id; mutations echo
  `expected_boot` + `expected_room`. Approvals never cross a restart
  or a rebind (both reject before any network, with zero-request
  proofs).
- Canonical connect binding: sources must name an existing
  same-provider account whose channel matches (Rumble excepted, no
  OAuth account); rooms/broadcast ids resolve from provider lookups
  (mock-routable bases), never from client fields.
- Exact provider ids: refresh and import require the verified user id
  to equal the account channel id — a matching display name with a
  different id rejects as foreign. Account upserts that change
  identity, removals, and logouts all invalidate live sessions/eras;
  upserts park bound sources offline.
- One atomic admission (`plan_mutation`): permission, capability,
  boot, room, session, intent, and ticket under a single lock — no
  rebind gap. One shared settle path for send/delete/ban with
  mid-flight annotation on intents, events, AND receipts (confirmed
  receipts carry the cancel/move warning; aborts resolve intents and
  link the rejection receipt to the intent, never leak pending).
- Dispatch immediately precedes the single mutating request
  (prepare→dispatch→fire split); ban user-lookup transport errors
  resolve rejected (nothing ran), never unknown. Cancel receipts
  distinguish queued vs already-dispatched.
- `ReconcileUnknown`: channel-authorized, evidence-required
  attestation for still-unknown outcomes — never a replay; decided
  verdicts refuse; durable row moves with the memory receipt.
- Repeats (incl. unknowns and reconciled) return the original receipt
  with exact request-count proofs (0 or 1 provider mutation).
- Events: future cursors resync via gap+snapshot; transport controls
  (delete/clear/conn/gap) apply backpressure on a tokio bus instead of
  dropping; inbound messages carry room generations and stale rooms
  drop at ingest. Workers are tracked per source (abort on
  replace/disconnect/logout) and report unexpected exits as offline.
- Connects serialize per source: concurrent connects cannot stack
  duplicate loops.

## Slice 6 progress (action boundary, earlier)

- Queued→dispatched→settled gate lifecycle shared by sends and
  moderation: atomic dispatch transition, cancel-before prevents
  execution (zero provider requests, proven), cancel-after annotates
  the real verdict (proven with a delayed mock).
- Mutations bind `expected_room` from the snapshot the operator
  approved; stale rooms reject before any network.
- Unknown stays Unknown end to end (receipt, row, repeat); rotation
  preserves unknown rows for reconciliation.
- Completed repeats return the original receipt unchanged; only
  in-flight repeats report `Duplicate`.
- Provider API hosts overridable for mock transports; mock-HTTP
  lifecycle suite covers confirmed/unknown repeats, stale rooms,
  both cancel windows, and unified moderation delete.
- Coordinator moderation intents give every delete/ban full
  pending→terminal tracking with Intent events.

## Slice 5 progress (persistence ownership, earlier)

- Lifetime kernel lock (`flock` on a never-unlinked anchor, held from
  before migration to shutdown): a second live owner refuses to boot;
  a crash releases ownership with no stale file. Verified live: lock
  anchor created, second boot refused while the first runs.
- Atomic reservation claims (`BEGIN IMMEDIATE`): connections racing
  one id serialize, exactly one executes (threaded test).
- Legacy ownerless rows migrate to tombstones via a forward-only v7
  repair; the claim path additionally treats any ownerless row as
  non-executable regardless of stored status.
- Every admitted command settles through one path; durability
  failures ride on returned and cached receipts alike.
- Tombstones retained without count deletion; request ids must be
  UUIDs.
- Transactional one-time account seeding.
- The Nix module declares `databasePath`, resolves credential paths
  with the unit suffix, and carries an enabled-module evaluation
  check.

## What is NOT done (do not deploy until closed)

1. **Native client cutover.** The desktop app is still standalone: it
   owns local accounts, subscriptions, and sends. Clients must become
   thin views over the server protocol (presentation + drafts only),
   with an explicit offline state when Nomad is unreachable.
    (Server side implements, but is not yet accepted for rollout:
    canonical SQLite accounts managed through admin commands,
    scope-derived session capabilities, durable dispatch markers with
    restart recovery to outcome-unknown, permission-filtered
    snapshots/events, authorized
    connect/disconnect/cancel/send/moderation/reconcile, atomic
    idempotency with payload digests, canonical connect binding with
    exact provider ids, boot+room generation binding, shared
    prepare→dispatch→fire→settle mutation path, restart-safe
    DB-first reconciliation, topology-serialized lifecycle, supervised
    workers. Client cutover, live verification, and paced acceptance
    below still gate any deployment.)
2. **Public HTTPS ingress decision.** LAN covers the three clients, but
   Kick webhooks need a provider-reachable endpoint routed to Nomad.
   Options: existing fleet ingress (preferred — resolve the current
   Fleetix/Caddy route first) or direct Nomad ingress. Nothing here
   invents a domain or opens a listener.
3. **Live verification.** No provider calls have been made from the
   server path. Verify per provider: receive, operator-authored send,
   moderation, revocation, Nomad sleep/resume behavior.
4. **Paced acceptance.** 100 events/s × 60 s with concurrent metadata
    editing, measured RSS/queues/latency, three clients sharing one
    session, screenshots, `.op` update. (Worker exits and panics are
    now observed with `Ended` preservation; only live-provider behavior
    and load remain.)

## Parent (canix) integration — proposal, not applied

Add the project input (pin a reviewed revision, never trunk):

```nix
# canix flake inputs
stream-manager.url = "github:caniko/stream-manager/<rev>";
```

Nomad (`root/hosts/nomad/...`, exact file per maintainer placement):

```nix
imports = [ inputs.stream-manager.nixosModules.stream-manager-server ];
services.stream-manager-server = {
  enable = true;
  instance = "nomad";
  listen = "127.0.0.1:8921";
  operators = [
    { id = "can"; role = "admin"; tokenSha256 = "<can-client-hash>"; channels = []; }
    { id = "dejana"; role = "operator"; tokenSha256 = "<dejana-client-hash>";
      channels = [ { provider = "twitch"; channel = "<channel>"; } ]; }
  ];
  accounts = [
    # { id = "twitch-main"; provider = "twitch"; channelId = "<numeric id>"; channelName = "<login>"; }
  ];
  clientIds = {
    # twitch = "<public client id>";
  };
  tokenKeyFile = config.age.secrets.stream-manager-token-key.path;
  kickClientSecretFile = config.age.secrets.stream-manager-kick-secret.path; # if Kick is used
  # kickRelayUrl / rumbleApiUrlFile / kickRelayBearerFile as needed
};
```

Clients (Home Manager per user/host):

```nix
services.stream-manager = {
  enable = true;
  chat.serverUrl = "http://<nomad-lan-or-vpn>:8921";
  chat.serverTokenPath = "<client bearer file, e.g. agenix path>";
};
```

Secrets (agenix, per consumer): runtime token key (Nomad only),
Kick app secret (Nomad only), per-client bearer tokens (each client),
Rumble API URL + relay bearer (Nomad only). Generate client tokens
with `stream-manager-server --gen-token`; store only hashes in Nix.

## Rollout order

1. Authorize the release revision; build + publish from Atlas via the
   approved cache workflow; back up any existing app databases.
2. Deploy Nomad server with auto-connect off; verify health, auth
   rejection, permission denials, idempotent duplicates.
3. Install clients (Can on Atlas; Can/Dejana on Nomad; Dejana on Murph);
   verify Check server and role behavior.
4. Resolve public ingress; register Kick webhooks explicitly.
5. Import provider tokens (admin); connect one source at a time.
6. Live-verify receive → operator-authored send → moderation, one
   provider at a time, on controlled test messages.
7. Record the final capability matrix and runbook (rotation,
   revocation, Nomad sleep/outage, gap recovery).

## Rollback

Disable provider connections and outbound admission, stop/unregister
affected ingress subscriptions, restore prior package/config
generations, restore database backups if an older binary cannot read
the schema. Never replay uncertain actions during recovery.
