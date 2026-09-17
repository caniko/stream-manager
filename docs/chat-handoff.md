# Chat handoff (externally blocked work)

## To go live per provider (operator steps, no secrets here)

1. Register one OAuth app per provider (Twitch, Google, Kick) with the
   loopback redirect `http://127.0.0.1:8912/callback` (or your own port —
   keep it loopback). Scopes: Twitch `user:read:chat` (+ `user:write:chat`
   for sending, moderation scopes for moderation); Google
   `youtube.readonly` (+ `youtube.force-ssl`); Kick `events:subscribe`
   (+ `chat:write`, moderation scopes).
2. Put the public client ids in Home Manager
   (`services.stream-manager.chat.{twitch,youtube,kick}ClientId`).
3. Provision the runtime key: 32 random bytes as raw/hex/base64 in a file
   only the operator can read, referenced by
   `services.stream-manager.tokenKeyPath`. Without it the token store
   reports **locked** — there is no plaintext fallback.
4. Put the Kick app secret in an agenix-provisioned file and reference it
   via `chat.kickClientSecretPath`. Kick requires it at token exchange.
5. Rumble: create the Live Stream API URL in the Rumble account settings,
   save it to a file, reference it via `chat.rumbleApiUrlPath`.
6. Kick relay (optional): run `kick-chat-relay` behind your reverse proxy
   with TLS, register the public URL + `chat.message.sent` subscription in
   the Kick developer portal, and set `chat.kickRelayUrl` plus the bearer
   file. See `nix/kick-relay.nix` (disabled by default; opens no firewall
   ports, registers nothing by itself).

## Deployment posture (central service)

Nomad owns canonical state (`stream-manager-server` +
`nixosModules.stream-manager-server`); Murph/Atlas/Nomad run clients.
Can administers, Dejana operates assigned channels. Full rollout order,
parent-integration snippets, and rollback live in
`docs/deployment.md`. Still open before any deployment: native client
cutover (desktop is still standalone), public HTTPS ingress decision,
live verification, paced acceptance, screenshots, `.op` update.

## Known limitations to verify live before claiming

- Twitch sender/broadcaster ids resolve at connect via `helix/users`;
  confirm EventSub subscription creation, keepalive behavior, requested
  reconnects, and Shared Chat against a real channel. The migration
  (skip-resubscribe) logic is reasoned, not live-tested.
- YouTube insert targets the collection endpoint (`.../liveChat/messages?part=snippet`);
  confirm against a real broadcast.
- YouTube gRPC `streamList` is documented but not wired; the REST
  fallback is. Quota behavior under real polls is unmeasured. Explicit
  broadcast selection needs a plan remote id to match against.
- Kick cross-channel user sends are refused until a channel directory
  lookup (needs `channel:read`) resolves target broadcaster ids.
- Kick ban/timeout and moderation-event subscriptions are planned, not
  wired. YouTube bans are planned, not wired. Rumble send/moderation has
  no verified endpoint.
- No live verification has been performed for any provider (no
  credentials existed in this environment).

## Suggested next measurements

- Real-poll YouTube quota use over a 1-hour live broadcast.
- Relay latency/throughput under a busy Kick channel.
- Paced 60-second UI workload (the current probe is an unpaced
  coordinator loop: 6,000 events, no rendering, no RSS measurement).
- Screenshot review of the Chat tab on a real display (only
  virtual-display launch has been exercised).
- Detached window: both windows mirror one iced `application` view;
  per-window routing needs a framework-level change if wanted.
