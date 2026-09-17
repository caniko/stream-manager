# Chat capabilities (reviewed 2026-09-15; corrections 2026-09-16, review round 2)

Private operator feed. Viewer messages are never forwarded between
platforms, never rendered into video, and never touch the masking
pipeline. Statuses below separate **documented** support, **implemented**
handling, **authorization** (runtime), and **live verification** (none yet
— no provider calls were made during implementation).

## Twitch

- Receive: EventSub WebSocket `wss://eventsub.wss.twitch.tv/ws`.
  Welcome (session id + server keepalive deadline, enforced with a
  reconnect-on-miss), notification, keepalive, ping, reconnect (resume at
  the given URL; no replay promised), revocation, close. Subscriptions
  are created through the Helix EventSub API after each welcome (409
  already-subscribed accepted for reconnects); the source is Live only
  after Helix confirms, and 401/403 stops the loop instead of retrying.
  Conditions address the resolved broadcaster id, never the auth user.
  Requested reconnects migrate subscriptions server-side (no recreate);
  dead connections always resubscribe fresh. Duplicate connects while
  receiving are refused, so one source owns one loop and one subscription
  set.
  One connection carries compatible subscriptions
  (`channel.chat.message`, `channel.chat.notification`,
  `channel.chat.message_delete`, `channel.chat.clear`); duplicate envelope
  ids are suppressed.
  Ref: <https://dev.twitch.tv/docs/eventsub/handling-websocket-events/>
- Send: `POST https://api.twitch.tv/helix/chat/messages` with `Client-Id`
  + user bearer; native `reply_parent_message_id`; verdict is
  `data[].is_sent` (`drop_reason` on refusal).
  Ref: <https://dev.twitch.tv/docs/chat/send-receive-messages/>
- Auth: user token; `user:read:chat` for reading, `user:write:chat`
  additionally for sending, `moderator:manage:chat_messages` /
  `moderator:manage:banned_users` for moderation.
  Ref: <https://dev.twitch.tv/docs/chat/authenticating/>
- Moderation: single-message delete (`DELETE …/moderation/chat`), ban and
  timeout (`POST …/moderation/bans`). User ids resolve on Twitch only —
  same names elsewhere are never touched.
- Shared Chat provenance (`source_broadcaster_user_login`) is kept as a
  badge; sends stay addressed to the resolved broadcaster id.
- Simulcasting policy rechecked 2026-09-15:
  <https://help.twitch.tv/s/article/simulcasting-guidelines> —
  no viewer-message relay is implemented, so no public output is produced
  here. Terms: <https://legal.twitch.com/en/legal/terms-of-service/>
- Limits: 500 characters per send (enforced before POST).

## YouTube

- Receive (primary documented path): gRPC
  `V3DataLiveChatMessageService.StreamList` on
  `youtube.googleapis.com:443` with a generated client from the published
  `stream_list.proto`. The app implements the documented REST `list`
  fallback: continuation tokens retained, `pollingIntervalMillis` honored
  (floored at 1 s), quota/rate-limit backoff.
  Ref: <https://developers.google.com/youtube/v3/live/streaming-live-chat>
  and <https://developers.google.com/youtube/v3/live/docs/liveChatMessages/list>
- The actual broadcast's `liveChatId` is resolved via
  `liveBroadcasts.list(mine=true, broadcastStatus=active)`; an explicit
  plan broadcast id wins, an absent selection errors instead of falling
  back to another broadcast. A channel or video id is never substituted.
  No active broadcast = explicit offline state, not polling.
- Receive loop: 401 stops the loop as unauthorized (reconnect the
  account); other errors back off with a visible gap. Live is announced
  only after a successfully parsed page.
- Send: `POST .../youtube/v3/liveChat/messages?part=snippet` (200
  characters, enforced; the earlier `/insert` suffixed path was wrong and
  is corrected here). No native reply exists: replies are refused,
  plain-message fallback is explicit.
- Moderation: `liveChatMessages.delete` implemented; bans are documented
  but not yet wired (matrix: planned). No timeout endpoint (unsupported).
- Paid/member events (`superChatEvent`, `superStickerEvent`, membership
  gifting, new sponsors) are preserved; `messageDeletedEvent` tombstones;
  `chatEndedEvent` ends the source.
- Auth: Google OAuth; readonly scope for reading, `youtube.force-ssl`
  for sending/moderation. One space-delimited `scope` parameter per
  RFC 6749 (no repeated keys). Separate read / +send / +mod onboarding
  tiers; moderation scopes are requested explicitly, never implied.
  Identity verified via `channels.list(mine)`, scopes via
  `oauth2/v3/tokeninfo` (recorded, never assumed). Logout revokes at the
  provider (best effort) and deletes the stored refresh; in-flight OAuth
  tasks cannot persist after logout or session reset.
- No broad searches, no aggressive polling: one poll loop per broadcast.

## Kick

- Receive: official webhooks (`chat.message.sent`) via the opt-in relay:
  public HTTPS ingress (operator reverse proxy) → `kick-chat-relay` →
  authenticated outbound envelope stream → desktop. Localhost is not
  assumed reachable (per Kick's own warning). The relay preserves the
  verified `Kick-Event-Type` in an envelope with the untouched body;
  desktops parse the official shape (numeric ids, `replies_to`, sender
  identity badges) and drop other channels on a shared stream with one
  visible gap rather than mislabeling.
  Ref: <https://docs.kick.com/llms.txt>, <https://docs.kick.com/apis/chat>,
  <https://docs.kick.com/events/event-types.md>,
  `events/introduction.md` and `events/webhook-security.md` in
  KickEngineering/KickDevDocs (reviewed 2026-09-15; payload shape
  rechecked 2026-09-16).
- Signature: RSA PKCS#1 v1.5 + SHA-256 over
  `messageId.timestamp.rawBody` (base64), verified against Kick's published
  public key (also fetchable from `/public/v1/public-key`). Never HMAC.
- Ingress guards: 64 KiB body cap, 5-minute timestamp skew (replay
  protection), channel allowlist, 10k-id dedupe window, 512-deep broadcast
  backlog. Rejections are `401`/`429`-shaped, never silent.
- Send: `POST /public/v1/chat` as `type: "user"` with integer
  `broadcaster_user_id` (verified 2026-09-15 against the published
  OpenAPI); limits 500 characters **and** 2048 UTF-8 bytes, both enforced.
  Verdict is `data.is_sent` + `data.message_id`. Only the operator's own
  channel is addressable until a channel directory lookup exists — other
  targets are refused, not mis-sent. Native `reply_to_message_id`.
- Delete: `DELETE /public/v1/chat/{message_id}`
  (scope `moderation:chat_message:manage`) — implemented.
  Ban/timeout/moderation events: documented, not yet wired (planned).
- Auth: OAuth authorization flow with PKCE **and** client secret
  (both required per Kick's token docs); secret is operator-provisioned at
  runtime, never embedded. Scopes: `events:subscribe` (read),
  `chat:write` (send), `moderation:chat_message:manage` + `moderation:ban`.
  Subscribe via the developer portal / events-subscription endpoint;
  unprocessed endpoints auto-unsubscribe after ~1 day (re-subscribe).
- Without relay access the source shows `Relay required`; everything else
  keeps working.

## Rumble (read-only)

- Receive: official Live Stream API polled every 15 s
  (`recent_messages`, `recent_rants`).
  Ref: <https://rumble.support/help/how-to-use-rumble-s-live-stream-api>
  (reviewed 2026-09-15).
- No stable message ids: occurrence-aware overlap reconciliation; repeats
  are occurrence-suffixed and counted as ambiguous; window movement is
  reported as possible loss, never claimed complete. An explicit plan
  stream id selects one livestream; otherwise the live one, else the
  first entry.
- The API URL is a bearer secret (responses may contain a stream key):
  configured as a file path, parsed narrowly, never logged or stored.
- Send/moderation: no verified official endpoint → unavailable in the
  matrix; the UI offers the external native-chat action instead.
  The manual metadata workflow does not imply chat writability.

## Execution and storage guarantees (2026-09-16 review rounds)

- Settlement is computed before ticket retirement: clean sends are not
  misflagged, and cancel-after-dispatch keeps the provider's real
  verdict. Cancel marks the gate only; the outcome resolves the intent.
- Metadata saves are single-writer: a save in flight queues one
  follow-up instead of stacking concurrent writers; errors never
  auto-retry.
- Refresh uses each provider's own client id/secret under the onboarding
  store identity; logout revokes at the provider (best effort) and deletes
  the stored refresh, and in-flight OAuth tasks cannot persist afterwards.
- Provider errors report status plus an allowlisted error code only —
  never response bodies.
- Refresh uses the onboarding store key and each provider's own client
  id/secret under the same lock; rotated tokens persist and continue the
  send. Logout revokes (best effort) and forgets; rotation-write trouble
  surfaces as a confirmed-with-note, never a silent retry.
- Connection results carry session + room generations; disconnects and
  rebinds advance the room so late results cannot restore state.
  Restored sources are bound to their database. Backoff and pacing
  sleeps are cancellable; stalled relay streams time out into a gap.
- The event bus is bounded (4,096) with per-source overflow gaps;
  messages evict before controls; tombstone-guard rotation marks the
  source desynchronized instead of silently forgetting deletions.
- Token keys: exact 32 bytes used unchanged; text forms precisely
  defined; fsync failures are errors; read-modify-write holds a
  cross-process file lock.
- Rumble selection fails closed on an absent id; reviews bind approval
  to edit generation + epoch; load failures are tagged like loads.
- Control events (delete/clear/conn/gap) carry per-source room
  generations; overflow gaps are the only untagged notices and report
  real drops. Disconnects and rebinds advance the room; duplicate
  connects while receiving are refused. Backoff and pacing sleeps are
  cancellable; stalled relay streams time out.
- Provider errors classify by status plus an allowlisted error code;
  bodies never reach errors or logs. Token keys are exact (32 raw bytes
  or precisely defined text); fsync failures are errors; rotations hold
  a cross-process file lock.

- Outbound sends go through one admission gate: cancel/logout/
  disconnect/rebind/session-reset cancel tickets; executors check before
  any network dispatch and annotate mid-flight moves (potentially-sent,
  never silently confirmed, never retried). Token refresh uses the same
  store key, client id, and secret as onboarding, and rotated tokens
  persist.
- Token store: AES-256-GCM, process-wide per-path rotation lock, unique
  exclusive temp files with fsync, 0600, key-format validation without
  silent mangling, truncated provider error detail.
- Runtime: bounded 4,096-event bus with per-source drop counters and
  overflow gaps; full allocation accounting under the byte ceiling;
  insertion-ordered dedupe/tombstone guards; relay lag terminates the
  stream into a reconnect gap.
- Metadata: atomic revision-guarded saves with follow-up convergence
  (no false conflicts), creations serialized behind saves, tagged load
  failures, approvals bound to edit generation + database epoch, CTA
  presets only from a fresh error-free review.

## Matrix legend (UI + `ChatCaps`)

- documented: Api / Manual / Unsupported (from the docs above).
- implemented: whether this checkout wires it.
- authorized: current account scopes/session (runtime, per account).
- verified: live provider confirmation — **none** as of this writing.
