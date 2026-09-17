# ADR-001: Live per-destination masking (next phase, not implemented)

Status: accepted design, deferred implementation. Live masking stays
**disabled** until the acceptance tests below pass. The MVP ships stored
`VideoProfile`s and a labelled native preview only.

## Constraints from the current system

- OBS emits one clean program stream. InstantClone forwards it unchanged to
  each enabled destination (`managed.rs` renders keys at startup; dashboard
  mutation endpoints return 403; delay/dock changes are ephemeral).
- Verified against `root/packages/instantclone/managed.rs`: the HTTP
  allowlist exposes monitoring and overlay/dock state, **not** a readable
  media relay and **not** arbitrary live compositing. Do not treat the
  publish URL as a readable relay.
- Secrets: provider keys and stream keys never appear in FFmpeg process
  arguments, logs, or the app DB. Reuse `SecretRef` (runtime paths) and the
  encrypted token store.

## Design

1. Preserve the clean path: OBS → InstantClone → primary/mirror
   destinations stays byte-identical (passthrough, original encoding).
2. Branch **before** destination forwarding for funnel destinations only.
3. Decode → composite (CTA banner/QR, opaque region mask, full slate from
   the reviewed revision) → re-encode **only** transformed variants.
4. Share one encode across destinations with identical video/audio/output
   profiles; otherwise separate encodes.
5. Transport: run separately routed InstantClone instance(s) (or the
   smallest explicit extension) carrying **disjoint** destination sets —
   transformed variants on one, passthrough on the other. No duplicate
   production egress, no funnel overlay in OBS's common output.
6. Encoders: probe at startup — AMD VAAPI, NVIDIA NVENC, explicit software
   fallback. Probe = capability list **plus** a real sample encode on the
   selected device (permissions included). GUI GPU selection is independent
   of the encoder device; describe CPU/GPU copies honestly, no zero-copy
   claims without measurement.
7. Runtime: bounded queues with backpressure, A/V sync correction,
   per-destination latency reporting, failure isolation. If a
   mask/compositor fails: stop that destination or cut to a verified safe
   slate — never silently fall back to unmasked video.

## Acceptance (all required before enablement)

- Synthetic-video tests: unchanged receiver gets bit-identical output;
  masked receiver shows the overlay at the specified geometry; encoded
  output inspected (not just preview pixels).
- Transition/reconnect cases: mid-stream profile change, destination
  reconnect, encoder failure → safe slate or stopped destination.
- VAAPI **and** NVENC sample encodes pass on real hardware; software
  fallback passes. An unavailable device is reported untested, not passed.
- Twitch outputs carry no funnel overlay and no degrading mask (policy
  check enforced on both copy and profile).

## Ordered tasks

1. Transport spike: confirm branch point against pinned InstantClone
   (relay readability, second instance routing, disjoint sets).
2. Compositor prototype (FFmpeg filter graph) driven by stored profiles.
3. Encoder probe + sample-encode harness (VAAPI/NVENC/software).
4. Bounded queue/backpressure/A-V sync + latency reporting.
5. Failure-isolation + safe-slate behavior with tests.
6. Synthetic-video acceptance suite in CI; wire enablement gate.
