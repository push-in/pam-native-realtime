# Changelog

## 0.4.0 - 2026-10-05

### Added

- iOS Pusher protocol 7 client (Pusher Channels, Laravel Reverb, Soketi) over
  `URLSessionWebSocketTask`: public/private/presence channels with auth,
  jittered reconnect backoff, Pusher error codes, heartbeat/pong timeout,
  handshake timeout, `NWPathMonitor` network recovery, re-subscription,
  coalesced batch delivery (`pusherNext`), whisper throttling, `token()`,
  `reconnect()` and status snapshots — the same contract as Android.
- XCTest mirror with a Network.framework Pusher server (`ios/Tests`).
  Uncompiled on the release machine; needs device validation.

## 0.3.0 - 2026-10-05

### Breaking

- The raw WebSocket API moved from `Realtime` to `RealtimeSocket`
  (same methods). `Realtime` is now a static, Laravel Echo style facade.
- Requires PAM Native `>=1.0.35 <2.0.0`.

### Added

- Pusher protocol 7 client (Pusher Channels, Laravel Reverb, Soketi):
  `Realtime::pusher($url, key:)` / `Realtime::reverb()` with
  `->auth($endpoint, $bearer)`, handshake headers and `->connect()`.
- Public, private and presence channels: `listen()`, `listenForWhisper()`,
  `listenToAll()`, `stopListening()`, `whisper()`, `subscribed()`, `error()`,
  `here()` / `joining()` / `leaving()` / `members()`, `leave()`.
- Native reconnect with jittered exponential backoff, Pusher error code
  handling (terminal 4000-4099 window, immediate 4200-4299), heartbeat with
  pong timeout, handshake timeout, network-change recovery and automatic
  channel re-authorization and re-subscription.
- Push-style delivery through one pending native read completed with a
  coalesced batch (quiet window + max delay): a burst costs one PHP callback
  and one render. Connection state, repeated whispers and channel errors are
  coalesced; overflow is reported with `onDropped()`.
- Native outbound whisper throttling (100 ms per event, newest payload wins).
- `RealtimeStatus` snapshots with channels and a diagnostic journal,
  `Realtime::token()` credential refresh, `socketId()` for `X-Socket-ID`.
- Android instrumented Pusher protocol suite (MockWebServer) and a standalone
  Gradle harness.

## 0.2.0 - 2026-08-23

- Support PAM Native 0.8 through 1.x on PHP 8.5.

## 0.1.0 - 2026-08-01

- Initial public release of the documented PAM Native package contract.
- Add bounded input validation, sequential integer protocol enums, automated
  package tests, and PHP 8.4/8.5 continuous integration.

