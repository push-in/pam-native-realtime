# Changelog

## 0.4.3 - 2026-11-18

- Fixed: liveness measures silence on a clock that counts deep sleep; returning to the foreground probes a socket silent for 30 s and reconnects if nothing answers in 5 s.
- Fixed: channel auth requests time out (8 s read / 10 s call) and retry with backoff.

## 0.4.2 - 2026-11-18

- Android: a recreated Activity no longer leaves the app without realtime. PAM Native retires the previous surface's module registry while the PHP runtime keeps running, and `RealtimeModule` closed every Pusher client and WebSocket there: PHP kept a stale `Connected` state, later calls hit a module with no client and nothing reconnected until the process was restarted. `RealtimeModule` is now a `SurfaceRetirableModule` that hands its live clients and sockets to the module replacing it (the read PHP left pending still completes).
- A failed native read (the client was closed or is unknown) no longer stops delivery silently: the connection opens the native client again once, with every channel and the latest bearer, reports `Disconnected` if that fails too, and `reconnect()` reopens a lost client.

## 0.4.1 - 2026-11-17

- Android 7.0/7.1 support: the plugin declares minSdk 24 and every newer Android API it uses has a version check and fallback. Requires PAM Native 1.53.1 or later.

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

