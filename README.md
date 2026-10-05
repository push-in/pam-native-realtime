<!-- pam:product-page:start -->
<div align="center">

# PAM Native Realtime

**Laravel Echo for PAM Native: Pusher and Reverb channels, owned by the platform.**

Laravel Echo style Pusher/Reverb channels with native reconnects and batched delivery.

[![Latest version](https://img.shields.io/packagist/v/pushinbr/pam-native-realtime?style=flat-square&label=stable)](https://packagist.org/packages/pushinbr/pam-native-realtime)
[![CI](https://img.shields.io/github/actions/workflow/status/push-in/pam-native-realtime/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/push-in/pam-native-realtime/actions)
![PHP](https://img.shields.io/badge/PHP-8.5-777BB4?style=flat-square&logo=php&logoColor=white)
![Android](https://img.shields.io/badge/Android-API%2026%2B-3DDC84?style=flat-square&logo=android&logoColor=white)
![iOS](https://img.shields.io/badge/iOS-15%2B-000000?style=flat-square&logo=apple&logoColor=white)

**[Documentation](https://push-in.github.io/pam-docs/native/overview/) · [Quick start](#quick-start) · [What you can build](#what-you-can-build) · [PAM ecosystem](https://push-in.github.io/pam-docs/ecosystem/) · [Issues](https://github.com/push-in/pam-native-realtime/issues)**

</div>

---

## Why PAM Native Realtime

Subscribe to public, private and presence channels on Pusher Channels, Laravel Reverb or Soketi. The socket, the protocol, channel auth, heartbeats, reconnects and re-subscription run natively; PHP receives coalesced batches. The public API is strictly typed for PHP 8.5; expensive or frame-sensitive work stays in Rust or the platform SDK instead of crossing the application boundary every frame.

| | |
| --- | --- |
| **Best for** | A focused capability you can add to any PAM Native application |
| **Native path** | OkHttp WebSocket · URLSessionWebSocketTask |
| **Application model** | Composer package + generated native integration |
| **Design rule** | Independent module; no feed, vertical, or application template bundled |

## What you can build

- Chat, presence, and collaborative state
- Live dashboards and operations consoles
- Realtime alerts and server-driven updates

## Quick start

Already have a PAM Native project? Add only this capability:

```bash
pam composer require pushinbr/pam-native-realtime
pam doctor --fix
```

New to PAM? Follow the **[five-minute PAM Native setup](https://push-in.github.io/pam-docs/native/overview/)** once, then return here. Your application stays a normal Composer project with a committed lockfile.
<!-- pam:product-page:end -->

## See it in action

```php
use Pam\Native\Realtime\PresenceMember;
use Pam\Native\Realtime\Realtime;
use Pam\Native\Realtime\RealtimeMessage;
use Pam\Native\Realtime\RealtimeStatus;
use Pam\Native\Realtime\Secret;

// Once, after sign-in (Laravel Reverb / Pusher Channels / Soketi).
Realtime::pusher('wss://ws.example.com', key: 'app-key')            // or Realtime::reverb('ws.example.com', 'app-key')
    ->auth('https://api.example.com/broadcasting/auth', Secret::value($token))
    ->header('Origin', 'https://example.com')
    ->onConnected(fn (RealtimeStatus $s) => $this->catchUp())        // first connect and every reconnect
    ->connect();

// Echo style channels, from any screen.
Realtime::channel('private-chat.42')                                 // or Realtime::private('chat.42')
    ->listen('message.sent', fn (RealtimeMessage $m) => $this->append($m->json()))
    ->listenForWhisper('typing', fn (RealtimeMessage $m) => $this->typing($m->userId));

Realtime::join('chat-typing.42')                                     // presence-chat-typing.42
    ->here(fn (array $members) => $this->online = count($members))
    ->joining(fn (PresenceMember $m) => ...)
    ->leaving(fn (PresenceMember $m) => ...)
    ->whisper('typing', ['typing' => true]);                         // sent as client-typing

Realtime::leave('chat.42');                                          // leaves chat.42, private-… and presence-…
Realtime::token($refreshed);                                         // new bearer; 401/403 channels retry at once
$headers['X-Socket-ID'] = Realtime::socketId() ?? '';                 // Laravel toOthers()
```

### One render per burst

Every PHP callback costs a full render in PAM Native, so frames never cross
one by one. The native client buffers events and completes PHP's single
pending read with **one batch** once the stream has been quiet for 32 ms (at
most 150 ms after the first buffered event; tune with `->coalesce()`). Within a
batch, the connection state, repeated whispers of the same sender and channel
errors are coalesced to their latest value; server events are never merged
and keep their order. If PHP is suspended long enough to overflow the bounded
queue (`->buffer()`), `onDropped(fn (int $count))` tells you to resynchronize.

There is no polling and no PHP timer: the read is a pending native completion,
like `Sensors::watch()`.

### Connection lifecycle (native)

- `ConnectionState`: `Connecting`, `Connected`, `Unavailable` (reconnect
  scheduled), `Disconnected` (explicit) and `Failed` (Pusher 4000-4099, retried
  after `->terminalRetry()` — 5 minutes by default, the Reverb `4009` case).
- Reconnect with jittered exponential backoff (`->backoff(400, 15000, 1.8)`),
  immediately for 4200-4299, after backoff for 4100-4199.
- `pusher:ping` after `activity_timeout` of silence (server value or
  `->heartbeat()`), reconnect when nothing answers in 12 s; a handshake that
  does not finish in 10 s is retried.
- Network callbacks: when connectivity returns, a pending backoff is skipped;
  when the default network changes, the socket is probed.
- Every channel is re-authorized and re-subscribed after each reconnect.
  Auth failures retry with backoff (350 ms – 8 s), `401`/`403` after 60 s or
  right after `Realtime::token()`.
- Outbound whispers are throttled natively to one frame per 100 ms per event
  (the newest payload wins), within the Pusher client-event limit.
- `Realtime::status(fn (RealtimeStatus $s) => ...)` returns the state, socket id,
  channels and a 40-entry diagnostic journal for support screens.

Hot reload safe: connecting again with the same name (`connect('default')`)
replaces the native client.

## What installation does

`pam add realtime` resolves the official compatible package, performs a non-mutating Composer preflight, updates the normal `composer.json` and `composer.lock`, refreshes generated native integration when required, and leaves the project ready for `pam doctor` validation.

Use `pam packages` to inspect availability and `pam remove realtime` to uninstall the capability safely. Direct Composer commands are an advanced interoperability path; PAM is the supported application workflow.

## API guide

| API | Responsibility |
| --- | --- |
| `Realtime` | Static facade: `pusher()`, `reverb()`, `channel()`, `private()`, `join()`, `presence()`, `leave()`, `token()`, `reconnect()`, `socketId()`, `state()`, `status()`, `disconnect()`, `connection($name)`. |
| `PusherConnector` | `auth()`, `bearer()`, `header()`, `backoff()`, `heartbeat()`, `handshakeTimeout()`, `terminalRetry()`, `coalesce()`, `buffer()`, `namespace()`, `onState()`, `onConnected()`, `onDropped()`, `connect()`. |
| `RealtimeConnection` | The live connection behind the facade (several named connections are supported). |
| `Channel` / `PresenceChannel` | `listen()`, `listenForWhisper()`, `listenToAll()`, `stopListening()`, `whisper()`, `subscribed()`, `error()`, `leave()`; presence adds `here()`, `joining()`, `leaving()`, `members()`. |
| `RealtimeMessage`, `PresenceMember`, `SubscriptionError`, `RealtimeStatus` | Typed payloads. `RealtimeMessage::$data` is the decoded JSON payload. |
| `ConnectionState`, `ChannelType` | Sequential int-backed enums. |
| `Secret` | Redacted bearer value (kept in memory only, never in snapshots). |
| `RealtimeSocket` | Low-level raw RFC 6455 socket (text/binary frames, long-poll reads) for non-Pusher protocols. |

Event names: leading dots are ignored, so `listen('message.sent')` matches a
`broadcastAs()` name sent as `message.sent` or `.message.sent`. With
`->namespace('App\Events')`, names without a leading dot are prefixed like
Laravel Echo.

## Platform support

Android API 26+ (OkHttp `5.3.0`) and iOS 15+ (`URLSessionWebSocketTask`,
`NWPathMonitor` for network-change recovery): the Pusher client, batching,
backoff, liveness, channel authorization and whisper throttling behave the
same on both platforms. The iOS implementation has not been validated on a
device yet; see `ios/Tests/PusherTests.swift`.
PAM Native `>=1.0.35 <2.0.0`. End-to-end encrypted channels
(`private-encrypted-*`) are rejected.

## Testing

```bash
pam tests/run.php                                                    # PHP contracts (fake native transport)
cd android && ANDROID_SERIAL=emulator-5558 \
  ../../../pam-native/android/gradlew -p . connectedDebugAndroidTest   # Pusher protocol suite against MockWebServer
```

## Production checklist

- Accept only authenticated `wss://` endpoints and an `https://` auth endpoint.
- Refresh credentials with `Realtime::token()` instead of reconnecting.
- Resynchronize from your API in `onConnected` (reconnects) and `onDropped`.
- Run `pam doctor`, `pam test`, and a signed release build on every supported platform.

## Compatibility and support

- [PAM documentation](https://push-in.github.io/pam-docs/introduction/)
- [PAM Native overview](https://push-in.github.io/pam-docs/native/overview/)
- [Plugin and native capability model](https://push-in.github.io/pam-docs/native/plugins/)
- [Report an issue](https://github.com/push-in/pam-native-realtime/issues)

Security vulnerabilities should be reported through the repository security policy or GitHub private vulnerability reporting, not a public issue.
