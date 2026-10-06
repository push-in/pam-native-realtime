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

`pam add realtime` (or `pam composer require pushinbr/pam-native-realtime` followed by `pam doctor --fix`) resolves the official compatible package, performs a non-mutating Composer preflight, updates the normal `composer.json` and `composer.lock`, refreshes generated native integration when required, and leaves the project ready for `pam doctor` validation. The package is a PAM Native plugin (module `realtime`); nothing is added to `pam-native.json`.

Use `pam packages` to inspect availability and `pam remove realtime` to uninstall the capability safely. Direct Composer commands are an advanced interoperability path; PAM is the supported application workflow.

- **Android:** merged permissions `INTERNET` and `ACCESS_NETWORK_STATE`
  (network-change recovery); dependency `com.squareup.okhttp3:okhttp:5.3.0`.
  No runtime permission.
- **iOS:** framework `Network` (`NWPathMonitor`); no Info.plist keys. The
  socket does not run while iOS suspends the app; once the app runs again the
  liveness checks reconnect and re-subscribe (call `Realtime::reconnect()` on
  `AppState::Active` to skip a pending backoff, as Zé Chat does).

## A real example: Zé Chat

Zé Chat talks to Laravel Reverb. One connection is opened lazily by the first
screen that subscribes; screens register handlers per channel, the bearer is
refreshed in place, and a socket waiting for its backoff reconnects as soon as
the app is active again:

```php
use Pam\Native\{App, AppState};
use Pam\Native\Realtime\{Channel, ConnectionState, PresenceChannel, PresenceMember, Realtime, RealtimeMessage, RealtimeStatus, Secret};

private static function connect(string $token): void
{
    if (Realtime::has()) {
        if (self::$token !== $token) {
            self::$token = $token;
            Realtime::token(Secret::value($token));          // rejected (401/403) channels retry at once
        }
        return;
    }
    self::$token = $token;
    Realtime::pusher('wss://api.example.com/app/app-key?protocol=7&client=zechat&version=1.0.0')
        ->auth('https://api.example.com/api/broadcasting/auth', Secret::value($token))
        ->header('Origin', 'https://api.example.com')
        ->terminalRetry(300)                                  // Reverb 4009: retry after 5 minutes, never give up
        ->onState(fn (RealtimeStatus $s) => self::broadcastState($s->state))
        ->connect();

    App::onStateChange(function (AppState $state): void {
        if ($state === AppState::Active && Realtime::state() === ConnectionState::Unavailable) {
            Realtime::reconnect();                            // skip the pending backoff
        }
    });
}

private static function listen(string $name): Channel
{
    $channel = Realtime::channel($name);                      // private-…/presence-… prefixes pick the type
    $channel->listenToAll(fn (RealtimeMessage $m) => self::dispatch($name, $m->event, $m->data, $m->userId));
    if ($channel instanceof PresenceChannel) {
        $channel
            ->here(fn (array $members) => self::online($name, array_map(fn (PresenceMember $m) => $m->id, $members)))
            ->joining(fn (PresenceMember $m) => self::joined($name, $m->id, $m->info))
            ->leaving(fn (PresenceMember $m) => self::left($name, $m->id));
    }
    return $channel;
}

// Typing indicator: client events are throttled natively (100 ms per event).
self::$channels["presence-chat.{$chatId}"]->whisper('client-typing', ['typing' => true]);

// Support screen.
Realtime::status(fn (RealtimeStatus $s) => $this->diagnostics = $s->journal);
```

A runnable minimal app is in [`example/`](example).

## API reference

All classes live in `Pam\Native\Realtime`.

### `Realtime` (static facade over the `default` connection)

| Method | Description |
| --- | --- |
| `pusher(string $url, ?string $key = null): PusherConnector` | Pusher protocol 7 client; `$url` must be `wss://`. With `$key`, `/app/<key>?protocol=7&client=pam-native…` is appended; without it, pass the full Pusher URL. |
| `reverb(string $host, string $key, int $port = 443): PusherConnector` | `wss://host[:port]` for Laravel Reverb. |
| `connection(string $name = 'default'): RealtimeConnection`, `has(string $name = 'default'): bool` | Named connections. `connection()` throws `LogicException` before `connect()`. |
| `channel(string $name): Channel` | Public, or private/presence when the name has the `private-`/`presence-` prefix. |
| `private(string $name): Channel`, `join(string $name)`/`presence(string $name): PresenceChannel` | Echo-style helpers that add the prefix. |
| `leave(string $name): void` | Leaves the public, `private-` and `presence-` variants. |
| `token(string\|Secret\|null $bearer): void` | New bearer for channel auth. |
| `reconnect(): void`, `disconnect(string $name = 'default'): void` | Lifecycle. |
| `socketId(): ?string`, `state(): ConnectionState` | Last pushed values (no native round trip). |
| `status(Closure(RealtimeStatus) $then): int` | Native snapshot with channels and the 40-entry journal. |

### `PusherConnector`

`auth(string $endpoint, string|Secret|null $bearer = null, array $headers = [])`
(HTTPS endpoint), `bearer()`, `header(string $name, string $value)`,
`backoff(int $initialMillis = 400, int $maxMillis = 15_000, float $multiplier = 1.8)`,
`heartbeat(int $activitySeconds = 30, int $pongSeconds = 12)`,
`handshakeTimeout(int $seconds = 10)`, `terminalRetry(int $seconds = 300)`,
`coalesce(int $quietMillis = 32, int $maxMillis = 150)`,
`buffer(int $capacity = 2_048, int $maxBatch = 512)`,
`namespace(?string $namespace)`, `onState(Closure(RealtimeStatus))`,
`onConnected(Closure(RealtimeStatus))` (every new session),
`onDropped(Closure(int))`, `connect(string $name = 'default'): RealtimeConnection`,
`configuration()`. `__debugInfo()` redacts the bearer.

### `RealtimeConnection`

Same operations as the facade for one named connection: `channel()`,
`private()`, `join()`, `presence()`, `leave()`, `channels()`, `token()`,
`reconnect()`, `status()`, `current(): RealtimeStatus`, `state()`,
`socketId()`, `onState()`, `onConnected()`, `onDropped()`, `disconnect()`,
`closed()`, readonly `name`.

### `Channel` / `PresenceChannel`

`Channel` (readonly `name`, `type`): `listen(string $event, Closure(RealtimeMessage))`,
`listenToAll(Closure(RealtimeMessage))` (whispers included),
`listenForWhisper(string $event, Closure(RealtimeMessage))`,
`stopListening(string $event, ?Closure $callback = null)`,
`stopListeningForWhisper()`, `whisper(string $event, array $data = [])`
(private/presence only, ≤ 10 KiB, sent as `client-<event>` unless already
prefixed), `subscribed(Closure())` (every (re)subscription; immediately when
already subscribed), `error(Closure(SubscriptionError))`, `isSubscribed()`,
`leave()`. `PresenceChannel` adds `here(Closure(list<PresenceMember>))`,
`joining(Closure(PresenceMember))`, `leaving(Closure(PresenceMember))`,
`members()`, `member(string $id): ?PresenceMember`, `count()`.

### Values and enums

| Type | Members |
| --- | --- |
| `RealtimeMessage` | `channel`, `event`, `raw`, `data` (decoded JSON), `userId`, `whisper`; `get(string $key, $default = null)`, `json(): array` |
| `PresenceMember` | `id`, `info`; `get()` |
| `SubscriptionError` | `channel`, `status` (HTTP), `message`, `retryInMillis`; `forbidden()` (401/403) |
| `RealtimeStatus` | `state`, `socketId`, `reason`, `attempt`, `retryInMillis`, `lastError`, `connectedAt`, `pending`, `channels`, `journal`; `connected()` |
| `Secret` | `value(string)`, `reveal()`; redacted in dumps |
| `ConnectionState` | `Connecting = 1`, `Connected`, `Unavailable`, `Disconnected`, `Failed = 5`; `connected()` |
| `ChannelType` | `Public = 1`, `Private`, `Presence`; `of(string)`, `allowsWhispers()` |

### `RealtimeSocket` (raw RFC 6455)

`connect(string $url, array $headers, array $protocols, Closure(?string $id, ?string $error) $complete, int $maxMessageBytes = 1_048_576)`,
`sendText()`, `sendBinary()` (`Closure(bool, ?string)`),
`poll(string $id, Closure(?RealtimeEvent, ?string), int $timeoutMillis = 25_000)`,
`state(string $id, Closure(RealtimeConnectionState))`,
`close(string $id, Closure(bool), int $code = 1000, string $reason = '')`.
`RealtimeEvent` carries `RealtimeEventKind` (`Connected = 1`, `Text`, `Binary`,
`Closed`, `Failure`, `Pong`, `Timeout = 7`), `payload` and `code`;
`RealtimeConnectionState` is `Connecting = 1`, `Open`, `Closing`, `Closed`,
`Failed = 5`.

### Errors

`InvalidArgumentException`: non-`wss://` URLs, a non-HTTPS auth endpoint,
invalid keys, hosts, ports, header names/values, connection or channel names,
`private-encrypted-*` channels, out-of-range tuning values, event names over
200 bytes, whispers over 10 KiB. `LogicException`: whispers on public channels,
operations on a closed connection, using the facade before `connect()`.
Network and auth failures never throw: they arrive as `ConnectionState`
changes, `SubscriptionError`s and the status journal.

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

| `pushinbr/pam-native-realtime` | `pushinbr/pam-native` | Android | iOS |
| --- | --- | --- | --- |
| 0.4.x | `>=1.0.35 <2.0.0` (tested with 1.14.x) | API 26+ | 15+ (Pusher client) |
| 0.3.x | `>=1.0.35 <2.0.0` | API 26+ | Raw socket only |

- [PAM documentation](https://push-in.github.io/pam-docs/introduction/)
- [PAM Native overview](https://push-in.github.io/pam-docs/native/overview/)
- [Plugin and native capability model](https://push-in.github.io/pam-docs/native/plugins/)
- [Report an issue](https://github.com/push-in/pam-native-realtime/issues)

Security vulnerabilities should be reported through the repository security policy or GitHub private vulnerability reporting, not a public issue.
