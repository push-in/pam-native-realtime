# API reference

Moved out of the README; nothing was removed.

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
