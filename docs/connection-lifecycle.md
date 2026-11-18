# Batching and connection lifecycle

Moved out of the README; nothing was removed.

## One render per burst

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

## Connection lifecycle (native)

- `ConnectionState`: `Connecting`, `Connected`, `Unavailable` (reconnect
  scheduled), `Disconnected` (explicit) and `Failed` (Pusher 4000-4099, retried
  after `->terminalRetry()` — 5 minutes by default, the Reverb `4009` case).
- Reconnect with jittered exponential backoff (`->backoff(400, 15000, 1.8)`),
  immediately for 4200-4299, after backoff for 4100-4199.
- `pusher:ping` after `activity_timeout` of silence (server value or
  `->heartbeat()`), reconnect when nothing answers in 12 s; a handshake that
  does not finish in 10 s is retried. Silence is measured on a clock that
  keeps counting while the device sleeps (Android `elapsedRealtime`, iOS
  `CLOCK_MONOTONIC`), so the first liveness check after Doze sees it.
- Back in the foreground, a `Connected` socket that has been silent for 30 s
  or more is pinged at once and dropped (reconnect) when nothing answers in
  5 s: a socket the server closed while the device slept no longer looks alive
  until the next liveness check. Android uses the host foreground event, iOS
  `didBecomeActive`; nothing runs in the background.
- Network callbacks: when connectivity returns, a pending backoff is skipped;
  when the default network changes, the socket is probed.
- Every channel is re-authorized and re-subscribed after each reconnect.
  Auth failures retry with backoff (350 ms – 8 s), `401`/`403` after 60 s or
  right after `Realtime::token()`. The auth request times out after 8 s (10 s
  for the whole call on Android) and retries like any other failure, so a
  slow API never leaves a channel unsubscribed while the socket is up.
- Outbound whispers are throttled natively to one frame per 100 ms per event
  (the newest payload wins), within the Pusher client-event limit.
- `Realtime::status(fn (RealtimeStatus $s) => ...)` returns the state, socket id,
  channels and a 40-entry diagnostic journal for support screens.

Android keeps the native clients across a recreated Activity (font size,
display size, language, a task the system trimmed): the retired module hands
them to the next one. If the native client is ever lost anyway (its pending
read fails), PHP opens it again with every channel; `reconnect()` does the
same after a failed reopen.

Hot reload safe: connecting again with the same name (`connect('default')`)
replaces the native client.
