# A real example: Zé Chat

Moved out of the README; nothing was removed.

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

A runnable minimal app is in [`example/`](../example).
