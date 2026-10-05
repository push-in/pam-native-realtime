<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;
use InvalidArgumentException;
use LogicException;

/**
 * Laravel Echo style realtime for PAM Native over the Pusher protocol
 * (Pusher Channels, Laravel Reverb, Soketi).
 *
 * ```php
 * Realtime::reverb('ws.example.com', key: 'app-key')
 *     ->auth('https://api.example.com/broadcasting/auth', Secret::value($token))
 *     ->onConnected(fn (RealtimeStatus $s) => $this->catchUp())
 *     ->connect();
 *
 * Realtime::private('chat.42')
 *     ->listen('message.sent', fn (RealtimeMessage $m) => $this->append($m->json()));
 *
 * Realtime::join('chat-typing.42')
 *     ->here(fn (array $members) => ...)
 *     ->listenForWhisper('typing', fn (RealtimeMessage $m) => ...)
 *     ->whisper('typing', ['typing' => true]);
 * ```
 *
 * The socket, the protocol, channel auth, heartbeats, reconnects with
 * exponential backoff and re-subscription all run natively. PHP receives
 * coalesced batches through a pending native read, never by polling.
 */
final class Realtime
{
    public const string DEFAULT = 'default';

    /** @var array<string, RealtimeConnection> */
    private static array $connections = [];

    private function __construct()
    {
    }

    /**
     * Configures a Pusher-protocol connection. With `$key`, `$url` is the
     * server origin (`wss://host[:port]`) and the `/app/{key}` path with the
     * protocol query is appended; without it `$url` is used verbatim.
     */
    public static function pusher(string $url, ?string $key = null): PusherConnector
    {
        if ($key !== null) {
            if (preg_match('/^[A-Za-z0-9_.-]{1,128}$/D', $key) !== 1) {
                throw new InvalidArgumentException('Invalid Pusher app key.');
            }
            $url = rtrim($url, '/').'/app/'.$key.'?protocol=7&client=pam-native&version=0.3.0&flash=false';
        }

        return new PusherConnector($url);
    }

    /** Laravel Reverb: `Realtime::reverb('ws.example.com', 'app-key')`. */
    public static function reverb(string $host, string $key, int $port = 443): PusherConnector
    {
        if (preg_match('/^[A-Za-z0-9.-]{1,253}$/D', $host) !== 1 || $port < 1 || $port > 65535) {
            throw new InvalidArgumentException('Invalid Reverb host or port.');
        }

        return self::pusher('wss://'.$host.($port === 443 ? '' : ':'.$port), $key);
    }

    public static function connection(string $name = self::DEFAULT): RealtimeConnection
    {
        return self::$connections[$name] ?? throw new LogicException("Realtime connection {$name} is not connected; call Realtime::pusher(...)->connect() first.");
    }

    public static function has(string $name = self::DEFAULT): bool
    {
        return isset(self::$connections[$name]);
    }

    /** Subscribes (once) and returns a channel by its full name, e.g. `private-chat.42`. */
    public static function channel(string $name): Channel
    {
        return self::connection()->channel($name);
    }

    public static function private(string $name): Channel
    {
        return self::connection()->private($name);
    }

    /** Presence channel `presence-{$name}`. */
    public static function join(string $name): PresenceChannel
    {
        return self::connection()->join($name);
    }

    /** Presence channel by full name (`presence-...`). */
    public static function presence(string $name): PresenceChannel
    {
        return self::connection()->presence($name);
    }

    /** Leaves `$name`, `private-$name` and `presence-$name` (Echo semantics). */
    public static function leave(string $name): void
    {
        if (self::has()) {
            self::connection()->leave($name);
        }
    }

    public static function token(string|Secret|null $bearer): void
    {
        self::connection()->token($bearer);
    }

    public static function reconnect(): void
    {
        self::connection()->reconnect();
    }

    public static function socketId(): ?string
    {
        return self::has() ? self::connection()->socketId() : null;
    }

    public static function state(): ConnectionState
    {
        return self::has() ? self::connection()->state() : ConnectionState::Disconnected;
    }

    /** @param Closure(RealtimeStatus): void $then */
    public static function status(Closure $then): int
    {
        return self::connection()->status($then);
    }

    public static function disconnect(string $name = self::DEFAULT): void
    {
        (self::$connections[$name] ?? null)?->disconnect();
    }

    /** @internal */
    public static function register(RealtimeConnection $connection): void
    {
        (self::$connections[$connection->name] ?? null)?->forget();
        self::$connections[$connection->name] = $connection;
    }

    /** @internal */
    public static function unregister(RealtimeConnection $connection): void
    {
        if ((self::$connections[$connection->name] ?? null) === $connection) {
            unset(self::$connections[$connection->name]);
        }
    }
}
