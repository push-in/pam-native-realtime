<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;
use InvalidArgumentException;

/**
 * Fluent configuration of a Pusher-protocol connection (Pusher Channels,
 * Laravel Reverb, Soketi). Created by `Realtime::pusher()` / `Realtime::reverb()`.
 */
final class PusherConnector
{
    private const string HEADER_NAME = '/^[!#$%&\'*+.^_`|~0-9A-Za-z-]{1,128}$/D';

    private ?string $authEndpoint = null;

    private ?Secret $bearer = null;

    /** @var array<string, string> */
    private array $authHeaders = [];

    /** @var array<string, string> */
    private array $headers = [];

    private int $backoffInitialMs = 400;

    private int $backoffMaxMs = 15_000;

    private float $backoffMultiplier = 1.8;

    private int $activityTimeoutMs = 30_000;

    private int $pongTimeoutMs = 12_000;

    private int $handshakeTimeoutMs = 10_000;

    private int $terminalRetryMs = 300_000;

    private int $quietMs = 32;

    private int $maxDelayMs = 150;

    private int $capacity = 2_048;

    private int $maxBatch = 512;

    private ?string $namespace = null;

    /** @var list<Closure(RealtimeStatus): void> */
    private array $onState = [];

    /** @var list<Closure(RealtimeStatus): void> */
    private array $onConnected = [];

    /** @var list<Closure(int): void> */
    private array $onDropped = [];

    /** @internal Use `Realtime::pusher()`. */
    public function __construct(private readonly string $url)
    {
        $parts = parse_url($url);
        if (!str_starts_with($url, 'wss://') || !is_array($parts) || ($parts['host'] ?? '') === '' || preg_match('/[\s\0]/', $url) === 1) {
            throw new InvalidArgumentException('Realtime connections require a valid wss:// URL.');
        }
    }

    /**
     * Authorizes private and presence channels with a POST of `socket_id` and
     * `channel_name` (Laravel `/broadcasting/auth`).
     *
     * @param array<string, string> $headers
     */
    public function auth(string $endpoint, string|Secret|null $bearer = null, array $headers = []): self
    {
        if (!str_starts_with($endpoint, 'https://') || filter_var($endpoint, FILTER_VALIDATE_URL) === false) {
            throw new InvalidArgumentException('The channel auth endpoint must be an https:// URL.');
        }
        $this->authEndpoint = $endpoint;
        $this->bearer($bearer);
        foreach ($headers as $name => $value) {
            $this->authHeaders[self::headerName($name)] = self::headerValue($value);
        }

        return $this;
    }

    public function bearer(string|Secret|null $token): self
    {
        $this->bearer = is_string($token) ? Secret::value($token) : $token;

        return $this;
    }

    /** Adds a WebSocket handshake header, e.g. `Origin`. */
    public function header(string $name, string $value): self
    {
        $this->headers[self::headerName($name)] = self::headerValue($value);

        return $this;
    }

    /** Reconnect delay: random in [initial, min(max, initial * multiplier^(attempt-1))]. */
    public function backoff(int $initialMillis = 400, int $maxMillis = 15_000, float $multiplier = 1.8): self
    {
        if ($initialMillis < 50 || $maxMillis < $initialMillis || $maxMillis > 600_000 || $multiplier < 1.0 || $multiplier > 10.0) {
            throw new InvalidArgumentException('Backoff requires 50 <= initial <= max <= 600000 ms and a multiplier in [1, 10].');
        }
        $this->backoffInitialMs = $initialMillis;
        $this->backoffMaxMs = $maxMillis;
        $this->backoffMultiplier = $multiplier;

        return $this;
    }

    /**
     * Sends `pusher:ping` after `$activitySeconds` without frames (the server
     * `activity_timeout` wins when lower) and reconnects when no frame answers
     * within `$pongSeconds`.
     */
    public function heartbeat(int $activitySeconds = 30, int $pongSeconds = 12): self
    {
        if ($activitySeconds < 1 || $activitySeconds > 600 || $pongSeconds < 1 || $pongSeconds > 120) {
            throw new InvalidArgumentException('Heartbeat activity must be 1-600 s and pong 1-120 s.');
        }
        $this->activityTimeoutMs = $activitySeconds * 1000;
        $this->pongTimeoutMs = $pongSeconds * 1000;

        return $this;
    }

    public function handshakeTimeout(int $seconds = 10): self
    {
        if ($seconds < 1 || $seconds > 120) {
            throw new InvalidArgumentException('Handshake timeout must be 1-120 s.');
        }
        $this->handshakeTimeoutMs = $seconds * 1000;

        return $this;
    }

    /** Wait before retrying after a terminal Pusher error (4000-4099, e.g. Reverb 4009). */
    public function terminalRetry(int $seconds = 300): self
    {
        if ($seconds < 1 || $seconds > 86_400) {
            throw new InvalidArgumentException('Terminal retry must be 1-86400 s.');
        }
        $this->terminalRetryMs = $seconds * 1000;

        return $this;
    }

    /**
     * Native batching window: events are released to PHP once the stream was
     * quiet for `$quietMillis`, or at the latest `$maxMillis` after the first
     * buffered event. Each release is one PHP callback and one render.
     */
    public function coalesce(int $quietMillis = 32, int $maxMillis = 150): self
    {
        if ($quietMillis < 0 || $quietMillis > 1000 || $maxMillis < $quietMillis || $maxMillis > 5000) {
            throw new InvalidArgumentException('Coalescing requires 0 <= quiet <= 1000 ms and quiet <= max <= 5000 ms.');
        }
        $this->quietMs = $quietMillis;
        $this->maxDelayMs = $maxMillis;

        return $this;
    }

    /** Native queue bound while PHP is busy (oldest coalescable entries are dropped first) and batch size. */
    public function buffer(int $capacity = 2_048, int $maxBatch = 512): self
    {
        if ($capacity < 16 || $capacity > 65_536 || $maxBatch < 1 || $maxBatch > 8_192) {
            throw new InvalidArgumentException('Buffer capacity must be 16-65536 and batches 1-8192 events.');
        }
        $this->capacity = $capacity;
        $this->maxBatch = $maxBatch;

        return $this;
    }

    /** Laravel Echo style event namespace, e.g. `App\Events`; names starting with `.` bypass it. */
    public function namespace(?string $namespace): self
    {
        $this->namespace = $namespace === null || $namespace === '' ? null : rtrim($namespace, '\\');

        return $this;
    }

    /** @param Closure(RealtimeStatus): void $callback */
    public function onState(Closure $callback): self
    {
        $this->onState[] = $callback;

        return $this;
    }

    /** @param Closure(RealtimeStatus): void $callback Runs on every new session (first connect and each reconnect). */
    public function onConnected(Closure $callback): self
    {
        $this->onConnected[] = $callback;

        return $this;
    }

    /** @param Closure(int): void $callback Runs when the native queue overflowed; resynchronize from your API. */
    public function onDropped(Closure $callback): self
    {
        $this->onDropped[] = $callback;

        return $this;
    }

    /** Opens the connection (replacing a previous one with the same name) and makes it the facade target. */
    public function connect(string $name = 'default'): RealtimeConnection
    {
        if (preg_match('/^[A-Za-z0-9_.:-]{1,64}$/D', $name) !== 1) {
            throw new InvalidArgumentException('Connection names use 1-64 letters, digits, ".", "_", ":" or "-".');
        }
        $connection = new RealtimeConnection($name, $this->namespace, $this->onState, $this->onConnected, $this->onDropped);
        Realtime::register($connection);
        $connection->open($this->configuration());

        return $connection;
    }

    /** @internal @return array<string, mixed> */
    public function configuration(): array
    {
        return [
            'url' => $this->url,
            'headers' => (object) $this->headers,
            'authEndpoint' => $this->authEndpoint ?? '',
            'authHeaders' => (object) $this->authHeaders,
            'bearer' => $this->bearer?->reveal() ?? '',
            'backoffInitialMs' => $this->backoffInitialMs,
            'backoffMaxMs' => $this->backoffMaxMs,
            'backoffMultiplier' => $this->backoffMultiplier,
            'activityTimeoutMs' => $this->activityTimeoutMs,
            'pongTimeoutMs' => $this->pongTimeoutMs,
            'handshakeTimeoutMs' => $this->handshakeTimeoutMs,
            'terminalRetryMs' => $this->terminalRetryMs,
            'quietMs' => $this->quietMs,
            'maxDelayMs' => $this->maxDelayMs,
            'capacity' => $this->capacity,
            'maxBatch' => $this->maxBatch,
        ];
    }

    /** @return array<string, mixed> */
    public function __debugInfo(): array
    {
        return ['url' => $this->url, 'auth' => $this->authEndpoint, 'bearer' => $this->bearer === null ? null : '[redacted]'];
    }

    private static function headerName(mixed $name): string
    {
        if (!is_string($name) || preg_match(self::HEADER_NAME, $name) !== 1) {
            throw new InvalidArgumentException('Invalid header name.');
        }

        return $name;
    }

    private static function headerValue(mixed $value): string
    {
        if (!is_string($value) || strlen($value) > 8192 || preg_match('/[\r\n\0]/', $value) === 1) {
            throw new InvalidArgumentException('Invalid header value.');
        }

        return $value;
    }
}
