<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

/** Connection status pushed on every state change and returned by `status()`. */
final readonly class RealtimeStatus
{
    /**
     * @param array<string, bool> $channels channel name => subscribed (snapshots only)
     * @param list<array{at: int, message: string}> $journal newest first (snapshots only)
     */
    public function __construct(
        public ConnectionState $state,
        public ?string $socketId = null,
        public string $reason = '',
        public int $attempt = 0,
        public int $retryInMillis = 0,
        public string $lastError = '',
        public ?int $connectedAt = null,
        public int $pending = 0,
        public array $channels = [],
        public array $journal = [],
    ) {
    }

    /** @internal @param array<array-key, mixed> $wire */
    public static function fromWire(array $wire): self
    {
        $channels = [];
        foreach (is_array($wire['ch'] ?? null) ? $wire['ch'] : [] as $channel) {
            if (is_array($channel) && is_string($channel['n'] ?? null)) {
                $channels[$channel['n']] = (bool) ($channel['s'] ?? false);
            }
        }
        $journal = [];
        foreach (is_array($wire['j'] ?? null) ? $wire['j'] : [] as $entry) {
            if (is_array($entry)) {
                $journal[] = ['at' => (int) ($entry['at'] ?? 0), 'message' => (string) ($entry['m'] ?? '')];
            }
        }
        $socketId = (string) ($wire['i'] ?? '');
        $connectedAt = (int) ($wire['t'] ?? 0);

        return new self(
            state: ConnectionState::tryFrom((int) ($wire['s'] ?? 0)) ?? ConnectionState::Connecting,
            socketId: $socketId === '' ? null : $socketId,
            reason: (string) ($wire['r'] ?? ''),
            attempt: (int) ($wire['a'] ?? 0),
            retryInMillis: (int) ($wire['w'] ?? 0),
            lastError: (string) ($wire['e'] ?? ''),
            connectedAt: $connectedAt > 0 ? $connectedAt : null,
            pending: (int) ($wire['q'] ?? 0),
            channels: $channels,
            journal: $journal,
        );
    }

    public function connected(): bool
    {
        return $this->state->connected();
    }
}
