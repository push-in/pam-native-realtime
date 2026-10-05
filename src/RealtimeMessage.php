<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use JsonException;

/**
 * One event received on a channel. `data` is the decoded JSON payload (Pusher
 * servers send it JSON-encoded) or the raw string when it is not JSON.
 */
final readonly class RealtimeMessage
{
    private const int MAX_JSON_DEPTH = 64;

    /** @var array<array-key, mixed>|string|int|float|bool|null */
    public array|string|int|float|bool|null $data;

    public function __construct(
        public string $channel,
        public string $event,
        public string $raw,
        public ?string $userId = null,
        public bool $whisper = false,
    ) {
        $this->data = self::decode($raw);
    }

    /** Reads a top-level key of an object payload. */
    public function get(string $key, mixed $default = null): mixed
    {
        return is_array($this->data) ? ($this->data[$key] ?? $default) : $default;
    }

    /** @return array<array-key, mixed> The payload when it is a JSON object/array, otherwise `[]`. */
    public function json(): array
    {
        return is_array($this->data) ? $this->data : [];
    }

    /** @return array<array-key, mixed>|string|int|float|bool|null */
    private static function decode(string $raw): array|string|int|float|bool|null
    {
        $trimmed = ltrim($raw);
        if ($trimmed === '' || !in_array($trimmed[0], ['{', '['], true)) {
            return $raw;
        }
        try {
            $decoded = json_decode($raw, true, self::MAX_JSON_DEPTH, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return $raw;
        }

        return is_array($decoded) ? $decoded : $raw;
    }
}
