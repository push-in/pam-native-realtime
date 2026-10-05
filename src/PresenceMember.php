<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

/** A member of a presence channel: the `user_id` and `user_info` returned by your auth endpoint. */
final readonly class PresenceMember
{
    /** @param array<array-key, mixed> $info */
    public function __construct(
        public string $id,
        public array $info = [],
    ) {
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return $this->info[$key] ?? $default;
    }
}
