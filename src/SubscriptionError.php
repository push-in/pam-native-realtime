<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

/**
 * A channel could not be subscribed. The native client keeps retrying:
 * `401`/`403` after 60 s (or immediately after `Realtime::token()`), other
 * failures with exponential backoff. `retryInMillis` is `-1` when no retry is
 * possible (for example a private channel without an auth endpoint).
 */
final readonly class SubscriptionError
{
    public function __construct(
        public string $channel,
        public int $status,
        public string $message,
        public int $retryInMillis,
    ) {
    }

    public function forbidden(): bool
    {
        return $this->status === 401 || $this->status === 403;
    }
}
