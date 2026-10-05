<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

enum ChannelType: int
{
    case Public = 1;
    case Private = 2;
    case Presence = 3;

    public static function of(string $channel): self
    {
        return match (true) {
            str_starts_with($channel, 'presence-') => self::Presence,
            str_starts_with($channel, 'private-') => self::Private,
            default => self::Public,
        };
    }

    /** Client events (whispers) are only allowed on authenticated channels. */
    public function allowsWhispers(): bool
    {
        return $this !== self::Public;
    }
}
