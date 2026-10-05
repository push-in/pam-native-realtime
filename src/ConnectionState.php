<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

/** Lifecycle of a Pusher/Reverb connection, mirroring pusher-js states. */
enum ConnectionState: int
{
    /** Opening the socket or waiting for `pusher:connection_established`. */
    case Connecting = 1;
    /** Handshake complete; channels are (re)subscribed automatically. */
    case Connected = 2;
    /** The link dropped; a reconnect is scheduled with exponential backoff. */
    case Unavailable = 3;
    /** Closed on purpose with `disconnect()`; no reconnect will happen. */
    case Disconnected = 4;
    /** The server refused the app (Pusher 4000-4099); retried after the terminal window. */
    case Failed = 5;

    public function connected(): bool
    {
        return $this === self::Connected;
    }
}
