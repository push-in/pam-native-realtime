<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

/** @internal Kind of one entry in a native realtime batch. */
enum FrameKind: int
{
    case State = 1;
    case Message = 2;
    case Whisper = 3;
    case Subscribed = 4;
    case SubscriptionFailed = 5;
    case MemberAdded = 6;
    case MemberRemoved = 7;
}
