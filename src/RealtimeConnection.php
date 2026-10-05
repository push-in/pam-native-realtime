<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;
use InvalidArgumentException;
use JsonException;
use LogicException;
use Pam\Native\Modules\NativeModuleResult;
use Pam\Native\Modules\NativeModules;

/**
 * A live Pusher/Reverb connection owned by the native runtime.
 *
 * Events reach PHP through one pending native `next` read that the platform
 * completes with a coalesced batch: no timers, no polling, and one PHP
 * callback (one render) per burst regardless of how many frames it held.
 */
final class RealtimeConnection
{
    private const string MODULE = 'realtime';

    private const string CHANNEL_NAME = '/^[A-Za-z0-9_\-=@,.;]{1,164}$/D';

    /** @var array<string, Channel> */
    private array $channels = [];

    private RealtimeStatus $status;

    private bool $closed = false;

    /**
     * @internal Use `Realtime::pusher(...)->connect()`.
     * @param list<Closure(RealtimeStatus): void> $onState
     * @param list<Closure(RealtimeStatus): void> $onConnected
     * @param list<Closure(int): void> $onDropped
     */
    public function __construct(
        public readonly string $name,
        private readonly ?string $namespace = null,
        private array $onState = [],
        private array $onConnected = [],
        private array $onDropped = [],
    ) {
        $this->status = new RealtimeStatus(ConnectionState::Connecting);
    }

    /** @internal @param array<string, mixed> $configuration */
    public function open(array $configuration): void
    {
        try {
            $json = json_encode($configuration, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        } catch (JsonException $error) {
            throw new InvalidArgumentException('Realtime configuration cannot be encoded.', previous: $error);
        }
        NativeModules::call(self::MODULE, 'pusherConnect', ['client' => $this->name, 'config' => $json], function (NativeModuleResult $result): void {
            if (!$result->succeeded() && !$this->closed) {
                $this->applyState(new RealtimeStatus(ConnectionState::Failed, reason: $result->message(), lastError: $result->message()));
            }
        });
        $this->next();
    }

    /** Returns the channel, subscribing natively on first use. Presence channels return a `PresenceChannel`. */
    public function channel(string $name): Channel
    {
        if (isset($this->channels[$name])) {
            return $this->channels[$name];
        }
        if (preg_match(self::CHANNEL_NAME, $name) !== 1) {
            throw new InvalidArgumentException('Invalid channel name.');
        }
        if (str_starts_with($name, 'private-encrypted-')) {
            throw new InvalidArgumentException('End-to-end encrypted channels are not supported.');
        }
        $this->assertOpen();
        $channel = ChannelType::of($name) === ChannelType::Presence
            ? new PresenceChannel($this, $name, $this->namespace)
            : new Channel($this, $name, $this->namespace);
        $this->channels[$name] = $channel;
        $this->command('pusherSubscribe', ['channel' => $name]);

        return $channel;
    }

    /** `private('chat.42')` is `channel('private-chat.42')`. */
    public function private(string $name): Channel
    {
        return $this->channel('private-'.$name);
    }

    /** `join('chat.42')` is the presence channel `presence-chat.42`. */
    public function join(string $name): PresenceChannel
    {
        return $this->presence('presence-'.$name);
    }

    public function presence(string $name): PresenceChannel
    {
        if (!str_starts_with($name, 'presence-')) {
            throw new InvalidArgumentException('Presence channel names start with "presence-".');
        }
        $channel = $this->channel($name);
        assert($channel instanceof PresenceChannel);

        return $channel;
    }

    public function leave(string $name): void
    {
        foreach ([$name, 'private-'.$name, 'presence-'.$name] as $candidate) {
            $channel = $this->channels[$candidate] ?? null;
            if ($channel === null) {
                continue;
            }
            unset($this->channels[$candidate]);
            $channel->detach();
            if (!$this->closed) {
                $this->command('pusherUnsubscribe', ['channel' => $candidate]);
            }
        }
    }

    /** @return array<string, Channel> */
    public function channels(): array
    {
        return $this->channels;
    }

    /** Replaces the bearer used for channel auth; channels rejected with 401/403 retry at once. */
    public function token(string|Secret|null $bearer): void
    {
        $this->assertOpen();
        $value = is_string($bearer) ? Secret::value($bearer)->reveal() : ($bearer?->reveal() ?? '');
        $this->command('pusherToken', ['bearer' => $value]);
    }

    /** Drops the socket and reconnects now, resetting the backoff (e.g. after the app returns to foreground). */
    public function reconnect(): void
    {
        $this->assertOpen();
        $this->command('pusherReconnect');
    }

    /** @param Closure(RealtimeStatus): void $then A native snapshot including channels and the diagnostic journal. */
    public function status(Closure $then): int
    {
        return NativeModules::call(self::MODULE, 'pusherStatus', ['client' => $this->name], function (NativeModuleResult $result) use ($then): void {
            if (!$result->succeeded()) {
                $then($this->status);

                return;
            }
            try {
                $wire = json_decode((string) ($result->values()['status'] ?? '{}'), true, 16, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                $wire = [];
            }
            $then(RealtimeStatus::fromWire(is_array($wire) ? $wire : []));
        });
    }

    /** The last state pushed by the native client. */
    public function current(): RealtimeStatus
    {
        return $this->status;
    }

    public function state(): ConnectionState
    {
        return $this->status->state;
    }

    /** The Pusher socket id, for Laravel's `X-Socket-ID` header (`toOthers()`). */
    public function socketId(): ?string
    {
        return $this->status->state === ConnectionState::Connected ? $this->status->socketId : null;
    }

    /** @param Closure(RealtimeStatus): void $callback */
    public function onState(Closure $callback): self
    {
        $this->onState[] = $callback;

        return $this;
    }

    /** @param Closure(RealtimeStatus): void $callback */
    public function onConnected(Closure $callback): self
    {
        $this->onConnected[] = $callback;

        return $this;
    }

    /** @param Closure(int): void $callback */
    public function onDropped(Closure $callback): self
    {
        $this->onDropped[] = $callback;

        return $this;
    }

    public function disconnect(): void
    {
        if ($this->closed) {
            return;
        }
        $this->forget();
        $this->command('pusherDisconnect', force: true);
        Realtime::unregister($this);
    }

    public function closed(): bool
    {
        return $this->closed;
    }

    /** @internal Stops delivery without touching the native client (it was replaced). */
    public function forget(): void
    {
        $this->closed = true;
        foreach ($this->channels as $channel) {
            $channel->detach();
        }
        $this->channels = [];
        $this->status = new RealtimeStatus(ConnectionState::Disconnected);
    }

    /** @internal */
    public function sendWhisper(string $channel, string $event, string $json): void
    {
        $this->assertOpen();
        $this->command('pusherWhisper', ['channel' => $channel, 'event' => $event, 'data' => $json]);
    }

    /** @internal Applies one native batch. Public for deterministic hosts and tests. @param array<array-key, mixed> $batch */
    public function dispatch(array $batch): void
    {
        foreach (is_array($batch['events'] ?? null) ? $batch['events'] : [] as $entry) {
            if ($this->closed) {
                return;
            }
            if (is_array($entry)) {
                $this->apply($entry);
            }
        }
        $dropped = (int) ($batch['dropped'] ?? 0);
        if ($dropped > 0 && !$this->closed) {
            foreach ($this->onDropped as $listener) {
                $listener($dropped);
            }
        }
    }

    private function next(): void
    {
        if ($this->closed) {
            return;
        }
        NativeModules::call(self::MODULE, 'pusherNext', ['client' => $this->name], function (NativeModuleResult $result): void {
            if ($this->closed || !$result->succeeded()) {
                return;
            }
            try {
                $batch = json_decode((string) ($result->values()['batch'] ?? '{}'), true, 32, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                $batch = [];
            }
            // Re-arm first: a throwing listener must never stop delivery.
            $this->next();
            $this->dispatch(is_array($batch) ? $batch : []);
        });
    }

    /** @param array<array-key, mixed> $entry */
    private function apply(array $entry): void
    {
        $kind = FrameKind::tryFrom((int) ($entry['k'] ?? 0));
        if ($kind === FrameKind::State) {
            $this->applyState(RealtimeStatus::fromWire($entry));

            return;
        }
        $channel = $this->channels[(string) ($entry['c'] ?? '')] ?? null;
        if ($channel === null || $kind === null) {
            return;
        }
        $user = (string) ($entry['u'] ?? '');
        match ($kind) {
            FrameKind::Message, FrameKind::Whisper => $channel->deliver(new RealtimeMessage(
                $channel->name,
                ltrim((string) ($entry['n'] ?? ''), '.'),
                (string) ($entry['d'] ?? ''),
                $user === '' ? null : $user,
                $kind === FrameKind::Whisper,
            )),
            FrameKind::Subscribed => $channel->markSubscribed(self::decodeObject((string) ($entry['m'] ?? ''))),
            FrameKind::SubscriptionFailed => $channel->fail(new SubscriptionError(
                $channel->name,
                (int) ($entry['h'] ?? 0),
                (string) ($entry['r'] ?? ''),
                (int) ($entry['w'] ?? -1),
            )),
            FrameKind::MemberAdded => $channel instanceof PresenceChannel
                ? $channel->memberAdded($user, self::decodeObject((string) ($entry['d'] ?? '')) ?? [])
                : null,
            FrameKind::MemberRemoved => $channel instanceof PresenceChannel ? $channel->memberRemoved($user) : null,
            FrameKind::State => null,
        };
    }

    private function applyState(RealtimeStatus $status): void
    {
        $previous = $this->status;
        $this->status = $status;
        if (!$status->connected()) {
            foreach ($this->channels as $channel) {
                $channel->reset();
            }
        }
        foreach ($this->onState as $listener) {
            $listener($status);
        }
        if ($status->connected() && (!$previous->connected() || $previous->socketId !== $status->socketId)) {
            foreach ($this->onConnected as $listener) {
                $listener($status);
            }
        }
    }

    /** @param array<string, string|int|float|bool> $values */
    private function command(string $method, array $values = [], bool $force = false): void
    {
        if ($this->closed && !$force) {
            return;
        }
        NativeModules::call(self::MODULE, $method, ['client' => $this->name, ...$values], static fn (NativeModuleResult $result): null => null);
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new LogicException("Realtime connection {$this->name} is closed.");
        }
    }

    /** @return array<array-key, mixed>|null */
    private static function decodeObject(string $json): ?array
    {
        if ($json === '') {
            return null;
        }
        try {
            $decoded = json_decode($json, true, 64, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return null;
        }

        return is_array($decoded) ? $decoded : null;
    }
}
