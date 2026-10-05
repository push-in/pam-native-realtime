<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;
use InvalidArgumentException;
use JsonException;
use LogicException;

/**
 * A subscribed Pusher/Reverb channel (Laravel Echo semantics).
 *
 * ```php
 * Realtime::channel('private-chat.42')
 *     ->listen('message.sent', fn (RealtimeMessage $m) => $this->append($m->json()))
 *     ->listenForWhisper('typing', fn (RealtimeMessage $m) => $this->typing($m->userId))
 *     ->whisper('typing', ['typing' => true]);
 * ```
 *
 * Subscription, channel authorization, re-subscription after reconnects and
 * delivery are native. A burst of frames reaches PHP as one batch, so all
 * listeners triggered by it share a single render.
 */
class Channel
{
    private const int MAX_WHISPER_BYTES = 10_240;

    /** @var array<string, list<Closure(RealtimeMessage): void>> */
    private array $listeners = [];

    /** @var list<Closure(RealtimeMessage): void> */
    private array $everything = [];

    /** @var list<Closure(): void> */
    private array $onSubscribed = [];

    /** @var list<Closure(SubscriptionError): void> */
    private array $onError = [];

    private bool $subscribed = false;

    private bool $left = false;

    public readonly ChannelType $type;

    /** @internal */
    public function __construct(
        private readonly RealtimeConnection $connection,
        public readonly string $name,
        private readonly ?string $namespace = null,
    ) {
        $this->type = ChannelType::of($name);
    }

    /**
     * Listens for a server event. Leading dots are ignored (`.message.sent`
     * and `message.sent` are the same); with a connector `namespace()` set,
     * names without a leading dot are prefixed like Laravel Echo.
     *
     * @param Closure(RealtimeMessage): void $callback
     */
    public function listen(string $event, Closure $callback): static
    {
        $this->listeners[$this->format($event)][] = $callback;

        return $this;
    }

    /** @param Closure(RealtimeMessage): void $callback Receives every event of this channel, whispers included. */
    public function listenToAll(Closure $callback): static
    {
        $this->everything[] = $callback;

        return $this;
    }

    /** @param Closure(RealtimeMessage): void $callback */
    public function listenForWhisper(string $event, Closure $callback): static
    {
        $this->listeners[self::whisperName($event)][] = $callback;

        return $this;
    }

    /** @param null|Closure(RealtimeMessage): void $callback Removes one listener, or all listeners of the event. */
    public function stopListening(string $event, ?Closure $callback = null): static
    {
        $this->remove($this->format($event), $callback);

        return $this;
    }

    /** @param null|Closure(RealtimeMessage): void $callback */
    public function stopListeningForWhisper(string $event, ?Closure $callback = null): static
    {
        $this->remove(self::whisperName($event), $callback);

        return $this;
    }

    /**
     * Sends a client event (`client-<event>`) to the other subscribers.
     * Natively throttled per event to one frame every 100 ms (the newest
     * payload wins), and dropped while the channel is not subscribed.
     *
     * @param array<array-key, mixed> $data
     */
    public function whisper(string $event, array $data = []): static
    {
        if (!$this->type->allowsWhispers()) {
            throw new LogicException('Whispers require a private or presence channel.');
        }
        try {
            $json = json_encode($data === [] ? new \stdClass() : $data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        } catch (JsonException $error) {
            throw new InvalidArgumentException('Whisper payload cannot be encoded.', previous: $error);
        }
        if (strlen($json) > self::MAX_WHISPER_BYTES) {
            throw new InvalidArgumentException('Whisper payloads are limited to 10 KiB.');
        }
        $this->connection->sendWhisper($this->name, self::whisperName($event), $json);

        return $this;
    }

    /** @param Closure(): void $callback Runs on every (re)subscription; immediately if already subscribed. */
    public function subscribed(Closure $callback): static
    {
        $this->onSubscribed[] = $callback;
        if ($this->subscribed) {
            $callback();
        }

        return $this;
    }

    /** @param Closure(SubscriptionError): void $callback */
    public function error(Closure $callback): static
    {
        $this->onError[] = $callback;

        return $this;
    }

    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    public function leave(): void
    {
        $this->connection->leave($this->name);
    }

    /** @internal */
    public function deliver(RealtimeMessage $message): void
    {
        if ($this->left) {
            return;
        }
        foreach ($this->listeners[$message->event] ?? [] as $listener) {
            $listener($message);
        }
        foreach ($this->everything as $listener) {
            $listener($message);
        }
    }

    /** @internal @param array<array-key, mixed>|null $members */
    public function markSubscribed(?array $members): void
    {
        $this->subscribed = true;
        foreach ($this->onSubscribed as $listener) {
            $listener();
        }
    }

    /** @internal */
    public function fail(SubscriptionError $error): void
    {
        $this->subscribed = false;
        foreach ($this->onError as $listener) {
            $listener($error);
        }
    }

    /** @internal Connection lost: the native client re-subscribes on reconnect. */
    public function reset(): void
    {
        $this->subscribed = false;
    }

    /** @internal */
    public function detach(): void
    {
        $this->left = true;
        $this->subscribed = false;
        $this->listeners = [];
        $this->everything = [];
        $this->onSubscribed = [];
        $this->onError = [];
    }

    private function format(string $event): string
    {
        if ($event === '' || strlen($event) > 200) {
            throw new InvalidArgumentException('Event names must be 1-200 bytes.');
        }
        if ($event[0] === '.' || $event[0] === '\\') {
            return substr($event, 1);
        }
        if ($this->namespace !== null && !str_starts_with($event, 'client-')) {
            return $this->namespace.'\\'.$event;
        }

        return $event;
    }

    private static function whisperName(string $event): string
    {
        $event = ltrim($event, '.');
        if ($event === '' || strlen($event) > 192) {
            throw new InvalidArgumentException('Whisper names must be 1-192 bytes.');
        }

        return str_starts_with($event, 'client-') ? $event : 'client-'.$event;
    }

    private function remove(string $event, ?Closure $callback): void
    {
        if ($callback === null) {
            unset($this->listeners[$event]);

            return;
        }
        $remaining = array_values(array_filter($this->listeners[$event] ?? [], static fn (Closure $listener): bool => $listener !== $callback));
        if ($remaining === []) {
            unset($this->listeners[$event]);
        } else {
            $this->listeners[$event] = $remaining;
        }
    }
}
