<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;

/**
 * A presence channel: who is here, who joins and who leaves.
 *
 * ```php
 * Realtime::join('chat-typing.42')
 *     ->here(fn (array $members) => $this->online = count($members))
 *     ->joining(fn (PresenceMember $m) => ...)
 *     ->leaving(fn (PresenceMember $m) => ...);
 * ```
 */
final class PresenceChannel extends Channel
{
    /** @var array<string, PresenceMember> */
    private array $members = [];

    /** @var list<Closure(list<PresenceMember>): void> */
    private array $onHere = [];

    /** @var list<Closure(PresenceMember): void> */
    private array $onJoining = [];

    /** @var list<Closure(PresenceMember): void> */
    private array $onLeaving = [];

    /** @param Closure(list<PresenceMember>): void $callback Runs with the full member list on every (re)subscription. */
    public function here(Closure $callback): static
    {
        $this->onHere[] = $callback;
        if ($this->isSubscribed()) {
            $callback($this->members());
        }

        return $this;
    }

    /** @param Closure(PresenceMember): void $callback */
    public function joining(Closure $callback): static
    {
        $this->onJoining[] = $callback;

        return $this;
    }

    /** @param Closure(PresenceMember): void $callback */
    public function leaving(Closure $callback): static
    {
        $this->onLeaving[] = $callback;

        return $this;
    }

    /** @return list<PresenceMember> */
    public function members(): array
    {
        return array_values($this->members);
    }

    public function member(string $id): ?PresenceMember
    {
        return $this->members[$id] ?? null;
    }

    public function count(): int
    {
        return count($this->members);
    }

    /** @internal @param array<array-key, mixed>|null $members */
    public function markSubscribed(?array $members): void
    {
        $this->members = [];
        foreach ($members ?? [] as $id => $info) {
            $this->members[(string) $id] = new PresenceMember((string) $id, is_array($info) ? $info : []);
        }
        parent::markSubscribed($members);
        $list = $this->members();
        foreach ($this->onHere as $listener) {
            $listener($list);
        }
    }

    /** @internal @param array<array-key, mixed> $info */
    public function memberAdded(string $id, array $info): void
    {
        $member = new PresenceMember($id, $info);
        $this->members[$id] = $member;
        foreach ($this->onJoining as $listener) {
            $listener($member);
        }
    }

    /** @internal */
    public function memberRemoved(string $id): void
    {
        $member = $this->members[$id] ?? new PresenceMember($id);
        unset($this->members[$id]);
        foreach ($this->onLeaving as $listener) {
            $listener($member);
        }
    }

    /** @internal */
    public function reset(): void
    {
        parent::reset();
        $this->members = [];
    }

    /** @internal */
    public function detach(): void
    {
        parent::detach();
        $this->members = [];
        $this->onHere = [];
        $this->onJoining = [];
        $this->onLeaving = [];
    }
}
