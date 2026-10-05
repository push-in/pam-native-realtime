<?php

declare(strict_types=1);

$packageAutoload = dirname(__DIR__).'/vendor/autoload.php';
if (is_file($packageAutoload)) {
    require $packageAutoload;
}
$roots = [
    'Pam\\Native\\Realtime\\' => dirname(__DIR__).'/src/',
    'Pam\\Native\\Testing\\' => dirname(__DIR__, 2).'/pam-native-testing/src/',
    'Pam\\Native\\' => dirname(__DIR__, 2).'/../pam-native/packages/native/src/',
];
spl_autoload_register(static function (string $class) use ($roots): void {
    foreach ($roots as $prefix => $root) {
        if (str_starts_with($class, $prefix)) {
            $file = $root.str_replace('\\', '/', substr($class, strlen($prefix))).'.php';
            if (is_file($file)) {
                require $file;
            }

            return;
        }
    }
});

use Pam\Native\Internal\Wire;
use Pam\Native\Realtime\ChannelType;
use Pam\Native\Realtime\ConnectionState;
use Pam\Native\Realtime\PresenceChannel;
use Pam\Native\Realtime\PresenceMember;
use Pam\Native\Realtime\Realtime;
use Pam\Native\Realtime\RealtimeEventKind;
use Pam\Native\Realtime\RealtimeMessage;
use Pam\Native\Realtime\RealtimeSocket;
use Pam\Native\Realtime\RealtimeStatus;
use Pam\Native\Realtime\Secret;
use Pam\Native\Realtime\SubscriptionError;
use Pam\Native\Testing\DispatchMode;
use Pam\Native\Testing\FakeNativeModuleTransport;
use Pam\Native\Testing\NativeTestHarness;

$tests = [];
$test = static function (string $name, Closure $body) use (&$tests): void {
    $tests[$name] = $body;
};
$check = static function (bool $condition, string $message): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
};
$throws = static function (string $class, Closure $body) use ($check): void {
    try {
        $body();
    } catch (Throwable $error) {
        $check($error instanceof $class, 'expected '.$class.', got '.$error::class.': '.$error->getMessage());

        return;
    }
    throw new RuntimeException('expected '.$class);
};
/** Installs the fake transport, connects the default client and queues deferred native reads. */
$boot = static function (int $reads = 4, ?Closure $configure = null): FakeNativeModuleTransport {
    $fake = NativeTestHarness::install();
    $fake->succeed('realtime', 'pusherConnect');
    for ($i = 0; $i < $reads; $i++) {
        $fake->respond('realtime', 'pusherNext', new Pam\Native\Testing\StubbedModuleResponse(Pam\Native\ModuleResultStatus::Success, '', DispatchMode::Deferred));
    }
    $connector = Realtime::pusher('wss://ws.example.test', key: 'app-key')
        ->auth('https://api.example.test/broadcasting/auth', Secret::value('token-1'));
    if ($configure !== null) {
        $connector = $configure($connector);
    }
    $connector->connect();

    return $fake;
};
$calls = static fn (FakeNativeModuleTransport $fake, string $method): array => array_values(array_filter(
    $fake->calls(),
    static fn ($call): bool => $call->method === $method,
));
/** Applies a native batch exactly like the pending read callback does. */
$deliver = static function (array $events, int $dropped = 0): void {
    Realtime::connection()->dispatch(['events' => $events, 'dropped' => $dropped]);
};

$test('pusher connector builds the Reverb URL and native configuration', static function () use ($check, $calls): void {
    $fake = NativeTestHarness::install();
    $fake->succeed('realtime', 'pusherConnect');
    $fake->respond('realtime', 'pusherNext', new Pam\Native\Testing\StubbedModuleResponse(Pam\Native\ModuleResultStatus::Success, '', DispatchMode::Deferred));
    $connection = Realtime::reverb('api.zechat.test', 'pushin-key')
        ->auth('https://api.zechat.test/api/broadcasting/auth', 'token-1', ['X-App' => 'ze'])
        ->header('Origin', 'https://api.zechat.test')
        ->backoff(400, 15_000, 1.8)
        ->heartbeat(30, 12)
        ->coalesce(40, 200)
        ->terminalRetry(300)
        ->connect();
    $connect = Wire::decodeMap($calls($fake, 'pusherConnect')[0]->payload);
    $config = json_decode($connect['config'], true, 8, JSON_THROW_ON_ERROR);
    $check($connect['client'] === 'default', 'client name');
    $check($config['url'] === 'wss://api.zechat.test/app/pushin-key?protocol=7&client=pam-native&version=0.3.0&flash=false', 'url '.$config['url']);
    $check($config['bearer'] === 'token-1' && $config['authHeaders']['X-App'] === 'ze' && $config['headers']['Origin'] === 'https://api.zechat.test', 'auth config');
    $check($config['quietMs'] === 40 && $config['maxDelayMs'] === 200 && $config['activityTimeoutMs'] === 30_000 && $config['terminalRetryMs'] === 300_000, 'timing config');
    $check(count($calls($fake, 'pusherNext')) === 1, 'exactly one pending native read');
    $check($connection->state() === ConnectionState::Connecting && Realtime::connection() === $connection, 'facade target');
    $check(!str_contains(print_r(Secret::value('token-1'), true), 'token-1'), 'secret redacted');
    NativeTestHarness::uninstall();
});

$test('a native batch drives every listener in one callback and re-arms one read', static function () use ($check, $calls): void {
    $fake = NativeTestHarness::install();
    $fake->succeed('realtime', 'pusherConnect');
    $events = [['k' => 1, 's' => 2, 'i' => '1.1', 'a' => 0, 'w' => 0, 'r' => '']];
    $events[] = ['k' => 4, 'c' => 'private-chat.42'];
    for ($i = 0; $i < 30; $i++) {
        $events[] = ['k' => 2, 'c' => 'private-chat.42', 'n' => $i % 2 === 0 ? '.message.sent' : 'message.sent', 'd' => json_encode(['n' => $i]), 'u' => ''];
    }
    $events[] = ['k' => 3, 'c' => 'private-chat.42', 'n' => 'client-typing', 'd' => '{"typing":true}', 'u' => '9'];
    $fake->succeed('realtime', 'pusherNext', ['batch' => json_encode(['events' => $events, 'dropped' => 0])], DispatchMode::Deferred);
    $fake->respond('realtime', 'pusherNext', new Pam\Native\Testing\StubbedModuleResponse(Pam\Native\ModuleResultStatus::Success, '', DispatchMode::Deferred));
    $fake->succeed('realtime', 'pusherSubscribe');
    Realtime::pusher('wss://ws.example.test', 'key')->auth('https://api.example.test/auth', 't')->connect();
    $received = [];
    $typing = null;
    $all = 0;
    $subscribed = 0;
    Realtime::channel('private-chat.42')
        ->listen('message.sent', static function (RealtimeMessage $m) use (&$received): void { $received[] = $m->get('n'); })
        ->listenForWhisper('typing', static function (RealtimeMessage $m) use (&$typing): void { $typing = $m; })
        ->listenToAll(static function () use (&$all): void { $all++; })
        ->subscribed(static function () use (&$subscribed): void { $subscribed++; });
    $check(Wire::decodeMap($calls($fake, 'pusherSubscribe')[0]->payload)['channel'] === 'private-chat.42', 'subscribe sent');
    $fake->flushOne();
    $check($received === range(0, 29), 'all 30 messages in order');
    $check($typing instanceof RealtimeMessage && $typing->whisper && $typing->userId === '9' && $typing->get('typing') === true, 'whisper');
    $check($all === 31 && $subscribed === 1, 'listenToAll and subscribed');
    $check(Realtime::socketId() === '1.1' && Realtime::state() === ConnectionState::Connected, 'state applied');
    $check(count($calls($fake, 'pusherNext')) === 2, 'one batch = one callback = one new read');
    NativeTestHarness::uninstall();
});

$test('presence channels expose here, joining, leaving and members', static function () use ($boot, $check, $deliver): void {
    $fake = $boot();
    $fake->succeed('realtime', 'pusherSubscribe');
    $here = null;
    $joined = null;
    $left = null;
    $channel = Realtime::join('chat-typing.42')
        ->here(static function (array $members) use (&$here): void { $here = $members; })
        ->joining(static function (PresenceMember $m) use (&$joined): void { $joined = $m; })
        ->leaving(static function (PresenceMember $m) use (&$left): void { $left = $m; });
    $check($channel instanceof PresenceChannel && $channel->name === 'presence-chat-typing.42' && $channel->type === ChannelType::Presence, 'presence channel');
    $deliver([
        ['k' => 1, 's' => 2, 'i' => '1.1'],
        ['k' => 4, 'c' => 'presence-chat-typing.42', 'm' => json_encode(['7' => ['name' => 'Ana'], '9' => ['name' => 'Bia']])],
        ['k' => 6, 'c' => 'presence-chat-typing.42', 'u' => '11', 'd' => '{"name":"Caio"}'],
        ['k' => 7, 'c' => 'presence-chat-typing.42', 'u' => '9'],
    ]);
    $check(is_array($here) && count($here) === 2 && $here[0]->id === '7' && $here[1]->get('name') === 'Bia', 'here');
    $check($joined?->id === '11' && $joined->get('name') === 'Caio', 'joining');
    $check($left?->id === '9' && $left->get('name') === 'Bia', 'leaving carries the known info');
    $check($channel->count() === 2 && $channel->member('11') !== null && $channel->member('9') === null, 'members');
    $late = null;
    $channel->here(static function (array $members) use (&$late): void { $late = count($members); });
    $check($late === 2, 'here registered after subscription runs immediately');
    $deliver([['k' => 1, 's' => 3, 'a' => 1, 'w' => 800, 'r' => 'lost']]);
    $check(!$channel->isSubscribed() && $channel->count() === 0, 'members reset while disconnected');
    NativeTestHarness::uninstall();
});

$test('onConnected fires once per session and onState for every change', static function () use ($boot, $check, $deliver): void {
    $sessions = [];
    $states = [];
    $boot(configure: static function ($c) use (&$sessions, &$states) {
        return $c
            ->onConnected(static function (RealtimeStatus $s) use (&$sessions): void { $sessions[] = $s->socketId; })
            ->onState(static function (RealtimeStatus $s) use (&$states): void { $states[] = $s->state; });
    });
    $deliver([['k' => 1, 's' => 2, 'i' => '1.1']]);
    $deliver([['k' => 1, 's' => 2, 'i' => '1.1', 'r' => 'Client event rejected']]);
    $deliver([['k' => 1, 's' => 3, 'a' => 2, 'w' => 900, 'r' => 'Pong timeout']]);
    $check(Realtime::connection()->current()->retryInMillis === 900 && Realtime::socketId() === null, 'unavailable status');
    $deliver([['k' => 1, 's' => 2, 'i' => '2.2']]);
    $check($sessions === ['1.1', '2.2'], 'sessions '.json_encode($sessions));
    $check($states === [ConnectionState::Connected, ConnectionState::Connected, ConnectionState::Unavailable, ConnectionState::Connected], 'states');
    NativeTestHarness::uninstall();
});

$test('whispers are prefixed, encoded and refused on public channels', static function () use ($boot, $check, $calls, $throws): void {
    $fake = $boot();
    $fake->succeed('realtime', 'pusherSubscribe');
    $fake->succeed('realtime', 'pusherSubscribe');
    $fake->succeed('realtime', 'pusherWhisper', ['sent' => true]);
    Realtime::private('chat.42')->whisper('typing', ['user' => 'Ana', 'typing' => true]);
    $payload = Wire::decodeMap($calls($fake, 'pusherWhisper')[0]->payload);
    $check($payload['channel'] === 'private-chat.42' && $payload['event'] === 'client-typing' && $payload['data'] === '{"user":"Ana","typing":true}', 'whisper payload');
    $throws(LogicException::class, static fn () => Realtime::channel('feed')->whisper('typing'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::private('chat.42')->whisper('big', ['x' => str_repeat('a', 11_000)]));
    NativeTestHarness::uninstall();
});

$test('subscription failures reach the channel error listener', static function () use ($boot, $check, $deliver): void {
    $fake = $boot();
    $fake->succeed('realtime', 'pusherSubscribe');
    $error = null;
    Realtime::private('chat.99')->error(static function (SubscriptionError $e) use (&$error): void { $error = $e; });
    $deliver([['k' => 5, 'c' => 'private-chat.99', 'h' => 403, 'r' => 'Auth HTTP 403', 'w' => 60_000]]);
    $check($error instanceof SubscriptionError && $error->forbidden() && $error->retryInMillis === 60_000, 'error delivered');
    NativeTestHarness::uninstall();
});

$test('leave unsubscribes natively and detaches listeners', static function () use ($boot, $check, $calls, $deliver): void {
    $fake = $boot();
    $fake->succeed('realtime', 'pusherSubscribe');
    $fake->succeed('realtime', 'pusherUnsubscribe');
    $hits = 0;
    $listener = static function () use (&$hits): void { $hits++; };
    $channel = Realtime::private('chat.42')->listen('message.sent', $listener);
    $deliver([['k' => 2, 'c' => 'private-chat.42', 'n' => 'message.sent', 'd' => '{}']]);
    $channel->stopListening('message.sent', $listener);
    $deliver([['k' => 2, 'c' => 'private-chat.42', 'n' => 'message.sent', 'd' => '{}']]);
    Realtime::leave('chat.42');
    $deliver([['k' => 2, 'c' => 'private-chat.42', 'n' => 'message.sent', 'd' => '{}']]);
    $check($hits === 1, 'hits '.$hits);
    $check(Wire::decodeMap($calls($fake, 'pusherUnsubscribe')[0]->payload)['channel'] === 'private-chat.42', 'unsubscribe');
    $check(Realtime::connection()->channels() === [], 'channel forgotten');
    NativeTestHarness::uninstall();
});

$test('a throwing listener never stops native delivery', static function () use ($check, $calls): void {
    $fake = NativeTestHarness::install();
    $fake->succeed('realtime', 'pusherConnect');
    $fake->succeed('realtime', 'pusherNext', ['batch' => json_encode(['events' => [['k' => 2, 'c' => 'feed', 'n' => 'boom', 'd' => '']]])], DispatchMode::Deferred);
    $fake->respond('realtime', 'pusherNext', new Pam\Native\Testing\StubbedModuleResponse(Pam\Native\ModuleResultStatus::Success, '', DispatchMode::Deferred));
    $fake->succeed('realtime', 'pusherSubscribe');
    Realtime::pusher('wss://ws.example.test', 'key')->connect();
    Realtime::channel('feed')->listen('boom', static function (): void { throw new DomainException('listener failed'); });
    try {
        $fake->flushOne();
    } catch (DomainException) {
    }
    $check(count($calls($fake, 'pusherNext')) === 2, 'read re-armed before dispatch');
    NativeTestHarness::uninstall();
});

$test('namespaces, overflow, status snapshots and replacement', static function () use ($boot, $check, $deliver): void {
    $dropped = 0;
    $fake = $boot(configure: static function ($c) use (&$dropped) {
        return $c->namespace('App\\Events')->onDropped(static function (int $n) use (&$dropped): void { $dropped += $n; });
    });
    $fake->succeed('realtime', 'pusherSubscribe');
    $names = [];
    Realtime::channel('orders')
        ->listen('OrderShipped', static function (RealtimeMessage $m) use (&$names): void { $names[] = $m->event; })
        ->listen('.order.custom', static function (RealtimeMessage $m) use (&$names): void { $names[] = $m->event; });
    $deliver([
        ['k' => 2, 'c' => 'orders', 'n' => 'App\\Events\\OrderShipped', 'd' => 'plain text'],
        ['k' => 2, 'c' => 'orders', 'n' => '.order.custom', 'd' => '[1,2]'],
    ], 5);
    $check($names === ['App\\Events\\OrderShipped', 'order.custom'] && $dropped === 5, 'namespace + dropped');
    $fake->succeed('realtime', 'pusherStatus', ['status' => json_encode(['k' => 1, 's' => 2, 'i' => '3.3', 'e' => 'old', 't' => 1_700_000_000_000, 'q' => 2, 'ch' => [['n' => 'orders', 's' => true]], 'j' => [['at' => 1, 'm' => 'Connected as 3.3']]])]);
    $status = null;
    Realtime::status(static function (RealtimeStatus $s) use (&$status): void { $status = $s; });
    $check($status?->socketId === '3.3' && $status->channels === ['orders' => true] && $status->journal[0]['message'] === 'Connected as 3.3' && $status->pending === 2, 'status snapshot');
    $old = Realtime::connection();
    $fake->succeed('realtime', 'pusherConnect');
    $fake->respond('realtime', 'pusherNext', new Pam\Native\Testing\StubbedModuleResponse(Pam\Native\ModuleResultStatus::Success, '', DispatchMode::Deferred));
    Realtime::pusher('wss://ws.example.test', 'key')->connect();
    $check($old->closed() && Realtime::connection() !== $old, 'same name replaces (hot reload safe)');
    $fake->succeed('realtime', 'pusherDisconnect');
    Realtime::disconnect();
    $check(!Realtime::has() && Realtime::state() === ConnectionState::Disconnected, 'disconnected');
    NativeTestHarness::uninstall();
});

$test('rejects insecure endpoints and unsupported channels', static function () use ($boot, $throws): void {
    $throws(InvalidArgumentException::class, static fn () => Realtime::pusher('ws://ws.example.test', 'key'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::pusher('wss://ws.example.test', 'key')->auth('http://api.example.test/auth'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::pusher('wss://ws.example.test', 'key')->header('Bad Header', 'x'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::pusher('wss://ws.example.test', 'key')->coalesce(50, 10));
    $boot();
    $throws(InvalidArgumentException::class, static fn () => Realtime::channel('private-encrypted-chat'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::channel('bad channel'));
    $throws(InvalidArgumentException::class, static fn () => Realtime::presence('chat.42'));
    NativeTestHarness::uninstall();
});

$test('raw socket keeps its typed contract', static function () use ($check): void {
    $fake = NativeTestHarness::install();
    $fake->succeed('realtime', 'connect', ['identifier' => 'socket-1']);
    $fake->succeed('realtime', 'poll', ['kind' => 3, 'payload' => base64_encode("\x00\x01"), 'code' => 0]);
    $id = null;
    $event = null;
    $socket = new RealtimeSocket();
    $socket->connect('wss://realtime.example.test/app', ['Authorization' => 'Bearer token'], ['pam.v1'], static function ($v) use (&$id): void { $id = $v; });
    $socket->poll('socket-1', static function ($v) use (&$event): void { $event = $v; }, 0);
    $check($id === 'socket-1' && $event?->kind === RealtimeEventKind::Binary && $event->binary() === "\x00\x01", 'raw socket');
    NativeTestHarness::uninstall();
});

$failed = 0;
foreach ($tests as $name => $body) {
    try {
        $body();
        fwrite(STDOUT, "PASS {$name}\n");
    } catch (Throwable $error) {
        $failed++;
        fwrite(STDERR, "FAIL {$name}: {$error->getMessage()} @ {$error->getFile()}:{$error->getLine()}\n");
        NativeTestHarness::uninstall();
    }
}
fwrite(STDOUT, count($tests)." tests, {$failed} failures\n");
exit($failed === 0 ? 0 : 1);
