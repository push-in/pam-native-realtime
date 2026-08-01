# PAM Native Realtime

Persistent RFC 6455 connections owned by the native runtime. PHP can render, suspend or reload without implementing socket framing or holding a network loop.

```bash
composer require pushinbr/pam-native-realtime
pam mobile codegen
pam mobile ios:prepare
```

```php
$realtime = new Pam\Native\Realtime\Realtime();
$realtime->connect('wss://api.example.com/socket', ['Authorization' => 'Bearer '.$token], ['pam.v1'], function (?string $id, ?string $error): void {});
$realtime->poll($id, function (?Pam\Native\Realtime\RealtimeEvent $event, ?string $error): void {});
```

Only `wss://` endpoints are accepted. Text and binary frames are bounded per connection. Incoming events use a 256-item native queue and one pending long poll, with deterministic timeout and lifecycle cleanup. Android uses OkHttp `5.3.0`; iOS uses URLSessionWebSocketTask.

Platform support: Android API 26+, iOS 15+, PAM Native 0.6.x.
