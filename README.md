# PAM Native Realtime

Persistent RFC 6455 connections owned by the native runtime. PHP can render, suspend or reload without implementing socket framing or holding a network loop.

```bash
pam add realtime
pam doctor
```

```php
$realtime = new Pam\Native\Realtime\Realtime();
$realtime->connect('wss://api.example.com/socket', ['Authorization' => 'Bearer '.$token], ['pam.v1'], function (?string $id, ?string $error): void {});
$realtime->poll($id, function (?Pam\Native\Realtime\RealtimeEvent $event, ?string $error): void {});
```

Only `wss://` endpoints are accepted. Text and binary frames are bounded per connection. Incoming events use a 256-item native queue and one pending long poll, with deterministic timeout and lifecycle cleanup. Android uses OkHttp `5.3.0`; iOS uses URLSessionWebSocketTask.

Platform support: Android API 26+, iOS 15+, PAM Native 0.6.x.


## What installation does

`pam add realtime` resolves the official compatible package, performs a non-mutating Composer preflight, updates the normal `composer.json` and `composer.lock`, refreshes generated native integration when required, and leaves the project ready for `pam doctor` validation.

Use `pam packages` to inspect availability and `pam remove realtime` to uninstall the capability safely. Direct Composer commands are an advanced interoperability path; PAM is the supported application workflow.

## API guide

| API | Responsibility |
| --- | --- |
| `Realtime` | Connect, send text/binary, poll, inspect state, and close. |
| `RealtimeEvent` / `RealtimeEventKind` | Consume normalized message, lifecycle, and timeout events. |
| `RealtimeConnectionState` | Read the typed connection state. |

All coded states, kinds, and variants are sequential integer-backed enums. Use enum cases in application code; do not depend on raw wire numbers.

## Production checklist

- Accept only authenticated `wss://` endpoints.
- Poll continuously while connected and handle bounded-queue pressure.
- Reconnect with jittered bounded backoff and refresh credentials when required.
- Run `pam doctor`, `pam test`, and a signed release build on every supported platform.
- Exercise denial, cancellation, backgrounding, process restart, and offline behavior before release.

## Troubleshooting

- **Connection is rejected:** verify TLS, headers, subprotocols, and server upgrade support.
- **Poll returns timeouts:** treat them as lifecycle ticks, not fatal errors.
- **Messages are dropped:** reduce producer rate or consume the 256-item queue faster.
- **Native integration is stale:** run `pam doctor --fix`, rebuild the native host, and inspect the first reported diagnostic.

## Compatibility and support

This package targets PAM Native `0.6.x`, Android API 26+, and iOS 15+ unless a platform-specific section above states a stricter requirement. Platform SDKs, credentials, entitlements, physical hardware, and store configuration remain application responsibilities.

- [PAM documentation](https://push-in.github.io/pam-docs/introduction/)
- [PAM Native overview](https://push-in.github.io/pam-docs/native/overview/)
- [Plugin and native capability model](https://push-in.github.io/pam-docs/native/plugins/)
- [Report an issue](https://github.com/push-in/pam-native-realtime/issues)

Security vulnerabilities should be reported through the repository security policy or GitHub private vulnerability reporting, not a public issue.
