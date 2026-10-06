# Realtime chat room demo

A one-screen PAM Native app for `pushinbr/pam-native-realtime`: a presence
chat room on Laravel Reverb (or Pusher Channels / Soketi) with member counts,
`message.sent` events, `client-typing` whispers, `onDropped()` resync, the
foreground reconnect Zé Chat uses and the native diagnostic journal.

1. Edit the constants at the top of `src/ChatRoom.php`: your `wss://` host,
   app key, `https://` channel-auth endpoint and a bearer token.
2. On the server, authorize `presence-room.1` and broadcast an event named
   `message.sent` with `user` and `body`; enable client events for whispers.

```bash
cd example
pam composer install
pam doctor --fix
pam dev            # or: pam build
```

The app installs the released package from Packagist. Plain `ws://` and `http://` endpoints are rejected by design.
