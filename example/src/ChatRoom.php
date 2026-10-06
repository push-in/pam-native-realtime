<?php

declare(strict_types=1);

namespace App;

use Pam\Native\App;
use Pam\Native\AppState;
use Pam\Native\Component;
use Pam\Native\Element;
use Pam\Native\Realtime\ConnectionState;
use Pam\Native\Realtime\PresenceChannel;
use Pam\Native\Realtime\PresenceMember;
use Pam\Native\Realtime\Realtime;
use Pam\Native\Realtime\RealtimeMessage;
use Pam\Native\Realtime\RealtimeStatus;
use Pam\Native\Realtime\Secret;
use Pam\Native\Realtime\SubscriptionError;
use Pam\Native\Style;
use Pam\Native\UI\Button;
use Pam\Native\UI\Column;
use Pam\Native\UI\SafeAreaView;
use Pam\Native\UI\Screen;
use Pam\Native\UI\Text;
use Pam\Native\UI\TextInput;

/**
 * A presence chat room on Laravel Reverb, Pusher Channels or Soketi.
 *
 * Server side (Laravel): broadcast `MessageSent` as `message.sent` on
 * `presence-room.1` and authorize the channel in routes/channels.php.
 * Whispers (`client-typing`) need "client events" enabled on the app.
 */
final class ChatRoom extends Component
{
    /** Edit these for your server. Realtime requires wss:// and an https:// auth endpoint. */
    private const string HOST = 'ws.example.com';
    private const string KEY = 'app-key';
    private const string AUTH_ENDPOINT = 'https://api.example.com/broadcasting/auth';
    private const string TOKEN = 'paste-a-bearer-token';
    private const string ROOM = 'room.1';

    private string $state = 'connecting';
    private int $online = 0;
    private string $typing = '';
    private string $draft = '';

    /** @var list<string> */
    private array $messages = [];

    public function boot(): void
    {
        Realtime::reverb(self::HOST, self::KEY)
            ->auth(self::AUTH_ENDPOINT, Secret::value(self::TOKEN))
            ->onState(function (RealtimeStatus $status): void {
                $this->state = strtolower($status->state->name).($status->lastError !== '' ? ' ('.$status->lastError.')' : '');
            })
            ->onConnected(function (RealtimeStatus $_status): void {
                // First connect and every reconnect: fetch what was missed from your API here.
            })
            ->onDropped(function (int $count): void {
                $this->messages[] = "[{$count} events dropped while suspended, resync]";
            })
            ->connect();

        Realtime::join(self::ROOM)
            ->here(function (array $members): void {
                $this->online = count($members);
            })
            ->joining(function (PresenceMember $member): void {
                $this->online++;
                $this->messages[] = '→ '.(string) $member->get('name', $member->id).' joined';
            })
            ->leaving(function (PresenceMember $member): void {
                $this->online = max(0, $this->online - 1);
                $this->messages[] = '← '.(string) $member->get('name', $member->id).' left';
            })
            ->listen('message.sent', function (RealtimeMessage $message): void {
                $this->messages[] = sprintf('%s: %s', (string) $message->get('user', '?'), (string) $message->get('body', ''));
                $this->typing = '';
            })
            ->listenForWhisper('typing', function (RealtimeMessage $message): void {
                $this->typing = ($message->userId ?? 'someone').' is typing…';
            })
            ->error(function (SubscriptionError $error): void {
                $this->state = sprintf('channel error %d: %s', $error->status, $error->message);
            });

        App::onStateChange(static function (AppState $state): void {
            if ($state === AppState::Active && Realtime::state() === ConnectionState::Unavailable) {
                Realtime::reconnect();
            }
        });
    }

    public function render(): Element
    {
        $lines = array_map(static fn (string $line): Text => Text::make($line), array_slice($this->messages, -20));

        return Screen::make(
            SafeAreaView::make(
                Column::make(
                    Text::make('presence-'.self::ROOM)->style(new Style(fontSize: 22, fontWeight: 700)),
                    Text::make(sprintf('%s · %d online · socket %s', $this->state, $this->online, Realtime::has() ? (Realtime::socketId() ?? '-') : '-')),
                    ...[
                        ...$lines,
                        $this->typing !== '' ? Text::make($this->typing) : null,
                        TextInput::make($this->draft)
                            ->placeholder('Type to whisper "typing"')
                            ->onChange($this->typed(...)),
                        Button::make('Diagnostics')->onPress($this->diagnostics(...)),
                    ],
                )->style(new Style(flexGrow: 1, padding: 16, gap: 8)),
            ),
        );
    }

    public function typed(string $value): void
    {
        $this->draft = $value;
        $channel = Realtime::has() ? Realtime::connection()->channels()['presence-'.self::ROOM] ?? null : null;
        if ($channel instanceof PresenceChannel && $channel->isSubscribed()) {
            $channel->whisper('typing', ['typing' => $value !== '']);   // throttled natively to 1 frame / 100 ms
        }
    }

    public function diagnostics(): void
    {
        Realtime::status(function (RealtimeStatus $status): void {
            foreach (array_slice($status->journal, 0, 5) as $entry) {   // newest first
                $this->messages[] = '[journal] '.date('H:i:s', intdiv($entry['at'], 1000)).' '.$entry['message'];
            }
        });
    }
}
