<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use Closure;
use InvalidArgumentException;
use JsonException;
use Pam\Native\Modules\NativeModuleResult;
use Pam\Native\Modules\NativeModules;

final class Realtime
{
    private const string MODULE = 'realtime';

    /** @param array<string,string> $headers @param list<string> $protocols @param Closure(?string,?string):void $complete */
    public function connect(string $url, array $headers, array $protocols, Closure $complete, int $maxMessageBytes = 1_048_576): int
    {
        if (!str_starts_with($url, 'wss://') || filter_var('https://'.substr($url, 6), FILTER_VALIDATE_URL) === false) {
            throw new InvalidArgumentException('Realtime connections require a valid wss:// URL.');
        }
        if ($maxMessageBytes < 1024 || $maxMessageBytes > 8_388_608) throw new InvalidArgumentException('Message limit must be between 1 KiB and 8 MiB.');
        foreach ($headers as $name => $value) {
            if (preg_match('/^[!#$%&\'*+.^_`|~0-9A-Za-z-]{1,128}$/D', $name) !== 1 || str_contains($value, "\r") || str_contains($value, "\n")) throw new InvalidArgumentException('Invalid WebSocket header.');
        }
        foreach ($protocols as $protocol) if (preg_match('/^[!#$%&\'*+.^_`|~0-9A-Za-z-]{1,128}$/D', $protocol) !== 1) throw new InvalidArgumentException('Invalid WebSocket subprotocol.');
        try { $headerJson = json_encode($headers, JSON_THROW_ON_ERROR); } catch (JsonException $e) { throw new InvalidArgumentException('Headers cannot be encoded.', previous:$e); }
        return NativeModules::call(self::MODULE, 'connect', ['url'=>$url,'headers'=>$headerJson,'protocols'=>implode(',', $protocols),'maxMessageBytes'=>$maxMessageBytes], static function(NativeModuleResult $result)use($complete):void{$id=$result->values()['identifier']??null;$complete(is_string($id)?$id:null,$result->succeeded()?null:$result->message());});
    }
    /** @param Closure(bool,?string):void $complete */
    public function sendText(string $identifier,string $payload,Closure $complete):int{return $this->send($identifier,$payload,false,$complete);}
    /** @param Closure(bool,?string):void $complete */
    public function sendBinary(string $identifier,string $payload,Closure $complete):int{return $this->send($identifier,base64_encode($payload),true,$complete);}
    /** @param Closure(?RealtimeEvent,?string):void $complete */
    public function poll(string $identifier,Closure $complete,int $timeoutMillis=25_000):int{if($timeoutMillis<0||$timeoutMillis>60_000)throw new InvalidArgumentException('Poll timeout must be between 0 and 60000 ms.');return NativeModules::call(self::MODULE,'poll',['identifier'=>$identifier,'timeoutMillis'=>$timeoutMillis],static function(NativeModuleResult $result)use($complete):void{if(!$result->succeeded()){$complete(null,$result->message());return;}$v=$result->values();$complete(new RealtimeEvent(RealtimeEventKind::tryFrom((int)($v['kind']??7))??RealtimeEventKind::Timeout,(string)($v['payload']??''),(int)($v['code']??0)),null);});}
    /** @param Closure(RealtimeConnectionState):void $complete */
    public function state(string $identifier,Closure $complete):int{return NativeModules::call(self::MODULE,'state',['identifier'=>$identifier],static fn(NativeModuleResult $r)=>$complete(RealtimeConnectionState::tryFrom((int)($r->values()['state']??5))??RealtimeConnectionState::Failed));}
    /** @param Closure(bool):void $complete */
    public function close(string $identifier,Closure $complete,int $code=1000,string $reason=''):int{if($code<1000||$code>4999||strlen($reason)>123)throw new InvalidArgumentException('Invalid WebSocket close code or reason.');return NativeModules::call(self::MODULE,'close',['identifier'=>$identifier,'code'=>$code,'reason'=>$reason],static fn(NativeModuleResult $r)=>$complete($r->succeeded()));}
    /** @param Closure(bool,?string):void $complete */
    private function send(string $identifier,string $payload,bool $binary,Closure $complete):int{if(strlen($payload)>8_388_608)throw new InvalidArgumentException('Outgoing message exceeds 8 MiB.');return NativeModules::call(self::MODULE,'send',['identifier'=>$identifier,'payload'=>$payload,'binary'=>$binary],static fn(NativeModuleResult $r)=>$complete($r->succeeded(),$r->succeeded()?null:$r->message()));}
}
