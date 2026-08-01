<?php
declare(strict_types=1);namespace Pam\Native\Realtime;final readonly class RealtimeEvent{public function __construct(public RealtimeEventKind $kind,public string $payload='',public int $code=0){}public function binary():?string{return $this->kind===RealtimeEventKind::Binary?base64_decode($this->payload,true)?:null:null;}}
