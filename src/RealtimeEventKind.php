<?php
declare(strict_types=1);namespace Pam\Native\Realtime;enum RealtimeEventKind:int{case Connected=1;case Text=2;case Binary=3;case Closed=4;case Failure=5;case Pong=6;case Timeout=7;}
