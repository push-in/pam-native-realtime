<?php
declare(strict_types=1);namespace Pam\Native\Realtime;enum RealtimeConnectionState:int{case Connecting=1;case Open=2;case Closing=3;case Closed=4;case Failed=5;}
