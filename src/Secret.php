<?php

declare(strict_types=1);

namespace Pam\Native\Realtime;

use InvalidArgumentException;

/**
 * A bearer credential for channel authorization. It is handed to the native
 * client in memory only (never persisted nor exposed in status snapshots) and
 * redacted from `var_dump()` / `print_r()`.
 */
final readonly class Secret
{
    private function __construct(private string $value)
    {
    }

    public static function value(string $value): self
    {
        if ($value === '' || strlen($value) > 16_384 || preg_match('/[\r\n\0]/', $value) === 1) {
            throw new InvalidArgumentException('Secret values must be 1-16384 bytes without line breaks.');
        }

        return new self($value);
    }

    /** @internal */
    public function reveal(): string
    {
        return $this->value;
    }

    /** @return array{value: string} */
    public function __debugInfo(): array
    {
        return ['value' => '[redacted]'];
    }
}
