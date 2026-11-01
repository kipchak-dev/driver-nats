<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

/**
 * One NATS server from the servers list: nats://host:4222, or tls://host:4222 to require TLS.
 */
final readonly class Server
{
    public function __construct(
        public string $host,
        public int $port,
        public bool $tls,
    ) {
    }

    public function address(): string
    {
        return 'tcp://' . (str_contains($this->host, ':') ? '[' . $this->host . ']' : $this->host) . ':' . $this->port;
    }

    public function __toString(): string
    {
        return ($this->tls ? 'tls' : 'nats') . '://' . (str_contains($this->host, ':') ? '[' . $this->host . ']' : $this->host)
            . ':' . $this->port;
    }
}
