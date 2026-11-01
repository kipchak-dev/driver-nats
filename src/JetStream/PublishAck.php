<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\JetStream;

/**
 * JetStream's confirmation that a message is stored.
 */
final readonly class PublishAck
{
    public function __construct(
        public string $stream,
        public int $sequence,
        /** True when the stream already had a message with this Nats-Msg-Id and kept only the first. */
        public bool $duplicate = false,
        public ?string $domain = null,
    ) {
    }
}
