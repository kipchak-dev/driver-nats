<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use Basis\Nats\Message\Payload;

/**
 * A received payload whose headers are exactly those NATS sent.
 *
 * basis-company/nats's Payload decodes every body as JSON and merges any "message.hdrs" member it finds into the
 * headers, so a publisher could forge headers by putting them in the body. This keeps the body as it arrived.
 */
final class RawPayload extends Payload
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(string $body, array $headers = [], ?string $subject = null)
    {
        $this->body = $body;
        $this->headers = $headers;
        $this->subject = $subject;
        $this->timestampNanos = null;
    }
}
