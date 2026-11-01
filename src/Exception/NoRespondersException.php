<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

/**
 * Nothing is subscribed to the subject: no service answers the request, or no JetStream stream captures the
 * subject a message was published to. NATS says so at once rather than letting the request time out.
 */
final class NoRespondersException extends NatsException
{
    public function __construct(public readonly string $subject, string $message = '')
    {
        parent::__construct($message !== '' ? $message : 'No responders are subscribed to "' . $subject . '"');
    }
}
