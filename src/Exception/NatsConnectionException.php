<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

/**
 * No server could be reached, the TLS handshake or authentication failed, or the connection was lost and could not
 * be re-established within reconnect_attempts.
 */
final class NatsConnectionException extends NatsException
{
}
