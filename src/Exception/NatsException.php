<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

use RuntimeException;

/**
 * NATS refused an operation, for example a permissions violation or a message larger than the server accepts.
 */
class NatsException extends RuntimeException
{
}
