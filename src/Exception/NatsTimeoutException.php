<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

/**
 * A request, a JetStream publish or a JetStream API call had no answer within its timeout.
 */
final class NatsTimeoutException extends NatsException
{
}
