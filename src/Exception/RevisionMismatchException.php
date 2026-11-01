<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

/**
 * A key-value create() or update(), or a publish with an expected last sequence, lost a race: the key or subject
 * has changed since the revision the caller read.
 */
final class RevisionMismatchException extends JetStreamException
{
}
