<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Exception;

/**
 * JetStream refused an API call or a publish.
 *
 * $errorCode is JetStream's own error code (10059 stream not found, 10014 consumer not found, 10058 stream name in
 * use with a different configuration, 10071 wrong last sequence, ...) and $status the HTTP-like status that comes
 * with it (400, 404, 503, ...).
 */
class JetStreamException extends NatsException
{
    public const int STREAM_NOT_FOUND = 10059;
    public const int CONSUMER_NOT_FOUND = 10014;
    public const int NO_MESSAGE_FOUND = 10037;
    public const int STREAM_NAME_IN_USE = 10058;
    public const int WRONG_LAST_SEQUENCE = 10071;

    public function __construct(
        string $message,
        public readonly ?int $errorCode = null,
        public readonly ?int $status = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $errorCode ?? 0, $previous);
    }

    /**
     * @param object{code?: int, err_code?: int, description?: string} $error The "error" member of a JetStream
     *                                                                       API response.
     */
    public static function fromError(object $error, string $context): self
    {
        $errorCode = isset($error->err_code) ? (int) $error->err_code : null;
        $status = isset($error->code) ? (int) $error->code : null;
        $message = '[JetStream] ' . $context . ': ' . ($error->description ?? 'unknown error')
            . ($errorCode !== null ? ' (' . $errorCode . ')' : '');

        return $errorCode === self::WRONG_LAST_SEQUENCE
            ? new RevisionMismatchException($message, $errorCode, $status)
            : new self($message, $errorCode, $status);
    }
}
