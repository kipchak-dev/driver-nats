<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use Basis\Nats\Message\Payload;
use InvalidArgumentException;
use JsonSerializable;
use Stringable;

/**
 * Checks subjects and headers and encodes payloads before they reach the socket.
 */
final class Wire
{
    /**
     * A subject to publish to: dot-separated tokens with no wildcards or whitespace.
     */
    public static function subject(string $subject): string
    {
        self::tokens($subject, 'subject');
        if (preg_match('/(^|\.)[*>](\.|$)/', $subject)) {
            throw new InvalidArgumentException('Subject "' . $subject . '" has a wildcard; messages are published to a concrete subject');
        }

        return $subject;
    }

    /**
     * A subject to subscribe to: "*" matches one token and a final ">" matches the rest.
     */
    public static function pattern(string $subject): string
    {
        $tokens = self::tokens($subject, 'subject');
        foreach ($tokens as $index => $token) {
            if ($token === '>' && $index !== count($tokens) - 1) {
                throw new InvalidArgumentException('Subject "' . $subject . '" has ">" before its last token');
            }
            if ($token !== '*' && $token !== '>' && preg_match('/[*>]/', $token)) {
                throw new InvalidArgumentException('Subject "' . $subject . '" mixes a wildcard into a token; "*" and ">" stand alone');
            }
        }

        return $subject;
    }

    /**
     * A stream, consumer or queue group name: no whitespace, ".", "*", ">", "/" or "\\".
     */
    public static function name(string $name, string $what): string
    {
        if ($name === '' || preg_match('~[\s.*>/\\\\\0]~', $name)) {
            throw new InvalidArgumentException(ucfirst($what) . ' "' . $name . '" must be non-empty with no whitespace, ".", "*", ">", "/" or "\\"');
        }

        return $name;
    }

    /**
     * The body and headers of a message. Strings and Stringable objects are sent as they are; arrays, objects and
     * other scalars as JSON; null as an empty body.
     *
     * @param array<string, string|int|float|bool> $headers
     */
    public static function payload(mixed $body, array $headers = []): Payload
    {
        return new RawPayload(self::body($body), self::headers($headers));
    }

    public static function body(mixed $body): string
    {
        return match (true) {
            $body === null => '',
            is_string($body) => $body,
            $body instanceof Stringable && !$body instanceof JsonSerializable => (string) $body,
            is_array($body), is_object($body), is_scalar($body) => json_encode($body, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION),
            default => throw new InvalidArgumentException('A ' . get_debug_type($body) . ' cannot be sent as a message body'),
        };
    }

    /**
     * @param array<mixed, mixed> $headers
     * @return array<string, string>
     */
    public static function headers(array $headers): array
    {
        $clean = [];
        foreach ($headers as $name => $value) {
            if (!is_string($name) || !preg_match('/^[!-9;-~]+$/', $name)) {
                throw new InvalidArgumentException('Header name "' . $name . '" must be printable ASCII with no spaces or ":"');
            }
            if (!is_scalar($value)) {
                throw new InvalidArgumentException('Header "' . $name . '" must have a string value');
            }
            $value = is_bool($value) ? ($value ? 'true' : 'false') : (string) $value;
            if (preg_match('/[\r\n\0]/', $value)) {
                throw new InvalidArgumentException('Header "' . $name . '" must not contain line breaks');
            }
            $clean[$name] = $value;
        }

        return $clean;
    }

    /**
     * The size of a message on the wire, headers included, as the server counts it against max_payload.
     */
    public static function size(Payload $payload): int
    {
        $size = strlen($payload->body);
        if ($payload->headers !== []) {
            $size += strlen("NATS/1.0\r\n\r\n");
            foreach ($payload->headers as $name => $value) {
                $size += strlen($name . ': ' . $value . "\r\n");
            }
        }

        return $size;
    }

    /**
     * @return list<string>
     */
    private static function tokens(string $subject, string $what): array
    {
        if ($subject === '' || preg_match('/[\s\0]/', $subject)) {
            throw new InvalidArgumentException(ucfirst($what) . ' "' . $subject . '" must be non-empty with no whitespace');
        }
        $tokens = explode('.', $subject);
        if (in_array('', $tokens, true)) {
            throw new InvalidArgumentException(ucfirst($what) . ' "' . $subject . '" has an empty token');
        }

        return $tokens;
    }
}
