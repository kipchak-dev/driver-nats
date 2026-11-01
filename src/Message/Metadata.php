<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Message;

use DateTimeImmutable;
use DateTimeZone;

/**
 * Where a JetStream message came from, read from its acknowledgement subject.
 */
final readonly class Metadata
{
    public function __construct(
        public string $stream,
        public string $consumer,
        /** How many times this message has been delivered, this time included. */
        public int $deliveries,
        public int $streamSequence,
        public int $consumerSequence,
        /** When the message was stored in the stream. */
        public DateTimeImmutable $timestamp,
        /** Messages left for this consumer after this one. */
        public int $pending,
        public ?string $domain = null,
    ) {
    }

    /**
     * Parses "$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<nanos>.<pending>", or the newer form with a
     * domain and account hash after "$JS.ACK"; null for any other subject.
     */
    public static function fromReplySubject(?string $replyTo): ?self
    {
        if ($replyTo === null || !str_starts_with($replyTo, '$JS.ACK.')) {
            return null;
        }

        $tokens = explode('.', $replyTo);
        if (count($tokens) === 9) {
            array_splice($tokens, 2, 0, ['_', '']);
        }
        if (count($tokens) < 11) {
            return null;
        }

        foreach ([6, 7, 8, 9, 10] as $index) {
            if (!ctype_digit($tokens[$index])) {
                return null;
            }
        }

        $nanos = $tokens[9];
        $seconds = strlen($nanos) > 9 ? substr($nanos, 0, -9) : '0';
        $micros = str_pad(substr(str_pad($nanos, 9, '0', STR_PAD_LEFT), -9, 6), 6, '0');
        $timestamp = DateTimeImmutable::createFromFormat('U.u', $seconds . '.' . $micros, new DateTimeZone('UTC'));

        return new self(
            stream: $tokens[4],
            consumer: $tokens[5],
            deliveries: (int) $tokens[6],
            streamSequence: (int) $tokens[7],
            consumerSequence: (int) $tokens[8],
            timestamp: $timestamp !== false ? $timestamp : new DateTimeImmutable('@0'),
            pending: (int) $tokens[10],
            domain: $tokens[2] === '_' || $tokens[2] === '' ? null : $tokens[2],
        );
    }
}
