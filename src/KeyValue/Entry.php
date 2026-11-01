<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\KeyValue;

use DateTimeImmutable;

/**
 * A key's current value in a bucket.
 */
final readonly class Entry
{
    public function __construct(
        public string $bucket,
        public string $key,
        public string $value,
        /** Pass to update() to change the value only if no one else has changed it since. */
        public int $revision,
        public DateTimeImmutable $created,
    ) {
    }

    /**
     * The value decoded from JSON: arrays for objects unless $associative is false.
     *
     * @throws \JsonException when the value is not JSON.
     */
    public function json(bool $associative = true): mixed
    {
        return json_decode($this->value, $associative, 512, JSON_THROW_ON_ERROR);
    }
}
