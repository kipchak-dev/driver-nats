<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\KeyValue;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;
use Kipchak\Driver\NATS\Exception\JetStreamException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\Exception\RevisionMismatchException;
use Kipchak\Driver\NATS\JetStream\JetStream;
use Kipchak\Driver\NATS\Library\Connection;

/**
 * A JetStream key-value bucket, stored as NATS's own clients store one (stream KV_<bucket>, subjects
 * $KV.<bucket>.<key>), so the nats CLI and other languages' clients see the same keys.
 *
 * Values are strings; arrays and objects are stored as JSON and read back with Entry::json().
 */
final class KeyValue
{
    private bool $ensured = false;

    /**
     * @param array<string, mixed> $streamConfig
     */
    public function __construct(
        private readonly JetStream $jetStream,
        public readonly string $bucket,
        private readonly array $streamConfig,
    ) {
    }

    /**
     * Creates the bucket, or updates it to match its settings. (The first write creates a missing bucket, but leaves
     * an existing one's settings alone.)
     *
     * @return array<string, mixed> The bucket's stream info.
     */
    public function ensure(): array
    {
        $info = $this->jetStream->ensureStreamConfig($this->stream(), $this->streamConfig);
        $this->ensured = true;

        return $info;
    }

    /**
     * The key's value, or null when the key does not exist, was deleted, or the bucket does not exist yet.
     */
    public function get(string $key): ?Entry
    {
        $last = $this->last($key);
        if ($last === null || $last['deleted']) {
            return null;
        }

        return new Entry($this->bucket, $key, $last['value'], $last['revision'], $last['created']);
    }

    /**
     * Sets the key's value and returns its new revision.
     */
    public function put(string $key, mixed $value): int
    {
        return $this->write($key, $value);
    }

    /**
     * Sets the key's value only if the key does not exist (or was deleted); returns the revision.
     *
     * @throws RevisionMismatchException when the key exists.
     */
    public function create(string $key, mixed $value): int
    {
        try {
            return $this->write($key, $value, ['Nats-Expected-Last-Subject-Sequence' => '0']);
        } catch (RevisionMismatchException $e) {
            $last = $this->last($key);
            if ($last === null || !$last['deleted']) {
                throw $e;
            }

            return $this->write($key, $value, ['Nats-Expected-Last-Subject-Sequence' => (string) $last['revision']]);
        }
    }

    /**
     * Sets the key's value only if its revision is still $revision; returns the new revision.
     *
     * @throws RevisionMismatchException when the key has changed since.
     */
    public function update(string $key, mixed $value, int $revision): int
    {
        return $this->write($key, $value, ['Nats-Expected-Last-Subject-Sequence' => (string) $revision]);
    }

    /**
     * Deletes the key, keeping its earlier revisions in the history. With $revision, only if the key is still at it.
     */
    public function delete(string $key, ?int $revision = null): void
    {
        $headers = ['KV-Operation' => 'DEL'];
        if ($revision !== null) {
            $headers['Nats-Expected-Last-Subject-Sequence'] = (string) $revision;
        }
        $this->write($key, '', $headers);
    }

    /**
     * Deletes the key and all its history.
     */
    public function purge(string $key): void
    {
        $this->write($key, '', ['KV-Operation' => 'PURGE', 'Nats-Rollup' => 'sub']);
    }

    /**
     * Deletes the bucket and every key in it; false when it did not exist.
     */
    public function destroy(): bool
    {
        $this->ensured = false;

        return $this->jetStream->deleteStream($this->stream());
    }

    /**
     * The bucket's stream info (config and state), or null when it does not exist yet.
     *
     * @return array<string, mixed>|null
     */
    public function info(): ?array
    {
        return $this->jetStream->streamInfo($this->stream());
    }

    /**
     * @param array<string, string> $headers
     */
    private function write(string $key, mixed $value, array $headers = []): int
    {
        $subject = $this->subject($key);
        if (!$this->ensured) {
            $this->createIfMissing();
        }

        try {
            return $this->jetStream->publish($subject, $value, $headers, expectedStream: $this->stream())->sequence;
        } catch (NoRespondersException) {
            // The bucket was deleted since: create it again, once.
            $this->createIfMissing();

            return $this->jetStream->publish($subject, $value, $headers, expectedStream: $this->stream())->sequence;
        }
    }

    private function createIfMissing(): void
    {
        $this->jetStream->ensureStreamConfig($this->stream(), $this->streamConfig, update: false);
        $this->ensured = true;
    }

    /**
     * The last message on the key, deletion markers included.
     *
     * @return array{value: string, revision: int, created: DateTimeImmutable, deleted: bool}|null
     */
    private function last(string $key): ?array
    {
        try {
            $response = $this->jetStream->api('STREAM.MSG.GET.' . $this->stream(), ['last_by_subj' => $this->subject($key)]);
        } catch (JetStreamException $e) {
            if (in_array($e->errorCode, [JetStreamException::NO_MESSAGE_FOUND, JetStreamException::STREAM_NOT_FOUND], true)) {
                return null;
            }
            throw $e;
        }

        $message = $response->message ?? null;
        if (!is_object($message)) {
            return null;
        }
        $headers = isset($message->hdrs) ? Connection::parseHeaders((string) base64_decode((string) $message->hdrs)) : [];
        $operation = strtoupper($headers['KV-Operation'] ?? '');

        return [
            'value' => isset($message->data) ? (string) base64_decode((string) $message->data) : '',
            'revision' => (int) $message->seq,
            'created' => self::time((string) ($message->time ?? '')),
            'deleted' => $operation === 'DEL' || $operation === 'PURGE',
        ];
    }

    private function subject(string $key): string
    {
        if (!preg_match('~^[-/_=.A-Za-z0-9]+$~', $key) || str_starts_with($key, '.') || str_ends_with($key, '.') || str_contains($key, '..')) {
            throw new InvalidArgumentException('Key "' . $key . '" may contain only letters, digits and - / _ = ., and must not start or end with "." or contain ".."');
        }

        return '$KV.' . $this->bucket . '.' . $key;
    }

    private function stream(): string
    {
        return 'KV_' . $this->bucket;
    }

    /**
     * Parses JetStream's RFC 3339 time with nanoseconds.
     */
    private static function time(string $time): DateTimeImmutable
    {
        $time = (string) preg_replace('/(\.\d{6})\d+/', '$1', $time);
        try {
            return (new DateTimeImmutable($time))->setTimezone(new DateTimeZone('UTC'));
        } catch (\Exception) {
            return new DateTimeImmutable('@0');
        }
    }
}
