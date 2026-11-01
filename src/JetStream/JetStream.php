<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\JetStream;

use InvalidArgumentException;
use Kipchak\Driver\NATS\Exception\JetStreamException;
use Kipchak\Driver\NATS\Exception\NatsConnectionException;
use Kipchak\Driver\NATS\Exception\NatsException;
use Kipchak\Driver\NATS\Exception\NatsTimeoutException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\KeyValue\KeyValue;
use Kipchak\Driver\NATS\Library\NatsClient;
use Kipchak\Driver\NATS\Library\RawPayload;
use Kipchak\Driver\NATS\Library\Wire;

/**
 * JetStream on one connection: acknowledged publishing, streams, pull consumers and key-value buckets.
 */
final class JetStream
{
    /** @var array<string, Consumer> */
    private array $consumers = [];

    /** @var array<string, KeyValue> */
    private array $buckets = [];

    public function __construct(private readonly NatsClient $nats)
    {
    }

    /**
     * Publishes a message to the stream that captures its subject and waits until the stream has stored it.
     *
     * Every message carries a Nats-Msg-Id ($id, or a random one), and a publish that is not confirmed (a timeout, or
     * the connection replaced while waiting) is sent once more with the same id, so the stream keeps a single copy. Pass your own $id (an order number, say) to make retries
     * of your own safe too, within the stream's duplicate_window.
     *
     * @param array<string, string|int|float|bool> $headers
     * @param ?string $expectedStream Fail unless the subject belongs to this stream.
     * @param ?int $expectedLastSequence Fail unless this is the stream's last sequence (optimistic concurrency).
     * @param ?int $expectedLastSubjectSequence Fail unless this is the last sequence on this subject.
     *
     * @throws NoRespondersException when no stream captures the subject.
     * @throws \Kipchak\Driver\NATS\Exception\RevisionMismatchException when an expected sequence does not match.
     * @throws JetStreamException when the stream refuses the message.
     * @throws NatsTimeoutException when the stream does not confirm within $timeout, twice.
     */
    public function publish(
        string $subject,
        mixed $payload = '',
        array $headers = [],
        ?string $id = null,
        ?string $expectedStream = null,
        ?int $expectedLastSequence = null,
        ?int $expectedLastSubjectSequence = null,
        ?float $timeout = null,
    ): PublishAck {
        $headers = Wire::headers($headers);
        if ($id !== null || !isset($headers['Nats-Msg-Id'])) {
            $headers['Nats-Msg-Id'] = $id ?? bin2hex(random_bytes(12));
        }
        if ($expectedStream !== null) {
            $headers['Nats-Expected-Stream'] = $expectedStream;
        }
        if ($expectedLastSequence !== null) {
            $headers['Nats-Expected-Last-Sequence'] = (string) $expectedLastSequence;
        }
        if ($expectedLastSubjectSequence !== null) {
            $headers['Nats-Expected-Last-Subject-Sequence'] = (string) $expectedLastSubjectSequence;
        }

        Wire::subject($subject);
        $message = new RawPayload(Wire::body($payload), Wire::headers($headers));

        try {
            try {
                $answer = $this->nats->requestMsg($subject, $message, $timeout);
            } catch (NatsTimeoutException | NatsConnectionException $e) {
                $this->nats->logger()?->warning('JetStream publish not confirmed; sending it again with the same message id', [
                    'subject' => $subject,
                    'id' => $headers['Nats-Msg-Id'],
                    'reason' => $e->getMessage(),
                ]);
                $answer = $this->nats->requestMsg($subject, $message, $timeout);
            }
        } catch (NoRespondersException $e) {
            throw new NoRespondersException($subject, 'No JetStream stream captures "' . $subject . '"; create one with ensureStream()');
        }

        $ack = json_decode($answer->payload->body);
        if (!is_object($ack)) {
            throw new NatsException('[JetStream] The answer to a publish on "' . $subject . '" was not a JetStream acknowledgement');
        }
        if (isset($ack->error)) {
            throw JetStreamException::fromError($ack->error, 'publish to "' . $subject . '"');
        }

        return new PublishAck((string) $ack->stream, (int) $ack->seq, (bool) ($ack->duplicate ?? false), isset($ack->domain) ? (string) $ack->domain : null);
    }

    /**
     * Creates the stream, or updates it to match, from the settings under streams.<name> in config merged with
     * $settings. Settings left out take NATS's defaults. Returns the stream's info.
     *
     * @param array<string, mixed> $settings
     * @return array<string, mixed>
     */
    public function ensureStream(string $name, array $settings = []): array
    {
        Wire::name($name, 'stream');
        $settings = array_replace($this->nats->config()->streams[$name] ?? [], $settings);

        return $this->ensureStreamConfig($name, Settings::stream($name, $settings));
    }

    /**
     * The stream's configuration and state, or null when there is no such stream.
     *
     * @return array<string, mixed>|null
     */
    public function streamInfo(string $name): ?array
    {
        try {
            return self::toArray($this->api('STREAM.INFO.' . Wire::name($name, 'stream')));
        } catch (JetStreamException $e) {
            if ($e->errorCode === JetStreamException::STREAM_NOT_FOUND) {
                return null;
            }
            throw $e;
        }
    }

    /**
     * The names of every stream in the account.
     *
     * @return list<string>
     */
    public function streamNames(): array
    {
        $names = [];
        do {
            $page = $this->api('STREAM.NAMES', ['offset' => count($names)]);
            $batch = (array) ($page->streams ?? []);
            array_push($names, ...array_map('strval', $batch));
        } while ($batch !== [] && count($names) < (int) ($page->total ?? 0));

        return $names;
    }

    /**
     * Deletes the stream and every message in it; false when there was no such stream.
     */
    public function deleteStream(string $name): bool
    {
        try {
            $this->api('STREAM.DELETE.' . Wire::name($name, 'stream'));

            return true;
        } catch (JetStreamException $e) {
            if ($e->errorCode === JetStreamException::STREAM_NOT_FOUND) {
                return false;
            }
            throw $e;
        }
    }

    /**
     * Removes the stream's messages, or only those on $subject. Returns how many were removed.
     */
    public function purgeStream(string $name, ?string $subject = null): int
    {
        $request = $subject === null ? [] : ['filter' => Wire::pattern($subject)];

        return (int) ($this->api('STREAM.PURGE.' . Wire::name($name, 'stream'), $request)->purged ?? 0);
    }

    /**
     * A durable pull consumer, from the settings under consumers.<name> in config merged with $settings. It is
     * created, or updated to match, on first use.
     *
     * @param ?string $stream The stream to read; defaults to consumers.<name>.stream.
     * @param array<string, mixed> $settings
     */
    public function consumer(string $name, ?string $stream = null, array $settings = []): Consumer
    {
        Wire::name($name, 'consumer');
        $settings = array_replace($this->nats->config()->consumers[$name] ?? [], $settings);
        $stream ??= isset($settings['stream']) ? (string) $settings['stream'] : null;
        if ($stream === null) {
            throw new InvalidArgumentException('Consumer "' . $name . '" has no stream: pass one, or set consumers.' . $name . '.stream in config');
        }
        Wire::name($stream, 'stream');
        $durable = isset($settings['name']) ? Wire::name((string) $settings['name'], 'consumer') : $name;
        unset($settings['stream'], $settings['name']);

        $request = Settings::consumer($stream, $durable, $settings);
        $key = $stream . '/' . $durable . '/' . md5(serialize($request));

        return $this->consumers[$key] ??= new Consumer($this->nats, $this, $stream, $durable, $request);
    }

    /**
     * The consumer's configuration and state, or null when there is no such consumer.
     *
     * @return array<string, mixed>|null
     */
    public function consumerInfo(string $stream, string $name): ?array
    {
        try {
            return self::toArray($this->api('CONSUMER.INFO.' . Wire::name($stream, 'stream') . '.' . Wire::name($name, 'consumer')));
        } catch (JetStreamException $e) {
            if (in_array($e->errorCode, [JetStreamException::CONSUMER_NOT_FOUND, JetStreamException::STREAM_NOT_FOUND], true)) {
                return null;
            }
            throw $e;
        }
    }

    /**
     * Deletes a consumer; false when there was no such consumer.
     */
    public function deleteConsumer(string $stream, string $name): bool
    {
        try {
            $this->api('CONSUMER.DELETE.' . Wire::name($stream, 'stream') . '.' . Wire::name($name, 'consumer'));

            return true;
        } catch (JetStreamException $e) {
            if (in_array($e->errorCode, [JetStreamException::CONSUMER_NOT_FOUND, JetStreamException::STREAM_NOT_FOUND], true)) {
                return false;
            }
            throw $e;
        }
    }

    /**
     * A key-value bucket, from the settings under buckets.<name> in config merged with $settings. It is created on
     * the first write.
     *
     * @param array<string, mixed> $settings
     */
    public function keyValue(string $bucket, array $settings = []): KeyValue
    {
        if (!preg_match('/^[A-Za-z0-9_-]+$/', $bucket)) {
            throw new InvalidArgumentException('Bucket "' . $bucket . '" may contain only letters, digits, "-" and "_"');
        }
        $settings = array_replace($this->nats->config()->buckets[$bucket] ?? [], $settings);
        $key = $bucket . '/' . md5(serialize($settings));

        return $this->buckets[$key] ??= new KeyValue($this, $bucket, Settings::bucket($bucket, $settings));
    }

    /**
     * Account usage and limits: memory, storage, streams, consumers.
     *
     * @return array<string, mixed>
     */
    public function accountInfo(): array
    {
        return self::toArray($this->api('INFO'));
    }

    /**
     * Calls the JetStream API directly: $command is the subject after "$JS.API.", such as "STREAM.INFO.ORDERS".
     *
     * @param array<string, mixed> $request
     *
     * @throws JetStreamException when JetStream answers with an error.
     */
    public function api(string $command, array $request = [], ?float $timeout = null): object
    {
        $subject = $this->nats->config()->apiPrefix() . '.' . $command;
        $body = $request === [] ? '' : json_encode((object) $request, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);

        try {
            try {
                $answer = $this->nats->requestMsg(Wire::subject($subject), new RawPayload($body), $timeout);
            } catch (NatsConnectionException) {
                // The request was lost with the old connection; API calls are safe to send again.
                $answer = $this->nats->requestMsg($subject, new RawPayload($body), $timeout);
            }
        } catch (NoRespondersException) {
            throw new NatsException('[JetStream] Nothing answered ' . $subject . ': JetStream is not enabled for this server or account');
        }

        $response = json_decode($answer->payload->body);
        if (!is_object($response)) {
            throw new NatsException('[JetStream] The answer to ' . $command . ' was not JSON');
        }
        if (isset($response->error)) {
            throw JetStreamException::fromError($response->error, $command);
        }

        return $response;
    }

    /**
     * @internal Creates a stream from JetStream configuration. When it exists with different settings, updates it,
     * or with $update false leaves it as it is.
     *
     * @param array<string, mixed> $config
     * @return array<string, mixed>
     */
    public function ensureStreamConfig(string $name, array $config, bool $update = true): array
    {
        try {
            return self::toArray($this->api('STREAM.CREATE.' . $name, $config));
        } catch (JetStreamException $e) {
            if ($e->errorCode !== JetStreamException::STREAM_NAME_IN_USE) {
                throw $e;
            }
        }

        return $update ? self::toArray($this->api('STREAM.UPDATE.' . $name, $config)) : (array) $this->streamInfo($name);
    }

    /**
     * @return array<string, mixed>
     */
    private static function toArray(object $response): array
    {
        $array = json_decode((string) json_encode($response), true) ?: [];
        unset($array['type']);

        return $array;
    }
}
