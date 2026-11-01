<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Msg;
use Basis\Nats\Message\Payload;
use Basis\Nats\Queue;
use Kipchak\Driver\NATS\Exception\NatsConnectionException;
use Kipchak\Driver\NATS\Exception\NatsException;
use Kipchak\Driver\NATS\Exception\NatsTimeoutException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\JetStream\JetStream;
use Kipchak\Driver\NATS\KeyValue\KeyValue;
use Kipchak\Driver\NATS\Message\Message;
use Psr\Log\LoggerInterface;

/**
 * One NATS connection: core publish, request/reply and subscriptions, and the way into JetStream and key-value
 * buckets.
 *
 * The connection is opened on first use and kept for the life of the worker, so requests after the first reuse it.
 */
final class NatsClient
{
    private readonly Client $client;

    private readonly Connection $connection;

    private readonly string $inboxPrefix;

    private ?Queue $inbox = null;

    private int $requestId = 0;

    private ?JetStream $jetStream = null;

    public function __construct(
        private readonly ConnectionConfig $config,
        private readonly ?LoggerInterface $logger = null,
    ) {
        $this->client = new Client(new Configuration(timeout: $config->timeout, reconnect: false));
        $this->client->skipInvalidMessages(true);
        $this->client->setName($config->clientName);
        $this->connection = new Connection($this->client, $config, $logger);
        $this->client->connection = $this->connection;
        $this->inboxPrefix = '_INBOX.' . bin2hex(random_bytes(11));
    }

    public function config(): ConnectionConfig
    {
        return $this->config;
    }

    public function logger(): ?LoggerInterface
    {
        return $this->logger;
    }

    /**
     * Publishes a message with no acknowledgement: subscribers listening now receive it, and nothing is kept.
     * For a message that must not be lost, publish to a JetStream stream with jetStream()->publish().
     *
     * @param mixed $payload A string as it is; arrays, objects and other scalars as JSON.
     * @param array<string, string|int|float|bool> $headers
     */
    public function publish(string $subject, mixed $payload = '', array $headers = [], ?string $replyTo = null): void
    {
        $this->send(Wire::subject($subject), Wire::payload($payload, $headers), $replyTo === null ? null : Wire::subject($replyTo));
    }

    /**
     * Sends a request and waits for the first answer.
     *
     * @param array<string, string|int|float|bool> $headers
     *
     * @throws NoRespondersException when nothing is subscribed to the subject.
     * @throws NatsTimeoutException when no answer comes within $timeout seconds (default: the connection's timeout).
     * @throws NatsConnectionException when the connection was lost while waiting, so no answer can come; the
     *                                 service may or may not have handled the request.
     */
    public function request(string $subject, mixed $payload = '', array $headers = [], ?float $timeout = null): Message
    {
        return Message::fromMsg($this->requestMsg(Wire::subject($subject), Wire::payload($payload, $headers), $timeout), $this);
    }

    /**
     * Subscribes to a subject, "*" and ">" wildcards allowed. Subscribers in the same queue group share the messages,
     * each going to one of them.
     */
    public function subscribe(string $subject, ?string $queue = null): Subscription
    {
        Wire::pattern($subject);
        $handler = $queue === null
            ? $this->client->subscribe($subject)
            : $this->client->subscribeQueue($subject, Wire::name($queue, 'queue group'));

        return new Subscription($this, $handler, $subject, $queue);
    }

    /**
     * Waits until the server has received everything published so far.
     *
     * @throws NatsTimeoutException when the server does not answer within the connection's timeout.
     */
    public function flush(): void
    {
        if (!$this->connection->ping()) {
            throw new NatsTimeoutException('NATS did not answer a ping within ' . $this->config->timeout . 's');
        }
    }

    /**
     * Whether the server answers a ping, connecting first when needed; for health checks.
     */
    public function ping(): bool
    {
        try {
            return $this->connection->ping();
        } catch (NatsException) {
            return false;
        }
    }

    /**
     * The server's INFO: server_name, version, jetstream, max_payload, cluster and so on.
     *
     * @return array<string, mixed>
     */
    public function serverInfo(): array
    {
        return json_decode((string) json_encode($this->connection->serverInfo()), true) ?: [];
    }

    public function jetStream(): JetStream
    {
        return $this->jetStream ??= new JetStream($this);
    }

    /**
     * A key-value bucket, created on first use from the settings under buckets.<name> in config.
     *
     * @param array<string, mixed> $settings Overrides for the bucket's settings in config.
     */
    public function keyValue(string $bucket, array $settings = []): KeyValue
    {
        return $this->jetStream()->keyValue($bucket, $settings);
    }

    /**
     * Closes the connection; the next operation opens a new one.
     */
    public function close(): void
    {
        $this->inbox = null;
        if ($this->connection->isConnected()) {
            $this->client->unsubscribeAll();
        }
        $this->connection->close();
    }

    /**
     * @internal Sends a request with a prepared payload and returns the raw answer, for JetStream.
     */
    public function requestMsg(string $subject, Payload $payload, ?float $timeout = null): Msg
    {
        $timeout ??= $this->config->timeout;
        $this->drain();

        $inbox = $this->inbox();
        $replyTo = $this->inboxPrefix . '.' . ++$this->requestId;
        $this->send($subject, $payload, $replyTo);
        $generation = $this->connection->generation();

        $deadline = microtime(true) + $timeout;
        while (($remaining = $deadline - microtime(true)) > 0) {
            $inbox->setTimeout($remaining);
            $answers = $inbox->fetchAll(1);
            if ($this->connection->generation() !== $generation) {
                throw new NatsConnectionException('The connection to NATS was replaced while waiting for an answer from "' . $subject . '", so none can come');
            }
            $violation = $this->connection->takeViolation($subject) ?? $this->connection->takeViolation($this->inboxPrefix . '.*');
            if ($violation !== null) {
                throw new NatsException('[NATS] ' . $violation);
            }
            foreach ($answers as $answer) {
                // Late answers to earlier requests that timed out are dropped.
                if ($answer->subject !== $replyTo) {
                    continue;
                }
                if (($answer->payload->headers['Status-Code'] ?? null) === '503' && $answer->payload->body === '') {
                    throw new NoRespondersException($subject);
                }

                return $answer;
            }
        }

        throw new NatsTimeoutException('No answer from "' . $subject . '" within ' . round($timeout, 3) . 's');
    }

    /**
     * @internal Changes each time the connection is (re-)established.
     */
    public function generation(): int
    {
        return $this->connection->generation();
    }

    /**
     * @internal The underlying basis-company/nats client.
     */
    public function basis(): Client
    {
        return $this->client;
    }

    /**
     * @internal A new subject no one else uses, for replies.
     */
    public function newInbox(): string
    {
        return $this->inboxPrefix . '.' . bin2hex(random_bytes(6));
    }

    /**
     * Handles whatever arrived while the worker was idle: answers the server's PINGs, and notices a connection the
     * server has closed, so the next operation goes out on a live one. A server error about an earlier operation
     * (a permissions violation on a publish, say) is logged rather than thrown at this one.
     */
    public function drain(): void
    {
        if (!$this->connection->isConnected()) {
            return;
        }
        for ($i = 0; $i < 1000; $i++) {
            try {
                if ($this->client->process(0) === null) {
                    return;
                }
            } catch (NatsConnectionException $e) {
                throw $e;
            } catch (NatsException $e) {
                $this->logger?->warning('NATS reported an error for an earlier operation', ['error' => $e->getMessage()]);
            }
        }
    }

    private function send(string $subject, Payload $payload, ?string $replyTo = null): void
    {
        $size = Wire::size($payload);
        $limit = $this->connection->maxPayload();
        if ($size > $limit) {
            throw new NatsException('A ' . $size . '-byte message to "' . $subject . '" is larger than the server accepts (max_payload ' . $limit . ' bytes)');
        }

        $this->client->publish($subject, $payload, $replyTo);
    }

    private function inbox(): Queue
    {
        if ($this->inbox === null) {
            $queue = $this->client->subscribe($this->inboxPrefix . '.*');
            assert($queue instanceof Queue);
            $this->inbox = $queue;
        }

        return $this->inbox;
    }
}
