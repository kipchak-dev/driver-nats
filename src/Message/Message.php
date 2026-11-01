<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Message;

use Basis\Nats\Message\Msg;
use Kipchak\Driver\NATS\Exception\NatsException;
use Kipchak\Driver\NATS\Library\NatsClient;
use LogicException;

/**
 * A message received from a subscription, a request or a JetStream consumer.
 *
 * JetStream messages are acknowledged with ack(), nak(), term() or inProgress(); the first of ack(), nak() and term()
 * settles the message and later calls do nothing.
 */
final class Message
{
    private bool $settled = false;

    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public readonly string $subject,
        public readonly string $body,
        public readonly array $headers = [],
        public readonly ?string $replyTo = null,
        private readonly ?NatsClient $client = null,
    ) {
    }

    public static function fromMsg(Msg $msg, ?NatsClient $client = null): self
    {
        return new self($msg->subject, $msg->payload->body, $msg->payload->headers, $msg->replyTo, $client);
    }

    /**
     * The body decoded from JSON: arrays for objects unless $associative is false.
     *
     * @throws \JsonException when the body is not JSON.
     */
    public function json(bool $associative = true): mixed
    {
        return json_decode($this->body, $associative, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * A header by name, case-insensitively.
     */
    public function header(string $name): ?string
    {
        if (array_key_exists($name, $this->headers)) {
            return $this->headers[$name];
        }
        foreach ($this->headers as $key => $value) {
            if (strcasecmp($key, $name) === 0) {
                return $value;
            }
        }

        return null;
    }

    /**
     * The stream, consumer, sequence numbers and delivery count of a JetStream message; null for core NATS.
     */
    public function metadata(): ?Metadata
    {
        return Metadata::fromReplySubject($this->replyTo);
    }

    public function isJetStream(): bool
    {
        return $this->replyTo !== null && str_starts_with($this->replyTo, '$JS.ACK.');
    }

    public function isSettled(): bool
    {
        return $this->settled;
    }

    /**
     * Acknowledges a JetStream message so it is not delivered again. With $confirm, waits for the server to record
     * the acknowledgement, so a lost acknowledgement cannot cause a redelivery.
     */
    public function ack(bool $confirm = false): void
    {
        if ($this->settle()) {
            if ($confirm) {
                $this->client()->request($this->replyTo, '+ACK');

                return;
            }
            $this->client()->publish($this->replyTo, '+ACK');
        }
    }

    /**
     * Asks JetStream to deliver the message again, after $delay seconds when given.
     */
    public function nak(?float $delay = null): void
    {
        if ($this->settle()) {
            // The delay is whole nanoseconds: JetStream ignores one written as a float.
            $nanoseconds = (int) round(max(0.0, (float) $delay) * 1_000_000_000);
            $this->client()->publish($this->replyTo, $nanoseconds > 0 ? '-NAK {"delay":' . $nanoseconds . '}' : '-NAK');
        }
    }

    /**
     * Tells JetStream never to deliver the message again, for one that can never be processed.
     */
    public function term(string $reason = ''): void
    {
        if ($this->settle()) {
            $reason = trim(str_replace(["\r", "\n"], ' ', $reason));
            $this->client()->publish($this->replyTo, $reason !== '' ? '+TERM ' . $reason : '+TERM');
        }
    }

    /**
     * Tells JetStream the message is still being worked on, resetting its ack_wait.
     */
    public function inProgress(): void
    {
        $this->jetStreamOnly();
        if (!$this->settled) {
            $this->client()->publish($this->replyTo, '+WPI');
        }
    }

    /**
     * Answers a request.
     *
     * @param array<string, string|int|float|bool> $headers
     */
    public function respond(mixed $payload, array $headers = []): void
    {
        if ($this->replyTo === null) {
            throw new LogicException('This message was not sent as a request, so there is nothing to answer');
        }
        $this->client()->publish($this->replyTo, $payload, $headers);
    }

    private function settle(): bool
    {
        $this->jetStreamOnly();
        if ($this->settled) {
            return false;
        }
        $this->settled = true;

        return true;
    }

    private function jetStreamOnly(): void
    {
        if (!$this->isJetStream()) {
            throw new LogicException('Only messages from a JetStream consumer are acknowledged');
        }
    }

    private function client(): NatsClient
    {
        return $this->client ?? throw new NatsException('This message is not attached to a connection');
    }
}
