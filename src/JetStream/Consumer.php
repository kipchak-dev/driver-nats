<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\JetStream;

use Basis\Nats\Queue;
use InvalidArgumentException;
use Kipchak\Driver\NATS\Exception\JetStreamException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\Library\NatsClient;
use Kipchak\Driver\NATS\Library\RawPayload;
use Kipchak\Driver\NATS\Message\Message;
use Throwable;

/**
 * A durable pull consumer: workers fetch messages in batches and acknowledge each one. Several workers can share
 * one consumer; each message goes to one of them, and comes back if it is not acknowledged within ack_wait.
 */
final class Consumer
{
    private bool $ensured = false;

    private bool $stopped = false;

    private ?Queue $inbox = null;

    private string $inboxSubject = '';

    private int $pull = 0;

    /**
     * @param array<string, mixed> $request The CONSUMER.CREATE request.
     */
    public function __construct(
        private readonly NatsClient $nats,
        private readonly JetStream $jetStream,
        public readonly string $stream,
        public readonly string $name,
        private readonly array $request,
    ) {
    }

    /**
     * Creates the consumer, or updates it to match its settings. Called by the first fetch; call it yourself to
     * create the consumer before any worker runs. Returns the consumer's info.
     *
     * @return array<string, mixed>
     */
    public function ensure(): array
    {
        $info = $this->jetStream->api('CONSUMER.CREATE.' . $this->stream . '.' . $this->name, $this->request);
        $this->ensured = true;

        return json_decode((string) json_encode($info), true) ?: [];
    }

    /**
     * Up to $batch messages, waiting up to $wait seconds (default: the connection's timeout) for them. Returns as
     * soon as $batch have arrived, or when the wait ends with fewer. With $wait 0, returns only what is waiting now.
     *
     * Acknowledge each message with ack(), nak() or term() before its ack_wait runs out.
     *
     * @return list<Message>
     */
    public function fetch(int $batch = 10, ?float $wait = null): array
    {
        $this->stopped = false;
        if ($batch < 1) {
            throw new InvalidArgumentException('A fetch needs a batch of at least 1');
        }
        $wait ??= $this->nats->config()->timeout;

        return iterator_to_array($this->messages($batch, $wait), false);
    }

    /**
     * The next message, waiting up to $wait seconds; null if none came.
     */
    public function next(?float $wait = null): ?Message
    {
        return $this->fetch(1, $wait)[0] ?? null;
    }

    /**
     * Fetches and handles messages until stop() is called, $limit messages have been handled, or $seconds have
     * passed. Returns how many messages were handled.
     *
     * A message is acknowledged when $handler returns, unless the handler settled it itself. When the handler
     * throws, the error is logged and the message is nak'd with a growing delay (1s, 2s, 4s ... up to 60s by
     * delivery count), so it is retried until the consumer's max_deliver, and the loop goes on.
     *
     * @param callable(Message, Consumer): void $handler
     */
    public function consume(callable $handler, ?int $limit = null, ?float $seconds = null, int $batch = 10): int
    {
        $this->stopped = false;
        $handled = 0;
        $deadline = $seconds === null ? null : microtime(true) + $seconds;

        while (!$this->stopped && ($limit === null || $handled < $limit)) {
            $wait = $deadline === null ? min(30.0, $this->nats->config()->pingInterval) : $deadline - microtime(true);
            if ($wait <= 0) {
                break;
            }

            // Each message is handled as it arrives, not when the pull ends.
            foreach ($this->messages($limit === null ? $batch : min($batch, $limit - $handled), $wait) as $message) {
                $handled++;
                try {
                    $handler($message, $this);
                    if (!$message->isSettled()) {
                        $message->ack();
                    }
                } catch (Throwable $e) {
                    $deliveries = $message->metadata()->deliveries ?? 1;
                    $this->nats->logger()?->error('JetStream message handler failed', [
                        'stream' => $this->stream,
                        'consumer' => $this->name,
                        'subject' => $message->subject,
                        'deliveries' => $deliveries,
                        'exception' => $e,
                    ]);
                    if (!$message->isSettled()) {
                        $message->nak(min(60.0, 2 ** max(0, $deliveries - 1)));
                    }
                }
                if ($this->stopped || ($limit !== null && $handled >= $limit)) {
                    break;
                }
            }
        }

        $this->handBack();

        return $handled;
    }

    /**
     * Ends consume() after the current message. Safe to call from a signal handler, to stop a worker on SIGTERM.
     */
    public function stop(): void
    {
        $this->stopped = true;
    }

    /**
     * @return array<string, mixed>|null
     */
    public function info(): ?array
    {
        return $this->jetStream->consumerInfo($this->stream, $this->name);
    }

    public function delete(): bool
    {
        $this->ensured = false;

        return $this->jetStream->deleteConsumer($this->stream, $this->name);
    }

    /**
     * Naks messages that arrived after consume() stopped, so they are delivered again at once rather than after
     * ack_wait.
     */
    private function handBack(): void
    {
        if ($this->inbox === null) {
            return;
        }
        $this->inbox->setTimeout(0);
        foreach ($this->inbox->fetchAll() as $msg) {
            if ($msg->replyTo !== null && str_starts_with($msg->replyTo, '$JS.ACK.')) {
                Message::fromMsg($msg, $this->nats)->nak();
            }
        }
    }

    /**
     * Messages from one pull, as they arrive, after creating the consumer again if it has gone.
     *
     * @return \Generator<int, Message>
     */
    private function messages(int $batch, float $wait): \Generator
    {
        if (!$this->ensured) {
            $this->ensure();
        }

        try {
            yield from $this->pull($batch, $wait);
        } catch (NoRespondersException) {
            // The consumer has gone (deleted, or removed after inactive_threshold): create it again, once.
            $this->ensure();
            yield from $this->pull($batch, $wait);
        }
    }

    /**
     * @return \Generator<int, Message>
     */
    private function pull(int $batch, float $wait): \Generator
    {
        $this->nats->drain();
        $inbox = $this->inbox();
        $replyTo = $this->inboxSubject . '.' . ++$this->pull;

        $request = ['batch' => $batch];
        if ($wait > 0) {
            $request['expires'] = (int) round($wait * 1_000_000_000);
        } else {
            $request['no_wait'] = true;
        }
        $this->nats->basis()->publish(
            $this->nats->config()->apiPrefix() . '.CONSUMER.MSG.NEXT.' . $this->stream . '.' . $this->name,
            new RawPayload((string) json_encode($request)),
            $replyTo,
        );

        // JetStream ends the pull itself when $wait runs out; the extra margin covers the round trip.
        $deadline = microtime(true) + max(0.0, $wait) + $this->nats->config()->timeout;
        $received = 0;
        $generation = $this->nats->generation();

        while ($received < $batch && !$this->stopped && ($remaining = $deadline - microtime(true)) > 0) {
            $inbox->setTimeout($remaining);
            $fetched = $inbox->fetchAll(1);
            if ($fetched === [] && $this->nats->generation() !== $generation) {
                // The pull request went with the old connection; consume() starts a new one.
                return;
            }
            foreach ($fetched as $msg) {
                if ($msg->replyTo !== null && str_starts_with($msg->replyTo, '$JS.ACK.')) {
                    $received++;
                    yield Message::fromMsg($msg, $this->nats);
                    continue;
                }

                $status = $msg->payload->headers['Status-Code'] ?? null;
                if ($status === null || $status === '100' || $msg->subject !== $replyTo) {
                    // A heartbeat, or the end of an earlier pull.
                    continue;
                }
                if ($status === '503') {
                    throw new NoRespondersException($this->stream . '.' . $this->name);
                }
                if ($status === '409') {
                    $reason = $msg->payload->headers['Status-Message'] ?? '';
                    if (stripos($reason, 'consumer deleted') !== false) {
                        $this->ensured = false;
                    } elseif (stripos($reason, 'exceeded max') !== false || stripos($reason, 'size exceeds') !== false) {
                        throw new JetStreamException('[JetStream] Pull from ' . $this->stream . '.' . $this->name . ' refused: ' . $reason, null, 409);
                    }
                }

                // 404 no messages, 408 the wait ended, 409 the pull was cut short.
                return;
            }
        }
    }

    private function inbox(): Queue
    {
        if ($this->inbox === null) {
            $this->inboxSubject = $this->nats->newInbox();
            $queue = $this->nats->basis()->subscribe($this->inboxSubject . '.*');
            assert($queue instanceof Queue);
            $this->inbox = $queue;
        }

        return $this->inbox;
    }
}
