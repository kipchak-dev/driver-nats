<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use Basis\Nats\Queue;
use Kipchak\Driver\NATS\Message\Message;
use Throwable;

/**
 * A core NATS subscription. Messages are buffered on the connection until read with next(), fetch() or each().
 *
 * Subscriptions belong in long-running workers (a CLI command or a FrankenPHP worker loop). One that goes out of
 * scope is unsubscribed, so a subscription made during a request does not keep collecting messages in the worker.
 */
final class Subscription
{
    private bool $stopped = false;

    private bool $active = true;

    public function __construct(
        private readonly NatsClient $nats,
        private readonly Queue $queue,
        public readonly string $subject,
        public readonly ?string $queueGroup = null,
    ) {
    }

    /**
     * The next message, waiting up to $timeout seconds (default: the connection's timeout); null if none came.
     */
    public function next(?float $timeout = null): ?Message
    {
        $messages = $this->fetch(1, $timeout);

        return $messages[0] ?? null;
    }

    /**
     * Up to $max messages: waits up to $timeout seconds (default: the connection's timeout) for the first, then
     * takes whatever else has already arrived without waiting further.
     *
     * @return list<Message>
     */
    public function fetch(int $max = 100, ?float $timeout = null): array
    {
        $this->stopped = false;

        return $this->receive($max, $timeout);
    }

    /**
     * @return list<Message>
     */
    private function receive(int $max, ?float $timeout): array
    {
        $deadline = microtime(true) + ($timeout ?? $this->nats->config()->timeout);
        $received = [];

        while ($received === [] && !$this->stopped && ($remaining = $deadline - microtime(true)) > 0) {
            $this->queue->setTimeout($remaining);
            $received = $this->queue->fetchAll(1);
        }
        if ($received !== [] && $max > 1) {
            $this->queue->setTimeout(0);
            $received = [...$received, ...$this->queue->fetchAll($max - 1)];
        }

        return array_map(fn($msg): Message => Message::fromMsg($msg, $this->nats), $received);
    }

    /**
     * Calls $handler with each message until stop() is called, $limit messages have been handled, or $seconds
     * have passed. A request's return value, when not null, is sent back as the answer. Returns how many messages
     * were handled.
     *
     * An exception from the handler is logged and the loop goes on, so one bad message does not stop a worker.
     *
     * @param callable(Message, Subscription): mixed $handler
     */
    public function each(callable $handler, ?int $limit = null, ?float $seconds = null): int
    {
        $this->stopped = false;
        $handled = 0;
        $deadline = $seconds === null ? null : microtime(true) + $seconds;

        while (!$this->stopped && ($limit === null || $handled < $limit)) {
            $wait = $deadline === null ? $this->nats->config()->pingInterval : $deadline - microtime(true);
            if ($wait <= 0) {
                break;
            }
            foreach ($this->receive($limit === null ? 100 : min(100, $limit - $handled), $wait) as $message) {
                $handled++;
                try {
                    $answer = $handler($message, $this);
                    if ($answer !== null && $message->replyTo !== null) {
                        $message->respond($answer);
                    }
                } catch (Throwable $e) {
                    $this->nats->logger()?->error('NATS message handler failed', [
                        'subject' => $message->subject,
                        'exception' => $e,
                    ]);
                }
                if ($this->stopped) {
                    break;
                }
            }
        }

        return $handled;
    }

    /**
     * Ends each() after the current message. Safe to call from a signal handler, to stop a worker on SIGTERM.
     */
    public function stop(): void
    {
        $this->stopped = true;
    }

    /**
     * Stops the subscription. basis-company/nats unsubscribes by subject, so this also ends any other subscription
     * to the same subject on this connection.
     */
    public function unsubscribe(): void
    {
        if ($this->active) {
            $this->active = false;
            $this->nats->basis()->unsubscribe($this->queue);
        }
    }

    public function __destruct()
    {
        try {
            $this->unsubscribe();
        } catch (Throwable) {
            // The connection is gone, and the subscription with it.
        }
    }
}
