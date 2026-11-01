<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Exception\JetStreamException;
use Kipchak\Driver\NATS\Exception\NatsException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\Exception\RevisionMismatchException;
use Kipchak\Driver\NATS\JetStream\JetStream;
use Kipchak\Driver\NATS\Message\Message;

final class JetStreamTest extends IntegrationCase
{
    private JetStream $js;

    protected function setUp(): void
    {
        $this->js = $this->client([
            'streams' => ['ORDERS' => ['subjects' => ['orders.>'], 'storage' => 'memory', 'duplicate_window' => 60]],
            'consumers' => [
                'mailer' => ['stream' => 'ORDERS', 'filter_subject' => 'orders.created', 'ack_wait' => 2, 'max_deliver' => 3],
            ],
        ])->jetStream();
        $this->js->deleteStream('ORDERS');
        $this->js->ensureStream('ORDERS');
    }

    public function testEnsureStreamCreatesUpdatesAndIsIdempotent(): void
    {
        $info = $this->js->streamInfo('ORDERS');
        $this->assertSame(['orders.>'], $info['config']['subjects']);
        $this->assertSame('memory', $info['config']['storage']);
        $this->assertSame(60_000_000_000, $info['config']['duplicate_window']);

        $this->js->ensureStream('ORDERS');
        $updated = $this->js->ensureStream('ORDERS', ['max_age' => 3600, 'description' => 'Orders']);

        $this->assertSame(3_600_000_000_000, $updated['config']['max_age']);
        $this->assertSame('Orders', $updated['config']['description']);
        $this->assertContains('ORDERS', $this->js->streamNames());
    }

    public function testMissingStreams(): void
    {
        $this->assertNull($this->js->streamInfo('NO_SUCH_STREAM'));
        $this->assertFalse($this->js->deleteStream('NO_SUCH_STREAM'));
        $this->assertNull($this->js->consumerInfo('NO_SUCH_STREAM', 'x'));
    }

    public function testImmutableSettingsCannotBeChanged(): void
    {
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('STREAM.UPDATE.ORDERS');

        $this->js->ensureStream('ORDERS', ['storage' => 'file']);
    }

    public function testPublishIsAcknowledged(): void
    {
        $first = $this->js->publish('orders.created', ['id' => 1]);
        $second = $this->js->publish('orders.paid', ['id' => 1]);

        $this->assertSame('ORDERS', $first->stream);
        $this->assertSame(1, $first->sequence);
        $this->assertSame(2, $second->sequence);
        $this->assertFalse($first->duplicate);
        $this->assertSame(2, $this->js->streamInfo('ORDERS')['state']['messages']);
    }

    public function testMessageIdsDeduplicate(): void
    {
        $first = $this->js->publish('orders.created', ['id' => 7], id: 'order-7');
        $again = $this->js->publish('orders.created', ['id' => 7], id: 'order-7');

        $this->assertFalse($first->duplicate);
        $this->assertTrue($again->duplicate);
        $this->assertSame($first->sequence, $again->sequence);
        $this->assertSame(1, $this->js->streamInfo('ORDERS')['state']['messages']);
    }

    public function testPublishWithoutAStream(): void
    {
        $this->expectException(NoRespondersException::class);
        $this->expectExceptionMessage('No JetStream stream captures "invoices.created"');

        $this->js->publish('invoices.created', 'x');
    }

    public function testExpectations(): void
    {
        $this->js->publish('orders.created', 'a');

        try {
            $this->js->publish('orders.created', 'b', expectedLastSequence: 5);
            $this->fail('Expected RevisionMismatchException');
        } catch (RevisionMismatchException $e) {
            $this->assertSame(JetStreamException::WRONG_LAST_SEQUENCE, $e->errorCode);
        }

        try {
            $this->js->publish('orders.created', 'b', expectedStream: 'INVOICES');
            $this->fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            $this->assertStringContainsString('expected stream does not match', $e->getMessage());
        }

        $this->assertSame(2, $this->js->publish('orders.created', 'b', expectedLastSequence: 1, expectedLastSubjectSequence: 1)->sequence);
    }

    public function testFetchAndAcknowledge(): void
    {
        $this->js->publish('orders.created', ['id' => 1], ['X-Source' => 'web']);
        $this->js->publish('orders.paid', ['id' => 1]);
        $this->js->publish('orders.created', ['id' => 2]);

        $messages = $this->js->consumer('mailer')->fetch(10, 0.5);

        $this->assertSame([['id' => 1], ['id' => 2]], array_map(static fn(Message $m): array => $m->json(), $messages), 'Only orders.created');
        $this->assertSame('web', $messages[0]->header('X-Source'));
        $metadata = $messages[0]->metadata();
        $this->assertSame('ORDERS', $metadata->stream);
        $this->assertSame('mailer', $metadata->consumer);
        $this->assertSame(1, $metadata->streamSequence);
        $this->assertSame(1, $metadata->deliveries);
        $this->assertSame(1, $metadata->pending);
        $this->assertEqualsWithDelta(time(), $metadata->timestamp->getTimestamp(), 5);

        foreach ($messages as $message) {
            $message->ack();
        }
        $this->assertSame([], $this->js->consumer('mailer')->fetch(10, 0));
        $this->assertSame(0, $this->js->consumerInfo('ORDERS', 'mailer')['num_ack_pending']);
    }

    public function testFetchReturnsAsSoonAsTheBatchIsFull(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->js->publish('orders.created', (string) $i);
        }

        $started = microtime(true);
        $messages = $this->js->consumer('mailer')->fetch(5, 5);

        $this->assertCount(5, $messages);
        $this->assertLessThan(1, microtime(true) - $started);
    }

    public function testFetchWaitsOnlyAsLongAsAsked(): void
    {
        $consumer = $this->js->consumer('mailer');
        $consumer->ensure();

        $started = microtime(true);
        $this->assertSame([], $consumer->fetch(1, 0), 'With no wait, an empty consumer answers at once');
        $this->assertLessThan(0.3, microtime(true) - $started);

        $started = microtime(true);
        $this->assertNull($consumer->next(0.5));
        $this->assertEqualsWithDelta(0.5, microtime(true) - $started, 0.3);
    }

    public function testNakRedeliversAfterTheDelay(): void
    {
        $this->js->publish('orders.created', 'retry me');
        $consumer = $this->js->consumer('mailer');

        $consumer->next(1)->nak(0.5);
        $this->assertNull($consumer->next(0.2), 'Not before the delay');

        $again = $consumer->next(2);
        $this->assertSame('retry me', $again?->body);
        $this->assertSame(2, $again->metadata()->deliveries);
        $again->ack(true);
    }

    public function testTermStopsRedelivery(): void
    {
        $this->js->publish('orders.created', 'poison');
        $consumer = $this->js->consumer('mailer');

        $consumer->next(1)->term('cannot parse');

        $this->assertNull($consumer->next(2.5), 'Not redelivered, even after ack_wait');
    }

    public function testUnacknowledgedMessagesComeBackAfterAckWait(): void
    {
        $this->js->publish('orders.created', 'slow');
        $consumer = $this->js->consumer('mailer');

        $first = $consumer->next(1);
        $first->inProgress();
        $this->assertNull($consumer->next(1.5), 'inProgress() reset the 2s ack_wait');

        $again = $consumer->next(3);
        $this->assertSame('slow', $again?->body);
        $this->assertSame(2, $again->metadata()->deliveries);
        $again->ack();
    }

    public function testSettlingTwiceDoesNothing(): void
    {
        $this->js->publish('orders.created', 'once');
        $message = $this->js->consumer('mailer')->next(1);

        $message->ack();
        $message->nak();
        $message->term();

        $this->assertTrue($message->isSettled());
        $this->assertNull($this->js->consumer('mailer')->next(2.5));
    }

    public function testConsumeAcknowledgesAndRetriesFailures(): void
    {
        foreach (['a', 'fail', 'b'] as $body) {
            $this->js->publish('orders.created', $body);
        }
        $consumer = $this->js->consumer('mailer');

        $seen = [];
        $handled = $consumer->consume(function (Message $message) use (&$seen): void {
            $seen[] = $message->body . '#' . $message->metadata()->deliveries;
            if ($message->body === 'fail') {
                throw new \RuntimeException('temporary failure');
            }
        }, seconds: 3);

        // "fail" is nak'd with a 1s delay, then 2s: delivered at 0s, 1s and 3s (max_deliver 3).
        $this->assertSame(['a#1', 'fail#1', 'b#1', 'fail#2'], array_slice($seen, 0, 4));
        $this->assertGreaterThanOrEqual(4, $handled);
        $this->assertContains('error JetStream message handler failed', $this->logged);
        $this->assertSame(0, $this->js->consumerInfo('ORDERS', 'mailer')['num_pending']);
    }

    public function testConsumeHandlesEachMessageAsItArrives(): void
    {
        $this->js->publish('orders.created', 'first');
        $consumer = $this->js->consumer('mailer');

        $started = microtime(true);
        $at = null;
        $consumer->consume(function () use (&$at, $started, $consumer): void {
            $at = microtime(true) - $started;
            $consumer->stop();
        }, batch: 10, seconds: 10);

        $this->assertLessThan(0.5, $at, 'Not held back until the batch of 10 filled');
        $this->assertLessThan(1, microtime(true) - $started);
    }

    public function testConsumeStopsAtTheLimit(): void
    {
        for ($i = 0; $i < 5; $i++) {
            $this->js->publish('orders.created', (string) $i);
        }
        $consumer = $this->js->consumer('mailer');

        $this->assertSame(2, $consumer->consume(static function (): void {
        }, limit: 2));
        $this->assertCount(3, $consumer->fetch(10, 0.5), 'The rest stay for the next worker');
    }

    public function testHandlersCanSettleMessagesThemselves(): void
    {
        $this->js->publish('orders.created', 'bad');
        $consumer = $this->js->consumer('mailer');

        $consumer->consume(static function (Message $message): void {
            $message->term('invalid');
        }, limit: 1);

        $this->assertNull($consumer->next(2.5));
    }

    public function testConsumersAreCreatedAgainWhenDeleted(): void
    {
        $consumer = $this->js->consumer('mailer');
        $consumer->ensure();
        $this->assertTrue($consumer->delete());
        $this->js->publish('orders.created', 'after delete');

        $this->assertSame('after delete', $this->js->consumer('mailer')->next(1)?->body);
        $this->assertNotNull($this->js->consumerInfo('ORDERS', 'mailer'));
    }

    public function testAdHocConsumersAndWorkQueues(): void
    {
        $this->js->ensureStream('JOBS', ['subjects' => ['jobs.*'], 'retention' => 'workqueue', 'storage' => 'memory']);
        try {
            $this->js->publish('jobs.resize', 'img-1');
            $this->js->publish('jobs.resize', 'img-2');

            $workerA = $this->js->consumer('resizer', 'JOBS', ['ack_wait' => 5]);
            $workerB = $this->client()->jetStream()->consumer('resizer', 'JOBS', ['ack_wait' => 5]);

            $a = $workerA->next(1);
            $b = $workerB->next(1);
            $this->assertEqualsCanonicalizing(['img-1', 'img-2'], [$a?->body, $b?->body], 'Workers share one consumer');
            $a->ack(true);
            $b->ack(true);

            $this->assertSame(0, $this->js->streamInfo('JOBS')['state']['messages'], 'A work queue removes acknowledged messages');
        } finally {
            $this->js->deleteStream('JOBS');
        }
    }

    public function testConsumerNeedsAStream(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Consumer "orphan" has no stream');

        $this->js->consumer('orphan');
    }

    public function testPurge(): void
    {
        $this->js->publish('orders.created', 'a');
        $this->js->publish('orders.paid', 'b');

        $this->assertSame(1, $this->js->purgeStream('ORDERS', 'orders.paid'));
        $this->assertSame(1, $this->js->purgeStream('ORDERS'));
    }

    public function testAccountInfoAndRawApi(): void
    {
        $this->assertArrayHasKey('streams', $this->js->accountInfo());
        $this->assertSame('ORDERS', $this->js->api('STREAM.INFO.ORDERS')->config->name);
    }

    public function testJetStreamDisabled(): void
    {
        $js = $this->client(['servers' => self::AUTH, 'username' => 'kipchak', 'password' => 'kipchak-password', 'jetstream_domain' => 'nowhere'])->jetStream();

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('JetStream is not enabled');
        $js->accountInfo();
    }

    public function testASignalStopsConsumeWithoutReconnecting(): void
    {
        if (!function_exists('pcntl_alarm')) {
            $this->markTestSkipped('Needs ext-pcntl');
        }
        $consumer = $this->js->consumer('mailer');
        pcntl_async_signals(true);
        pcntl_signal(SIGALRM, static fn() => $consumer->stop());
        pcntl_alarm(1);

        $started = microtime(true);
        try {
            $consumer->consume(static function (): void {
            }, seconds: 10);
        } finally {
            pcntl_signal(SIGALRM, SIG_DFL);
        }

        $this->assertLessThan(2, microtime(true) - $started);
        $this->assertNotContains('warning NATS connection lost; reconnecting', $this->logged);
    }
}
