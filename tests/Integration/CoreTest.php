<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Exception\NatsException;
use Kipchak\Driver\NATS\Exception\NatsTimeoutException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;
use Kipchak\Driver\NATS\Message\Message;

final class CoreTest extends IntegrationCase
{
    public function testPublishAndSubscribe(): void
    {
        $nats = $this->client();
        $subscription = $nats->subscribe('greetings.*');

        $nats->publish('greetings.en', ['text' => 'hello'], ['X-Trace' => 'abc', 'X-Retries' => 2]);
        $nats->publish('greetings.ar', 'marhaba');
        $nats->publish('farewells.en', 'goodbye');

        $first = $subscription->next(1);
        $this->assertNotNull($first);
        $this->assertSame('greetings.en', $first->subject);
        $this->assertSame(['text' => 'hello'], $first->json());
        $this->assertSame(['X-Trace' => 'abc', 'X-Retries' => '2'], $first->headers);

        $second = $subscription->next(1);
        $this->assertSame('marhaba', $second?->body);
        $this->assertSame([], $second->headers);

        $this->assertNull($subscription->next(0.2), 'farewells.en does not match greetings.*');
    }

    public function testFetchReturnsWhatHasArrivedWithoutWaitingForTheRest(): void
    {
        $nats = $this->client();
        $subscription = $nats->subscribe('batch.>');
        for ($i = 1; $i <= 5; $i++) {
            $nats->publish('batch.item', (string) $i);
        }
        $nats->flush();

        $started = microtime(true);
        $messages = $subscription->fetch(100, 2);

        $this->assertSame(['1', '2', '3', '4', '5'], array_map(static fn(Message $m): string => $m->body, $messages));
        $this->assertLessThan(0.5, microtime(true) - $started);
    }

    public function testEmptyAndBinaryBodies(): void
    {
        $nats = $this->client();
        $subscription = $nats->subscribe('bodies');
        $binary = random_bytes(4096) . "\r\nMSG fake 1 2\r\n";

        $nats->publish('bodies', '');
        $nats->publish('bodies', $binary, ['X' => 'y']);

        $this->assertSame('', $subscription->next(1)?->body);
        $this->assertSame($binary, $subscription->next(1)?->body);
    }

    public function testHeadersCannotBeForgedThroughTheBody(): void
    {
        $nats = $this->client();
        $subscription = $nats->subscribe('forged');

        $nats->publish('forged', ['message' => ['hdrs' => base64_encode("X-Admin: true\r\n")]]);

        $this->assertSame([], $subscription->next(1)?->headers);
    }

    public function testQueueGroupsShareMessages(): void
    {
        $publisher = $this->client();
        $first = $this->client()->subscribe('jobs', 'workers');
        $second = $this->client()->subscribe('jobs', 'workers');
        $everyone = $this->client()->subscribe('jobs');
        // Give every SUB time to reach the server before publishing.
        usleep(100_000);

        for ($i = 0; $i < 40; $i++) {
            $publisher->publish('jobs', (string) $i);
        }
        $publisher->flush();

        $a = count($first->fetch(100, 1));
        $b = count($second->fetch(100, 1));
        $this->assertSame(40, $a + $b, 'Each message goes to one member of the group');
        $this->assertGreaterThan(0, $a);
        $this->assertGreaterThan(0, $b);
        $this->assertCount(40, $everyone->fetch(100, 1), 'A subscriber outside the group gets every message');
    }

    public function testRequestReply(): void
    {
        $this->responder('echo.upper');

        $reply = $this->client()->request('echo.upper', 'salaam', ['X-Trace' => 't-1'], 5);

        $this->assertSame('SALAAM', $reply->json()['answer']);
        $this->assertSame('t-1', $reply->json()['trace']);
    }

    public function testRequestsAreSpreadAcrossAQueueGroup(): void
    {
        $this->responder('svc.quote', 'pricing', 20);
        $this->responder('svc.quote', 'pricing', 20);
        $nats = $this->client();

        $answeredBy = [];
        for ($i = 0; $i < 20; $i++) {
            $answeredBy[$nats->request('svc.quote', 'q' . $i, timeout: 5)->json()['by']] = true;
        }

        $this->assertCount(2, $answeredBy);
    }

    public function testNoRespondersFailsAtOnce(): void
    {
        $started = microtime(true);
        try {
            $this->client()->request('nobody.listens', 'x', timeout: 5);
            $this->fail('Expected NoRespondersException');
        } catch (NoRespondersException $e) {
            $this->assertSame('nobody.listens', $e->subject);
        }
        $this->assertLessThan(1, microtime(true) - $started);
    }

    public function testRequestTimesOutWhenTheResponderIsSilent(): void
    {
        $silent = $this->client();
        $subscription = $silent->subscribe('silent.service');
        $silent->flush();
        $nats = $this->client();

        $started = microtime(true);
        try {
            $nats->request('silent.service', 'x', timeout: 0.5);
            $this->fail('Expected NatsTimeoutException');
        } catch (NatsTimeoutException $e) {
            $this->assertStringContainsString('silent.service', $e->getMessage());
        }
        $this->assertEqualsWithDelta(0.5, microtime(true) - $started, 0.3);

        // The connection is still usable, and a late answer to the timed-out request is not mistaken for this one.
        $this->responder('echo.after');
        $this->assertSame('LATE', $nats->request('echo.after', 'late', timeout: 5)->json()['answer']);
    }

    public function testMessagesLargerThanTheServerAcceptsAreRefusedBeforeSending(): void
    {
        $nats = $this->client();
        $limit = $nats->serverInfo()['max_payload'];

        try {
            $nats->publish('big', str_repeat('x', $limit + 1));
            $this->fail('Expected NatsException');
        } catch (NatsException $e) {
            $this->assertStringContainsString('max_payload ' . $limit, $e->getMessage());
        }

        // The connection was not closed by the server for a payload violation.
        $this->assertTrue($nats->ping());
    }

    public function testServerInfo(): void
    {
        $info = $this->client()->serverInfo();

        $this->assertTrue($info['jetstream']);
        $this->assertTrue($info['headers']);
        $this->assertMatchesRegularExpression('/^2\.\d+\.\d+/', $info['version']);
    }

    public function testEachAnswersRequestsAndStops(): void
    {
        $server = $this->client();
        $subscription = $server->subscribe('each.test');
        $server->flush();
        $client = $this->client();

        $client->publish('each.test', 'one');
        $client->publish('each.test', 'boom');
        $client->publish('each.test', 'stop');
        $client->publish('each.test', 'never');
        $client->flush();

        $seen = [];
        $handled = $subscription->each(function (Message $message, $subscription) use (&$seen): void {
            $seen[] = $message->body;
            if ($message->body === 'boom') {
                throw new \RuntimeException('handler failed');
            }
            if ($message->body === 'stop') {
                $subscription->stop();
            }
        }, seconds: 2);

        $this->assertSame(3, $handled);
        $this->assertSame(['one', 'boom', 'stop'], $seen);
        $this->assertContains('error NATS message handler failed', $this->logged);
    }

    public function testUnsubscribe(): void
    {
        $nats = $this->client();
        $subscription = $nats->subscribe('unsub.test');
        $subscription->unsubscribe();
        $subscription->unsubscribe();
        $nats->publish('unsub.test', 'x');

        $this->assertNull($subscription->next(0.2));
    }

    public function testASubscriptionOutOfScopeIsUnsubscribed(): void
    {
        $nats = $this->client();
        (function () use ($nats): void {
            $nats->subscribe('scoped.test');
        })();

        $this->assertSame([], $nats->basis()->getSubscriptions());
    }

    public function testAnAsyncErrorIsLoggedNotThrownAtTheNextOperation(): void
    {
        $nats = $this->client(['servers' => self::AUTH, 'username' => 'limited', 'password' => 'limited-password']);
        $subscription = $nats->subscribe('allowed.y');
        $nats->publish('forbidden.fire-and-forget', 'x');
        $nats->flush();

        $nats->publish('allowed.y', 'fine');
        $this->assertSame('fine', $subscription->next(1)?->body);
        $this->assertContains('warning NATS error', $this->logged);
    }
}
