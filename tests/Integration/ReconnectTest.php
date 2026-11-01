<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Exception\NatsConnectionException;

/**
 * Restarts the plain NATS server, so these run last and take a few seconds each.
 */
final class ReconnectTest extends IntegrationCase
{
    private static function restartNats(): void
    {
        self::compose('restart', '--timeout', '0', 'nats');
        self::waitForPort(54222);
    }

    public function testReconnectsAndRestoresSubscriptionsAfterTheServerRestarts(): void
    {
        $listener = $this->client(['reconnect_attempts' => 30, 'reconnect_wait' => 0.2]);
        $subscription = $listener->subscribe('after.restart', 'workers');
        $listener->flush();

        self::restartNats();

        $publisher = $this->client();
        // The listener notices the closed socket when it next reads, reconnects and subscribes again.
        $this->assertNull($subscription->next(0.5));
        $publisher->publish('after.restart', 'back');
        $this->assertSame('back', $subscription->next(2)?->body);
        $this->assertContains('warning NATS connection lost; reconnecting', $this->logged);
    }

    public function testAnIdleWorkerSendsOnALiveConnectionAfterARestart(): void
    {
        $nats = $this->client(['reconnect_attempts' => 30, 'reconnect_wait' => 0.2]);
        $nats->flush();
        $listener = $this->client(['reconnect_attempts' => 30, 'reconnect_wait' => 0.2]);

        self::restartNats();
        $subscription = $listener->subscribe('idle.worker');
        $listener->flush();

        // The next request on the idle connection notices the server closed it, rather than writing into it.
        $nats->drain();
        $nats->publish('idle.worker', 'not lost');
        $this->assertSame('not lost', $subscription->next(2)?->body);
    }

    public function testGivesUpAfterReconnectAttempts(): void
    {
        $nats = $this->client(['reconnect_attempts' => 2, 'reconnect_wait' => 0.1, 'timeout' => 0.5]);
        $nats->flush();

        self::compose('stop', '--timeout', '0', 'nats');
        try {
            $started = microtime(true);
            $nats->drain();
            $nats->publish('x', 'y');
            $nats->flush();
            $this->fail('Expected NatsConnectionException');
        } catch (NatsConnectionException $e) {
            $this->assertStringContainsString('could not reconnect', $e->getMessage());
            $this->assertLessThan(3, microtime(true) - $started);
        } finally {
            self::compose('start', 'nats');
            self::waitForPort(54222);
        }

        // Once NATS is back, the next operation connects again.
        $this->assertTrue($nats->ping());
    }

    public function testARequestLostToAReconnectFailsAtOnce(): void
    {
        $silent = $this->client();
        $keep = $silent->subscribe('silent.restart');
        $silent->flush();
        $nats = $this->client(['reconnect_attempts' => 50, 'reconnect_wait' => 0.2]);
        $nats->flush();

        exec('(sleep 0.5; docker compose -f ' . escapeshellarg(self::COMPOSE) . ' restart --timeout 0 nats) > /dev/null 2>&1 &');
        $started = microtime(true);
        try {
            $nats->request('silent.restart', 'x', timeout: 15);
            $this->fail('Expected NatsConnectionException');
        } catch (NatsConnectionException $e) {
            $this->assertStringContainsString('replaced while waiting', $e->getMessage());
            $this->assertLessThan(10, microtime(true) - $started, 'Not left to run into the 15s timeout');
        } finally {
            self::waitForPort(54222);
        }
        $this->assertTrue($nats->ping());
    }
}
