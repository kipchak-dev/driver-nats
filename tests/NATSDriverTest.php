<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests;

use DI\Container;
use InvalidArgumentException;
use Kipchak\Driver\Config\Config;
use Kipchak\Driver\NATS\Exception\NatsConnectionException;
use Kipchak\Driver\NATS\Library\NatsClient;
use Kipchak\Driver\NATS\NATS;
use PHPUnit\Framework\TestCase;

final class NATSDriverTest extends TestCase
{
    private function primeDriver(array $config): Container
    {
        $container = new Container();
        Config::initialise($container);
        $container->set('drivers.config', ['kipchak.nats' => $config]);
        NATS::initialise($container);

        return $container;
    }

    public function testRegistersOneContainerEntryPerConnection(): void
    {
        $container = $this->primeDriver(['connections' => [
            'default' => ['servers' => 'nats://nats:4222'],
            'events' => ['servers' => ['nats://events-1:4222', 'nats://events-2:4222']],
        ]]);

        $this->assertTrue($container->has('drivers.messaging.nats.default'));
        $this->assertTrue($container->has('drivers.messaging.nats.events'));
        $this->assertInstanceOf(NatsClient::class, NATS::get());
        $this->assertCount(2, NATS::get('events')->config()->servers);
        $this->assertSame(NATS::get(), NATS::get('default'), 'One client per connection per worker');
    }

    public function testCreatingAClientDoesNotConnect(): void
    {
        $this->primeDriver(['connections' => ['default' => ['servers' => 'nats://127.0.0.1:1', 'timeout' => 0.2]]]);

        $client = NATS::get();
        $this->assertInstanceOf(NatsClient::class, $client);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('Could not connect to NATS (nats://127.0.0.1:1:');
        $client->publish('orders.created', 'x');
    }

    public function testPingIsFalseWhenNatsIsDown(): void
    {
        $this->primeDriver(['connections' => ['default' => ['servers' => 'nats://127.0.0.1:1', 'timeout' => 0.2]]]);

        $this->assertFalse(NATS::get()->ping());
    }

    public function testNoConnectionsRegistersNothing(): void
    {
        $container = $this->primeDriver([]);

        $this->assertFalse($container->has('drivers.messaging.nats.default'));
    }

    public function testBadConfigFailsWhenTheClientIsFirstUsed(): void
    {
        $this->primeDriver(['connections' => ['default' => ['servers' => 'http://nats']]]);

        $this->expectException(InvalidArgumentException::class);
        NATS::get();
    }

    public function testCustomConfigFile(): void
    {
        $container = new Container();
        Config::initialise($container);
        $container->set('drivers.config', ['nats.custom' => ['connections' => ['bus' => ['servers' => 'nats']]]]);
        NATS::initialise($container, 'nats.custom');

        $this->assertTrue($container->has('drivers.messaging.nats.bus'));
    }
}
