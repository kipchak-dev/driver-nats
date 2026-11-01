<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS;

use Kipchak\Core\Interfaces;
use Kipchak\Driver\Config\Config;
use Kipchak\Driver\NATS\Library\ConnectionConfig;
use Kipchak\Driver\NATS\Library\NatsClient;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * NATS connections from config/kipchak.nats.php, each a NatsClient in the container under
 * drivers.messaging.nats.<name>. A connection opens on first use and stays open for the life of the worker;
 * warnings (lost connections, retried publishes, failed handlers) go to the Logger driver.
 */
class NATS implements Interfaces\Driver
{
    protected static ?ContainerInterface $container = null;

    public static function initialise(ContainerInterface $container, ?string $configFileName = null): void
    {
        self::$container = $container;
        self::set($configFileName);
    }

    public static function set(?string $configFileName = null): void
    {
        $config = Config::get($configFileName ?? 'kipchak.nats');

        if (empty($config['connections'])) {
            return;
        }

        foreach ($config['connections'] as $connectionName => $connectionDetails) {
            self::$container->set(
                'drivers.messaging.nats.' . $connectionName,
                function (ContainerInterface $c) use ($connectionName, $connectionDetails): NatsClient {
                    return new NatsClient(ConnectionConfig::fromArray((string) $connectionName, (array) $connectionDetails), self::logger());
                }
            );
        }
    }

    public static function get(?string $configKey = null): NatsClient
    {
        return self::$container->get('drivers.messaging.nats.' . ($configKey ?? 'default'));
    }

    private static function logger(): ?LoggerInterface
    {
        if (self::$container->has('drivers.logger')) {
            $logger = self::$container->get('drivers.logger');
            if ($logger instanceof LoggerInterface) {
                return $logger;
            }
        }

        return null;
    }
}
