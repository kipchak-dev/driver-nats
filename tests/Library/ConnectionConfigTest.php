<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Library;

use InvalidArgumentException;
use Kipchak\Driver\NATS\Library\ConnectionConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ConnectionConfigTest extends TestCase
{
    public function testDefaults(): void
    {
        $config = ConnectionConfig::fromArray('default', ['servers' => 'nats://nats:4222']);

        $this->assertCount(1, $config->servers);
        $this->assertSame('nats', $config->servers[0]->host);
        $this->assertSame(4222, $config->servers[0]->port);
        $this->assertFalse($config->servers[0]->tls);
        $this->assertSame('kipchak', $config->clientName);
        $this->assertSame(2.0, $config->timeout);
        $this->assertSame(3, $config->reconnectAttempts);
        $this->assertSame(0.1, $config->reconnectWait);
        $this->assertSame(30.0, $config->pingInterval);
        $this->assertTrue($config->tlsVerify);
        $this->assertSame('$JS.API', $config->apiPrefix());
        $this->assertSame([], $config->streams);
    }

    public static function servers(): iterable
    {
        yield 'nats scheme' => ['nats://nats.internal:4333', 'nats.internal', 4333, false, 'nats://nats.internal:4333'];
        yield 'tls scheme' => ['tls://nats:4222', 'nats', 4222, true, 'tls://nats:4222'];
        yield 'no scheme, default port' => ['nats', 'nats', 4222, false, 'nats://nats:4222'];
        yield 'host and port' => ['10.0.0.5:5222', '10.0.0.5', 5222, false, 'nats://10.0.0.5:5222'];
        yield 'IPv6' => ['nats://[::1]:4222', '::1', 4222, false, 'nats://[::1]:4222'];
        yield 'trailing slash' => ['nats://nats:4222/', 'nats', 4222, false, 'nats://nats:4222'];
    }

    #[DataProvider('servers')]
    public function testParsesServers(string $url, string $host, int $port, bool $tls, string $display): void
    {
        $server = ConnectionConfig::fromArray('default', ['servers' => [$url]])->servers[0];

        $this->assertSame($host, $server->host);
        $this->assertSame($port, $server->port);
        $this->assertSame($tls, $server->tls);
        $this->assertSame($display, (string) $server);
    }

    public function testIPv6AddressIsBracketedForTheSocket(): void
    {
        $this->assertSame('tcp://[::1]:4222', ConnectionConfig::fromArray('d', ['servers' => 'nats://[::1]:4222'])->servers[0]->address());
    }

    public static function badConfigs(): iterable
    {
        yield 'no servers' => [[], 'servers is required'];
        yield 'empty servers' => [['servers' => []], 'servers is required'];
        yield 'http scheme' => [['servers' => 'http://nats:4222'], 'each server must be a URL'];
        yield 'credentials in URL' => [['servers' => 'nats://user:pass@nats:4222'], 'with no credentials'];
        yield 'path' => [['servers' => 'nats://nats:4222/x'], 'each server must be a URL'];
        yield 'port out of range' => [['servers' => 'nats://nats:70000'], 'between 1 and 65535'];
        yield 'two auth methods' => [['servers' => 'nats', 'username' => 'a', 'token' => 'b'], 'choose one of'];
        yield 'missing creds file' => [['servers' => 'nats', 'credentials' => '/no/such/file.creds'], 'readable .creds file'];
        yield 'zero timeout' => [['servers' => 'nats', 'timeout' => 0], 'timeout must be more than 0'];
        yield 'negative reconnects' => [['servers' => 'nats', 'reconnect_attempts' => -1], 'reconnect_attempts must be 0 or more'];
        yield 'cert without key' => [['servers' => 'nats', 'tls' => ['cert' => '/a.pem']], 'both tls.cert and tls.key'];
        yield 'tls not an array' => [['servers' => 'nats', 'tls' => true], 'tls must be an array'];
        yield 'bad domain' => [['servers' => 'nats', 'jetstream_domain' => 'a.b'], 'jetstream_domain may contain only'];
        yield 'streams not keyed' => [['servers' => 'nats', 'streams' => [['subjects' => 'a']]], 'streams must map each name'];
    }

    #[DataProvider('badConfigs')]
    public function testRejectsBadConfig(array $config, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        ConnectionConfig::fromArray('orders', $config);
    }

    public function testErrorsNameTheConnection(): void
    {
        $this->expectExceptionMessage('NATS connection "orders": servers is required');

        ConnectionConfig::fromArray('orders', []);
    }

    public function testEmptyCredentialsCountAsUnset(): void
    {
        $config = ConnectionConfig::fromArray('d', ['servers' => 'nats', 'username' => '', 'token' => 'secret', 'nkey_seed' => '']);

        $this->assertNull($config->username);
        $this->assertSame('secret', $config->token);
        $this->assertNull($config->nkeySeed);
    }

    public function testJetStreamDomainPrefix(): void
    {
        $this->assertSame('$JS.hub.API', ConnectionConfig::fromArray('d', ['servers' => 'nats', 'jetstream_domain' => 'hub'])->apiPrefix());
    }

    public function testSecretsAreHiddenFromStackTraces(): void
    {
        $parameters = (new \ReflectionMethod(ConnectionConfig::class, '__construct'))->getParameters();
        $sensitive = array_map(
            static fn(\ReflectionParameter $p): string => $p->getName(),
            array_filter($parameters, static fn(\ReflectionParameter $p): bool => $p->getAttributes(\SensitiveParameter::class) !== []),
        );

        $this->assertSame(['password', 'token', 'nkeySeed'], array_values($sensitive));
    }
}
