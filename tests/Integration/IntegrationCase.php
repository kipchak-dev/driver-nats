<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Library\ConnectionConfig;
use Kipchak\Driver\NATS\Library\NatsClient;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

/**
 * Against NATS in Docker: start it with tests/Integration/start-services.sh. Skipped when it is not running.
 */
abstract class IntegrationCase extends TestCase
{
    protected const string PLAIN = 'nats://127.0.0.1:54222';
    protected const string AUTH = 'nats://127.0.0.1:54223';
    protected const string TLS = 'tls://localhost:54224';
    protected const string MTLS = 'tls://localhost:54225';
    protected const string CERTS = __DIR__ . '/.certs';
    protected const string COMPOSE = __DIR__ . '/compose.yaml';

    /** @var list<string> */
    protected array $logged = [];

    /** @var list<NatsClient> */
    private array $clients = [];

    /** @var list<resource> */
    private array $processes = [];

    /** @var list<array<int, resource>> Kept open so the responders can write. */
    private array $pipes = [];

    public static function setUpBeforeClass(): void
    {
        $socket = @fsockopen('127.0.0.1', 54222, $code, $message, 0.5);
        if ($socket === false) {
            self::markTestSkipped('NATS is not running; start it with tests/Integration/start-services.sh');
        }
        fclose($socket);
    }

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            try {
                $client->close();
            } catch (\Throwable) {
            }
        }
        foreach ($this->processes as $process) {
            proc_terminate($process);
            proc_close($process);
        }
        $this->clients = $this->processes = $this->pipes = [];
    }

    protected function client(array $overrides = []): NatsClient
    {
        $logger = new class ($this->logged) extends AbstractLogger {
            public function __construct(private array &$logged)
            {
            }

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->logged[] = $level . ' ' . $message;
            }
        };

        return $this->clients[] = new NatsClient(ConnectionConfig::fromArray('default', $overrides + ['servers' => self::PLAIN]), $logger);
    }

    /**
     * Starts responder.php in its own process and waits until it is subscribed.
     */
    protected function responder(string $subject, ?string $group = null, int $count = 1, float $seconds = 10, string $server = self::PLAIN): void
    {
        $command = [PHP_BINARY, __DIR__ . '/responder.php', $server, $subject, (string) $group, (string) $count, (string) $seconds];
        $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $this->assertIsResource($process);
        $this->processes[] = $process;

        stream_set_timeout($pipes[1], 10);
        if (fgets($pipes[1]) !== "ready\n") {
            proc_terminate($process);
            $this->fail('The responder did not start: ' . stream_get_contents($pipes[2]));
        }
        $this->pipes[] = $pipes;
    }

    protected static function compose(string ...$arguments): void
    {
        exec('docker compose -f ' . escapeshellarg(self::COMPOSE) . ' ' . implode(' ', array_map('escapeshellarg', $arguments)) . ' 2>&1', $output, $code);
        if ($code !== 0) {
            self::fail('docker compose ' . implode(' ', $arguments) . ' failed: ' . implode("\n", $output));
        }
    }

    protected static function waitForPort(int $port, float $seconds = 15): void
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            $socket = @fsockopen('127.0.0.1', $port, $code, $message, 0.2);
            if ($socket !== false) {
                fclose($socket);

                return;
            }
            usleep(100_000);
        }
        self::fail('Port ' . $port . ' did not open');
    }
}
