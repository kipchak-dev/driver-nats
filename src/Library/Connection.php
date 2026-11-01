<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use Basis\Nats\Client;
use Basis\Nats\Connection as BasisConnection;
use Basis\Nats\Message\Connect;
use Basis\Nats\Message\Info;
use Basis\Nats\Message\Msg;
use Basis\Nats\Message\Ping;
use Basis\Nats\Message\Pong;
use Basis\Nats\Message\Prototype;
use Basis\Nats\Message\Subscribe;
use Basis\Nats\Message\Unsubscribe;
use Basis\Nats\NKeys\Authenticator;
use Basis\Nats\NKeys\CredentialsParser;
use Basis\Nats\NKeys\SecretKey;
use Kipchak\Driver\NATS\Exception\NatsConnectionException;
use Kipchak\Driver\NATS\Exception\NatsException;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * The socket under basis-company/nats's Client, replacing the library's own Connection for a long-lived worker.
 *
 * - TLS verifies the server's certificate and host name, against tls.ca or the system's trusted CAs.
 * - A lost connection is re-established at most reconnect_attempts times, across every server in turn, so a request
 *   fails rather than hangs while NATS is down. Subscriptions, queue groups included, are restored afterwards.
 * - CONNECT asks for no_responders, so a request nobody answers fails at once, and is confirmed with a PING, so bad
 *   credentials fail on connecting rather than on a later read.
 * - INFO is read as JSON, so fields added by newer servers do not break it.
 * - The server's PINGs are answered whenever the socket is read, and a quiet connection is pinged every
 *   ping_interval seconds while waiting, so a dead one is noticed.
 */
final class Connection extends BasisConnection
{
    /** @var resource|null */
    private $socket = null;

    private int $serverIndex = 0;

    /** Counts successful connections, so a caller can tell the connection was replaced while it waited. */
    private int $generation = 0;

    private ?Server $server = null;

    private ?object $info = null;

    private float $lastReadAt = 0.0;

    private ?float $pingSentAt = null;

    /** @var array<string, array{subject: string, group: ?string}> */
    private array $subscriptions = [];

    /** @var list<Prototype> Messages read while waiting for a PONG, returned by the next getMessage(). */
    private array $pending = [];

    /** @var array<string, string> Recent permissions violations by subject, for the request waiting on one. */
    private array $violations = [];

    public function __construct(
        private readonly Client $natsClient,
        private readonly ConnectionConfig $config,
        ?LoggerInterface $logger = null,
    ) {
        $this->logger = $logger;
    }

    /**
     * The server this connection is on, or null before the first operation.
     */
    public function server(): ?Server
    {
        return $this->server;
    }

    /**
     * The largest payload, headers included, that the server accepts.
     */
    public function maxPayload(): int
    {
        $this->init();

        return (int) ($this->info->max_payload ?? 1048576);
    }

    /**
     * The server's INFO, decoded.
     */
    public function serverInfo(): object
    {
        $this->init();

        return $this->info ?? (object) [];
    }

    /**
     * Changes each time the connection is (re-)established. Anything sent before a change was lost with the old
     * connection.
     */
    public function generation(): int
    {
        return $this->generation;
    }

    /**
     * The permissions violation the server reported for $subject since the last call, if any.
     */
    public function takeViolation(string $subject): ?string
    {
        $violation = $this->violations[$subject] ?? null;
        unset($this->violations[$subject]);

        return $violation;
    }

    public function isConnected(): bool
    {
        return is_resource($this->socket);
    }

    public function getInfoMessage(): Info
    {
        $this->init();

        return new Info(array_intersect_key((array) $this->info, get_class_vars(Info::class)));
    }

    public function getConnectMessage(): Connect
    {
        return new Connect(['name' => $this->config->clientName, 'lang' => 'php', 'headers' => true]);
    }

    public function getMessage(null|int|float $timeout = 0): ?Prototype
    {
        if ($this->pending !== []) {
            return array_shift($this->pending);
        }

        $this->init();
        $timeout = $timeout === null ? $this->config->timeout : (float) $timeout;
        $deadline = microtime(true) + $timeout;
        $generation = $this->generation;

        while (true) {
            if ($this->generation !== $generation) {
                // Reconnected: return so the caller can re-send what was lost with the old connection.
                return null;
            }

            $remaining = max(0.0, $deadline - microtime(true));
            $read = [$this->socket];
            $write = $except = null;
            $seconds = (int) floor($remaining);
            $ready = @stream_select($read, $write, $except, $seconds, (int) (($remaining - $seconds) * 1_000_000));

            if ($ready === false) {
                if (!is_resource($this->socket) || feof($this->socket)) {
                    $this->reconnect('waiting for data failed');
                    continue;
                }

                // Interrupted by a signal: return, so a loop can see whether it was asked to stop.
                return null;
            }

            if ($ready === 0) {
                $this->keepAlive();
                if (microtime(true) >= $deadline) {
                    return null;
                }
                continue;
            }

            $message = $this->readMessage();
            if ($message !== null) {
                return $message;
            }
        }
    }

    public function ping(): bool
    {
        // Messages that arrive before the PONG are kept for the next getMessage(), in order.
        $held = $this->pending;
        $this->pending = [];
        $this->sendMessage(new Ping([]));
        $generation = $this->generation;
        $deadline = microtime(true) + $this->config->timeout;
        $answered = false;

        try {
            while (($remaining = $deadline - microtime(true)) > 0) {
                if ($this->generation !== $generation) {
                    // The PING went with the old connection: ask the new one.
                    $this->sendMessage(new Ping([]));
                    $generation = $this->generation;
                }
                $message = $this->getMessage($remaining);
                if ($message instanceof Pong) {
                    $answered = true;
                    break;
                }
                if ($message !== null) {
                    $held[] = $message;
                }
            }
        } finally {
            $this->pending = $held;
        }

        return $answered;
    }

    public function sendMessage(Prototype $message): void
    {
        if ($message instanceof Unsubscribe) {
            unset($this->subscriptions[$message->sid]);
            if (!is_resource($this->socket)) {
                // Nothing to tell a server we are not connected to.
                return;
            }
        }

        $this->init();

        $line = $message->render() . "\r\n";
        try {
            $this->write($line);
        } catch (NatsConnectionException $e) {
            $this->reconnect($e->getMessage());
            $this->write($line);
        }

        if ($message instanceof Subscribe) {
            $this->subscriptions[$message->sid] = ['subject' => $message->subject, 'group' => $message->group];
        }
        if ($message instanceof Ping) {
            $this->pingSentAt = microtime(true);
        }
    }

    public function setLogger(?LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    public function setTimeout(float $value): void
    {
        if (is_resource($this->socket)) {
            $seconds = (int) floor($value);
            stream_set_timeout($this->socket, $seconds, (int) (($value - $seconds) * 1_000_000));
        }
    }

    public function setPacketSize(int $size): void
    {
    }

    public function close(): void
    {
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
        $this->socket = null;
        $this->server = null;
        $this->pending = [];
    }

    protected function init(): void
    {
        if (is_resource($this->socket)) {
            return;
        }

        $this->connect();
    }

    protected function enableTls(bool $requireClientCert): void
    {
        $method = STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT;
        $error = null;
        set_error_handler(static function (int $code, string $message) use (&$error): bool {
            $error = $message;

            return true;
        });
        try {
            $enabled = stream_socket_enable_crypto($this->socket, true, $method);
        } finally {
            restore_error_handler();
        }

        if ($enabled !== true) {
            throw new NatsConnectionException('TLS handshake failed' . ($error !== null ? ': ' . self::sslError($error) : ''));
        }
    }

    protected function getPayload(int $length): string
    {
        $data = '';
        $wanted = $length + 2;
        while (strlen($data) < $wanted) {
            $chunk = fread($this->socket, $wanted - strlen($data));
            if ($chunk === false || ($chunk === '' && (feof($this->socket) || stream_get_meta_data($this->socket)['timed_out']))) {
                throw new NatsConnectionException('the connection closed while reading a message');
            }
            $data .= $chunk;
        }

        return substr($data, 0, $length);
    }

    /**
     * Connects to the first server that answers, starting with the current one.
     */
    private function connect(): void
    {
        $servers = $this->config->servers;
        $failures = [];

        foreach (array_keys($servers) as $offset) {
            $index = ($this->serverIndex + $offset) % count($servers);
            try {
                $this->open($servers[$index]);
                $this->serverIndex = $index;

                return;
            } catch (Throwable $e) {
                $this->close();
                $failures[] = $servers[$index] . ': ' . $e->getMessage();
            }
        }

        throw new NatsConnectionException('Could not connect to NATS (' . implode('; ', $failures) . ')');
    }

    private function open(Server $server): void
    {
        $context = stream_context_create(['ssl' => $this->tlsOptions($server)]);
        $socket = @stream_socket_client($server->address(), $errorCode, $errorMessage, $this->config->timeout, STREAM_CLIENT_CONNECT, $context);
        if ($socket === false) {
            throw new NatsConnectionException($errorMessage !== '' ? $errorMessage : 'connection failed (' . $errorCode . ')');
        }
        $this->socket = $socket;
        $this->setTimeout($this->config->timeout);

        if ($this->config->tlsHandshakeFirst) {
            $this->enableTls(true);
        }

        $line = $this->readLine();
        if ($line === null || !str_starts_with($line, 'INFO ')) {
            throw new NatsConnectionException('the server did not introduce itself with INFO');
        }
        $info = json_decode(substr($line, 5));
        if (!is_object($info)) {
            throw new NatsConnectionException('the server sent an INFO that is not JSON');
        }

        $wantsTls = $server->tls || $this->config->tlsCert !== null || $this->config->tlsCa !== null;
        if (!$this->config->tlsHandshakeFirst && (($info->tls_required ?? false) || $wantsTls)) {
            if (!($info->tls_required ?? false) && !($info->tls_available ?? false)) {
                throw new NatsConnectionException('TLS is configured but the server does not offer it');
            }
            $this->enableTls(true);
        }

        $this->write('CONNECT ' . json_encode($this->connectOptions($server, $info), JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\r\nPING\r\n");

        while (true) {
            $line = $this->readLine();
            if ($line === null) {
                throw new NatsConnectionException('the server closed the connection after CONNECT');
            }
            if ($line === 'PONG') {
                break;
            }
            if (str_starts_with($line, '-ERR')) {
                throw new NatsConnectionException('the server refused the connection: ' . trim(substr($line, 4), " '"));
            }
            if ($line === 'PING') {
                $this->write("PONG\r\n");
            }
        }

        $this->info = $info;
        $this->server = $server;
        $this->generation++;
        $this->lastReadAt = microtime(true);
        $this->pingSentAt = null;

        foreach ($this->subscriptions as $sid => $subscription) {
            $this->write((new Subscribe(['sid' => (string) $sid, 'subject' => $subscription['subject'], 'group' => $subscription['group']]))->render() . "\r\n");
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function connectOptions(Server $server, object $info): array
    {
        $options = [
            'verbose' => false,
            'pedantic' => false,
            'tls_required' => $server->tls,
            'name' => $this->config->clientName,
            'lang' => 'php',
            'version' => '1.0',
            'protocol' => 1,
            'echo' => true,
            'headers' => true,
            'no_responders' => true,
        ];

        if ($this->config->username !== null) {
            $options['user'] = $this->config->username;
            $options['pass'] = (string) $this->config->password;
        } elseif ($this->config->token !== null) {
            $options['auth_token'] = $this->config->token;
        } elseif ($this->config->nkeySeed !== null || $this->config->credentials !== null) {
            $seed = $this->config->nkeySeed;
            if ($this->config->credentials !== null) {
                $credentials = CredentialsParser::fromFile($this->config->credentials);
                if (!isset($credentials['jwt'], $credentials['nkey'])) {
                    throw new NatsConnectionException('the credentials file has no user JWT and NKey seed');
                }
                $options['jwt'] = $credentials['jwt'];
                $seed = $credentials['nkey'];
            }
            if (!isset($info->nonce)) {
                throw new NatsConnectionException('the server sent no nonce to sign for NKey authentication');
            }
            if (!function_exists('sodium_crypto_sign_detached')) {
                throw new NatsConnectionException('NKey authentication needs ext-sodium or paragonie/sodium_compat');
            }
            $signer = new Authenticator(SecretKey::fromSeed((string) $seed));
            $options['nkey'] = $signer->getPublicKey();
            $options['sig'] = $signer->sign((string) $info->nonce);
        }

        return $options;
    }

    /**
     * @return array<string, mixed>
     */
    private function tlsOptions(Server $server): array
    {
        $options = [
            'peer_name' => $server->host,
            'verify_peer' => $this->config->tlsVerify,
            'verify_peer_name' => $this->config->tlsVerify,
            'allow_self_signed' => false,
            'SNI_enabled' => true,
        ];
        if ($this->config->tlsCa !== null) {
            $options['cafile'] = $this->config->tlsCa;
        }
        if ($this->config->tlsCert !== null) {
            $options['local_cert'] = $this->config->tlsCert;
            $options['local_pk'] = (string) $this->config->tlsKey;
        }

        return $options;
    }

    /**
     * Reads one protocol line and handles it; returns the message for the Client, or null for one handled here.
     */
    private function readMessage(): ?Prototype
    {
        try {
            $line = $this->readLine();
        } catch (NatsConnectionException $e) {
            $this->reconnect($e->getMessage());

            return null;
        }
        if ($line === null) {
            $this->reconnect('the server closed the connection');

            return null;
        }
        if ($line === '') {
            return null;
        }

        $this->lastReadAt = microtime(true);
        $this->pingSentAt = null;

        if ($line === 'PING') {
            $this->write("PONG\r\n");

            return null;
        }
        if ($line === 'PONG') {
            return new Pong([]);
        }
        if ($line === '+OK') {
            return null;
        }
        if (str_starts_with($line, 'INFO ')) {
            $info = json_decode(substr($line, 5));
            if (is_object($info)) {
                $this->info = (object) array_merge((array) $this->info, (array) $info);
            }

            return null;
        }
        if (str_starts_with($line, '-ERR')) {
            $error = trim(substr($line, 4), " '");
            if (stripos($error, 'stale connection') !== false) {
                $this->reconnect('the server closed a stale connection');

                return null;
            }
            $this->logger?->warning('NATS error', ['connection' => $this->config->name, 'error' => $error]);
            if (preg_match('/^Permissions Violation for \w+ to "?([^"\s]+)/i', $error, $match)) {
                // The connection stays open, and the error belongs to an earlier publish or subscribe: kept for a
                // request waiting on that subject rather than thrown at whatever reads next.
                $this->violations[$match[1]] = $error;
                if (count($this->violations) > 100) {
                    array_shift($this->violations);
                }

                return null;
            }
            $this->close();
            throw new NatsException('[NATS] ' . $error);
        }
        if (str_starts_with($line, 'MSG ') || str_starts_with($line, 'HMSG ')) {
            try {
                return $this->parseMessage($line);
            } catch (NatsConnectionException $e) {
                $this->reconnect($e->getMessage());

                return null;
            }
        }

        throw new NatsException('[NATS] Unexpected protocol line from the server: ' . substr($line, 0, 80));
    }

    private function parseMessage(string $line): Msg
    {
        $withHeaders = str_starts_with($line, 'HMSG ');
        $parts = preg_split('/\s+/', trim($line));
        array_shift($parts);
        $count = count($parts);
        $expected = $withHeaders ? [4, 5] : [3, 4];
        if (!in_array($count, $expected, true)) {
            throw new NatsException('[NATS] Malformed message line: ' . substr($line, 0, 80));
        }

        $replyTo = $count === $expected[1] ? $parts[2] : null;
        $length = (int) $parts[$count - 1];
        $headerLength = $withHeaders ? (int) $parts[$count - 2] : 0;

        $data = $this->getPayload($length);
        $headers = $headerLength > 0 ? self::parseHeaders(substr($data, 0, $headerLength)) : [];

        $message = new Msg([
            'subject' => $parts[0],
            'sid' => $parts[1],
            'replyTo' => $replyTo,
            'length' => $length,
            'hlength' => $withHeaders ? $headerLength : null,
        ]);
        $message->payload = new RawPayload(substr($data, $headerLength), $headers, $parts[0]);
        $message->setClient($this->natsClient);

        return $message;
    }

    /**
     * Parses a "NATS/1.0 [status [description]]" header block. A status becomes the Status-Code and Status-Message
     * headers, as basis-company/nats does; repeated headers are joined with ", ".
     *
     * @return array<string, string>
     */
    public static function parseHeaders(string $block): array
    {
        $lines = explode("\r\n", $block);
        $first = array_shift($lines);
        $headers = [];

        if (preg_match('~^NATS/\d+\.\d+\s+(\d{3})\s*(.*)$~', (string) $first, $status)) {
            $headers['Status-Code'] = $status[1];
            if ($status[2] !== '') {
                $headers['Status-Message'] = trim($status[2]);
            }
        }

        foreach ($lines as $row) {
            $colon = strpos($row, ':');
            if ($colon === false || $colon === 0) {
                continue;
            }
            $name = substr($row, 0, $colon);
            $value = trim(substr($row, $colon + 1));
            $headers[$name] = isset($headers[$name]) ? $headers[$name] . ', ' . $value : $value;
        }

        return $headers;
    }

    /**
     * Reads one CRLF-terminated line, without the CRLF; null when the server has closed the connection.
     */
    private function readLine(): ?string
    {
        $error = null;
        set_error_handler(static function (int $code, string $message) use (&$error): bool {
            $error = $message;

            return true;
        });
        try {
            $line = fgets($this->socket);
        } finally {
            restore_error_handler();
        }

        if ($line === false && $error !== null) {
            throw new NatsConnectionException(self::sslError($error));
        }
        if ($line === false) {
            if (feof($this->socket)) {
                return null;
            }
            if (stream_get_meta_data($this->socket)['timed_out']) {
                throw new NatsConnectionException('the server did not answer within ' . $this->config->timeout . 's');
            }

            return null;
        }

        return rtrim($line, "\r\n");
    }

    /**
     * "fgets(): SSL operation failed with code 1. OpenSSL Error messages: error:0A000086:SSL routines::certificate
     * verify failed" becomes "certificate verify failed (error:0A000086)".
     */
    private static function sslError(string $warning): string
    {
        $warning = (string) preg_replace('/^\w+\(\): /', '', $warning);
        if (preg_match_all('/error:([0-9A-F]+):[^:]*::?([^\n]+)/', $warning, $errors, PREG_SET_ORDER)) {
            return implode('; ', array_map(static fn(array $e): string => trim($e[2]) . ' (error:' . $e[1] . ')', $errors));
        }

        return trim((string) preg_replace('/\s+/', ' ', $warning));
    }

    private function write(string $data): void
    {
        $length = strlen($data);
        $written = 0;
        while ($written < $length) {
            $result = @fwrite($this->socket, substr($data, $written));
            if ($result === false || $result === 0) {
                throw new NatsConnectionException('the connection closed while sending');
            }
            $written += $result;
        }
    }

    /**
     * Pings a connection that has been quiet for ping_interval, and reconnects when the ping goes unanswered for
     * as long again.
     */
    private function keepAlive(): void
    {
        $now = microtime(true);
        if ($this->pingSentAt !== null) {
            if ($now - $this->pingSentAt > $this->config->pingInterval) {
                $this->reconnect('the server did not answer a ping');
            }

            return;
        }
        if ($now - $this->lastReadAt > $this->config->pingInterval) {
            $this->write("PING\r\n");
            $this->pingSentAt = $now;
        }
    }

    /**
     * Drops the connection and connects again, trying each server in turn, reconnect_attempts times in all.
     */
    private function reconnect(string $reason): void
    {
        $from = $this->server;
        $this->close();

        if ($this->config->reconnectAttempts === 0) {
            throw new NatsConnectionException('Lost the connection to NATS (' . $reason . ') and reconnect_attempts is 0');
        }

        $this->logger?->warning('NATS connection lost; reconnecting', [
            'connection' => $this->config->name,
            'server' => $from !== null ? (string) $from : null,
            'reason' => $reason,
        ]);

        $last = null;
        for ($attempt = 1; $attempt <= $this->config->reconnectAttempts; $attempt++) {
            if ($attempt > 1 && $this->config->reconnectWait > 0) {
                usleep((int) ($this->config->reconnectWait * 1_000_000));
            }
            // After the first attempt, start from the next server rather than the one that failed.
            $this->serverIndex = ($this->serverIndex + ($attempt > 1 ? 1 : 0)) % count($this->config->servers);
            try {
                $this->connect();
                $this->logger?->info('NATS reconnected', ['connection' => $this->config->name, 'server' => (string) $this->server]);

                return;
            } catch (NatsConnectionException $e) {
                $last = $e;
            }
        }

        throw new NatsConnectionException('Lost the connection to NATS (' . $reason . ') and could not reconnect: ' . $last?->getMessage(), 0, $last);
    }
}
