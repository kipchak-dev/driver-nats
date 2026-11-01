<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Library;

use InvalidArgumentException;

/**
 * One connection's settings from kipchak.nats, validated when the client is created.
 */
final readonly class ConnectionConfig
{
    /**
     * @param list<Server> $servers Tried in order, on connecting and on reconnecting.
     * @param array<string, array<string, mixed>> $streams JetStream streams by name, for ensureStream().
     * @param array<string, array<string, mixed>> $consumers Pull consumers by name, for consumer().
     * @param array<string, array<string, mixed>> $buckets Key-value buckets by name, for keyValue().
     */
    public function __construct(
        public string $name,
        public array $servers,
        public string $clientName,
        public ?string $username,
        #[\SensitiveParameter] public ?string $password,
        #[\SensitiveParameter] public ?string $token,
        #[\SensitiveParameter] public ?string $nkeySeed,
        public ?string $credentials,
        public float $timeout,
        public int $reconnectAttempts,
        public float $reconnectWait,
        public float $pingInterval,
        public ?string $tlsCa,
        public ?string $tlsCert,
        public ?string $tlsKey,
        public bool $tlsVerify,
        public bool $tlsHandshakeFirst,
        public ?string $jetStreamDomain,
        public array $streams,
        public array $consumers,
        public array $buckets,
    ) {
    }

    public static function fromArray(string $name, array $config): self
    {
        $servers = $config['servers'] ?? null;
        $servers = is_string($servers) ? [$servers] : $servers;
        if (!is_array($servers) || $servers === []) {
            throw self::invalid($name, 'servers is required: one or more URLs such as "nats://nats:4222"');
        }
        $servers = array_map(static fn(mixed $server): Server => self::server($name, $server), array_values($servers));

        $authMethods = array_filter([
            'username' => self::optionalString($config['username'] ?? null),
            'token' => self::optionalString($config['token'] ?? null),
            'nkey_seed' => self::optionalString($config['nkey_seed'] ?? null),
            'credentials' => self::optionalString($config['credentials'] ?? null),
        ], static fn(?string $value): bool => $value !== null);
        if (count($authMethods) > 1) {
            throw self::invalid($name, 'choose one of username, token, nkey_seed and credentials, not ' . implode(' and ', array_keys($authMethods)));
        }
        $credentials = $authMethods['credentials'] ?? null;
        if ($credentials !== null && !is_readable($credentials)) {
            throw self::invalid($name, 'credentials must be the path of a readable .creds file');
        }

        $timeout = (float) ($config['timeout'] ?? 2.0);
        if ($timeout <= 0) {
            throw self::invalid($name, 'timeout must be more than 0 seconds');
        }
        $reconnectAttempts = (int) ($config['reconnect_attempts'] ?? 3);
        if ($reconnectAttempts < 0) {
            throw self::invalid($name, 'reconnect_attempts must be 0 or more');
        }

        $tls = $config['tls'] ?? [];
        if (!is_array($tls)) {
            throw self::invalid($name, 'tls must be an array');
        }
        $tlsCert = self::optionalString($tls['cert'] ?? null);
        $tlsKey = self::optionalString($tls['key'] ?? null);
        if (($tlsCert === null) !== ($tlsKey === null)) {
            throw self::invalid($name, 'client certificates need both tls.cert and tls.key');
        }

        $domain = self::optionalString($config['jetstream_domain'] ?? null);
        if ($domain !== null && !preg_match('/^[A-Za-z0-9_-]+$/', $domain)) {
            throw self::invalid($name, 'jetstream_domain may contain only letters, digits, "-" and "_"');
        }

        return new self(
            name: $name,
            servers: $servers,
            clientName: (string) ($config['name'] ?? 'kipchak'),
            username: $authMethods['username'] ?? null,
            password: self::optionalString($config['password'] ?? null),
            token: $authMethods['token'] ?? null,
            nkeySeed: $authMethods['nkey_seed'] ?? null,
            credentials: $credentials,
            timeout: $timeout,
            reconnectAttempts: $reconnectAttempts,
            reconnectWait: max(0.0, (float) ($config['reconnect_wait'] ?? 0.1)),
            pingInterval: max(1.0, (float) ($config['ping_interval'] ?? 30)),
            tlsCa: self::optionalString($tls['ca'] ?? null),
            tlsCert: $tlsCert,
            tlsKey: $tlsKey,
            tlsVerify: (bool) ($tls['verify'] ?? true),
            tlsHandshakeFirst: (bool) ($tls['handshake_first'] ?? false),
            jetStreamDomain: $domain,
            streams: self::definitions($name, 'streams', $config['streams'] ?? []),
            consumers: self::definitions($name, 'consumers', $config['consumers'] ?? []),
            buckets: self::definitions($name, 'buckets', $config['buckets'] ?? []),
        );
    }

    /**
     * The subject prefix of the JetStream API: "$JS.API", or "$JS.<domain>.API" for a JetStream domain.
     */
    public function apiPrefix(): string
    {
        return $this->jetStreamDomain === null ? '$JS.API' : '$JS.' . $this->jetStreamDomain . '.API';
    }

    private static function server(string $name, mixed $server): Server
    {
        if (!is_string($server)
            || !preg_match('~^(?:(?<scheme>nats|tls)://)?(?<host>\[[0-9A-Fa-f:.]+\]|[^\s:/?#@\[\]]+)(?::(?<port>\d{1,5}))?/?$~', $server, $match)) {
            throw self::invalid($name, 'each server must be a URL such as "nats://nats:4222" or "tls://nats:4222", with no credentials or path');
        }
        $port = isset($match['port']) && $match['port'] !== '' ? (int) $match['port'] : 4222;
        if ($port < 1 || $port > 65535) {
            throw self::invalid($name, 'server port must be between 1 and 65535');
        }

        return new Server(trim($match['host'], '[]'), $port, ($match['scheme'] ?? '') === 'tls');
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private static function definitions(string $name, string $key, mixed $definitions): array
    {
        if (!is_array($definitions)) {
            throw self::invalid($name, $key . ' must be an array keyed by name');
        }
        foreach ($definitions as $definitionName => $definition) {
            if (!is_string($definitionName) || !is_array($definition)) {
                throw self::invalid($name, $key . ' must map each name to an array of settings');
            }
        }

        return $definitions;
    }

    private static function optionalString(mixed $value): ?string
    {
        return $value === null || $value === '' ? null : (string) $value;
    }

    private static function invalid(string $name, string $problem): InvalidArgumentException
    {
        return new InvalidArgumentException('NATS connection "' . $name . '": ' . $problem . '.');
    }
}
