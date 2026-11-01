<?php

use function Kipchak\Core\env;

return [
    'connections' => [
        'default' => [
            // One or more servers, tried in order on connecting and on reconnecting. Use tls:// to require TLS.
            'servers' => [env('NATS_URL', 'nats://nats:4222')],

            // Shown in the server's connection list (nats server report connections).
            'name' => 'kipchak-api',

            // Authentication: at most one of username/password, token, nkey_seed or credentials (a .creds file with
            // a user JWT and NKey seed, as issued by nsc). NKeys and credentials need ext-sodium.
            // 'username' => env('NATS_USER', ''),
            // 'password' => env('NATS_PASSWORD', ''),
            // 'token' => env('NATS_TOKEN', ''),
            // 'nkey_seed' => env('NATS_NKEY_SEED', ''),
            // 'credentials' => '/run/secrets/nats.creds',

            // Seconds to connect, and to wait for each request, JetStream publish or API call.
            'timeout' => 2,

            // When the connection drops, how many times to try to reconnect (across all servers) before an
            // operation fails, and the seconds between attempts. 0 fails at once.
            'reconnect_attempts' => 3,
            'reconnect_wait' => 0.1,

            // Seconds of silence while waiting (in a consumer or subscription loop) before pinging the server.
            'ping_interval' => 30,

            // TLS. 'ca' verifies the server's certificate (the system's trusted CAs are used otherwise); 'cert'
            // and 'key' send a client certificate. 'handshake_first' is for servers set to handshake_first.
            // 'tls' => [
            //     'ca' => '/etc/ssl/nats/ca.pem',
            //     'cert' => '/etc/ssl/nats/client.pem',
            //     'key' => '/etc/ssl/nats/client-key.pem',
            //     'handshake_first' => false,
            // ],

            // For a JetStream domain (a leaf node with its own JetStream, say).
            // 'jetstream_domain' => 'hub',

            // JetStream streams, created or updated with $nats->jetStream()->ensureStream('ORDERS'). Durations are
            // in seconds; other JetStream fields go under 'options'.
            'streams' => [
                'ORDERS' => [
                    'subjects' => ['orders.>'],
                    'storage' => 'file',          // file or memory
                    'retention' => 'limits',      // limits, interest or workqueue
                    'replicas' => 1,
                    'max_age' => 7 * 86400,       // keep messages for a week
                    'duplicate_window' => 120,    // seconds in which a repeated Nats-Msg-Id is dropped
                ],
            ],

            // Durable pull consumers, used with $nats->jetStream()->consumer('order-mailer').
            'consumers' => [
                'order-mailer' => [
                    'stream' => 'ORDERS',
                    'filter_subject' => 'orders.created',
                    'ack_wait' => 30,             // seconds before an unacknowledged message is delivered again
                    'max_deliver' => 5,           // give up on a message after this many deliveries
                ],
            ],

            // Key-value buckets, used with $nats->keyValue('settings'); created on the first write.
            'buckets' => [
                'settings' => [
                    'history' => 5,               // revisions kept per key (1 to 64)
                    'ttl' => 0,                   // seconds before a value expires; 0 keeps values
                ],
            ],
        ],
    ],
];
