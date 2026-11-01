# NATS and JetStream Driver for Kipchak

A NATS driver for Kipchak: messaging between services, request and reply, durable streams and key-value buckets,
over one connection per worker. NATS is a single small server that does the work of a message broker, a work
queue and a key-value store, which makes it a lighter alternative to the [RabbitMQ](https://kipchak.dev/docs/driver-rabbitmq) and
[Kafka](https://kipchak.dev/docs/driver-kafka) drivers.

* **Core NATS**: `publish()` to subjects, `subscribe()` with `*` and `>` wildcards and queue groups, and
  `request()` for request and reply. A request that nothing listens to fails at once rather than timing out.
* **JetStream publishing**: `jetStream()->publish()` waits until the stream has stored the message. Every message
  carries a `Nats-Msg-Id`, so the stream keeps one copy even when a publish is retried. Expected-sequence checks
  give optimistic concurrency.
* **Streams and consumers from config**: declare streams and durable pull consumers in `kipchak.nats`, with
  durations in seconds. `ensureStream()` creates or updates a stream to match.
* **Consumers**: `fetch()` messages in batches, or `consume()` them in a worker loop that acknowledges each one,
  retries failures with a growing delay, and stops cleanly on a signal. `ack()`, `nak($delay)`, `term()` and
  `inProgress()` are all there.
* **Key-value buckets**: `get()`, `put()`, `create()`, `update()` against a revision, `delete()`, `purge()` and
  per-bucket TTLs. Buckets are stored the way NATS's own clients store them, so the `nats` CLI and other languages
  see the same keys.
* **Production settings**: several servers with failover; TLS that verifies the server's certificate and host
  name, with a CA file and client certificates; username and password, token, NKey or credentials-file
  authentication; and JetStream domains.
* **Built for FrankenPHP's long-lived workers**: each worker keeps its connection open across requests, answers
  the server's pings between them, and reconnects (a bounded number of times) when NATS restarts. Warnings go to
  the [Logger driver](https://kipchak.dev/docs/driver-logger).

The wire protocol comes from [basis-company/nats](https://github.com/basis-company/nats.php) (MIT). The driver
replaces its connection handling with its own, to verify TLS, bound reconnects, fail over across servers and keep
headers exactly as sent. Tested against NATS 2.12.

## Installation

```bash
composer require kipchak/driver-nats
```

Initialise it in `drivers/drivers.php`, after the Logger driver:

```php
use Kipchak\Driver\NATS\NATS;

NATS::initialise($container);
```

Each connection in the config file becomes a client in the container under `drivers.messaging.nats.<name>`.
`NATS::get()` returns the `default` connection, and `NATS::get('events')` another. Nothing connects until the
first operation.

NKey and credentials-file authentication need the sodium extension, which the Kipchak runtime images include.

## Configuration

Copy [`sample.config.php`](sample.config.php) to `config/kipchak.nats.php`:

```php
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
```

The config is checked when a connection is first used, and a mistake names the connection and the setting.

**Stream settings**: `subjects`, `description`, `retention`, `storage`, `replicas`, `max_age`, `max_bytes`,
`max_msgs`, `max_msg_size`, `max_msgs_per_subject`, `max_consumers`, `discard` (`old` or `new`),
`duplicate_window`, `allow_rollup`, `deny_delete`, `deny_purge`, `allow_direct` and `compression` (`none` or
`s2`).

**Consumer settings**: `stream`, `name` (the durable name, when it differs from the key), `filter_subject` (one
subject or a list), `description`, `deliver_policy` (`all`, `last`, `new`, `last_per_subject`,
`by_start_sequence` with `start_sequence`, or `by_start_time` with `start_time`), `ack_policy` (`explicit`,
`none` or `all`), `ack_wait`, `max_deliver`, `backoff` (a list of seconds), `max_ack_pending`, `max_waiting`,
`max_batch`, `inactive_threshold`, `replay_policy`, `headers_only`, `replicas` and `memory_storage`.

**Bucket settings**: `history`, `ttl`, `max_bytes`, `max_value_size`, `storage`, `replicas`, `description` and
`compression`.

Any other JetStream field can be passed in its own name under `options`. An unknown setting is an error rather
than being silently ignored.

## Usage

```php
use Kipchak\Driver\NATS\NATS;

$nats = NATS::get();
```

### Publishing and subscribing

```php
$nats->publish('orders.created', ['id' => 1042, 'total' => 99.5], headers: ['X-Trace' => $traceId]);
```

A string is sent as it is; arrays, objects and other scalars are sent as JSON. Core NATS delivers a message to
whoever is subscribed at that moment and keeps nothing, so use [JetStream](#publishing-to-a-stream) for messages
that must not be lost.

Subscriptions belong in long-running workers:

```php
$subscription = $nats->subscribe('orders.*');           // or subscribe('orders.*', queue: 'mailers')

$message = $subscription->next(5);                      // waits up to 5 seconds; null if nothing came
$message->subject;                                      // 'orders.created'
$message->json();                                       // ['id' => 1042, 'total' => 99.5]
$message->header('X-Trace');

$messages = $subscription->fetch(100, 5);               // up to 100: waits for the first, takes what else has arrived

$subscription->unsubscribe();
```

Subscribers that share a queue group share the messages: each one goes to one member of the group. A subscription
that goes out of scope is unsubscribed, so one made during a request does not go on collecting messages in the
worker.

### Request and reply

```php
use Kipchak\Driver\NATS\Exception\NatsTimeoutException;
use Kipchak\Driver\NATS\Exception\NoRespondersException;

try {
    $quote = $nats->request('pricing.quote', ['sku' => 'A-1'], timeout: 2)->json();
} catch (NoRespondersException) {
    // Nothing is subscribed to pricing.quote: answer 503 at once.
} catch (NatsTimeoutException) {
    // The service did not answer in time: answer 504.
}
```

A service answers in a worker loop. `each()` sends the handler's return value back as the reply:

```php
use Kipchak\Driver\NATS\Message\Message;

$nats->subscribe('pricing.quote', queue: 'pricing')->each(
    fn(Message $request) => ['sku' => $request->json()['sku'], 'price' => 12.5],
);
```

Run several copies of the worker with the same queue group and NATS spreads the requests across them.

### Streams

```php
$js = $nats->jetStream();

$js->ensureStream('ORDERS');                            // from streams.ORDERS in config
$js->ensureStream('AUDIT', ['subjects' => ['audit.>'], 'max_age' => 90 * 86400]);
```

`ensureStream()` creates the stream, or updates it to match the settings, and returns its info. Settings left out
take NATS's defaults, so the config describes the whole stream. Call it from a deployment step or a worker's
start-up rather than on every request. Settings JetStream cannot change in place, such as `storage`, raise a
`JetStreamException`.

```php
$js->streamInfo('ORDERS');                              // config and state, or null when there is no such stream
$js->streamNames();
$js->purgeStream('ORDERS', 'orders.cancelled');         // returns how many messages were removed
$js->deleteStream('ORDERS');
```

### Publishing to a stream

```php
$ack = $js->publish('orders.created', $order, id: 'order-' . $order['id']);

$ack->stream;      // 'ORDERS'
$ack->sequence;    // 1042
$ack->duplicate;   // true when the stream already had order-1042 and kept only the first
```

`publish()` returns once the stream has stored the message. A publish that is not confirmed (a timeout, or the
connection replaced while waiting) is sent once more with the same message ID, so the stream still keeps one
copy. Pass your own `id`, such as an order number, to make your own retries safe as well, within the stream's
`duplicate_window`.

Expectations turn a publish into a compare-and-set:

```php
$js->publish('accounts.42', $event, expectedLastSubjectSequence: $lastSeenSequence);   // RevisionMismatchException if someone got there first
$js->publish('orders.created', $order, expectedStream: 'ORDERS');                      // fails if the subject maps to another stream
```

A subject that no stream captures raises `NoRespondersException` at once.

### Consumers

A consumer is a durable cursor on a stream. Several workers can share one: each message goes to one of them, and
comes back if it is not acknowledged within `ack_wait`.

```php
$consumer = $js->consumer('order-mailer');              // from consumers.order-mailer in config
$consumer = $js->consumer('resizer', 'JOBS', ['ack_wait' => 60]);

foreach ($consumer->fetch(10, 2) as $message) {         // up to 10, waiting up to 2 seconds for them
    sendConfirmation($message->json());
    $message->ack();
}
```

The consumer is created, or updated to match its settings, on first use. If it is deleted, or removed after
`inactive_threshold`, it is created again.

Settle each message once:

```php
$message->ack();                // done; ack(true) waits for the server to record it
$message->nak(30);              // try again in 30 seconds (nak() for at once)
$message->term('bad payload');  // never deliver this again
$message->inProgress();         // still working: restart ack_wait

$message->metadata()->deliveries;       // 1 the first time
$message->metadata()->streamSequence;
$message->metadata()->timestamp;        // when the stream stored it
```

`fetch()` returns when the batch is full or the wait ends. With a wait of 0 it returns only what is waiting now,
which suits a request. In a worker, use `consume()`.

### Consuming in a worker

`consume()` handles messages as they arrive. A message is acknowledged when the handler returns, unless the
handler settled it itself. When the handler throws, the error is logged and the message is nak'd with a delay that
grows with each delivery (1s, 2s, 4s, up to 60s) until the consumer's `max_deliver`, and the loop goes on.

```php
<?php

// bin/order-mailer.php: php bin/order-mailer.php

require __DIR__ . '/../vendor/autoload.php';

use Kipchak\Core\Api;
use Kipchak\Driver\NATS\Message\Message;
use Kipchak\Driver\NATS\NATS;

Api::bootCli();   // drivers only: no middlewares or routes

$consumer = NATS::get()->jetStream()->consumer('order-mailer');

// Stop after the current message when the container is stopped.
pcntl_async_signals(true);
pcntl_signal(SIGTERM, fn() => $consumer->stop());

$consumer->consume(function (Message $message): void {
    sendConfirmation($message->json());
});
```

`consume()` also takes a `limit` on messages, a number of `seconds` to run, and the `batch` size to ask for.
Messages that arrive after `stop()` are handed back at once rather than after `ack_wait`. Run as many copies as
the work needs.

### Key-value buckets

```php
$settings = $nats->keyValue('settings');               // from buckets.settings in config

$settings->put('theme', ['mode' => 'dark']);           // returns the new revision
$entry = $settings->get('theme');                      // null when missing or deleted
$entry->json();                                        // ['mode' => 'dark']
$entry->revision;
$entry->created;

$settings->update('theme', ['mode' => 'light'], $entry->revision);   // RevisionMismatchException if it changed since
$settings->create('locks.report', getmypid());         // only if absent: a simple lock
$settings->delete('theme');                            // keeps the history
$settings->purge('theme');                             // removes the history too
```

The bucket is created on the first write, from its settings. An existing bucket keeps its settings; call
`ensure()` to update them. Keys may contain letters, digits and `- / _ = .`. Optimistic updates make safe counters
and flags without a lock:

```php
use Kipchak\Driver\NATS\Exception\RevisionMismatchException;

do {
    $entry = $settings->get('visits');
    try {
        $entry === null ? $settings->create('visits', 1) : $settings->update('visits', (int) $entry->value + 1, $entry->revision);
        break;
    } catch (RevisionMismatchException) {
        // Someone else wrote first: read again.
    }
} while (true);
```

### Errors

* `NatsConnectionException`: no server could be reached, TLS or authentication failed, or the connection was lost
  and could not be re-established. Also raised when the connection is replaced while a `request()` waits, since
  the answer can no longer arrive and the service may or may not have handled the request. Answer 503.
* `NoRespondersException`: nothing is subscribed to the request's subject, or no stream captures a JetStream
  publish. Answer 503.
* `NatsTimeoutException`: no answer within the timeout. Answer 504.
* `JetStreamException`: JetStream refused an operation. `errorCode` is JetStream's own (10059 stream not found,
  10014 consumer not found, 10058 stream name in use with other settings, 10071 wrong last sequence, ...) and
  `status` the HTTP-like status that comes with it.
* `RevisionMismatchException` (a `JetStreamException`): a key-value `create()` or `update()`, or a publish with
  an expected sequence, lost a race.
* `NatsException`: the base class of all of these. Also raised for a message larger than the server's
  `max_payload` (checked before sending, so the server does not close the connection) and for a permissions
  violation on a request.

Subjects, header names and values, and names of streams, consumers and buckets are checked before anything is
sent, so a value with a space or a line break cannot alter the protocol. Those raise `InvalidArgumentException`.

### Diagnostics

```php
$nats->ping();                    // whether the server answers; never throws
$nats->serverInfo();              // server_name, version, jetstream, max_payload, ...
$nats->jetStream()->accountInfo();
$nats->jetStream()->api('STREAM.INFO.ORDERS');   // any JetStream API call
```

## Reconnecting and FrankenPHP worker mode

Each worker keeps one connection per configured connection and reuses it for every request. Between requests the
connection sits idle, so at the start of each operation the driver reads whatever arrived meanwhile: it answers
the server's pings, and notices a connection the server has closed and replaces it before sending.

When the connection drops, the driver tries `reconnect_attempts` times across the servers, `reconnect_wait`
seconds apart, and restores subscriptions, queue groups included. A request then fails rather than hangs while
NATS is down. What was in flight when the connection dropped is handled like this:

* A core publish is lost, as in every NATS client. Use JetStream for messages that must arrive.
* A JetStream publish or API call is sent again on the new connection. Publishes are safe to send twice because
  of the message ID.
* A `request()` fails at once with `NatsConnectionException`, because the service may already have acted on it.
  Retry it yourself if the service is idempotent.
* A consumer's pending pull is replaced by a new one.

In testing, three NATS restarts under steady load from eight workers (about 22,000 requests) failed one request,
a `request()` in flight at the moment of a restart.

A permissions violation on a fire-and-forget publish is logged, not thrown at the next unrelated operation. Other
server errors close the connection: the operation fails with `NatsException` and the next one connects again.

## Testing

```bash
composer install
vendor/bin/phpunit --testsuite Unit

# Integration tests run against NATS 2.12 in Docker: plain, authenticated (password and NKey), TLS and mutual TLS
# servers. The reconnect tests restart the plain server.
tests/Integration/start-services.sh
vendor/bin/phpunit
docker compose -f tests/Integration/compose.yaml down -v
```

## What is a Kipchak Driver?

Kipchak Drivers are used to connect Kipchak to various data sources or storage systems.

They provide a standardized interface for interacting with different data sources, allowing developers to
focus on building their applications rather than dealing with the complexities of each data source.

Drivers are basically Container Dependencies injected into Kipchak's Service Container.

## Licence

MIT
