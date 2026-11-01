<?php

// A NATS service in its own process for the request/reply tests: answers requests on a subject with the body in
// upper case, until it has answered $count or $seconds have passed. Prints "ready" once subscribed.
// Usage: php responder.php <server> <subject> [queue group] [count] [seconds]

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

use Kipchak\Driver\NATS\Library\ConnectionConfig;
use Kipchak\Driver\NATS\Library\NatsClient;
use Kipchak\Driver\NATS\Message\Message;

[, $server, $subject] = $argv;
$group = ($argv[3] ?? '') !== '' ? $argv[3] : null;
$count = (int) ($argv[4] ?? 1);
$seconds = (float) ($argv[5] ?? 10);

$nats = new NatsClient(ConnectionConfig::fromArray('responder', ['servers' => $server, 'reconnect_attempts' => 100, 'reconnect_wait' => 0.2]));
$subscription = $nats->subscribe($subject, $group);
$nats->flush();
fwrite(STDOUT, "ready\n");

$subscription->each(
    static fn(Message $message): array => ['answer' => strtoupper($message->body), 'by' => getmypid(), 'trace' => $message->header('X-Trace')],
    $count,
    $seconds,
);
