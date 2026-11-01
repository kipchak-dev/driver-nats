<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\JetStream;

use InvalidArgumentException;
use Kipchak\Driver\NATS\Exception\JetStreamException;
use Kipchak\Driver\NATS\Exception\RevisionMismatchException;
use Kipchak\Driver\NATS\JetStream\Settings;
use PHPUnit\Framework\TestCase;

final class SettingsTest extends TestCase
{
    public function testStream(): void
    {
        $this->assertSame([
            'name' => 'ORDERS',
            'subjects' => ['orders.>'],
            'description' => 'Orders',
            'retention' => 'workqueue',
            'storage' => 'memory',
            'discard' => 'new',
            'num_replicas' => 3,
            'max_bytes' => 1048576,
            'max_msgs_per_subject' => 10,
            'allow_rollup_hdrs' => true,
            'max_age' => 604800000000000,
            'duplicate_window' => 1500000000,
            'metadata' => ['team' => 'shop'],
        ], Settings::stream('ORDERS', [
            'subjects' => 'orders.>',
            'description' => 'Orders',
            'retention' => 'WorkQueue',
            'storage' => 'memory',
            'discard' => 'new',
            'replicas' => 3,
            'max_bytes' => 1048576,
            'max_msgs_per_subject' => 10,
            'allow_rollup' => true,
            'max_age' => 7 * 86400,
            'duplicate_window' => 1.5,
            'options' => ['metadata' => ['team' => 'shop']],
        ]));
    }

    public function testRejectsUnknownSettings(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Stream "ORDERS" has unknown settings: maxage');

        Settings::stream('ORDERS', ['maxage' => 60]);
    }

    public function testRejectsUnknownChoices(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('storage must be one of file, memory');

        Settings::stream('ORDERS', ['storage' => 'disk']);
    }

    public function testRejectsBadDurations(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max_age must not be negative');

        Settings::stream('ORDERS', ['max_age' => -1]);
    }

    public function testConsumer(): void
    {
        $this->assertSame([
            'stream_name' => 'ORDERS',
            'config' => [
                'name' => 'mailer',
                'durable_name' => 'mailer',
                'ack_policy' => 'explicit',
                'filter_subject' => 'orders.created',
                'deliver_policy' => 'new',
                'max_deliver' => 5,
                'max_ack_pending' => 100,
                'ack_wait' => 30000000000,
                'inactive_threshold' => 3600000000000,
                'backoff' => [1000000000, 5000000000, 30000000000],
            ],
            'action' => '',
        ], Settings::consumer('ORDERS', 'mailer', [
            'filter_subject' => 'orders.created',
            'deliver_policy' => 'new',
            'max_deliver' => 5,
            'max_ack_pending' => 100,
            'ack_wait' => 30,
            'inactive_threshold' => 3600,
            'backoff' => [1, 5, 30],
        ]));
    }

    public function testConsumerWithSeveralFiltersAndAStartTime(): void
    {
        $config = Settings::consumer('ORDERS', 'audit', [
            'filter_subject' => ['orders.created', 'orders.paid'],
            'deliver_policy' => 'by_start_time',
            'start_time' => new \DateTimeImmutable('2026-10-29 11:30:00.25 +04:00'),
        ])['config'];

        $this->assertSame(['orders.created', 'orders.paid'], $config['filter_subjects']);
        $this->assertArrayNotHasKey('filter_subject', $config);
        $this->assertSame('2026-10-29T07:30:00.250000Z', $config['opt_start_time']);
    }

    public function testStartPoliciesNeedTheirStart(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('by_start_sequence needs start_sequence');

        Settings::consumer('ORDERS', 'replay', ['deliver_policy' => 'by_start_sequence']);
    }

    public function testBucketFollowsNatsLayout(): void
    {
        $config = Settings::bucket('settings', ['history' => 5, 'ttl' => 60, 'storage' => 'memory', 'compression' => true]);

        $this->assertSame('KV_settings', $config['name']);
        $this->assertSame(['$KV.settings.>'], $config['subjects']);
        $this->assertSame(5, $config['max_msgs_per_subject']);
        $this->assertSame(60_000_000_000, $config['max_age']);
        $this->assertSame(60_000_000_000, $config['duplicate_window'], 'The duplicate window never exceeds the TTL');
        $this->assertSame('memory', $config['storage']);
        $this->assertSame('new', $config['discard']);
        $this->assertTrue($config['allow_rollup_hdrs']);
        $this->assertTrue($config['deny_delete']);
        $this->assertTrue($config['allow_direct']);
        $this->assertSame('s2', $config['compression']);
    }

    public function testBucketHistoryLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('history must be between 1 and 64');

        Settings::bucket('settings', ['history' => 65]);
    }

    public function testJetStreamErrors(): void
    {
        $error = JetStreamException::fromError((object) ['code' => 404, 'err_code' => 10059, 'description' => 'stream not found'], 'STREAM.INFO.X');
        $this->assertSame('[JetStream] STREAM.INFO.X: stream not found (10059)', $error->getMessage());
        $this->assertSame(JetStreamException::STREAM_NOT_FOUND, $error->errorCode);
        $this->assertSame(404, $error->status);

        $this->assertInstanceOf(
            RevisionMismatchException::class,
            JetStreamException::fromError((object) ['code' => 400, 'err_code' => 10071, 'description' => 'wrong last sequence: 2'], 'publish'),
        );
    }
}
