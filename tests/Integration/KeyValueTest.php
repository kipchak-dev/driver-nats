<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Exception\RevisionMismatchException;
use Kipchak\Driver\NATS\KeyValue\KeyValue;

final class KeyValueTest extends IntegrationCase
{
    private KeyValue $kv;

    protected function setUp(): void
    {
        $this->kv = $this->client(['buckets' => ['settings' => ['history' => 3, 'storage' => 'memory']]])->keyValue('settings');
        $this->kv->destroy();
    }

    public function testMissingBucketsAndKeys(): void
    {
        $this->assertNull($this->kv->info());
        $this->assertNull($this->kv->get('theme'), 'Reading does not create the bucket');
        $this->assertNull($this->kv->info());

        $this->kv->put('other', 'x');
        $this->assertNull($this->kv->get('theme'));
    }

    public function testPutGetAndRevisions(): void
    {
        $first = $this->kv->put('theme', 'dark');
        $second = $this->kv->put('theme', ['mode' => 'light', 'contrast' => 'high']);

        $entry = $this->kv->get('theme');
        $this->assertSame('settings', $entry->bucket);
        $this->assertSame('theme', $entry->key);
        $this->assertSame(['mode' => 'light', 'contrast' => 'high'], $entry->json());
        $this->assertSame($second, $entry->revision);
        $this->assertGreaterThan($first, $second);
        $this->assertEqualsWithDelta(time(), $entry->created->getTimestamp(), 5);

        $info = $this->kv->info();
        $this->assertSame('KV_settings', $info['config']['name']);
        $this->assertSame(3, $info['config']['max_msgs_per_subject']);
    }

    public function testUpdateOnlyFromTheCurrentRevision(): void
    {
        $revision = $this->kv->put('counter', '1');
        $next = $this->kv->update('counter', '2', $revision);

        $this->expectException(RevisionMismatchException::class);
        $this->kv->update('counter', '3', $revision);
        $this->assertSame('2', $this->kv->get('counter')->value);
        $this->assertGreaterThan($revision, $next);
    }

    public function testCreateOnlyWhenAbsent(): void
    {
        $this->kv->create('lock', 'worker-1');

        try {
            $this->kv->create('lock', 'worker-2');
            $this->fail('Expected RevisionMismatchException');
        } catch (RevisionMismatchException) {
        }
        $this->assertSame('worker-1', $this->kv->get('lock')->value);

        $this->kv->delete('lock');
        $this->assertNull($this->kv->get('lock'));
        $this->kv->create('lock', 'worker-2');
        $this->assertSame('worker-2', $this->kv->get('lock')->value, 'A deleted key can be created again');
    }

    public function testDeleteWithARevision(): void
    {
        $revision = $this->kv->put('k', 'v');
        $this->kv->put('k', 'v2');

        try {
            $this->kv->delete('k', $revision);
            $this->fail('Expected RevisionMismatchException');
        } catch (RevisionMismatchException) {
        }
        $this->assertSame('v2', $this->kv->get('k')->value);
    }

    public function testPurgeRemovesHistory(): void
    {
        $this->kv->put('k', '1');
        $this->kv->put('k', '2');
        $this->kv->put('k', '3');
        $this->assertSame(3, $this->kv->info()['state']['messages']);

        $this->kv->purge('k');

        $this->assertNull($this->kv->get('k'));
        $this->assertSame(1, $this->kv->info()['state']['messages'], 'Only the purge marker is left');
    }

    public function testKeysWithDotsAndSlashes(): void
    {
        $this->kv->put('users/42.profile', 'x');

        $this->assertSame('x', $this->kv->get('users/42.profile')->value);
    }

    public function testRejectsBadKeys(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->kv->put('has space', 'x');
    }

    public function testTheBucketIsCreatedAgainWhenDeleted(): void
    {
        $this->kv->put('k', 'v');
        $this->kv->destroy();

        $this->kv->put('k', 'again');
        $this->assertSame('again', $this->kv->get('k')->value);
    }

    public function testAnExistingBucketKeepsItsSettings(): void
    {
        $this->kv->put('k', 'v');
        $other = $this->client()->keyValue('settings', ['history' => 10, 'storage' => 'memory']);

        $other->put('k', 'v2');

        $this->assertSame(3, $other->info()['config']['max_msgs_per_subject'], 'Writing does not reconfigure the bucket');
        $this->assertSame(10, $other->ensure()['config']['max_msgs_per_subject'], 'ensure() does');
    }

    public function testValuesExpire(): void
    {
        $kv = $this->client()->keyValue('sessions', ['ttl' => 1, 'storage' => 'memory']);
        $kv->destroy();
        try {
            $kv->put('s1', 'token');
            $this->assertSame('token', $kv->get('s1')?->value);
            usleep(1_600_000);
            $this->assertNull($kv->get('s1'));
        } finally {
            $kv->destroy();
        }
    }

    public function testTheNatsCliSeesTheSameKeys(): void
    {
        $this->kv->put('shared', 'from-php');

        exec('docker run --rm natsio/nats-box:latest nats --server nats://host.docker.internal:54222 kv get settings shared --raw 2>&1', $output, $code);
        if ($code !== 0) {
            $this->markTestSkipped('nats-box could not reach the server: ' . implode(' ', $output));
        }
        $this->assertSame('from-php', trim(implode('', $output)));
    }
}
