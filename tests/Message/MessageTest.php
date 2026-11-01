<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Message;

use Kipchak\Driver\NATS\Message\Message;
use Kipchak\Driver\NATS\Message\Metadata;
use LogicException;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    public function testJsonAndHeaders(): void
    {
        $message = new Message('orders.created', '{"id":7}', ['Nats-Msg-Id' => 'order-7']);

        $this->assertSame(['id' => 7], $message->json());
        $this->assertSame(7, $message->json(false)->id);
        $this->assertSame('order-7', $message->header('Nats-Msg-Id'));
        $this->assertSame('order-7', $message->header('nats-msg-id'));
        $this->assertNull($message->header('X-Missing'));
        $this->assertFalse($message->isJetStream());
        $this->assertNull($message->metadata());
    }

    public function testJsonThrowsOnOtherBodies(): void
    {
        $this->expectException(\JsonException::class);

        (new Message('a', 'not json'))->json();
    }

    public function testOnlyJetStreamMessagesAreAcknowledged(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Only messages from a JetStream consumer');

        (new Message('a', '', [], '_INBOX.abc'))->ack();
    }

    public function testOnlyRequestsAreAnswered(): void
    {
        $this->expectException(LogicException::class);

        (new Message('a', ''))->respond('x');
    }

    public function testMetadataFromTheOriginalAckSubject(): void
    {
        $metadata = Metadata::fromReplySubject('$JS.ACK.ORDERS.mailer.3.1042.17.1793259000123456789.5');

        $this->assertNotNull($metadata);
        $this->assertSame('ORDERS', $metadata->stream);
        $this->assertSame('mailer', $metadata->consumer);
        $this->assertSame(3, $metadata->deliveries);
        $this->assertSame(1042, $metadata->streamSequence);
        $this->assertSame(17, $metadata->consumerSequence);
        $this->assertSame(5, $metadata->pending);
        $this->assertNull($metadata->domain);
        $this->assertSame('2026-10-29 07:30:00.123456', $metadata->timestamp->format('Y-m-d H:i:s.u'));
    }

    public function testMetadataFromTheNewerAckSubject(): void
    {
        $metadata = Metadata::fromReplySubject('$JS.ACK.hub.ACCHASH.ORDERS.mailer.1.9.9.1793259000000000500.0.rand0m');

        $this->assertNotNull($metadata);
        $this->assertSame('hub', $metadata->domain);
        $this->assertSame('ORDERS', $metadata->stream);
        $this->assertSame(9, $metadata->streamSequence);
        $this->assertSame('000000', $metadata->timestamp->format('u'));
    }

    public function testNoMetadataForOtherSubjects(): void
    {
        $this->assertNull(Metadata::fromReplySubject(null));
        $this->assertNull(Metadata::fromReplySubject('_INBOX.abc.1'));
        $this->assertNull(Metadata::fromReplySubject('$JS.ACK.ORDERS.mailer.x.1.1.1.1'));
        $this->assertNull(Metadata::fromReplySubject('$JS.ACK.short'));
    }
}
