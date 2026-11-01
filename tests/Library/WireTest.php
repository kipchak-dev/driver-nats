<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Library;

use InvalidArgumentException;
use Kipchak\Driver\NATS\Library\Connection;
use Kipchak\Driver\NATS\Library\Wire;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class WireTest extends TestCase
{
    public static function publishSubjects(): iterable
    {
        yield 'simple' => ['orders', true];
        yield 'tokens' => ['orders.created.eu', true];
        yield 'dollar' => ['$KV.settings.theme', true];
        yield 'star' => ['orders.*', false];
        yield 'gt' => ['orders.>', false];
        yield 'empty' => ['', false];
        yield 'space' => ['orders created', false];
        yield 'tab' => ["orders\tcreated", false];
        yield 'newline' => ["orders\r\nPUB evil 0", false];
        yield 'empty token' => ['orders..created', false];
        yield 'leading dot' => ['.orders', false];
        yield 'trailing dot' => ['orders.', false];
    }

    #[DataProvider('publishSubjects')]
    public function testPublishSubjects(string $subject, bool $valid): void
    {
        if (!$valid) {
            $this->expectException(InvalidArgumentException::class);
        }

        $this->assertSame($subject, Wire::subject($subject));
    }

    public static function patterns(): iterable
    {
        yield 'star' => ['orders.*.eu', true];
        yield 'final gt' => ['orders.>', true];
        yield 'both' => ['*.created.>', true];
        yield 'gt in the middle' => ['orders.>.eu', false];
        yield 'mixed token' => ['orders.cre*', false];
        yield 'space' => ['orders .>', false];
    }

    #[DataProvider('patterns')]
    public function testSubscriptionPatterns(string $subject, bool $valid): void
    {
        if (!$valid) {
            $this->expectException(InvalidArgumentException::class);
        }

        $this->assertSame($subject, Wire::pattern($subject));
    }

    public static function bodies(): iterable
    {
        yield 'string' => ['hello', 'hello'];
        yield 'binary' => ["\x00\xff", "\x00\xff"];
        yield 'null' => [null, ''];
        yield 'array' => [['id' => 7, 'tags' => ['a/b']], '{"id":7,"tags":["a/b"]}'];
        yield 'list' => [[1, 2], '[1,2]'];
        yield 'int' => [42, '42'];
        yield 'float keeps its fraction' => [2.0, '2.0'];
        yield 'bool' => [false, 'false'];
        yield 'unicode stays readable' => [['name' => 'Qalāwūn'], '{"name":"Qalāwūn"}'];
        yield 'json serializable' => [new class implements \JsonSerializable {
            public function jsonSerialize(): array
            {
                return ['x' => 1];
            }
        }, '{"x":1}'];
        yield 'stringable' => [new class implements \Stringable {
            public function __toString(): string
            {
                return 'as-is';
            }
        }, 'as-is'];
    }

    #[DataProvider('bodies')]
    public function testEncodesBodies(mixed $body, string $expected): void
    {
        $this->assertSame($expected, Wire::body($body));
    }

    public function testRejectsBodiesThatCannotBeEncoded(): void
    {
        $this->expectException(\JsonException::class);

        Wire::body(['bad' => NAN]);
    }

    public function testHeaders(): void
    {
        $this->assertSame(
            ['Nats-Msg-Id' => 'abc', 'X-Count' => '3', 'X-Flag' => 'true', 'X-Off' => 'false'],
            Wire::headers(['Nats-Msg-Id' => 'abc', 'X-Count' => 3, 'X-Flag' => true, 'X-Off' => false]),
        );
    }

    public static function badHeaders(): iterable
    {
        yield 'colon in name' => [['X:Y' => 'v']];
        yield 'space in name' => [['X Y' => 'v']];
        yield 'numeric name' => [[0 => 'v']];
        yield 'newline in value' => [['X' => "v\r\nInjected: yes"]];
        yield 'array value' => [['X' => ['v']]];
    }

    #[DataProvider('badHeaders')]
    public function testRejectsHeadersThatWouldBreakTheProtocol(array $headers): void
    {
        $this->expectException(InvalidArgumentException::class);

        Wire::headers($headers);
    }

    public function testSizeCountsTheHeaderBlock(): void
    {
        $this->assertSame(5, Wire::size(Wire::payload('hello')));
        $this->assertSame(strlen("NATS/1.0\r\nA: b\r\n\r\nhello"), Wire::size(Wire::payload('hello', ['A' => 'b'])));
    }

    public function testNames(): void
    {
        $this->assertSame('ORDERS_v2-eu', Wire::name('ORDERS_v2-eu', 'stream'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Stream "ORDERS.EU"');
        Wire::name('ORDERS.EU', 'stream');
    }

    public function testParsesHeaderBlocks(): void
    {
        $this->assertSame(
            ['Status-Code' => '503'],
            Connection::parseHeaders("NATS/1.0 503\r\n\r\n"),
        );
        $this->assertSame(
            ['Status-Code' => '408', 'Status-Message' => 'Request Timeout', 'Nats-Pending-Messages' => '1'],
            Connection::parseHeaders("NATS/1.0 408 Request Timeout\r\nNats-Pending-Messages: 1\r\n\r\n"),
        );
        $this->assertSame(
            ['X-Tag' => 'a, b', 'Url' => 'https://x:1/y'],
            Connection::parseHeaders("NATS/1.0\r\nX-Tag: a\r\nX-Tag: b\r\nUrl: https://x:1/y\r\n\r\n"),
        );
    }
}
