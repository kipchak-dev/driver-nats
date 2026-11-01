<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\Tests\Integration;

use Kipchak\Driver\NATS\Exception\NatsConnectionException;
use Kipchak\Driver\NATS\Exception\NatsException;

final class ConnectionTest extends IntegrationCase
{
    private static function nkey(): array
    {
        return array_map('trim', file(self::CERTS . '/nkey.txt'));
    }

    public function testUsernameAndPassword(): void
    {
        $nats = $this->client(['servers' => self::AUTH, 'username' => 'kipchak', 'password' => 'kipchak-password']);

        $this->assertTrue($nats->ping());
    }

    public function testWrongPasswordFailsOnConnecting(): void
    {
        $nats = $this->client(['servers' => self::AUTH, 'username' => 'kipchak', 'password' => 'wrong']);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('Authorization Violation');
        $nats->publish('anything', 'x');
    }

    public function testNoCredentialsFailsOnConnecting(): void
    {
        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('Authorization Violation');
        $this->client(['servers' => self::AUTH])->flush();
    }

    public function testNKey(): void
    {
        [$seed] = self::nkey();
        $nats = $this->client(['servers' => self::AUTH, 'nkey_seed' => $seed]);

        $subscription = $nats->subscribe('nkey.test');
        $nats->publish('nkey.test', 'signed in');
        $this->assertSame('signed in', $subscription->next(1)?->body);
    }

    public function testCredentialsFileWithAnNKeySeedButNoJwtIsRefused(): void
    {
        $file = tempnam(sys_get_temp_dir(), 'creds');
        file_put_contents($file, "-----BEGIN USER NKEY SEED-----\n" . self::nkey()[0] . "\n------END USER NKEY SEED------\n");

        try {
            $this->expectException(NatsConnectionException::class);
            $this->expectExceptionMessage('no user JWT and NKey seed');
            $this->client(['servers' => self::AUTH, 'credentials' => $file])->flush();
        } finally {
            unlink($file);
        }
    }

    public function testPermissionViolationsFailTheOperationAndKeepTheConnection(): void
    {
        $nats = $this->client(['servers' => self::AUTH, 'username' => 'limited', 'password' => 'limited-password']);
        $subscription = $nats->subscribe('allowed.x');

        try {
            $nats->request('forbidden.service', 'x', timeout: 1);
            $this->fail('Expected NatsException');
        } catch (NatsException $e) {
            $this->assertStringContainsString('Permissions Violation', $e->getMessage());
        }

        $nats->publish('allowed.x', 'still connected');
        $this->assertSame('still connected', $subscription->next(1)?->body);
    }

    public function testTlsVerifiesTheServerAgainstTheGivenCa(): void
    {
        $nats = $this->client(['servers' => self::TLS, 'tls' => ['ca' => self::CERTS . '/ca.pem']]);

        $this->assertTrue($nats->ping());
        $this->assertTrue($nats->serverInfo()['tls_required']);
    }

    public function testTlsIsUsedWhenTheServerRequiresItEvenForANatsUrl(): void
    {
        $nats = $this->client(['servers' => 'nats://localhost:54224', 'tls' => ['ca' => self::CERTS . '/ca.pem']]);

        $this->assertTrue($nats->ping());
    }

    public function testTlsRefusesAServerSignedByAnotherCa(): void
    {
        $nats = $this->client(['servers' => self::TLS, 'tls' => ['ca' => self::CERTS . '/other-ca.pem']]);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('TLS handshake failed: certificate verify failed (error:0A000086)');
        $nats->flush();
    }

    public function testTlsRefusesAServerNotInTheSystemTrustStore(): void
    {
        $nats = $this->client(['servers' => self::TLS]);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('TLS handshake failed');
        $nats->flush();
    }

    public function testTlsChecksTheHostName(): void
    {
        // The certificate names localhost only.
        $nats = $this->client(['servers' => 'tls://127.0.0.1:54224', 'tls' => ['ca' => self::CERTS . '/ca.pem']]);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('TLS handshake failed');
        $nats->flush();
    }

    public function testTlsVerificationCanBeTurnedOff(): void
    {
        $nats = $this->client(['servers' => self::TLS, 'tls' => ['verify' => false]]);

        $this->assertTrue($nats->ping());
    }

    public function testTlsToAServerWithoutTlsFails(): void
    {
        $nats = $this->client(['servers' => 'tls://127.0.0.1:54222']);

        $this->expectException(NatsConnectionException::class);
        $this->expectExceptionMessage('the server does not offer it');
        $nats->flush();
    }

    public function testMutualTls(): void
    {
        $nats = $this->client(['servers' => self::MTLS, 'tls' => [
            'ca' => self::CERTS . '/ca.pem',
            'cert' => self::CERTS . '/client.pem',
            'key' => self::CERTS . '/client-key.pem',
        ]]);

        $this->assertTrue($nats->ping());
    }

    public function testMutualTlsWithoutAClientCertificateFails(): void
    {
        $nats = $this->client(['servers' => self::MTLS, 'tls' => ['ca' => self::CERTS . '/ca.pem']]);

        $this->expectException(NatsConnectionException::class);
        $nats->flush();
    }

    public function testFailsOverToTheNextServer(): void
    {
        $nats = $this->client(['servers' => ['nats://127.0.0.1:1', self::PLAIN], 'timeout' => 1]);

        $this->assertTrue($nats->ping());
    }

    public function testUnreachableServersFailQuickly(): void
    {
        $nats = $this->client(['servers' => ['nats://127.0.0.1:1', 'nats://127.0.0.1:2'], 'timeout' => 1]);

        $started = microtime(true);
        try {
            $nats->publish('x', 'y');
            $this->fail('Expected NatsConnectionException');
        } catch (NatsConnectionException $e) {
            $this->assertStringContainsString('nats://127.0.0.1:1:', $e->getMessage());
            $this->assertStringContainsString('nats://127.0.0.1:2:', $e->getMessage());
        }
        $this->assertLessThan(1, microtime(true) - $started);
    }

    public function testClientNameIsSentToTheServer(): void
    {
        $nats = $this->client(['name' => 'kipchak-tests']);
        $nats->flush();

        $connections = json_decode((string) file_get_contents('http://127.0.0.1:58222/connz'), true);
        if ($connections === null) {
            $this->markTestSkipped('The monitoring port is not published');
        }
        $this->assertContains('kipchak-tests', array_column($connections['connections'], 'name'));
    }
}
