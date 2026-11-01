<?php

declare(strict_types=1);

namespace Kipchak\Driver\NATS\JetStream;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use InvalidArgumentException;

/**
 * Turns the stream, consumer and bucket settings in kipchak.nats (durations in seconds, readable names) into
 * JetStream API configuration. Anything not covered can be passed as JetStream's own fields under 'options'.
 */
final class Settings
{
    private const array STREAM_KEYS = [
        'subjects', 'description', 'retention', 'storage', 'replicas', 'max_age', 'max_bytes', 'max_msgs',
        'max_msg_size', 'max_msgs_per_subject', 'max_consumers', 'discard', 'duplicate_window', 'allow_rollup',
        'deny_delete', 'deny_purge', 'allow_direct', 'compression', 'options',
    ];

    private const array CONSUMER_KEYS = [
        'stream', 'name', 'filter_subject', 'description', 'deliver_policy', 'start_sequence', 'start_time',
        'ack_policy', 'ack_wait', 'max_deliver', 'backoff', 'max_ack_pending', 'max_waiting', 'max_batch',
        'inactive_threshold', 'replay_policy', 'headers_only', 'replicas', 'memory_storage', 'options',
    ];

    private const array BUCKET_KEYS = [
        'history', 'ttl', 'max_bytes', 'max_value_size', 'storage', 'replicas', 'description', 'compression', 'options',
    ];

    /**
     * @param array<string, mixed> $settings
     * @return array<string, mixed>
     */
    public static function stream(string $name, array $settings): array
    {
        self::known('stream "' . $name . '"', $settings, self::STREAM_KEYS);

        $config = ['name' => $name];
        if (isset($settings['subjects'])) {
            $config['subjects'] = self::list($settings['subjects'], 'stream "' . $name . '" subjects');
        }
        self::copy($settings, $config, 'description', 'description', 'string');
        self::choice($settings, $config, 'retention', 'retention', ['limits', 'interest', 'workqueue'], $name);
        self::choice($settings, $config, 'storage', 'storage', ['file', 'memory'], $name);
        self::choice($settings, $config, 'discard', 'discard', ['old', 'new'], $name);
        self::choice($settings, $config, 'compression', 'compression', ['none', 's2'], $name);
        self::copy($settings, $config, 'replicas', 'num_replicas', 'int');
        self::copy($settings, $config, 'max_bytes', 'max_bytes', 'int');
        self::copy($settings, $config, 'max_msgs', 'max_msgs', 'int');
        self::copy($settings, $config, 'max_msg_size', 'max_msg_size', 'int');
        self::copy($settings, $config, 'max_msgs_per_subject', 'max_msgs_per_subject', 'int');
        self::copy($settings, $config, 'max_consumers', 'max_consumers', 'int');
        self::copy($settings, $config, 'allow_rollup', 'allow_rollup_hdrs', 'bool');
        self::copy($settings, $config, 'deny_delete', 'deny_delete', 'bool');
        self::copy($settings, $config, 'deny_purge', 'deny_purge', 'bool');
        self::copy($settings, $config, 'allow_direct', 'allow_direct', 'bool');
        self::duration($settings, $config, 'max_age', 'max_age');
        self::duration($settings, $config, 'duplicate_window', 'duplicate_window');

        return array_replace($config, self::options($settings, 'stream "' . $name . '"'));
    }

    /**
     * @param array<string, mixed> $settings
     * @return array<string, mixed> The body of a CONSUMER.CREATE request.
     */
    public static function consumer(string $stream, string $name, array $settings): array
    {
        self::known('consumer "' . $name . '"', $settings, self::CONSUMER_KEYS);

        $config = ['name' => $name, 'durable_name' => $name, 'ack_policy' => 'explicit'];
        if (isset($settings['filter_subject'])) {
            $filters = self::list($settings['filter_subject'], 'consumer "' . $name . '" filter_subject');
            if (count($filters) === 1) {
                $config['filter_subject'] = $filters[0];
            } else {
                $config['filter_subjects'] = $filters;
            }
        }
        self::copy($settings, $config, 'description', 'description', 'string');
        self::choice($settings, $config, 'deliver_policy', 'deliver_policy', ['all', 'last', 'new', 'last_per_subject', 'by_start_sequence', 'by_start_time'], $name);
        self::choice($settings, $config, 'ack_policy', 'ack_policy', ['explicit', 'none', 'all'], $name);
        self::choice($settings, $config, 'replay_policy', 'replay_policy', ['instant', 'original'], $name);
        self::copy($settings, $config, 'start_sequence', 'opt_start_seq', 'int');
        self::copy($settings, $config, 'max_deliver', 'max_deliver', 'int');
        self::copy($settings, $config, 'max_ack_pending', 'max_ack_pending', 'int');
        self::copy($settings, $config, 'max_waiting', 'max_waiting', 'int');
        self::copy($settings, $config, 'max_batch', 'max_batch', 'int');
        self::copy($settings, $config, 'replicas', 'num_replicas', 'int');
        self::copy($settings, $config, 'headers_only', 'headers_only', 'bool');
        self::copy($settings, $config, 'memory_storage', 'mem_storage', 'bool');
        self::duration($settings, $config, 'ack_wait', 'ack_wait');
        self::duration($settings, $config, 'inactive_threshold', 'inactive_threshold');

        if (isset($settings['start_time'])) {
            $start = $settings['start_time'];
            $start = $start instanceof DateTimeInterface ? $start : new DateTimeImmutable((string) $start);
            $config['opt_start_time'] = DateTimeImmutable::createFromInterface($start)->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s.u\Z');
        }
        if (($config['deliver_policy'] ?? null) === 'by_start_sequence' && !isset($config['opt_start_seq'])) {
            throw new InvalidArgumentException('Consumer "' . $name . '": deliver_policy by_start_sequence needs start_sequence');
        }
        if (($config['deliver_policy'] ?? null) === 'by_start_time' && !isset($config['opt_start_time'])) {
            throw new InvalidArgumentException('Consumer "' . $name . '": deliver_policy by_start_time needs start_time');
        }
        if (isset($settings['backoff'])) {
            $config['backoff'] = array_map(
                static fn(mixed $seconds): int => self::nanoseconds($seconds, 'consumer "' . $name . '" backoff'),
                self::list($settings['backoff'], 'consumer "' . $name . '" backoff', false),
            );
        }

        return [
            'stream_name' => $stream,
            'config' => array_replace($config, self::options($settings, 'consumer "' . $name . '"')),
            'action' => '',
        ];
    }

    /**
     * The stream behind a key-value bucket, configured as NATS's own clients do.
     *
     * @param array<string, mixed> $settings
     * @return array<string, mixed>
     */
    public static function bucket(string $bucket, array $settings): array
    {
        self::known('bucket "' . $bucket . '"', $settings, self::BUCKET_KEYS);

        $history = (int) ($settings['history'] ?? 1);
        if ($history < 1 || $history > 64) {
            throw new InvalidArgumentException('Bucket "' . $bucket . '": history must be between 1 and 64');
        }
        $ttl = (float) ($settings['ttl'] ?? 0);

        $config = [
            'name' => 'KV_' . $bucket,
            'subjects' => ['$KV.' . $bucket . '.>'],
            'retention' => 'limits',
            'max_msgs_per_subject' => $history,
            'max_age' => self::nanoseconds($ttl, 'bucket "' . $bucket . '" ttl'),
            'max_bytes' => (int) ($settings['max_bytes'] ?? -1),
            'max_msg_size' => (int) ($settings['max_value_size'] ?? -1),
            'storage' => self::oneOf($settings['storage'] ?? 'file', ['file', 'memory'], 'bucket "' . $bucket . '" storage'),
            'num_replicas' => (int) ($settings['replicas'] ?? 1),
            'discard' => 'new',
            'duplicate_window' => self::nanoseconds($ttl > 0 && $ttl < 120 ? $ttl : 120, 'bucket "' . $bucket . '" ttl'),
            'allow_rollup_hdrs' => true,
            'deny_delete' => true,
            'deny_purge' => false,
            'allow_direct' => true,
        ];
        if (isset($settings['description'])) {
            $config['description'] = (string) $settings['description'];
        }
        if (!empty($settings['compression'])) {
            $config['compression'] = 's2';
        }

        return array_replace($config, self::options($settings, 'bucket "' . $bucket . '"'));
    }

    public static function nanoseconds(mixed $seconds, string $what): int
    {
        if (!is_int($seconds) && !is_float($seconds) && !(is_string($seconds) && is_numeric($seconds))) {
            throw new InvalidArgumentException(ucfirst($what) . ' must be a number of seconds');
        }
        if ((float) $seconds < 0) {
            throw new InvalidArgumentException(ucfirst($what) . ' must not be negative');
        }

        return (int) round((float) $seconds * 1_000_000_000);
    }

    /**
     * @param array<string, mixed> $settings
     * @param list<string> $allowed
     */
    private static function known(string $what, array $settings, array $allowed): void
    {
        $unknown = array_diff(array_keys($settings), $allowed);
        if ($unknown !== []) {
            throw new InvalidArgumentException(ucfirst($what) . ' has unknown settings: ' . implode(', ', $unknown)
                . '. Known settings are ' . implode(', ', $allowed) . '; pass other JetStream fields under "options"');
        }
    }

    /**
     * @return list<mixed>
     */
    private static function list(mixed $value, string $what, bool $strings = true): array
    {
        $list = is_array($value) ? array_values($value) : [$value];
        foreach ($list as $item) {
            if ($strings && (!is_string($item) || $item === '')) {
                throw new InvalidArgumentException(ucfirst($what) . ' must be a string or a list of strings');
            }
        }

        return $list;
    }

    private static function copy(array $settings, array &$config, string $from, string $to, string $type): void
    {
        if (!array_key_exists($from, $settings) || $settings[$from] === null) {
            return;
        }
        $config[$to] = match ($type) {
            'int' => (int) $settings[$from],
            'bool' => (bool) $settings[$from],
            default => (string) $settings[$from],
        };
    }

    private static function duration(array $settings, array &$config, string $from, string $to): void
    {
        if (isset($settings[$from])) {
            $config[$to] = self::nanoseconds($settings[$from], $from);
        }
    }

    /**
     * @param list<string> $choices
     */
    private static function choice(array $settings, array &$config, string $from, string $to, array $choices, string $name): void
    {
        if (isset($settings[$from])) {
            $config[$to] = self::oneOf($settings[$from], $choices, '"' . $name . '" ' . $from);
        }
    }

    /**
     * @param list<string> $choices
     */
    private static function oneOf(mixed $value, array $choices, string $what): string
    {
        $value = strtolower((string) $value);
        if (!in_array($value, $choices, true)) {
            throw new InvalidArgumentException(ucfirst($what) . ' must be one of ' . implode(', ', $choices));
        }

        return $value;
    }

    /**
     * @return array<string, mixed>
     */
    private static function options(array $settings, string $what): array
    {
        $options = $settings['options'] ?? [];
        if (!is_array($options)) {
            throw new InvalidArgumentException(ucfirst($what) . ' options must be an array of JetStream fields');
        }

        return $options;
    }
}
