<?php
/**
 * This file is part of the prooph/arangodb-event-store.
 * (c) 2017 prooph software GmbH <contact@prooph.de>
 * (c) 2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\ArangoDb;

use ArangoDb\Connection;
use ArangoDb\Vpack;
use ArangoDBClient\UpdatePolicy;
use ArangoDBClient\Urls;
use function Prooph\EventStore\ArangoDb\Fn\eventStreamsBatch;
use function Prooph\EventStore\ArangoDb\Fn\execute;
use function Prooph\EventStore\ArangoDb\Fn\projectionsBatch;

final class TestUtil
{
    public static function getClient(): Connection
    {
        $connection = new Connection(self::getConnectionParams());
        $connection->connect();
        return $connection;
    }

    public static function getDatabaseName(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return $GLOBALS['arangodb_dbname'];
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function setupCollections(Connection $connection): void
    {
        execute($connection, null, ...eventStreamsBatch());
        execute($connection, null, ...projectionsBatch());
    }

    public static function deleteCollection(Connection $connection, string $collection): void
    {
        try {
            $connection->delete(Urls::URL_COLLECTION . '/' . $collection, Vpack::fromArray([]));
        } catch (\ArangoDBClient\ServerException $e) {
            // needed if test deletes collection
        }
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = getenv();

        return isset(
            $env['arangodb_username'],
            $env['arangodb_password'],
            $env['arangodb_host'],
            $env['arangodb_dbname']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            Connection::HOST => getenv('arangodb_host'),
            Connection::MAX_CHUNK_SIZE => 64,
            Connection::VST_VERSION => Connection::VST_VERSION_11,
        ];
    }
}
