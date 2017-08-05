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

use ArangoDBClient\Connection;
use ArangoDBClient\ConnectionOptions;
use ArangoDBClient\UpdatePolicy;
use ArangoDBClient\Urls;
use function Prooph\EventStore\ArangoDb\Fn\eventStreamsBatch;
use function Prooph\EventStore\ArangoDb\Fn\projectionsBatch;

final class TestUtil
{
    public static function getClient(): Connection
    {
        return new Connection(self::getConnectionParams());
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
        eventStreamsBatch($connection)->process();
        projectionsBatch($connection)->process();
    }

    public static function deleteCollection(Connection $connection, string $collection): void
    {
        try {
            $connection->delete(Urls::URL_COLLECTION . '/' . $collection);
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
            ConnectionOptions::OPTION_AUTH_TYPE => 'Basic',
            ConnectionOptions::OPTION_CONNECTION => 'Close',
            ConnectionOptions::OPTION_TIMEOUT => 3,
            ConnectionOptions::OPTION_RECONNECT => false,
            ConnectionOptions::OPTION_CREATE => false,
            ConnectionOptions::OPTION_UPDATE_POLICY => UpdatePolicy::LAST,
            ConnectionOptions::OPTION_AUTH_USER => getenv('arangodb_username'),
            ConnectionOptions::OPTION_AUTH_PASSWD => getenv('arangodb_password'),
            ConnectionOptions::OPTION_ENDPOINT => getenv('arangodb_host'),
            ConnectionOptions::OPTION_DATABASE => getenv('arangodb_dbname'),
        ];
    }
}
