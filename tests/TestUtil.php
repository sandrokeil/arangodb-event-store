<?php
/**
 * This file is part of the prooph/arangodb-event-store.
 * (c) 2017-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2017-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\ArangoDb;

use ArangoDb\Client;
use ArangoDb\ClientOptions;
use ArangoDb\TransactionalClient;
use ArangoDb\Type\Batch;
use ArangoDb\Type\Collection;
use ArangoDb\Type\Database;
use Psr\Http\Client\ClientInterface;
use function Prooph\EventStore\ArangoDb\Fn\eventStreamsBatch;
use function Prooph\EventStore\ArangoDb\Fn\projectionsBatch;

final class TestUtil
{
    public static function getClient(): TransactionalClient
    {
        $type = 'application/' . (\getenv('USE_VPACK') === 'true' ? 'x-velocypack' : 'json');
        $params = self::getConnectionParams();

        return new TransactionalClient(
            new Client(
            $params,
            [
                'Content-Type' => [$type],
                'Accept' => [$type],
            ]
        ));
    }

    public static function createDatabase(): void
    {
        $type = 'application/' . (\getenv('USE_VPACK') === 'true' ? 'x-velocypack' : 'json');
        $params = self::getConnectionParams();

        if ($params[ClientOptions::OPTION_DATABASE] === '_system') {
            throw new \RuntimeException('"_system" database can not be created. Choose another database for tests.');
        }

        $params[ClientOptions::OPTION_DATABASE] = '_system';

        $client = new Client(
            $params,
            [
                'Content-Type' => [$type],
                'Accept' => [$type],
            ]
        );
        $client->sendRequest(Database::create(self::getDatabaseName())->toRequest());
    }

    public static function dropDatabase(): void
    {
        $type = 'application/json';
        $params = self::getConnectionParams();

        if ($params[ClientOptions::OPTION_DATABASE] === '_system') {
            throw new \RuntimeException('"_system" database can not be dropped. Choose another database for tests.');
        }

        $params[ClientOptions::OPTION_DATABASE] = '_system';

        $client = new Client(
            $params,
            [
                'Content-Type' => [$type],
                'Accept' => [$type],
            ]
        );
        $client->sendRequest(Database::delete(self::getDatabaseName())->toRequest());
    }

    public static function getDatabaseName(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return \getenv('arangodb_dbname');
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function setupCollections(ClientInterface $connection): void
    {
        $connection->sendRequest(
            Batch::fromTypes(...eventStreamsBatch())->toRequest()
        );
        $connection->sendRequest(
            Batch::fromTypes(...projectionsBatch())->toRequest()
        );
    }

    public static function deleteCollection(ClientInterface $connection, string $collection): void
    {
        try {
            $connection->sendRequest(
                Collection::delete($collection)->toRequest()
            );
        } catch (\Throwable $e) {
            // needed if test deletes collection
        }
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = \getenv();

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
            ClientOptions::OPTION_ENDPOINT => \getenv('arangodb_host'),
            ClientOptions::OPTION_DATABASE => \getenv('arangodb_dbname'),
        ];
    }
}
