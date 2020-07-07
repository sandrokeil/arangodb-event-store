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

use ArangoDb\Handler\Statement;
use ArangoDb\Http\Client;
use ArangoDb\Http\ClientOptions;
use ArangoDb\Http\TransactionalClient;
use ArangoDb\Http\TransactionSupport;
use ArangoDb\Http\TypeSupport;
use ArangoDb\Statement\ArrayStreamHandlerFactory;
use ArangoDb\Statement\StreamHandlerFactoryInterface;
use ArangoDb\Type\Batch;
use ArangoDb\Type\Collection;
use ArangoDb\Type\Database;
use Fig\Http\Message\StatusCodeInterface;
use Laminas\Diactoros\Request;
use Laminas\Diactoros\Response;
use Laminas\Diactoros\StreamFactory;
use function Prooph\EventStore\ArangoDb\Func\eventStreamsBatch;
use function Prooph\EventStore\ArangoDb\Func\projectionsBatch;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamFactoryInterface;

final class TestUtil
{
    /**
     * @var Client
     */
    private static $connection;

    /**
     * @var TransactionalClient
     */
    private static $transactionalConnection;

    public static function getStatementHandler(): Statement
    {
        return new Statement(self::getClient(), self::getStreamHandlerFactory());
    }

    public static function getClient($sharedConnection = false): TypeSupport
    {
        if (! isset(self::$connection) || ! $sharedConnection) {
            $params = self::getConnectionParams();

            $connection = new Client(
                $params,
                self::getRequestFactory(),
                self::getResponseFactory(),
                self::getStreamFactory()
            );
            if (! $sharedConnection) {
                return $connection;
            }
            self::$connection = $connection;
        }

        return self::$connection;
    }

    public static function getTransactionalClient($sharedConnection = false): TransactionSupport
    {
        if (! isset(self::$transactionalConnection) || ! $sharedConnection) {
            $connection = new TransactionalClient(
                self::getClient(),
                self::getResponseFactory()
            );

            if (! $sharedConnection) {
                return $connection;
            }

            self::$transactionalConnection = $connection;
        }

        return self::$transactionalConnection;
    }

    public static function getResponseFactory(): ResponseFactoryInterface
    {
        return new class() implements ResponseFactoryInterface {
            public function createResponse(int $code = 200, string $reasonPhrase = ''): ResponseInterface
            {
                $response = new Response();

                if ($reasonPhrase !== '') {
                    return $response->withStatus($code, $reasonPhrase);
                }

                return $response->withStatus($code);
            }
        };
    }

    public static function getRequestFactory(): RequestFactoryInterface
    {
        return new class() implements RequestFactoryInterface {
            public function createRequest(string $method, $uri): RequestInterface
            {
                $type = 'application/json';

                $request = new Request($uri, $method);
                $request = $request->withAddedHeader('Content-Type', $type);

                return $request->withAddedHeader('Accept', $type);
            }
        };
    }

    public static function getStreamFactory(): StreamFactoryInterface
    {
        return new StreamFactory();
    }

    public static function getStreamHandlerFactory(): StreamHandlerFactoryInterface
    {
        return new ArrayStreamHandlerFactory();
    }

    public static function createDatabase(): void
    {
        $params = self::getConnectionParams();

        if ($params[ClientOptions::OPTION_DATABASE] === '_system') {
            throw new \RuntimeException('"_system" database can not be created. Choose another database for tests.');
        }

        $params[ClientOptions::OPTION_DATABASE] = '_system';

        $client = new Client($params, self::getRequestFactory(), self::getResponseFactory(), self::getStreamFactory());
        $response = $client->sendRequest(
            Database::create(self::getDatabaseName())->toRequest(self::getRequestFactory(), self::getStreamFactory())
        );

        if ($response->getStatusCode() !== StatusCodeInterface::STATUS_CREATED) {
            self::dropDatabase();
            throw new \RuntimeException($response->getBody()->getContents());
        }
    }

    public static function dropDatabase(): void
    {
        $params = self::getConnectionParams();

        if ($params[ClientOptions::OPTION_DATABASE] === '_system') {
            throw new \RuntimeException('"_system" database can not be dropped. Choose another database for tests.');
        }

        $params[ClientOptions::OPTION_DATABASE] = '_system';

        $client = new Client(
            $params,
            self::getRequestFactory(),
            self::getResponseFactory(),
            self::getStreamFactory()
        );
        $client->sendRequest(
            Database::delete(self::getDatabaseName())->toRequest(self::getRequestFactory(), self::getStreamFactory())
        );
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

    public static function setupCollections(TypeSupport $connection): void
    {
        $connection->sendType(
            Batch::fromTypes(...eventStreamsBatch())
        );
        $connection->sendType(
            Batch::fromTypes(...projectionsBatch())
        );
    }

    public static function deleteCollection(TypeSupport $connection, string $collection): void
    {
        try {
            $connection->sendType(
                Collection::delete($collection)
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
