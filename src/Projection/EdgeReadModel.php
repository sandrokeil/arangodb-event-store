<?php

declare(strict_types=1);

namespace Prooph\EventStore\ArangoDb\Projection;

use ArangoDb\RequestFailedException;
use ArangoDb\Vpack;
use ArangoDBClient\Collection;
use ArangoDb\Connection;
use ArangoDBClient\Urls;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\Projection\ReadModel;

class EdgeReadModel implements ReadModel
{
    /**
     * @var array
     */
    private $stack = [];

    /**
     * Connection
     *
     * @var Connection
     */
    private $connection;

    private $collectionName;

    public function __construct(Connection $connection, string $collectionName)
    {
        $this->connection = $connection;
        $this->collectionName = $collectionName;
    }

    public function init(): void
    {
        try {
            $this->connection->post(
                Urls::URL_COLLECTION,
                Vpack::fromArray(
                    [
                        'name' => $this->collectionName,
                        'keyOptions' => [
                            'allowUserKeys' => true,
                        ],
                        'type' => Collection::TYPE_EDGE,
                    ]
                ),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function isInitialized(): bool
    {
        try {
            $this->connection->get(
                Urls::URL_COLLECTION . '/' . $this->collectionName
            );
        } catch (RequestFailedException $e) {
            return false;
        }
        return true;
    }

    public function reset(): void
    {
        try {
            $this->connection->put(
                Urls::URL_COLLECTION . '/' . $this->collectionName . '/truncate'
            );
        } catch (RequestFailedException $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    protected function insert(array $data): void
    {
        try {
            $this->connection->post(
                Urls::URL_DOCUMENT . '/' . $this->collectionName,
                Vpack::fromArray([
                    $data,
                ]),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function delete(): void
    {
        try {
            $this->connection->delete(
                Urls::URL_DOCUMENT . '/' . $this->collectionName
            );
        } catch (RequestFailedException $e) {
            // ignore
        }
    }

    public function stack(string $operation, ...$args): void
    {
        $this->stack[] = [$operation, $args];
    }

    public function persist(): void
    {
        foreach ($this->stack as list($operation, $args)) {
            $this->{$operation}(...$args);
        }

        $this->stack = [];
    }

}
