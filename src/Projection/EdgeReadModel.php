<?php

declare(strict_types=1);

namespace Prooph\EventStore\ArangoDb\Projection;

use ArangoDBClient\Collection;
use ArangoDb\Connection;
use function Prooph\EventStore\ArangoDb\Fn\execute;
use Prooph\EventStore\ArangoDb\Type\CreateCollection;
use Prooph\EventStore\ArangoDb\Type\DeleteCollection;
use Prooph\EventStore\ArangoDb\Type\InsertDocument;
use Prooph\EventStore\ArangoDb\Type\ReadCollection;
use Prooph\EventStore\ArangoDb\Type\TruncateCollection;
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
        execute(
            $this->connection,
            [
            ],
            CreateCollection::with(
                $this->collectionName,
                [
                    'keyOptions' => [
                        'allowUserKeys' => true,
                    ],
                    'type' => Collection::TYPE_EDGE,
                ]
            )
        );
    }

    public function isInitialized(): bool
    {
        try {
            execute(
                $this->connection,
                [],
                ReadCollection::with($this->collectionName)
            );
        } catch (\Throwable $e) {
            return false;
        }
        return true;
    }

    public function reset(): void
    {
        execute(
            $this->connection,
            [],
            TruncateCollection::with($this->collectionName)
        );
    }

    protected function insert(array $data)
    {
        try {
            execute(
                $this->connection,
                [],
                InsertDocument::with($this->collectionName, [$data])
            );
        } catch (\Throwable $e) {
            return false;
        }
    }

    public function delete(): void
    {
        try {
            execute(
                $this->connection,
                [
                ],
                DeleteCollection::with($this->collectionName)
            );
        } catch (\Throwable $e) {

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
