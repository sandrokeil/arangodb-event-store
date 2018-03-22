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

namespace Prooph\EventStore\ArangoDb\Projection;

use ArangoDb\Connection;
use ArangoDb\Cursor;
use ArangoDb\RequestFailedException;
use ArangoDb\Vpack;
use ArangoDBClient\Statement;
use ArangoDBClient\Urls;
use Prooph\EventStore\ArangoDb\EventStore as ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\Exception;
use Prooph\EventStore\ArangoDb\Exception\ProjectionNotFound;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Projection\ProjectionManager as ProophProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Projector as ProophProjector;
use Prooph\EventStore\Projection\Query as ProophQuery;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector as ProophReadModelProjector;

final class ProjectionManager implements ProophProjectionManager
{
    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    public function __construct(
        EventStore $eventStore,
        Connection $connection,
        string $eventStreamsTable = 'event_streams',
        string $projectionsTable = 'projections'
    ) {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof ArangoDbEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function createQuery(): ProophQuery
    {
        return new Query($this->eventStore, $this->connection, $this->eventStreamsTable);
    }

    public function createProjection(
        string $name,
        array $options = []
    ): ProophProjector {
        return new Projector(
            $this->eventStore,
            $this->connection,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[ProophProjector::DEFAULT_LOCK_TIMEOUT_MS] ?? ProophProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[ProophProjector::OPTION_CACHE_SIZE] ?? ProophProjector::DEFAULT_CACHE_SIZE,
            $options[ProophProjector::OPTION_PERSIST_BLOCK_SIZE] ?? ProophProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[ProophProjector::OPTION_SLEEP] ?? ProophProjector::DEFAULT_SLEEP,
            $options[ProophProjector::OPTION_PCNTL_DISPATCH] ?? ProophProjector::DEFAULT_PCNTL_DISPATCH
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        array $options = []
    ): ProophReadModelProjector {
        return new ReadModelProjector(
            $this->eventStore,
            $this->connection,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[ProophReadModelProjector::OPTION_LOCK_TIMEOUT_MS] ?? ProophReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[ProophReadModelProjector::OPTION_PERSIST_BLOCK_SIZE] ?? ProophReadModelProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[ProophReadModelProjector::OPTION_SLEEP] ?? ProophReadModelProjector::DEFAULT_SLEEP,
            $options[ProophReadModelProjector::OPTION_PCNTL_DISPATCH] ?? ProophReadModelProjector::DEFAULT_PCNTL_DISPATCH
        );
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        if ($deleteEmittedEvents) {
            $status = ProjectionStatus::DELETING_INCL_EMITTED_EVENTS()->getValue();
        } else {
            $status = ProjectionStatus::DELETING()->getValue();
        }

        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name,
                    ['status' => $status]
                ,
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }
    }

    public function resetProjection(string $name): void
    {
        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name,
                    ['status' => ProjectionStatus::RESETTING()->getValue()],
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }
    }

    public function stopProjection(string $name): void
    {
        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name,
                    ['status' => ProjectionStatus::STOPPING()->getValue()]
                ,
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }
    }

    public function fetchProjectionNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->fetchProjectionNamesBy(false, $filter, $limit, $offset);
    }

    public function fetchProjectionNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->fetchProjectionNamesBy(true, $filter, $limit, $offset);
    }

    private function fetchProjectionNamesBy(
        bool $isRegex,
        ?string $filter,
        int $limit = 20,
        int $offset = 0
    ) {
        if (1 > $limit) {
            throw new OutOfRangeException(
                'Invalid limit "'.$limit.'" given. Must be greater than 0.'
            );
        }

        if (0 > $offset) {
            throw new OutOfRangeException(
                'Invalid offset "'.$offset.'" given. Must be greater or equal than 0.'
            );
        }

        $values = [];
        $where = [];

        if ($isRegex) {
            if (empty($filter) || false === @preg_match("/$filter/", '')) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            $where[] = 'c._key =~ @name';
            $values['name'] = $filter;
        } elseif (null !== $filter) {
            $where[] = 'c._key == @name';
            $values['name'] = $filter;
        }

        $filter = implode(' AND ', $where);

        if (count($where)) {
            $filter = ' FILTER ' . $filter;
        }

        $aql = <<<'EOF'
FOR c IN  @@collection
%filter%
SORT c._key ASC
LIMIT @offset, @limit
RETURN {
    "name": c._key
}
EOF;
        $cursor = $this->connection->query(
                [
                    Statement::ENTRY_QUERY => str_replace('%filter%', $filter, $aql),
                    Statement::ENTRY_BINDVARS => array_merge(
                        [
                            '@collection' => $this->projectionsTable,
                            'offset' => $offset,
                            'limit' => $limit,
                        ],
                        $values
                    ),
                    Statement::ENTRY_BATCHSIZE => 100,
                ],
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );

        $projectionNames = [];

        try {
            $cursor->rewind();
            while ($cursor->valid()) {
                $projectionNames[] = $cursor->current()['name'];
                $cursor->next();
            }
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($this->projectionsTable, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }

        return $projectionNames;
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        try {
            $response = $this->connection->get(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }

        return ProjectionStatus::byValue($response->get('status'));
    }

    public function fetchProjectionStreamPositions(string $name): array
    {
        try {
            $response = $this->connection->get(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }
        $result = json_decode($response->getBody(), true);

        return $result['position'];
    }

    public function fetchProjectionState(string $name): array
    {
        try {
            $response = $this->connection->get(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $name
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($name, $e->getBody());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }
        $result = json_decode($response->getBody(), true);

        return $result['state'];
    }
}
