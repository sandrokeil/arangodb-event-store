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

use ArangoDb\Exception\ServerException;
use ArangoDb\Handler\StatementHandler;
use ArangoDb\Http\TypeSupport;
use ArangoDb\Type\Document;
use ArangoDb\Type\DocumentType;
use ArangoDb\Util\Json;
use Fig\Http\Message\StatusCodeInterface;
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
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;

final class ProjectionManager implements ProophProjectionManager
{
    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * @var StatementHandler
     */
    protected $statementHandler;

    /**
     * @var string
     */
    protected $documentClass = Document::class;

    public function __construct(
        EventStore $eventStore,
        TypeSupport $client,
        StatementHandler $statementHandler,
        string $eventStreamsTable = 'event_streams',
        string $projectionsTable = 'projections'
    ) {
        $this->eventStore = $eventStore;
        $this->client = $client;
        $this->statementHandler = $statementHandler;
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
        return new Query($this->eventStore, $this->statementHandler, $this->eventStreamsTable);
    }

    public function createProjection(
        string $name,
        array $options = []
    ): ProophProjector {
        return new Projector(
            $this->eventStore,
            $this->client,
            $this->statementHandler,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[ProophProjector::DEFAULT_LOCK_TIMEOUT_MS] ?? ProophProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[ProophProjector::OPTION_CACHE_SIZE] ?? ProophProjector::DEFAULT_CACHE_SIZE,
            $options[ProophProjector::OPTION_PERSIST_BLOCK_SIZE] ?? ProophProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[ProophProjector::OPTION_SLEEP] ?? ProophProjector::DEFAULT_SLEEP,
            $options[ProophProjector::OPTION_PCNTL_DISPATCH] ?? ProophProjector::DEFAULT_PCNTL_DISPATCH,
            $options[ProophProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? ProophProjector::DEFAULT_UPDATE_LOCK_THRESHOLD,
            $options[Projector::OPTION_GAP_DETECTION] ?? null
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        array $options = []
    ): ProophReadModelProjector {
        return new ReadModelProjector(
            $this->eventStore,
            $this->client,
            $this->statementHandler,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[ProophReadModelProjector::OPTION_LOCK_TIMEOUT_MS] ?? ProophReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[ProophReadModelProjector::OPTION_PERSIST_BLOCK_SIZE] ?? ProophReadModelProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[ProophReadModelProjector::OPTION_SLEEP] ?? ProophReadModelProjector::DEFAULT_SLEEP,
            $options[ProophReadModelProjector::OPTION_PCNTL_DISPATCH] ?? ProophReadModelProjector::DEFAULT_PCNTL_DISPATCH,
            $options[ProophReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? ProophReadModelProjector::DEFAULT_UPDATE_LOCK_THRESHOLD,
            $options[Projector::OPTION_GAP_DETECTION] ?? null
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
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $name,
                    ['status' => $status],
                    DocumentType::FLAG_SILENT
                )
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw Exception\RuntimeException::fromServerException($e);
        }
    }

    public function resetProjection(string $name): void
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $name,
                    ['status' => ProjectionStatus::RESETTING()->getValue()],
                    DocumentType::FLAG_SILENT
                )
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw Exception\RuntimeException::fromServerException($e);
        }
    }

    public function stopProjection(string $name): void
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $name,
                    ['status' => ProjectionStatus::STOPPING()->getValue()],
                    DocumentType::FLAG_SILENT
                )
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
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
            if (empty($filter) || false === @\preg_match("/$filter/", '')) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            $where[] = 'c._key =~ @name';
            $values['name'] = $filter;
        } elseif (null !== $filter) {
            $where[] = 'c._key == @name';
            $values['name'] = $filter;
        }

        $filter = \implode(' AND ', $where);

        if (\count($where)) {
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

        try {
            $cursor = $this->statementHandler->create(
                \str_replace('%filter%', $filter, $aql),
                \array_merge(
                    [
                        '@collection' => $this->projectionsTable,
                        'offset' => $offset,
                        'limit' => $limit,
                    ],
                    $values
                ),
                100
            );

            $projectionNames = [];

            $cursor->rewind();
            while ($cursor->valid()) {
                $projectionNames[] = $cursor->current()['name'];
                $cursor->next();
            }
        } catch (ServerException $e) {
            if ($e->getCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($this->projectionsTable, $e->getResponse()->getBody()->getContents());
            }
            throw Exception\RuntimeException::fromServerException($e);
        }

        return $projectionNames;
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::read($this->projectionsTable . '/' . $name)
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw Exception\RuntimeException::fromServerException($e);
        }

        return ProjectionStatus::byValue(Json::decode($response->getBody()->getContents())['status'] ?? '');
    }

    public function fetchProjectionStreamPositions(string $name): array
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::read($this->projectionsTable . '/' . $name)
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw Exception\RuntimeException::fromServerException($e);
        }

        if ($content = $response->getBody()->getContents()) {
            return Json::decode($content)['position'] ?? [];
        }

        return [];
    }

    public function fetchProjectionState(string $name): array
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::read($this->projectionsTable . '/' . $name)
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw Exception\RuntimeException::fromServerException($e);
        }

        if ($content = $response->getBody()->getContents()) {
            return Json::decode($content)['state'] ?? [];
        }

        return [];
    }
}
