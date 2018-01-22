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
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\EventStore as ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\Exception\ProjectionAlreadyExistsException;
use Prooph\EventStore\ArangoDb\Exception\ProjectionNotFound;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector as ProophReadModelProjector;
use Prooph\EventStore\StreamName;

final class ReadModelProjector implements ProophReadModelProjector
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
    private $name;

    /**
     * @var ReadModel
     */
    private $readModel;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * @var array
     */
    private $streamPositions = [];

    /**
     * @var int
     */
    private $persistBlockSize;

    /**
     * @var array
     */
    private $state = [];

    /**
     * @var ProjectionStatus
     */
    private $status;

    /**
     * @var callable|null
     */
    private $initCallback;

    /**
     * @var Closure|null
     */
    private $handler;

    /**
     * @var array
     */
    private $handlers = [];

    /**
     * @var boolean
     */
    private $isStopped = false;

    /**
     * @var ?string
     */
    private $currentStreamName;

    /**
     * @var int lock timeout in milliseconds
     */
    private $lockTimeoutMs;

    /**
     * @var int
     */
    private $eventCounter = 0;

    /**
     * @var int
     */
    private $sleep;

    /**
     * @var bool
     */
    private $triggerPcntlSignalDispatch;

    /**
     * @var array|null
     */
    private $query;

    public function __construct(
        EventStore $eventStore,
        Connection $connection,
        string $name,
        ReadModel $readModel,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $persistBlockSize,
        int $sleep,
        bool $triggerPcntlSignalDispatch = false
    ) {
        if ($triggerPcntlSignalDispatch && !extension_loaded('pcntl')) {
            throw Exception\ExtensionNotLoadedException::withName('pcntl');
        }

        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->name = $name;
        $this->readModel = $readModel;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
        $this->persistBlockSize = $persistBlockSize;
        $this->sleep = $sleep;
        $this->status = ProjectionStatus::IDLE();
        $this->triggerPcntlSignalDispatch = $triggerPcntlSignalDispatch;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (!$eventStore instanceof ArangoDbEventStore
        ) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): ProophReadModelProjector
    {
        if (null !== $this->initCallback) {
            throw new Exception\RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;

        return $this;
    }

    public function fromStreams(string ...$streamNames): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    public function fromCategory(string $name): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    public function fromCategories(string ...$names): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    public function fromAll(): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    public function when(array $handlers): ProophReadModelProjector
    {
        if (null !== $this->handler || !empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (!is_string($eventName)) {
                throw new Exception\InvalidArgumentException('Invalid event name given, string expected');
            }

            if (!$handler instanceof Closure) {
                throw new Exception\InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler,
                $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): ProophReadModelProjector
    {
        if (null !== $this->handler || !empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function reset(): void
    {
        $this->streamPositions = [];

        $callback = $this->initCallback;

        $this->readModel->reset();

        $this->state = [];

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;
            }
        }

        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name,
                Vpack::fromArray(
                    [
                        'state' => $this->state,
                        'status' => ProjectionStatus::STOPPING()->getValue(),
                        'position' => $this->streamPositions,
                    ]
                ),
                [
                    'silent' => true,
                    'mergeObjects' => false,
                ]
            );
        } catch (RequestFailedException $e) {
            // ignore not found error
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }
    }

    public function stop(): void
    {
        $this->isStopped = true;

        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name,
                Vpack::fromArray(
                    [
                        'status' => ProjectionStatus::IDLE()->getValue(),
                    ]
                ),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            // ignore not found error
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }

        $this->status = ProjectionStatus::IDLE();
    }

    public function delete(bool $deleteProjection): void
    {
        try {
            $this->connection->delete(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable,
                Vpack::fromArray(
                    [$this->name]
                )
            );
        } catch (RequestFailedException $e) {
            // ignore
            if ($e->getHttpCode() !== 422) {
                throw RuntimeException::fromServerException($e);
            }
        }

        if ($deleteProjection) {
            $this->readModel->delete();
        }

        $this->isStopped = true;

        $callback = $this->initCallback;

        $this->state = [];

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;
            }
        }

        $this->streamPositions = [];
    }

    public function getState(): array
    {
        return $this->state;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function readModel(): ReadModel
    {
        return $this->readModel;
    }

    public function run(bool $keepRunning = true): void
    {
        if (null === $this->query
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        switch ($this->fetchRemoteStatus()) {
            case ProjectionStatus::STOPPING():
                $this->stop();

                return;
            case ProjectionStatus::DELETING():
                $this->delete(false);

                return;
            case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                $this->delete(true);

                return;
            case ProjectionStatus::RESETTING():
                $this->reset();
                break;
            default:
                break;
        }

        $this->createProjection();
        $this->acquireLock();

        if (!$this->readModel->isInitialized()) {
            $this->readModel->init();
        }

        $this->prepareStreamPositions();
        $this->load();

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;

        try {
            do {
                foreach ($this->streamPositions as $streamName => $position) {
                    try {
                        $streamEvents = $this->eventStore->load(new StreamName($streamName), $position + 1);
                    } catch (Exception\StreamNotFound $e) {
                        // ignore
                        continue;
                    }

                    if ($singleHandler) {
                        $this->handleStreamWithSingleHandler($streamName, $streamEvents);
                    } else {
                        $this->handleStreamWithHandlers($streamName, $streamEvents);
                    }

                    if ($this->isStopped) {
                        break;
                    }
                }

                if (0 === $this->eventCounter) {
                    usleep($this->sleep);
                    $this->updateLock();
                } else {
                    $this->persist();
                }

                $this->eventCounter = 0;

                if ($this->triggerPcntlSignalDispatch) {
                    pcntl_signal_dispatch();
                }

                switch ($this->fetchRemoteStatus()) {
                    case ProjectionStatus::STOPPING():
                        $this->stop();
                        break;
                    case ProjectionStatus::DELETING():
                        $this->delete(false);
                        break;
                    case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS():
                        $this->delete(true);
                        break;
                    case ProjectionStatus::RESETTING():
                        $this->reset();
                        break;
                    default:
                        break;
                }

                $this->prepareStreamPositions();
            } while ($keepRunning && !$this->isStopped);
        } catch (ProjectionAlreadyExistsException $projectionAlreadyExistsException) {
            // throw it in finally
        } finally {
            if (isset($projectionAlreadyExistsException)) {
                throw $projectionAlreadyExistsException;
            }
            $this->releaseLock();
        }
    }

    private function fetchRemoteStatus(): ProjectionStatus
    {
        $response = null;

        try {
            $response = $this->connection->get(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name
            );
        } catch (RequestFailedException $e) {
            // ignore
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }

        if (!$response) {
            return ProjectionStatus::RUNNING();
        }

        $status = $response->get('status');

        if (empty($status)) {
            return ProjectionStatus::RUNNING();
        }

        return ProjectionStatus::byValue($status);
    }

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;
            $this->eventCounter++;

            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function handleStreamWithHandlers(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;

            if (!isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $this->eventCounter++;

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName)
        {
            /**
             * @var ReadModelProjector
             */
            private $projector;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(ReadModelProjector $projector, ?string &$streamName)
            {
                $this->projector = $projector;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->projector->stop();
            }

            public function readModel(): ReadModel
            {
                return $this->projector->readModel();
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    private function load(): void
    {
        $response = null;

        try {
            $response = $this->connection->get(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name
            );
        } catch (RequestFailedException $e) {
            // ignore
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }
        if (!$response) {
            return;
        }

        $result = json_decode($response->getBody(), true);

        if (isset($result['position'], $result['state'])) {
            $this->streamPositions = array_merge($this->streamPositions, $result['position']);
            $state = $result['state'];

            if (!empty($state)) {
                $this->state = $state;
            }
        }
    }

    private function createProjection(): void
    {
        try {
            $this->connection->post(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable,
                Vpack::fromArray([
                    [
                        '_key' => $this->name,
                        'position' => (object)null,
                        'state' => (object)null,
                        'status' => $this->status->getValue(),
                        'locked_until' => null,
                    ],
                ]),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            // we ignore any occurring error here (duplicate projection)
            $httpCode = $e->getHttpCode();

            if ($httpCode !== 400 && $httpCode !== 404 && $httpCode !== 409) {
                throw RuntimeException::fromServerException($e);
            }
        }
    }

    private function acquireLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $nowString = $now->format('Y-m-d\TH:i:s.u');

        $lockUntilString = $this->createLockUntilString($now);

        $aql = <<<'EOF'
FOR c IN @@collection
FILTER c._key == @name AND (c.locked_until == null OR c.locked_until < @nowString)
UPDATE c WITH 
{ 
    locked_until: @lockedUntil, 
    status: @status
} 
IN @@collection
RETURN NEW
EOF;

        $cursor = $this->connection->query(
            Vpack::fromArray(
                [
                    Statement::ENTRY_QUERY => $aql,
                    Statement::ENTRY_BINDVARS => [
                        '@collection' => $this->projectionsTable,
                        'name' => $this->name,
                        'lockedUntil' => $lockUntilString,
                        'nowString' => $nowString,
                        'status' => ProjectionStatus::RUNNING()->getValue(),
                    ],
                    Statement::ENTRY_BATCHSIZE => 1000,
                ]
            ),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );

        try {
            $cursor->rewind();
            if ($cursor->count() === 0) {
                throw new Exception\RuntimeException('Another projection process is already running');
            }
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($this->projectionsTable, $e->getBody());
            }
            throw RuntimeException::fromServerException($e);
        }

        $this->status = ProjectionStatus::RUNNING();
    }

    private function updateLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        $lockUntilString = $this->createLockUntilString($now);

        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name,
                Vpack::fromArray(
                    [
                        'locked_until' => $lockUntilString,
                    ]
                ),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            if ($e->getHttpCode() === 404) {
                throw ProjectionNotFound::with($this->name, $e->getBody());
            }
            throw RuntimeException::fromServerException($e);
        }
    }

    private function releaseLock(): void
    {
        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name,
                Vpack::fromArray(
                    [
                        'status' => ProjectionStatus::IDLE()->getValue(),
                        'locked_until' => null,
                    ]
                ),
                ['silent' => true]
            );
        } catch (RequestFailedException $e) {
            //  ignore not found error
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }

        $this->status = ProjectionStatus::IDLE();
    }

    private function persist(): void
    {
        $this->readModel->persist();

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        $lockUntilString = $this->createLockUntilString($now);

        try {
            $this->connection->patch(
                Urls::URL_DOCUMENT . '/' . $this->projectionsTable . '/' . $this->name,
                Vpack::fromArray(
                    [
                        'position' => $this->streamPositions,
                        'state' => $this->state,
                        'locked_until' => $lockUntilString,
                    ]
                ),
                [
                    'silent' => true,
                    'mergeObjects' => false,
                ]
            );
        } catch (RequestFailedException $e) {
            //  ignore not found error
            if ($e->getHttpCode() !== 404) {
                throw RuntimeException::fromServerException($e);
            }
        }
    }

    private function prepareStreamPositions(): void
    {
        $streamPositions = [];

        if (isset($this->query['all'])) {
            $aql = <<<'EOF'
FOR c IN  @@collection
FILTER c.real_stream_name !~ '^\\$'
RETURN {
    "real_stream_name": c.real_stream_name
}
EOF;

            $cursor = $this->connection->query(
                Vpack::fromArray(
                    [
                        Statement::ENTRY_QUERY => $aql,
                        Statement::ENTRY_BINDVARS => [
                            '@collection' => $this->eventStreamsTable,
                        ],
                        Statement::ENTRY_BATCHSIZE => 1000,
                    ]
                ),
                [
                    Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
                ]
            );

            $cursor->rewind();

            while ($cursor->valid()) {
                $streamPositions[$cursor->current()['real_stream_name']] = 0;
                $cursor->next();
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        if (isset($this->query['categories'])) {
            $aql = <<<'EOF'
FOR c IN  @@collection
FILTER c.category IN @categories
RETURN {
    "real_stream_name": c.real_stream_name
}
EOF;

            $cursor = $this->connection->query(
                Vpack::fromArray(
                    [
                        Statement::ENTRY_QUERY => $aql,
                        Statement::ENTRY_BINDVARS => [
                            '@collection' => $this->eventStreamsTable,
                            'categories' => $this->query['categories'],
                        ],
                        Statement::ENTRY_BATCHSIZE => 1000,
                    ]
                ),
                [
                    Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
                ]
            );

            $cursor->rewind();

            while ($cursor->valid()) {
                $streamPositions[$cursor->current()['real_stream_name']] = 0;
                $cursor->next();
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        // stream names given
        foreach ($this->query['streams'] as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->streamPositions = array_merge($streamPositions, $this->streamPositions);
    }

    private function createLockUntilString(DateTimeImmutable $from): string
    {
        $micros = (string)((int)$from->format('u') + ($this->lockTimeoutMs * 1000));

        $secs = substr($micros, 0, -6);

        if ('' === $secs) {
            $secs = 0;
        }

        $resultMicros = substr($micros, -6);

        return $from->modify('+' . $secs . ' seconds')->format('Y-m-d\TH:i:s') . '.' . $resultMicros;
    }
}
