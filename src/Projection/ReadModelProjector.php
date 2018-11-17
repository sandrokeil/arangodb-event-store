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
use ArangoDb\Statement;
use ArangoDb\Type\Cursor;
use ArangoDb\Type\Document;
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use Fig\Http\Message\StatusCodeInterface;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\EventStore as ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\Exception\ProjectionAlreadyExistsException;
use Prooph\EventStore\ArangoDb\Exception\ProjectionNotFound;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector as ProophReadModelProjector;
use Prooph\EventStore\StreamName;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use function Prooph\EventStore\ArangoDb\Fn\responseContentAsArray;

final class ReadModelProjector implements ProophReadModelProjector
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

    /**
     * @var MetadataMatcher|null
     */
    private $metadataMatcher;

    public function __construct(
        EventStore $eventStore,
        ClientInterface $client,
        string $name,
        ReadModel $readModel,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $persistBlockSize,
        int $sleep,
        bool $triggerPcntlSignalDispatch = false
    ) {
        if ($triggerPcntlSignalDispatch && ! \extension_loaded('pcntl')) {
            throw Exception\ExtensionNotLoadedException::withName('pcntl');
        }

        $this->eventStore = $eventStore;
        $this->client = $client;
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

        if (! $eventStore instanceof ArangoDbEventStore
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

        if (\is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName, MetadataMatcher $metadataMatcher = null): ProophReadModelProjector
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;
        $this->metadataMatcher = $metadataMatcher;

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
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! \is_string($eventName)) {
                throw new Exception\InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new Exception\InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler,
                $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): ProophReadModelProjector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
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

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
                $this->state = $result;
            }
        }

        try {
            $update = Document::updateOne(
                $this->projectionsTable . '/' . $this->name,
                [
                    'state' => $this->state,
                    'status' => ProjectionStatus::STOPPING()->getValue(),
                    'position' => $this->streamPositions,
                ],
                Document::FLAG_REPLACE_OBJECTS | Document::FLAG_SILENT
            );
            $response = $this->client->sendRequest($update->toRequest());
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $update);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }

        try {
            $this->eventStore->delete(new StreamName($this->name));
        } catch (Exception\StreamNotFound $exception) {
            // ignore
        }
    }

    public function stop(): void
    {
        $this->isStopped = true;

        try {
            $update = Document::updateOne(
                $this->projectionsTable . '/' . $this->name,
                [
                    'status' => ProjectionStatus::IDLE()->getValue(),
                ],
                Document::FLAG_SILENT
            );
            $response = $this->client->sendRequest($update->toRequest());
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $update);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
        $this->status = ProjectionStatus::IDLE();
    }

    public function delete(bool $deleteProjection): void
    {
        try {
            $delete = Document::deleteOne($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendRequest($delete->toRequest());

            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_UNPROCESSABLE_ENTITY
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $delete);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }

        if ($deleteProjection) {
            $this->readModel->delete();
        }

        $this->isStopped = true;

        $callback = $this->initCallback;

        $this->state = [];

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
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

        if (! $this->readModel->isInitialized()) {
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
                        $streamEvents = $this->eventStore->load(new StreamName($streamName), $position + 1, null, $this->metadataMatcher);
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
                    \usleep($this->sleep);
                    $this->updateLock();
                } else {
                    $this->persist();
                }

                $this->eventCounter = 0;

                if ($this->triggerPcntlSignalDispatch) {
                    \pcntl_signal_dispatch();
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
            } while ($keepRunning && ! $this->isStopped);
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
            $read = Document::read($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendRequest($read->toRequest());
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $read);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }
        if (! $response) {
            return ProjectionStatus::RUNNING();
        }
        $status = responseContentAsArray($response, 'status');

        if (empty($status)) {
            return ProjectionStatus::RUNNING();
        }

        return ProjectionStatus::byValue($status);
    }

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                \pcntl_signal_dispatch();
            }
            /* @var Message $event */
            $this->streamPositions[$streamName] = $key;
            $this->eventCounter++;

            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;

                $this->status = $this->fetchRemoteStatus();

                if (! $this->status->is(ProjectionStatus::RUNNING()) && ! $this->status->is(ProjectionStatus::IDLE())) {
                    $this->isStopped = true;
                }
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function handleStreamWithHandlers(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;

        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                \pcntl_signal_dispatch();
            }
            /* @var Message $event */
            $this->streamPositions[$streamName] = $key;

            if (! isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $this->eventCounter++;

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
            }

            if ($this->eventCounter === $this->persistBlockSize) {
                $this->persist();
                $this->eventCounter = 0;

                $this->status = $this->fetchRemoteStatus();

                if (! $this->status->is(ProjectionStatus::RUNNING()) && ! $this->status->is(ProjectionStatus::IDLE())) {
                    $this->isStopped = true;
                }
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    private function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName) {
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
            $read = Document::read($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendRequest($read->toRequest());
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $read);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }
        if (! $response) {
            return;
        }
        $result = responseContentAsArray($response);

        if (isset($result['position'], $result['state'])) {
            $this->streamPositions = \array_merge($this->streamPositions, $result['position']);
            $state = $result['state'];

            if (! empty($state)) {
                $this->state = $state;
            }
        }
    }

    private function createProjection(): void
    {
        try {
            $create = Document::create(
                $this->projectionsTable,
                [
                    '_key' => $this->name,
                    'position' => (object) null,
                    'state' => (object) null,
                    'status' => $this->status->getValue(),
                    'locked_until' => null,
                ],
                Document::FLAG_SILENT
            );
            $response = $this->client->sendRequest($create->toRequest());
            $httpStatusCode = $response->getStatusCode();

            // we ignore any occurring error here (duplicate projection)
            if (
                (
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_BAD_REQUEST
                && $httpStatusCode !== StatusCodeInterface::STATUS_CONFLICT
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $create);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
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

        try {
            $cursor = new Statement(
                $this->client,
                Cursor::create(
                    $aql,
                    [
                        '@collection' => $this->projectionsTable,
                        'name' => $this->name,
                        'lockedUntil' => $lockUntilString,
                        'nowString' => $nowString,
                        'status' => ProjectionStatus::RUNNING()->getValue(),
                    ]
                )->toRequest(),
                [Statement::ENTRY_TYPE => Statement::ENTRY_TYPE_ARRAY]
            );

            if (\count($cursor) === 0) {
                throw new Exception\RuntimeException('Another projection process is already running');
            }
        } catch (ServerException $e) {
            if ($e->getCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($this->projectionsTable, $e->getResponse()->getBody()->getContents());
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
            $response = $this->client->sendRequest(
                Document::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    ['locked_until' => $lockUntilString],
                    Document::FLAG_SILENT
                )->toRequest()
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($this->name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    private function releaseLock(): void
    {
        try {
            $response = $this->client->sendRequest(
                Document::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    [
                        'status' => ProjectionStatus::IDLE()->getValue(),
                        'locked_until' => null,
                    ],
                    Document::FLAG_SILENT
                )->toRequest()
            );
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw ProjectionNotFound::with($this->name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }

        $this->status = ProjectionStatus::IDLE();
    }

    private function persist(): void
    {
        $this->readModel->persist();

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $lockUntilString = $this->createLockUntilString($now);

        try {
            $response = $this->client->sendRequest(
                Document::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    [
                        'position' => $this->streamPositions,
                        'state' => $this->state,
                        'locked_until' => $lockUntilString,
                    ],
                    Document::FLAG_REPLACE_OBJECTS | Document::FLAG_SILENT
                )->toRequest()
            );
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw ProjectionNotFound::with($this->name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
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

            try {
                $cursor = new Statement(
                    $this->client,
                    Cursor::create(
                        $aql,
                        [
                            '@collection' => $this->eventStreamsTable,
                        ],
                        1000
                    )->toRequest(),
                    [Statement::ENTRY_TYPE => Statement::ENTRY_TYPE_ARRAY]
                );

                $cursor->rewind();
                while ($cursor->valid()) {
                    $streamPositions[$cursor->current()['real_stream_name']] = 0;
                    $cursor->next();
                }
                $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);

                return;
            } catch (ServerException $e) {
                throw RuntimeException::fromServerException($e);
            }
        }

        if (isset($this->query['categories'])) {
            $aql = <<<'EOF'
FOR c IN  @@collection
FILTER c.category IN @categories
RETURN {
    "real_stream_name": c.real_stream_name
}
EOF;
            try {
                $cursor = new Statement(
                    $this->client,
                    Cursor::create(
                        $aql,
                        [
                            '@collection' => $this->eventStreamsTable,
                            'categories' => $this->query['categories'],
                        ],
                        1000
                    )->toRequest(),
                    [Statement::ENTRY_TYPE => Statement::ENTRY_TYPE_ARRAY]
                );

                $cursor->rewind();
                while ($cursor->valid()) {
                    $streamPositions[$cursor->current()['real_stream_name']] = 0;
                    $cursor->next();
                }
                $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);

                return;
            } catch (ServerException $e) {
                throw RuntimeException::fromServerException($e);
            }
        }

        // stream names given
        foreach ($this->query['streams'] as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->streamPositions = \array_merge($streamPositions, $this->streamPositions);
    }

    private function createLockUntilString(DateTimeImmutable $from): string
    {
        $micros = (string) ((int) $from->format('u') + ($this->lockTimeoutMs * 1000));

        $secs = \substr($micros, 0, -6);

        if ('' === $secs) {
            $secs = 0;
        }

        $resultMicros = \substr($micros, -6);

        return $from->modify('+' . $secs . ' seconds')->format('Y-m-d\TH:i:s') . '.' . $resultMicros;
    }
}
