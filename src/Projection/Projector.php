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
use ArrayIterator;
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use EmptyIterator;
use Fig\Http\Message\StatusCodeInterface;
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
use Prooph\EventStore\Projection\Projector as ProophProjector;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamIterator\MergedStreamIterator;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\Util\ArrayCache;
use Psr\Http\Client\ClientExceptionInterface;

final class Projector implements ProophProjector
{
    public const OPTION_GAP_DETECTION = 'gap_detection';

    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var TypeSupport
     */
    private $client;

    /**
     * @var string
     */
    private $name;

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
     * @var ArrayCache
     */
    private $cachedStreamNames;

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
     * @var int
     */
    private $updateLockThreshold;

    /**
     * @var array|null
     */
    private $query;

    /**
     * @var bool
     */
    private $streamCreated = false;

    /**
     * @var DateTimeImmutable
     */
    private $lastLockUpdate;

    /**
     * @var MetadataMatcher|null
     */
    private $metadataMatcher;

    /**
     * @var GapDetection|null
     */
    private $gapDetection;

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
        string $name,
        string $eventStreamsTable,
        string $projectionsTable,
        int $lockTimeoutMs,
        int $cacheSize,
        int $persistBlockSize,
        int $sleep,
        bool $triggerPcntlSignalDispatch = false,
        int $updateLockThreshold = 0,
        GapDetection $gapDetection = null
    ) {
        if ($triggerPcntlSignalDispatch && ! \extension_loaded('pcntl')) {
            throw Exception\ExtensionNotLoadedException::withName('pcntl');
        }

        $this->eventStore = $eventStore;
        $this->client = $client;
        $this->statementHandler = $statementHandler;
        $this->name = $name;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;
        $this->lockTimeoutMs = $lockTimeoutMs;
        $this->cachedStreamNames = new ArrayCache($cacheSize);
        $this->persistBlockSize = $persistBlockSize;
        $this->sleep = $sleep;
        $this->status = ProjectionStatus::IDLE();
        $this->triggerPcntlSignalDispatch = $triggerPcntlSignalDispatch;
        $this->updateLockThreshold = $updateLockThreshold;
        $this->gapDetection = $gapDetection;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof ArangoDbEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): ProophProjector
    {
        if (null !== $this->initCallback) {
            throw new RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (\is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName, MetadataMatcher $metadataMatcher = null): ProophProjector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;
        $this->metadataMatcher = $metadataMatcher;

        return $this;
    }

    public function fromStreams(string ...$streamNames): ProophProjector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    public function fromCategory(string $name): ProophProjector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    public function fromCategories(string ...$names): ProophProjector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    public function fromAll(): ProophProjector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    public function when(array $handlers): ProophProjector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
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

    public function whenAny(Closure $handler): ProophProjector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function emit(Message $event): void
    {
        if (! $this->streamCreated || ! $this->eventStore->hasStream(new StreamName($this->name))) {
            $this->eventStore->create(new Stream(new StreamName($this->name), new EmptyIterator()));
            $this->streamCreated = true;
        }

        $this->linkTo($this->name, $event);
    }

    public function linkTo(string $streamName, Message $event): void
    {
        $sn = new StreamName($streamName);

        if ($this->cachedStreamNames->has($streamName)) {
            $append = true;
        } else {
            $this->cachedStreamNames->rollingAppend($streamName);
            $append = $this->eventStore->hasStream($sn);
        }

        if ($append) {
            $this->eventStore->appendTo($sn, new ArrayIterator([$event]));
        } else {
            $this->eventStore->create(new Stream($sn, new ArrayIterator([$event])));
        }
    }

    public function reset(): void
    {
        $this->streamPositions = [];

        $callback = $this->initCallback;

        $this->state = [];

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
                $this->state = $result;
            }
        }

        try {
            $update = ($this->documentClass)::updateOne(
                $this->projectionsTable . '/' . $this->name,
                [
                    'state' => $this->state,
                    'status' => ProjectionStatus::STOPPING()->getValue(),
                    'position' => $this->streamPositions,
                ],
                DocumentType::FLAG_REPLACE_OBJECTS | DocumentType::FLAG_SILENT
            );
            $response = $this->client->sendType($update);
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response);
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
            $update = ($this->documentClass)::updateOne(
                $this->projectionsTable . '/' . $this->name,
                [
                    'status' => ProjectionStatus::IDLE()->getValue(),
                ],
                DocumentType::FLAG_SILENT
            );
            $response = $this->client->sendType($update);
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
        $this->status = ProjectionStatus::IDLE();
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        try {
            $delete = ($this->documentClass)::deleteOne($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendType($delete);

            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_UNPROCESSABLE_ENTITY
            ) {
                throw RuntimeException::fromErrorResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }

        if ($deleteEmittedEvents) {
            try {
                $this->eventStore->delete(new StreamName($this->name));
            } catch (Exception\StreamNotFound $e) {
                // ignore
            }
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

    public function run(bool $keepRunning = true): void
    {
        if (null === $this->query
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new RuntimeException('No handlers configured');
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

        $this->prepareStreamPositions();
        $this->load();

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;

        try {
            do {
                $eventStreams = [];

                foreach ($this->streamPositions as $streamName => $position) {
                    try {
                        $eventStreams[$streamName] = $this->eventStore->load(new StreamName($streamName), $position + 1,
                            null, $this->metadataMatcher);
                    } catch (Exception\StreamNotFound $e) {
                        // ignore
                        continue;
                    }
                }

                $streamEvents = new MergedStreamIterator(\array_keys($eventStreams), ...\array_values($eventStreams));

                if ($singleHandler) {
                    $gapDetected = ! $this->handleStreamWithSingleHandler($streamEvents);
                } else {
                    $gapDetected = ! $this->handleStreamWithHandlers($streamEvents);
                }

                if ($gapDetected && $this->gapDetection) {
                    $sleep = $this->gapDetection->getSleepForNextRetry();

                    \usleep($sleep);
                    $this->gapDetection->trackRetry();
                    $this->persist();
                } else {
                    $this->gapDetection && $this->gapDetection->resetRetries();

                    if (0 === $this->eventCounter) {
                        \usleep($this->sleep);
                        $this->updateLock();
                    } else {
                        $this->persist();
                    }
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
                        if ($keepRunning) {
                            $this->startAgain();
                        }
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
            $read = ($this->documentClass)::read($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendType($read);
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }
        if (! $response) {
            return ProjectionStatus::RUNNING();
        }
        if ($content = $response->getBody()->getContents()) {
            $status = Json::decode($content)['status'] ?? [];
        }

        if (empty($status)) {
            return ProjectionStatus::RUNNING();
        }

        return ProjectionStatus::byValue($status);
    }

    private function handleStreamWithSingleHandler(MergedStreamIterator $events): bool
    {
        $handler = $this->handler;

        /* @var Message $event */
        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                \pcntl_signal_dispatch();
            }
            $this->currentStreamName = $events->streamName();

            if ($this->gapDetection
                && $this->gapDetection->isGapInStreamPosition((int) $this->streamPositions[$this->currentStreamName], (int) $key)
                && $this->gapDetection->shouldRetryToFillGap(new \DateTimeImmutable('now', new DateTimeZone('UTC')), $event)
            ) {
                return false;
            }

            $this->streamPositions[$this->currentStreamName] = $key;
            $this->eventCounter++;

            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
            }

            $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

            if ($this->isStopped) {
                break;
            }
        }

        return true;
    }

    private function handleStreamWithHandlers(MergedStreamIterator $events): bool
    {
        /* @var Message $event */
        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                \pcntl_signal_dispatch();
            }
            $this->currentStreamName = $events->streamName();

            if ($this->gapDetection
                && $this->gapDetection->isGapInStreamPosition((int) $this->streamPositions[$this->currentStreamName], (int) $key)
                && $this->gapDetection->shouldRetryToFillGap(new \DateTimeImmutable('now', new DateTimeZone('UTC')), $event)
            ) {
                return false;
            }

            $this->streamPositions[$this->currentStreamName] = $key;

            $this->eventCounter++;

            if (! isset($this->handlers[$event->messageName()])) {
                $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

                if ($this->isStopped) {
                    break;
                }

                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
            }

            $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

            if ($this->isStopped) {
                break;
            }
        }

        return true;
    }

    private function persistAndFetchRemoteStatusWhenBlockSizeThresholdReached(): void
    {
        if ($this->eventCounter === $this->persistBlockSize) {
            $this->persist();
            $this->eventCounter = 0;

            $this->status = $this->fetchRemoteStatus();

            if (! $this->status->is(ProjectionStatus::RUNNING()) && ! $this->status->is(ProjectionStatus::IDLE())) {
                $this->isStopped = true;
            }
        }
    }

    private function createHandlerContext(?string &$streamName)
    {
        return new class($this, $streamName) {
            /**
             * @var Projector
             */
            private $projector;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(Projector $projector, ?string &$streamName)
            {
                $this->projector = $projector;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->projector->stop();
            }

            public function linkTo(string $streamName, Message $event): void
            {
                $this->projector->linkTo($streamName, $event);
            }

            public function emit(Message $event): void
            {
                $this->projector->emit($event);
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
            $read = ($this->documentClass)::read($this->projectionsTable . '/' . $this->name);
            $response = $this->client->sendType($read);
            $httpStatusCode = $response->getStatusCode();

            if ((
                    $httpStatusCode < StatusCodeInterface::STATUS_OK
                    || $httpStatusCode > StatusCodeInterface::STATUS_MULTIPLE_CHOICES
                )
                && $httpStatusCode !== StatusCodeInterface::STATUS_NOT_FOUND
            ) {
                throw RuntimeException::fromErrorResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            // ignore
        }
        if (! $response) {
            return;
        }
        if ($content = $response->getBody()->getContents()) {
            $result = Json::decode($content);
        }

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
            $create = ($this->documentClass)::create(
                $this->projectionsTable,
                [
                    '_key' => $this->name,
                    'position' => (object) null,
                    'state' => (object) null,
                    'status' => $this->status->getValue(),
                    'locked_until' => null,
                ],
                DocumentType::FLAG_SILENT
            );
            $response = $this->client->sendType($create);
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
                throw RuntimeException::fromErrorResponse($response);
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
            $cursor = $this->statementHandler->create(
                $aql,
                [
                    '@collection' => $this->projectionsTable,
                    'name' => $this->name,
                    'lockedUntil' => $lockUntilString,
                    'nowString' => $nowString,
                    'status' => ProjectionStatus::RUNNING()->getValue(),
                ]
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
        $this->lastLockUpdate = $now;
    }

    private function updateLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        if (! $this->shouldUpdateLock($now)) {
            return;
        }
        $lockUntilString = $this->createLockUntilString($now);

        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    ['locked_until' => $lockUntilString],
                    DocumentType::FLAG_SILENT
                )
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw ProjectionNotFound::with($this->name, $response->getBody()->getContents());
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
        $this->lastLockUpdate = $now;
    }

    private function releaseLock(): void
    {
        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    [
                        'status' => ProjectionStatus::IDLE()->getValue(),
                        'locked_until' => null,
                    ],
                    DocumentType::FLAG_SILENT
                )
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
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $lockUntilString = $this->createLockUntilString($now);

        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    [
                        'position' => $this->streamPositions,
                        'state' => $this->state,
                        'locked_until' => $lockUntilString,
                    ],
                    DocumentType::FLAG_REPLACE_OBJECTS | DocumentType::FLAG_SILENT
                )
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
                $cursor = $this->statementHandler->create(
                    $aql,
                    [
                        '@collection' => $this->eventStreamsTable,
                    ],
                    1000
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
                $cursor = $this->statementHandler->create(
                    $aql,
                    [
                        '@collection' => $this->eventStreamsTable,
                        'categories' => $this->query['categories'],
                    ],
                    1000
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

    private function shouldUpdateLock(DateTimeImmutable $now): bool
    {
        if ($this->lastLockUpdate === null || $this->updateLockThreshold === 0) {
            return true;
        }

        $intervalSeconds = \floor($this->updateLockThreshold / 1000);

        //Create an interval based on seconds
        $updateLockThreshold = new \DateInterval("PT{$intervalSeconds}S");
        //and manually add split seconds
        $updateLockThreshold->f = ($this->updateLockThreshold % 1000) / 1000;

        $threshold = $this->lastLockUpdate->add($updateLockThreshold);

        return $threshold <= $now;
    }

    private function startAgain(): void
    {
        $this->isStopped = false;

        $newStatus = ProjectionStatus::RUNNING();

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        try {
            $response = $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->projectionsTable . '/' . $this->name,
                    [
                        'status' => $newStatus->getValue(),
                        'locked_until' => $this->createLockUntilString($now),
                    ],
                    DocumentType::FLAG_SILENT
                )
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

        $this->status = $newStatus;
        $this->lastLockUpdate = $now;
    }
}
