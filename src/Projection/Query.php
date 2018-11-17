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
use Closure;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\EventStore as ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Projection\Query as ProophQuery;
use Prooph\EventStore\StreamName;
use Psr\Http\Client\ClientInterface;

final class Query implements ProophQuery
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
     * @var array
     */
    private $streamPositions = [];

    /**
     * @var array
     */
    private $state = [];

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
    private $currentStreamName = null;

    /**
     * @var array|null
     */
    private $query;

    /**
     * @var MetadataMatcher|null
     */
    private $metadataMatcher;

    public function __construct(EventStore $eventStore, ClientInterface $client, string $eventStreamsTable)
    {
        $this->eventStore = $eventStore;
        $this->client = $client;
        $this->eventStreamsTable = $eventStreamsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof ArangoDbEventStore
        ) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function init(Closure $callback): ProophQuery
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

    public function fromStream(string $streamName, MetadataMatcher $metadataMatcher = null): ProophQuery
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;
        $this->metadataMatcher = $metadataMatcher;

        return $this;
    }

    public function fromStreams(string ...$streamNames): ProophQuery
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    public function fromCategory(string $name): ProophQuery
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    public function fromCategories(string ...$names): ProophQuery
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    public function fromAll(): ProophQuery
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    public function when(array $handlers): ProophQuery
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

            $this->handlers[$eventName] = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    public function whenAny(Closure $handler): ProophQuery
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

        if (\is_callable($callback)) {
            $result = $callback();

            if (\is_array($result)) {
                $this->state = $result;

                return;
            }
        }

        $this->state = [];
    }

    public function stop(): void
    {
        $this->isStopped = true;
    }

    public function getState(): array
    {
        return $this->state;
    }

    public function run(): void
    {
        if (null === $this->query
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;
        $this->prepareStreamPositions();

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
    }

    private function handleStreamWithSingleHandler(string $streamName, Iterator $events): void
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $event) {
            /* @var Message $event */
            $this->streamPositions[$streamName]++;

            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
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

            if (! isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (\is_array($result)) {
                $this->state = $result;
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
             * @var Query
             */
            private $query;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(Query $query, ?string &$streamName)
            {
                $this->query = $query;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->query->stop();
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
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
}
