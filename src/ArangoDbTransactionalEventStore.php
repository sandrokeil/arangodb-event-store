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

namespace Prooph\EventStore\ArangoDb;

use ArangoDb\Handler\StatementHandler;
use ArangoDb\Http\TransactionSupport;
use ArangoDb\Type\DocumentType;
use Fig\Http\Message\StatusCodeInterface;
use Iterator;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\ArangoDb\Guard\DeletedStreamNotFoundGuard;
use Prooph\EventStore\ArangoDb\Guard\HttpStatusCodeGuard;
use Prooph\EventStore\ArangoDb\Guard\StreamExistsGuard;
use Prooph\EventStore\ArangoDb\Guard\StreamNotFoundGuard;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalEventStore;
use Psr\Http\Client\ClientExceptionInterface;

final class ArangoDbTransactionalEventStore extends EventStore implements TransactionalEventStore
{
    /**
     * @var TransactionSupport
     */
    protected $client;

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     * @var bool
     */
    private $inTransaction = false;

    /**
     * Event Store
     *
     * @var ArangoDbEventStore
     */
    private $eventStore;

    public function __construct(
        MessageFactory $messageFactory,
        TransactionSupport $client,
        StatementHandler $statementHandler,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsCollection = 'event_streams',
        bool $disableTransactionHandling = false
    ) {
        parent::__construct(
            $messageFactory,
            $client,
            $statementHandler,
            $persistenceStrategy,
            $loadBatchSize,
            $eventStreamsCollection
        );
        $this->eventStore = new ArangoDbEventStore(
            $messageFactory,
            $client,
            $statementHandler,
            $persistenceStrategy,
            $loadBatchSize,
            $eventStreamsCollection
        );
        $this->disableTransactionHandling = $disableTransactionHandling;
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        if ($this->disableTransactionHandling) {
            $this->eventStore->updateStreamMetadata($streamName, $newMetadata);

            return;
        }
        $this->client->add(
            ($this->documentClass)::updateOne(
                $this->eventStreamsCollection . '/' . $this->persistenceStrategy->generateCollectionName($streamName),
                [
                    'metadata' => $newMetadata,
                ],
                DocumentType::FLAG_REPLACE_OBJECTS | DocumentType::FLAG_SILENT
            )->useGuard(StreamNotFoundGuard::withStreamName($streamName, true))
        );
    }

    public function create(Stream $stream): void
    {
        if ($this->disableTransactionHandling) {
            $this->eventStore->create($stream);

            return;
        }
        $streamName = $stream->streamName();
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        $this->client->addList(
            ...$this->persistenceStrategy->createCollection(
                $collectionName, StreamExistsGuard::withStreamName($streamName)
            )
        );

        $this->client->add(
            ($this->documentClass)::create(
                $this->eventStreamsCollection,
                $this->createEventStreamData($stream),
                DocumentType::FLAG_SILENT
            )->useGuard(HttpStatusCodeGuard::withoutContentId(StatusCodeInterface::STATUS_CONFLICT))
        );

        $data = $this->persistenceStrategy->prepareData($stream->streamEvents());

        if (! empty($data)) {
            $this->client->add(
                ($this->documentClass)::create(
                    $collectionName,
                    $data,
                    DocumentType::FLAG_SILENT
                )
            );
        }
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        if ($this->disableTransactionHandling) {
            $this->eventStore->appendTo($streamName, $streamEvents);

            return;
        }
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        $data = $this->persistenceStrategy->prepareData($streamEvents);

        if (empty($data)) {
            return;
        }

        $this->client->add(
            ($this->documentClass)::create(
                $collectionName,
                $data,
                DocumentType::FLAG_SILENT
            )->useGuard(HttpStatusCodeGuard::withoutContentId(StatusCodeInterface::STATUS_CONFLICT))
        );
    }

    public function delete(StreamName $streamName): void
    {
        if ($this->disableTransactionHandling) {
            $this->eventStore->delete($streamName);

            return;
        }
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        $this->client->addList(
            ($this->collectionClass)::delete(
                $collectionName
            )->useGuard(StreamNotFoundGuard::withStreamName($streamName)),
            ($this->documentClass)::deleteOne(
                $this->eventStreamsCollection . '/' . $collectionName
            )->useGuard(DeletedStreamNotFoundGuard::withStreamName($streamName))
        );
    }

    public function beginTransaction(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if ($this->inTransaction) {
            throw new TransactionAlreadyStarted();
        }
        $this->inTransaction = true;
    }

    public function commit(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }
        if (! $this->inTransaction) {
            throw new TransactionNotStarted();
        }

        try {
            $response = $this->client->send();

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }

            $this->inTransaction = false;
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function rollback(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }
        if (! $this->inTransaction) {
            throw new TransactionNotStarted();
        }
        $this->inTransaction = false;
        $this->client->reset();
    }

    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    public function transactional(callable $callable)
    {
        $this->beginTransaction();

        try {
            $result = $callable($this);
            $this->commit();
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }

        return $result ?: true;
    }
}
