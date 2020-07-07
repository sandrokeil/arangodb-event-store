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

use ArangoDb\Type\Batch;
use ArangoDb\Type\DocumentType;
use Fig\Http\Message\StatusCodeInterface;
use Iterator;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\ArangoDb\Guard\DeletedStreamNotFoundGuard;
use Prooph\EventStore\ArangoDb\Guard\HttpStatusCodeGuard;
use Prooph\EventStore\ArangoDb\Guard\StreamExistsGuard;
use Prooph\EventStore\ArangoDb\Guard\StreamNotFoundGuard;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Psr\Http\Client\ClientExceptionInterface;

final class ArangoDbEventStore extends EventStore
{
    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        try {
            $this->client->sendType(
                ($this->documentClass)::updateOne(
                    $this->eventStreamsCollection . '/' . $this->persistenceStrategy->generateCollectionName($streamName),
                    [
                        'metadata' => $newMetadata,
                    ],
                    DocumentType::FLAG_REPLACE_OBJECTS | DocumentType::FLAG_SILENT
                )->useGuard(StreamNotFoundGuard::withStreamName($streamName))
            );
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        try {
            // create collection
            $batch = Batch::fromTypes(
                ...$this->persistenceStrategy->createCollection(
                $collectionName,
                StreamExistsGuard::withStreamName($streamName)
            )
            );
            $response = $this->client->sendType($batch);

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }

            $types = [
                ($this->documentClass)::create(
                    $this->eventStreamsCollection,
                    $this->createEventStreamData($stream),
                    DocumentType::FLAG_SILENT
                )->useGuard(HttpStatusCodeGuard::withoutContentId(StatusCodeInterface::STATUS_CONFLICT)),
            ];

            $data = $this->persistenceStrategy->prepareData($stream->streamEvents());

            if (! empty($data)) {
                $types[] = ($this->documentClass)::create(
                    $collectionName,
                    $data,
                    DocumentType::FLAG_SILENT
                );
            }

            // save stream and events
            $batch = Batch::fromTypes(...$types);

            $response = $this->client->sendType($batch);

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        $data = $this->persistenceStrategy->prepareData($streamEvents);

        if (empty($data)) {
            return;
        }

        try {
            $response = $this->client->sendType(
                ($this->documentClass)::create(
                    $collectionName,
                    $data,
                    DocumentType::FLAG_SILENT
                )
            );
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_NOT_FOUND) {
                throw StreamNotFound::with($streamName);
            }
            if ($response->getStatusCode() === StatusCodeInterface::STATUS_CONFLICT) {
                throw RuntimeException::fromErrorResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }

    public function delete(StreamName $streamName): void
    {
        try {
            $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

            $batch = Batch::fromTypes(
                ($this->collectionClass)::delete(
                    $collectionName
                )->useGuard(StreamNotFoundGuard::withStreamName($streamName)),
                ($this->documentClass)::deleteOne(
                    $this->eventStreamsCollection . '/' . $collectionName
                )->useGuard(DeletedStreamNotFoundGuard::withStreamName($streamName))
            );
            $response = $this->client->sendType($batch);

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }
}
