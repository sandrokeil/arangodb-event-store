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

use ArangoDb\BatchResult;
use ArangoDb\Type\Batch;
use ArangoDb\Type\Collection;
use ArangoDb\Type\Document;
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
            $response = $this->client->sendRequest(
                Document::updateOne(
                    $this->eventStreamsCollection . '/' . $this->persistenceStrategy->generateCollectionName($streamName),
                    [
                        'metadata' => $newMetadata,
                    ],
                    Document::FLAG_REPLACE_OBJECTS | Document::FLAG_SILENT
                )->toRequest()
            );
            StreamNotFoundGuard::withStreamName($streamName)($response);
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
            $response = $this->client->sendRequest($batch->toRequest());

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }
            BatchResult::fromResponse($response)->validateBatch($batch);

            $types = [
                Document::create(
                    $this->eventStreamsCollection,
                    $this->createEventStreamData($stream),
                    Document::FLAG_SILENT
                )->useGuard(HttpStatusCodeGuard::withoutContentId(StatusCodeInterface::STATUS_CONFLICT)),
            ];

            $data = $this->persistenceStrategy->prepareData($stream->streamEvents());

            if (! empty($data)) {
                $types[] = Document::create(
                    $collectionName,
                    $data,
                    Document::FLAG_SILENT
                );
            }

            // save stream and events
            $batch = Batch::fromTypes(...$types);

            $response = $this->client->sendRequest($batch->toRequest());

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }
            BatchResult::fromResponse($response)->validateBatch($batch);
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
            $response = $this->client->sendRequest(
                Document::create(
                    $collectionName,
                    $data,
                    Document::FLAG_SILENT
                )->toRequest()
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
                Collection::delete(
                    $collectionName
                )->useGuard(StreamNotFoundGuard::withStreamName($streamName)),
                Document::deleteOne(
                    $this->eventStreamsCollection . '/' . $collectionName
                )->useGuard(DeletedStreamNotFoundGuard::withStreamName($streamName))
            );
            $response = $this->client->sendRequest($batch->toRequest());

            if ($response->getStatusCode() !== StatusCodeInterface::STATUS_OK) {
                throw RuntimeException::fromResponse($response);
            }
            BatchResult::fromResponse($response)->validateBatch($batch);
        } catch (ClientExceptionInterface $e) {
            throw RuntimeException::fromServerException($e);
        }
    }
}
