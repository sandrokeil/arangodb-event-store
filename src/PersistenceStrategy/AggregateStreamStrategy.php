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

namespace Prooph\EventStore\ArangoDb\PersistenceStrategy;

use Iterator;
use Prooph\EventStore\ArangoDb\Exception;
use Prooph\EventStore\ArangoDb\Iterator\JsonAggregateStreamIterator;
use Prooph\EventStore\ArangoDb\JsonIterator;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Type\CreateCollection;
use Prooph\EventStore\ArangoDb\Type\CreateIndex;
use Prooph\EventStore\StreamName;

final class AggregateStreamStrategy implements PersistenceStrategy
{
    /**
     * @var int
     */
    private $offsetNumber;

    public function __construct()
    {
        $this->offsetNumber = (int) str_pad('1', strlen((string) PHP_INT_MAX) - 1, '0');
    }

    public function jsonIterator(Iterator $streamEvents): JsonIterator
    {
        return new JsonAggregateStreamIterator($streamEvents, $this);
    }

    public function padPosition(int $position): int
    {
        if ($position === PHP_INT_MAX) {
            return $position;
        }
        // TODO max length in JS is -3 to start with 1
        return $this->offsetNumber + $position - 1;
    }

    public function offsetNumber(): int
    {
        return $this->offsetNumber;
    }

    public function createCollection(string $collectionName): array
    {
        $collection = CreateCollection::with(
            $collectionName,
            [
                'keyOptions' => [
                    'allowUserKeys' => true,
                    'type' => 'traditional',
                ],
            ]
        );

        $aggregateVersionIndex = CreateIndex::with(
            $collectionName,
            [
                'type' => 'skiplist',
                'fields' => [
                    'metadata._aggregate_version',
                ],
                'selectivityEstimate' => 1,
                'unique' => true,
                'sparse' => false,
            ]
        );

        $eventIdIndex = CreateIndex::with(
            $collectionName,
            [
                'type' => 'hash',
                'fields' => [
                    'event_id',
                ],
                'selectivityEstimate' => 1,
                'unique' => true,
                'sparse' => false,
            ]
        );

        $sortingIndex = CreateIndex::with(
            $collectionName,
            [
                'type' => 'skiplist',
                'fields' => [
                    '_key',
                ],
                'selectivityEstimate' => 1,
                'unique' => true,
                'sparse' => false,
            ]
        );

        return [
            $collection,
            $eventIdIndex,
            $aggregateVersionIndex,
            $sortingIndex,
        ];
    }

    public function prepareData(Iterator $streamEvents): iterable
    {
        $data = [];

        foreach ($streamEvents as $event) {
            if (! isset($event->metadata()['_aggregate_version'])) {
                throw new Exception\RuntimeException('_aggregate_version is missing in metadata');
            }
            $data[] = [
                '_key' => (string) $this->padPosition($event->metadata()['_aggregate_version']),
                'event_id' => $event->uuid()->toString(),
                'event_name' => $event->messageName(),
                'payload' => $event->payload(),
                'metadata' => $event->metadata(),
                'created_at' => $event->createdAt()->format('Y-m-d\TH:i:s.u'),
            ];
        }

        return $data;
    }

    public function generateCollectionName(StreamName $streamName): string
    {
        return 'c' . sha1($streamName->toString());
    }
}
