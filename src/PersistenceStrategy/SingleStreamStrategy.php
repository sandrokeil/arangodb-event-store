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

use ArangoDb\Guard\Guard;
use ArangoDb\Type\Collection;
use ArangoDb\Type\Index;
use Iterator;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\StreamName;

final class SingleStreamStrategy implements PersistenceStrategy
{
    /**
     * @var int
     */
    private $offsetNumber;

    public function __construct()
    {
        $this->offsetNumber = (int) \str_pad('1', \strlen((string) PHP_INT_MAX) - 1, '0');
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

    public function createCollection(string $collectionName, Guard $collectionGuard): array
    {
        $collection = Collection::create(
            $collectionName,
            [
                'keyOptions' => [
                    'allowUserKeys' => false,
                    'type' => 'autoincrement',
                    'increment' => 1,
                    'offset' => $this->padPosition(1),
                ],
            ]
        );
        $collection->useGuard($collectionGuard);

        $aggregateIndex = Index::create(
            $collectionName,
            [
                'type' => 'skiplist',
                'fields' => [
                    'metadata._aggregate_type',
                    'metadata._aggregate_id',
                    'metadata._aggregate_version',
                ],
                'unique' => true,
                'sparse' => false,
            ]
        );

        $aggregateLookupIndex = Index::create(
            $collectionName,
            [
                'type' => 'skiplist',
                'fields' => [
                    'metadata._aggregate_id',
                    'metadata._aggregate_type',
                    '_key',
                ],
                'unique' => false,
                'sparse' => false,
            ]
        );

        $eventIdIndex = Index::create(
            $collectionName,
            [
                'type' => 'hash',
                'fields' => [
                    'event_id',
                ],
                'unique' => true,
                'sparse' => false,
            ]
        );

        $sortingIndex = Index::create(
            $collectionName,
            [
                'type' => 'skiplist',
                'fields' => [
                    '_key',
                ],
                'unique' => true,
                'sparse' => false,
            ]
        );

        return [
            $collection,
            $eventIdIndex,
            $aggregateIndex,
            $aggregateLookupIndex,
            $sortingIndex,
        ];
    }

    public function prepareData(Iterator $streamEvents): iterable
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $data[] = [
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
        return 'c' . \sha1($streamName->toString());
    }
}
