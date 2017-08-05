<?php
/**
 * This file is part of the prooph/arangodb-event-store.
 * (c) 2017 prooph software GmbH <contact@prooph.de>
 * (c) 2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\ArangoDb\PersistenceStrategy;

use Iterator;
use Prooph\EventStore\ArangoDb\JsonIterator;
use Prooph\EventStore\ArangoDb\Iterator\JsonSimpleStreamIterator;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Type\CreateCollection;
use Prooph\EventStore\ArangoDb\Type\CreateIndex;
use Prooph\EventStore\StreamName;

final class SimpleStreamStrategy implements PersistenceStrategy
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
        return new JsonSimpleStreamIterator($streamEvents);
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
                    'allowUserKeys' => false,
                    'type' => 'autoincrement',
                    'increment' => 1,
                    'offset' => $this->padPosition(1),
                ],
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
        return 'c' . sha1($streamName->toString());
    }
}
