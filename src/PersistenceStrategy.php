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

namespace Prooph\EventStore\ArangoDb;

use Iterator;
use Prooph\EventStore\ArangoDb\JsonIterator;
use Prooph\EventStore\StreamName;

interface PersistenceStrategy
{
    public function createCollection(string $collectionName): array;

    public function prepareData(Iterator $streamEvents): iterable;

    public function generateCollectionName(StreamName $streamName): string;

    public function padPosition(int $position): int;

    public function offsetNumber(): int;

    public function jsonIterator(Iterator $streamEvents): JsonIterator;
}
