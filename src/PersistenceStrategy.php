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

use ArangoDb\Guard\Guard;
use Iterator;
use Prooph\EventStore\StreamName;

interface PersistenceStrategy
{
    public function createCollection(string $collectionName, Guard $collectionGuard): array;

    public function prepareData(Iterator $streamEvents): iterable;

    public function generateCollectionName(StreamName $streamName): string;

    public function padPosition(int $position): int;

    public function offsetNumber(): int;
}
