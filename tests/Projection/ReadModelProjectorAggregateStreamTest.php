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

namespace ProophTest\EventStore\ArangoDb\Projection;

use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\PersistenceStrategy\AggregateStreamStrategy;

/**
 * @group Projection
 * @group ReadModel
 * @group AggregateStream
 */
class ReadModelProjectorAggregateStreamTest extends AbstractReadModelProjectorTest
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new AggregateStreamStrategy();
    }
}
