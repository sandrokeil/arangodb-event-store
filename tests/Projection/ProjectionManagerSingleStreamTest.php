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

namespace ProophTest\EventStore\ArangoDb\Projection;

use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\PersistenceStrategy\SingleStreamStrategy;

/**
 * @group Projection
 * @group Manager
 * @group SingleStream
 */
class ProjectionManagerSingleStreamTest extends AbstractProjectionManagerTest
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new SingleStreamStrategy();
    }
}
