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

namespace ProophTest\EventStore\ArangoDb;

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ArangoDb\ArangoDbTransactionalEventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\PersistenceStrategy\SimpleStreamStrategy;

/**
 * @group TransactionalEventStore
 * @group SimpleStream
 */
class TransactionalEventStoreSimpleStreamTest extends AbstractTransactionalEventStoreTest
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new SimpleStreamStrategy();
    }

    protected function setUp(): void
    {
        TestUtil::createDatabase();
        $this->client = TestUtil::getTransactionalClient();
        TestUtil::setupCollections($this->client);

        $this->eventStore = new ArangoDbTransactionalEventStore(
            new FQCNMessageFactory(),
            $this->client,
            TestUtil::getStatementHandler(),
            $this->getPersistenceStrategy()
        );
    }
}
