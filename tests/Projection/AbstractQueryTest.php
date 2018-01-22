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

use ArangoDb\Connection;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreQueryTest as BaseTestCase;

abstract class AbstractQueryTest extends BaseTestCase
{
    /**
     * @var Connection
     */
    private $connection;

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;

    protected function setUp(): void
    {
        $this->connection = TestUtil::getClient();
        TestUtil::setupCollections($this->connection);

        $this->eventStore = new EventStore(
            new FQCNMessageFactory(),
            $this->connection,
            $this->getPersistenceStrategy()
        );
        $this->projectionManager = new ProjectionManager($this->eventStore, $this->connection);
    }

    protected function tearDown(): void
    {
        TestUtil::deleteCollection($this->connection, 'event_streams');

        TestUtil::deleteCollection($this->connection, 'projections');
        TestUtil::deleteCollection($this->connection, 'c' . sha1('user-123'));
        // these tables are used only in some test cases
        TestUtil::deleteCollection($this->connection, 'c' . sha1('user-234'));
        TestUtil::deleteCollection($this->connection, 'c' . sha1('$iternal-345'));
        TestUtil::deleteCollection($this->connection, 'c' . sha1('guest-345'));
        TestUtil::deleteCollection($this->connection, 'c' . sha1('guest-456'));
        TestUtil::deleteCollection($this->connection, 'c' . sha1('foo'));
        TestUtil::deleteCollection($this->connection, 'c' . sha1('test_projection'));

        unset($this->connection);
    }
}
