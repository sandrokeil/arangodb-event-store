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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ArangoDb\ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Projection\AbstractEventStoreQueryTest as BaseTestCase;
use Psr\Http\Client\ClientExceptionInterface;

abstract class AbstractQueryTest extends BaseTestCase
{
    /**
     * @var ClientExceptionInterface
     */
    private $client;

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;

    protected function setUp(): void
    {
        TestUtil::createDatabase();
        $this->client = TestUtil::getClient();
        TestUtil::setupCollections($this->client);

        $this->eventStore = new ArangoDbEventStore(
            new FQCNMessageFactory(),
            $this->client,
            $this->getPersistenceStrategy()
        );
        $this->projectionManager = new ProjectionManager($this->eventStore, $this->client);
    }

    protected function tearDown(): void
    {
        TestUtil::dropDatabase();

        unset($this->client);
    }
}
