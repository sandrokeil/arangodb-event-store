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
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\ArangoDb\Exception\ProjectionNotFound;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\EventStoreDecorator;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Projection\AbstractProjectionManagerTest as BaseTestCase;
use Psr\Http\Client\ClientInterface;

abstract class AbstractProjectionManagerTest extends BaseTestCase
{
    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * @var EventStore
     */
    protected $eventStore;

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_event_store_instance_passed(): void
    {
        $this->expectException(\Prooph\EventStore\Exception\InvalidArgumentException::class);

        $eventStore = $this->prophesize(ProophEventStore::class);

        new ProjectionManager($eventStore->reveal(), $this->client);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(ProophEventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new ProjectionManager($wrappedEventStore->reveal(), $this->client);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_with_missing_db_table(): void
    {
        $this->expectException(ProjectionNotFound::class);

        TestUtil::deleteCollection($this->client, 'projections');

        $this->projectionManager->fetchProjectionNames(null, 200, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_using_invalid_regex(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid regex pattern given');

        $this->projectionManager->fetchProjectionNamesRegex('invalid)', 10, 0);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_fetching_projection_names_regex_with_missing_db_table(): void
    {
        $this->expectException(ProjectionNotFound::class);

        TestUtil::deleteCollection($this->client, 'projections');

        $this->projectionManager->fetchProjectionNamesRegex('^foo', 200, 0);
    }

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
