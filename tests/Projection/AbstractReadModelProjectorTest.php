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
use ArrayIterator;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\ArangoDb\Projection\ReadModelProjector;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Mock\ReadModelMock;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Projection\AbstractEventStoreReadModelProjectorTest;

abstract class AbstractReadModelProjectorTest extends AbstractEventStoreReadModelProjectorTest
{
    /**
     * @var ProjectionManager
     */
    protected $projectionManager;

    /**
     * @var EventStore
     */
    protected $eventStore;

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

    /**
     * @test
     */
    public function it_updates_read_model_using_when_and_loads_and_continues_again(): void
    {
        $this->prepareEventStream('user-123');

        $readModel = new ReadModelMock();

        $projection = $this->projectionManager->createReadModelProjection('test_projection', $readModel);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModel()->stack('insert', 'name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModel()->stack('update', 'name', $event->payload()['name']);

                    if ($event->metadata()['_aggregate_version'] === 50) {
                        $this->stop();
                    }
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $readModel->read('name'));

        $events = [];
        for ($i = 51; $i < 100; $i++) {
            $events[] = UsernameChanged::with([
                'name' => uniqid('name_'),
            ], $i);
        }
        $events[] = UsernameChanged::with([
            'name' => 'Oliver',
        ], 100);

        $this->eventStore->appendTo(new StreamName('user-123'), new ArrayIterator($events));

        $projection = $this->projectionManager->createReadModelProjection('test_projection', $readModel);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event): void {
                    $this->readModel()->stack('insert', 'name', $event->payload()['name']);
                },
                UsernameChanged::class => function ($state, Message $event): void {
                    $this->readModel()->stack('update', 'name', $event->payload()['name']);

                    if ($event->metadata()['_aggregate_version'] === 100) {
                        $this->stop();
                    }
                },
            ])
            ->run();

        $this->assertEquals('Oliver', $readModel->read('name'));

        $projection->reset();

        $this->assertFalse($readModel->hasKey('name'));
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

        new ReadModelProjector(
            $wrappedEventStore->reveal(),
            $this->connection,
            'test_projection',
            new ReadModelMock(),
            'event_streams',
            'projections',
            1,
            1,
            1
        );
    }

    /**
     * @test
     */
    public function it_throws_exception_when_unknown_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(ProophEventStore::class);
        $connection = $this->prophesize(Connection::class);
        $readModel = $this->prophesize(ReadModel::class);

        new ReadModelProjector(
            $eventStore->reveal(),
            $connection->reveal(),
            'test_projection',
            $readModel->reveal(),
            'event_streams',
            'projections',
            10,
            10,
            10
        );
    }

    /**
     * @test
     */
    public function it_dispatches_pcntl_signals_when_enabled(): void
    {
        if (! extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . realpath(__DIR__) . '/isolated-read-model-projection.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = proc_open($command, $descriptorSpec, $pipes);
        $processDetails = proc_get_status($projectionProcess);
        sleep(1);
        posix_kill($processDetails['pid'], SIGQUIT);
        sleep(1);

        $processDetails = proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }
}
