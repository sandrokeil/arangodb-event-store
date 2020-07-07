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

use ArangoDb\Handler\StatementHandler;
use ArangoDb\Http\TransactionSupport;
use ArangoDb\Http\TypeSupport;
use ArrayIterator;
use DateTimeImmutable;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\ArangoDbTransactionalEventStore;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\ArangoDb\Projection\Projector;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Projection\Projector as ProophProjector;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Projection\AbstractEventStoreProjectorTest;

abstract class AbstractProjectorTest extends AbstractEventStoreProjectorTest
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
     * @var TypeSupport
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
            TestUtil::getStatementHandler(),
            $this->getPersistenceStrategy()
        );
        $this->projectionManager = new ProjectionManager($this->eventStore, $this->client, TestUtil::getStatementHandler());
    }

    protected function setUpEventStoreWithControlledConnection(TransactionSupport $connection): \Prooph\EventStore\TransactionalEventStore
    {
        return new ArangoDbTransactionalEventStore(
            new FQCNMessageFactory(),
            $connection,
            TestUtil::getStatementHandler(),
            $this->getPersistenceStrategy(),
            10000,
            'event_streams',
            true
        );
    }

    protected function tearDown(): void
    {
        TestUtil::dropDatabase();
        unset($this->client);
    }

    /**
     * @test
     */
    public function it_updates_state_using_when_and_persists_with_block_size(): void
    {
        $this->prepareEventStream('user-123');

        $testCase = $this;

        $projection = $this->projectionManager->createProjection('test_projection', [
            ProophProjector::OPTION_PERSIST_BLOCK_SIZE => 10,
        ]);

        $projection
            ->fromAll()
            ->when([
                UserCreated::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    return $state;
                },
                UsernameChanged::class => function ($state, Message $event) use ($testCase): array {
                    $testCase->assertEquals('user-123', $this->streamName());
                    $state['name'] = $event->payload()['name'];

                    if ($event->payload()['name'] === 'Sascha') {
                        $this->stop();
                    }

                    return $state;
                },
            ])
            ->run();

        $this->assertEquals('Sascha', $projection->getState()['name']);
    }

    /**
     * @test
     */
    public function it_handles_existing_projection_table(): void
    {
        $this->prepareEventStream('user-123');

        $projection = $this->projectionManager->createProjection('test_projection');

        $projection
            ->init(function (): array {
                return ['count' => 0];
            })
            ->fromAll()
            ->when([
                UsernameChanged::class => function (array $state, Message $event): array {
                    $state['count']++;

                    return $state;
                },
            ])
            ->run(false);

        $this->assertEquals(49, $projection->getState()['count']);

        $projection->run(false);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_wrapped_event_store_instance_passed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStore = $this->prophesize(ProophEventStore::class);
        $wrappedEventStore = $this->prophesize(EventStoreDecorator::class);
        $StatementHandler = $this->prophesize(StatementHandler::class);
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new Projector(
            $wrappedEventStore->reveal(),
            $this->client,
            $StatementHandler->reveal(),
            'test_projection',
            'event_streams',
            'projections',
            1,
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
        $connection = $this->prophesize(TypeSupport::class);
        $StatementHandler = $this->prophesize(StatementHandler::class);

        new Projector(
            $eventStore->reveal(),
            $connection->reveal(),
            $StatementHandler->reveal(),
            'test_projection',
            'event_streams',
            'projections',
            10,
            10,
            10,
            10000
        );
    }

    /**
     * @test
     */
    public function it_dispatches_pcntl_signals_when_enabled(): void
    {
        if (! \extension_loaded('pcntl')) {
            $this->markTestSkipped('The PCNTL extension is not available.');

            return;
        }

        $command = 'exec php ' . \realpath(__DIR__) . '/isolated-projection.php';
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        /**
         * Created process inherits env variables from this process.
         * Script returns with non-standard code SIGUSR1 from the handler and -1 else
         */
        $projectionProcess = \proc_open($command, $descriptorSpec, $pipes);
        $processDetails = \proc_get_status($projectionProcess);
        \sleep(1);
        \posix_kill($processDetails['pid'], SIGQUIT);
        \sleep(1);

        $processDetails = \proc_get_status($projectionProcess);
        $this->assertEquals(
            SIGUSR1,
            $processDetails['exitcode']
        );
    }

    /**
     * @test
     */
    public function it_detects_gap_and_performs_retry(): void
    {
        $this->markTestSkipped('Find out how to test it!');
    }

    /**
     * @test
     */
    public function it_continues_when_retry_limit_is_reached_and_gap_not_filled(): void
    {
        $this->markTestSkipped('Find out how to test it!');
    }

    /**
     * @test
     */
    public function it_does_not_perform_retry_when_event_is_older_than_detection_window(): void
    {
        $this->markTestSkipped('Find out how to test it!');
    }

    protected function prepareEventStreamWithOneEvent(string $name, DateTimeImmutable $createdAt = null): void
    {
        $events = [];

        if ($createdAt) {
            $events[] = UserCreated::withPayloadAndSpecifiedCreatedAt([
                'name' => 'Alex',
            ], 1, $createdAt);
        } else {
            $events[] = UserCreated::with([
                'name' => 'Alex',
            ], 1);
        }

        $this->eventStore->create(new Stream(new StreamName($name), new ArrayIterator($events)));
    }
}
