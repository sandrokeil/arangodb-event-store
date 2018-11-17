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
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\ArangoDb\ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\ArangoDb\Projection\Projector;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Projection\Projector as ProophProjector;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use ProophTest\EventStore\Projection\AbstractEventStoreProjectorTest;
use Psr\Http\Client\ClientInterface;

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
     * @var ClientInterface
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
        $wrappedEventStore->getInnerEventStore()->willReturn($eventStore->reveal())->shouldBeCalled();

        new Projector(
            $wrappedEventStore->reveal(),
            $this->client,
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
        $connection = $this->prophesize(ClientInterface::class);

        new Projector(
            $eventStore->reveal(),
            $connection->reveal(),
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
}
