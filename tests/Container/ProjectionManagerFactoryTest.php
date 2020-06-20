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

namespace ProophTest\EventStore\ArangoDb\Container;

use ArangoDb\Handler\StatementHandler;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\ArangoDb\ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\Container\ProjectionManagerFactory;
use Prooph\EventStore\ArangoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\EventStore as ProophEventStore;
use ProophTest\EventStore\ArangoDb\TestUtil;
use Psr\Container\ContainerInterface;

/**
 * @group Container
 */
class ProjectionManagerFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_service(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'connection' => 'my_connection',
            'statement_handler' => StatementHandler::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new ArangoDbEventStore(
            $this->createMock(MessageFactory::class),
            TestUtil::getClient(),
            TestUtil::getStatementHandler(),
            $this->createMock(PersistenceStrategy::class)
        );

        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(StatementHandler::class)->willReturn(TestUtil::getStatementHandler())->shouldBeCalled();
        $container->get(ProophEventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();

        $factory = new ProjectionManagerFactory();
        $projectionManager = $factory($container->reveal());

        $this->assertInstanceOf(ProjectionManager::class, $projectionManager);
    }

    /**
     * @test
     */
    public function it_creates_service_via_callstatic(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'connection' => 'my_connection',
            'statement_handler' => StatementHandler::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = new ArangoDbEventStore(
            $this->createMock(MessageFactory::class),
            TestUtil::getClient(),
            TestUtil::getStatementHandler(),
            $this->createMock(PersistenceStrategy::class)
        );

        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(StatementHandler::class)->willReturn(TestUtil::getStatementHandler())->shouldBeCalled();
        $container->get(ProophEventStore::class)->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();

        $name = 'default';
        $pdo = ProjectionManagerFactory::$name($container->reveal());

        $this->assertInstanceOf(ProjectionManager::class, $pdo);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $projectionName = 'custom';
        ProjectionManagerFactory::$projectionName('invalid container');
    }
}
