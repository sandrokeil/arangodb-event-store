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

use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\ArangoDb\Container\ArangoDbEventStoreFactory;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\ArangoDb\PersistenceStrategy;
use Prooph\EventStore\Exception\ConfigurationException;
use Prooph\EventStore\Metadata\MetadataEnricher;
use Prooph\EventStore\Plugin\Plugin;
use ProophTest\EventStore\ArangoDb\TestUtil;
use Psr\Container\ContainerInterface;

/**
 * @group Container
 */
final class EventStoreFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter_via_connection_service(): void
    {
        $config['prooph']['event_store']['default'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
            'wrap_action_event_emitter' => false,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $factory = new ArangoDbEventStoreFactory();
        $eventStore = $factory($container->reveal());

        $this->assertInstanceOf(EventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_wraps_action_event_emitter(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $eventStoreName = 'custom';
        $eventStore = ArangoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_injects_plugins(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $featureMock = $this->getMockForAbstractClass(Plugin::class);
        $featureMock->expects($this->once())->method('attachToEventStore');

        $container->get('plugin')->willReturn($featureMock);

        $eventStoreName = 'custom';
        $eventStore = ArangoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_plugin_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Plugin plugin does not implement the Plugin interface');

        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
            'plugins' => ['plugin'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config)->shouldBeCalled();
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $container->get('plugin')->willReturn('notAValidPlugin');

        $eventStoreName = 'custom';
        ArangoDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_injects_metadata_enrichers(): void
    {
        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
            'metadata_enrichers' => ['metadata_enricher1', 'metadata_enricher2'],
        ];

        $metadataEnricher1 = $this->prophesize(MetadataEnricher::class);
        $metadataEnricher2 = $this->prophesize(MetadataEnricher::class);

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $container->get('metadata_enricher1')->willReturn($metadataEnricher1->reveal());
        $container->get('metadata_enricher2')->willReturn($metadataEnricher2->reveal());

        $eventStoreName = 'custom';
        $eventStore = ArangoDbEventStoreFactory::$eventStoreName($container->reveal());

        $this->assertInstanceOf(ActionEventEmitterEventStore::class, $eventStore);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_metadata_enricher_configured(): void
    {
        $this->expectException(ConfigurationException::class);
        $this->expectExceptionMessage('Metadata enricher foobar does not implement the MetadataEnricher interface');

        $config['prooph']['event_store']['custom'] = [
            'connection' => 'my_connection',
            'persistence_strategy' => PersistenceStrategy\AggregateStreamStrategy::class,
            'metadata_enrichers' => ['foobar'],
        ];

        $client = TestUtil::getClient();

        $container = $this->prophesize(ContainerInterface::class);

        $container->get('config')->willReturn($config);
        $container->get('my_connection')->willReturn($client)->shouldBeCalled();
        $container->get(FQCNMessageFactory::class)->willReturn(new FQCNMessageFactory())->shouldBeCalled();
        $container->get(PersistenceStrategy\AggregateStreamStrategy::class)->willReturn(new PersistenceStrategy\AggregateStreamStrategy())->shouldBeCalled();

        $container->get('foobar')->willReturn('foobar');

        $eventStoreName = 'custom';
        ArangoDbEventStoreFactory::$eventStoreName($container->reveal());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_invalid_container_given(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $eventStoreName = 'custom';
        ArangoDbEventStoreFactory::$eventStoreName('invalid container');
    }
}
