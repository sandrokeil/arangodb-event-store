<?php
/**
 * This file is part of the prooph/arangodb-event-store.
 * (c) 2017 prooph software GmbH <contact@prooph.de>
 * (c) 2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\ArangoDb\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\ArangoDb\EventStore;
use Prooph\EventStore\ArangoDb\Exception\InvalidArgumentException;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\Exception\ConfigurationException;
use Prooph\EventStore\Metadata\MetadataEnricher;
use Prooph\EventStore\Metadata\MetadataEnricherAggregate;
use Prooph\EventStore\Metadata\MetadataEnricherPlugin;
use Prooph\EventStore\Plugin\Plugin;
use Psr\Container\ContainerInterface;

class EventStoreFactory implements
    ProvidesDefaultOptions,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     EventStore::class => [EventStoreFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): ProophEventStore
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): ProophEventStore
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        $eventStore = new EventStore(
            $container->get($config['message_factory']),
            $container->get($config['connection']),
            $container->get($config['persistence_strategy']),
            $config['load_batch_size'],
            $config['event_streams_table'],
            $config['disable_transaction_handling']
        );

        if (! $config['wrap_action_event_emitter']) {
            return $eventStore;
        }

        $wrapper = $this->createActionEventEmitterEventStore($eventStore);

        foreach ($config['plugins'] as $pluginAlias) {
            $plugin = $container->get($pluginAlias);

            if (! $plugin instanceof Plugin) {
                throw ConfigurationException::configurationError(sprintf(
                    'Plugin %s does not implement the Plugin interface',
                    $pluginAlias
                ));
            }

            $plugin->attachToEventStore($wrapper);
        }

        $metadataEnrichers = [];

        foreach ($config['metadata_enrichers'] as $metadataEnricherAlias) {
            $metadataEnricher = $container->get($metadataEnricherAlias);

            if (! $metadataEnricher instanceof MetadataEnricher) {
                throw ConfigurationException::configurationError(sprintf(
                    'Metadata enricher %s does not implement the MetadataEnricher interface',
                    $metadataEnricherAlias
                ));
            }

            $metadataEnrichers[] = $metadataEnricher;
        }

        if (count($metadataEnrichers) > 0) {
            $plugin = new MetadataEnricherPlugin(
                new MetadataEnricherAggregate($metadataEnrichers)
            );

            $plugin->attachToEventStore($wrapper);
        }

        return $wrapper;
    }

    protected function createActionEventEmitterEventStore(EventStore $eventStore): ActionEventEmitterEventStore
    {
        return new ActionEventEmitterEventStore(
            $eventStore,
            new ProophActionEventEmitter([
                ActionEventEmitterEventStore::EVENT_APPEND_TO,
                ActionEventEmitterEventStore::EVENT_CREATE,
                ActionEventEmitterEventStore::EVENT_LOAD,
                ActionEventEmitterEventStore::EVENT_LOAD_REVERSE,
                ActionEventEmitterEventStore::EVENT_DELETE,
                ActionEventEmitterEventStore::EVENT_HAS_STREAM,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_UPDATE_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_NAMES_REGEX,
                ActionEventEmitterEventStore::EVENT_FETCH_CATEGORY_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_CATEGORY_NAMES_REGEX,
            ])
        );
    }

    public function defaultOptions(): iterable
    {
        return [
            'load_batch_size' => 1000,
            'event_streams_table' => 'event_streams',
            'message_factory' => FQCNMessageFactory::class,
            'wrap_action_event_emitter' => true,
            'metadata_enrichers' => [],
            'plugins' => [],
            'disable_transaction_handling' => false,
        ];
    }

    public function dimensions(): iterable
    {
        return ['prooph', 'event_store'];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'connection',
            'persistence_strategy',
        ];
    }
}
