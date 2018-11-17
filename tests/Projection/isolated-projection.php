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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\ArangoDb\ArangoDbEventStore;
use Prooph\EventStore\ArangoDb\PersistenceStrategy\SimpleStreamStrategy;
use Prooph\EventStore\ArangoDb\Projection\ProjectionManager;
use Prooph\EventStore\ArangoDb\Projection\Projector;
use ProophTest\EventStore\ArangoDb\TestUtil;
use ProophTest\EventStore\Mock\UserCreated;

require __DIR__ . '/../../vendor/autoload.php';

$connection = TestUtil::getClient();

$eventStore = new ArangoDbEventStore(
    new FQCNMessageFactory(),
    $connection,
    new SimpleStreamStrategy()
);

$projectionManager = new ProjectionManager(
    $eventStore,
    $connection
);
$projection = $projectionManager->createProjection(
    'test_projection',
    [
        Projector::OPTION_PCNTL_DISPATCH => true,
    ]
);
\pcntl_signal(SIGQUIT, function () use ($projection) {
    $projection->stop();
    exit(SIGUSR1);
});
$projection
    ->fromStream('user-123')
    ->when([
        UserCreated::class => function (array $state, UserCreated $event): array {
            return $state;
        },
    ])
    ->run();
