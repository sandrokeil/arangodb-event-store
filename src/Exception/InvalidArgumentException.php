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

namespace Prooph\EventStore\ArangoDb\Exception;

use Prooph\EventStore\Exception\InvalidArgumentException as EventStoreInvalidArgumentException;

class InvalidArgumentException extends EventStoreInvalidArgumentException implements ArangoDbEventStoreException
{
}
