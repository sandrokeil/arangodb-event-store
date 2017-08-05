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

namespace Prooph\EventStore\ArangoDb\Exception;

use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;

class RuntimeException extends EventStoreRuntimeException implements ArangoDbEventStoreException
{
    public static function fromErrorResponse(string $body)
    {
        $data = json_decode($body, true) ?: [];

        return new self(sprintf(
                'Code: %s Error Number: %s Error Message: %s',
                $data['code'] ?? '',
                $data['errorNum'] ?? '',
                $data['errorMessage'] ?? ''
            )
        );
    }

    public static function fromServerException(\ArangoDBClient\ServerException $e)
    {
        return new self($e->getMessage(), $e->getCode(), $e);
    }
}
