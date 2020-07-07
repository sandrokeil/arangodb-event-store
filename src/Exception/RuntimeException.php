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

use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;
use Psr\Http\Message\ResponseInterface;

class RuntimeException extends EventStoreRuntimeException implements ArangoDbEventStoreException
{
    public static function fromErrorResponse(ResponseInterface $response)
    {
        $body = $response->getBody()->getContents();
        $data = \json_decode($body, true) ?: [];

        return new self(\sprintf(
                'Code: %s Error Number: %s Error Message: %s Raw: %s',
                $data['code'] ?? '',
                $data['errorNum'] ?? '',
                $data['errorMessage'] ?? '',
                $body
            )
        );
    }

    public static function fromServerException(\Throwable $e)
    {
        return new self($e->getMessage(), $e->getCode(), $e);
    }

    public static function fromResponse(ResponseInterface $response)
    {
        return new self($response->getReasonPhrase(), $response->getStatusCode());
    }
}
