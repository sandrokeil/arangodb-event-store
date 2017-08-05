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

use ArangoDBClient\HttpResponse;

class ProjectionAlreadyExistsException extends RuntimeException
{
    /**
     * @var HttpResponse
     */
    private $response;

    public function response(): ?HttpResponse
    {
        return $this->response;
    }

    public static function with(string $projectionName, ?HttpResponse $response): ProjectionAlreadyExistsException
    {
        $self = new self(sprintf('Another projection process is already running for projection "%s"', $projectionName));
        $self->response = $response;

        return $self;
    }
}