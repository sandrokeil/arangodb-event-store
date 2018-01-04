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

use ArangoDb\Response;

class ProjectionNotCreatedException extends RuntimeException
{
    /**
     * @var Response
     */
    private $response;

    public function response(): ?Response
    {
        return $this->response;
    }

    public static function with(string $projectionName, ?Response $response): ProjectionNotCreatedException
    {
        $self = new self(sprintf('Projection "%s" was not created', $projectionName));
        $self->response = $response;

        return $self;
    }
}
