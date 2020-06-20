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

use Prooph\EventStore\Exception\ProjectionNotFound as ProophProjectionNotFound;

class ProjectionNotFound extends ProophProjectionNotFound
{
    /**
     * Response JSON
     *
     * @var string
     */
    private $responseBody;

    public function response(): ?string
    {
        return $this->responseBody;
    }

    public static function with(string $name, string $responseBody): ProjectionNotFound
    {
        $self = new self('A projection with name "' . $name . '" could not be found.');
        $self->responseBody = $responseBody;

        return $self;
    }
}
