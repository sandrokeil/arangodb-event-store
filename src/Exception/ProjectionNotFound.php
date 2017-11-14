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
use Prooph\EventStore\Exception\ProjectionNotFound as ProophProjectionNotFound;

class ProjectionNotFound extends ProophProjectionNotFound
{
    /**
     * @var Response
     */
    private $response;

    public function response(): ?Response
    {
        return $this->response;
    }

    public static function with(string $name, ?Response $response): ProjectionNotFound
    {
        $self = new self('A projection with name "' . $name . '" could not be found.');
        $self->response = $response;

        return $self;
    }
}
