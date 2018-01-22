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

namespace Prooph\EventStore\ArangoDb\Type;

use ArangoDb\Response;

interface Type
{
    public function toHttp(): iterable;

    public function toJs(): string;

    public function collectionName(): string;

    public function checkResponse(Response $response, string $rId = null): ?int;
}
