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

final class WrongTypeClass extends LogicException
{
    public static function forType(string $type, string $fqcn)
    {
        return new self(
            \sprintf('The provided class name "%s" does not implement "%s".', $fqcn, $type)
        );
    }
}
