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

namespace Prooph\EventStore\ArangoDb\Iterator;

use IteratorIterator;
use Prooph\EventStore\ArangoDb\JsonIterator;

final class JsonSimpleStreamIterator extends IteratorIterator implements JsonIterator
{
    public function asJson(): string
    {
        $json = '[';

        foreach ($this->getInnerIterator() as $event) {
            $json .= json_encode([
                    'event_id' => $event->uuid()->toString(),
                    'event_name' => $event->messageName(),
                    'payload' => $event->payload(),
                    'metadata' => $event->metadata(),
                    'created_at' => $event->createdAt()->format('Y-m-d\TH:i:s.u'),
                ]) . ',';
        }

        return $json . ']';
    }
}
