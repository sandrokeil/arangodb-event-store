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

namespace Prooph\EventStore\ArangoDb\Func;

use ArangoDb\Type;

/**
 * @return Type\Type[]
 */
function eventStreamsBatch(): array
{
    return [
        Type\Collection::create(
            'event_streams',
            [
                'keyOptions' => [
                    'allowUserKeys' => true,
                    'type' => 'traditional',
                ],
            ]
        ),
        Type\Index::create(
            'event_streams',
            [
                'type' => 'hash',
                'fields' => [
                    'real_stream_name',
                ],
                'selectivityEstimate' => 1,
                'unique' => true,
                'sparse' => false,
            ]
        ),
        Type\Index::create(
            'event_streams',
            [
                'type' => 'skiplist',
                'fields' => [
                    'category',
                ],
                'selectivityEstimate' => 1,
                'unique' => false,
                'sparse' => false,
            ]
        ),
    ];
}

/**
 * @return Type\Type[]
 */
function projectionsBatch(): array
{
    return [
        Type\Collection::create(
            'projections',
            [
                'keyOptions' => [
                    'allowUserKeys' => true,
                    'type' => 'traditional',
                ],
            ]),
        Type\Index::create(
            'projections',
            [
                'type' => 'skiplist',
                'fields' => [
                    '_key',
                ],
                'selectivityEstimate' => 1,
                'unique' => true,
                'sparse' => false,
            ]
        ),
    ];
}
