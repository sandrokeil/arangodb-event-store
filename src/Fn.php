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

namespace Prooph\EventStore\ArangoDb\Fn;

use ArangoDBClient\Batch;
use ArangoDBClient\BatchPart;
use ArangoDBClient\Connection;
use ArangoDBClient\HttpResponse;
use ArangoDBClient\Urls;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\ArangoDb\Type;
use Prooph\EventStore\ArangoDb\Type\InsertDocument;

function executeInTransaction(Connection $connection, ?array $onError, Type\Type ...$batches): void
{
    $actions = '';
    $collections = [];
    $return = [];

    foreach ($batches as $key => $type) {
        $collections[] = $type->collectionName();
        $actions .= str_replace('var rId', 'var rId' . $key, $type->toJs());
        $return[] = 'rId' . $key;
    }

    try {
        $response = $connection->post(
            Urls::URL_TRANSACTION,
            json_encode(
                [
                    'collections' => [
                        'write' => array_unique($collections),
                    ],
                    'action' => sprintf("function () {var db = require('@arangodb').db;%s return {%s}}", $actions,
                        implode(',', $return)),
                ]
            )
        );
    } catch (\ArangoDBClient\ServerException | \ArangoDBClient\Exception $e) {
        $error = $e->getCode();
        if (isset($onError[0][$error]) && method_exists($onError[0][$error][0], 'with')) {
            $args = array_slice($onError[0][$error], 1);
            $args[] = null;
            throw call_user_func_array($onError[0][$error][0] . '::with', $args);
        }
        throw RuntimeException::fromServerException($e);
    }

    foreach ($batches as $key => $batch) {
        checkResponse($response, $onError[$key] ?? null, $batches[$key], 'rId' . $key);
    }
}

function execute(Connection $connection, ?array $onError, Type\Type ...$batches): void
{
    $batchSize = count($batches);
    $batch = new Batch($connection, ['batchSize' => $batchSize, 'startCapture' => false]);

    foreach ($batches as $type) {
        if ($type instanceof InsertDocument) {
            $batch->getBatchParts()->setSize($batchSize + $type->count() - 1);
            foreach ($type->toHttp() as $item) {
                $batch->append(...$item);
            }
            continue;
        }
        $batch->append(...$type->toHttp());
    }

    /* @var $batchPart BatchPart */
    foreach ($batch->process()->getBatchParts() as $key => $batchPart) {
        checkResponse($batchPart->getResponse(), $onError[$key] ?? null, $batches[$key]);
    }
}

function checkResponse(HttpResponse $response, ?array $onError, Type\Type $type, string $rId = null): void
{
    /* @var $response HttpResponse */

    $httpCode = $response->getHttpCode();
    if ($httpCode < 200 || $httpCode > 300) {
        $error = $httpCode;
    } else {
        $error = $type->checkResponse($response, $rId);
    }

    if ($error) {
        if (isset($onError[$error]) && method_exists($onError[$error][0], 'with')) {
            $args = array_slice($onError[$error], 1);
            $args[] = $response;
            throw call_user_func_array($onError[$error][0] . '::with', $args);
        }
        throw RuntimeException::fromErrorResponse($response->getBody());
    }
}

function eventStreamsBatch(Connection $connection): Batch
{
    $batch = new Batch($connection, ['batchSize' => 3, 'startCapture' => false]);

    $batch->append(
        ...Type\CreateCollection::with(
            'event_streams',
            [
                'keyOptions' => [
                    'allowUserKeys' => false,
                    'type' => 'autoincrement',
                    'increment' => 1,
                    'offset' => 1,
                ],
            ]
        )->toHttp()
    );

    $batch->append(
        ...Type\CreateIndex::with(
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
        )->toHttp()
    );

    $batch->append(
        ...Type\CreateIndex::with(
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
        )->toHttp()
    );

    return $batch;
}

function projectionsBatch(Connection $connection): Batch
{
    $batch = new Batch($connection, ['batchSize' => 2, 'startCapture' => false]);

    $batch->append(
        ...Type\CreateCollection::with(
            'projections',
            [
                'keyOptions' => [
                    'allowUserKeys' => true,
                    'type' => 'traditional',
                ],
            ]
        )->toHttp()
    );

    $batch->append(
        ...Type\CreateIndex::with(
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
        )->toHttp()
    );

    return $batch;
}
