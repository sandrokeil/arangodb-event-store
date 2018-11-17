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

namespace Prooph\EventStore\ArangoDb\Fn;

use ArangoDb\Http\VpackStream;
use ArangoDb\Type;
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;
use Velocypack\Vpack;

//function executeInTransaction(ClientInterface $connection, ?array $onError, Type\Type ...$batches): void
//{
//    $actions = '';
//    $collections = [];
//    $return = [];
//
//    foreach ($batches as $key => $type) {
//        $collections[] = $type->collectionName();
//        $actions .= str_replace('var rId', 'var rId' . $key, $type->toJs());
//        $return[] = 'rId' . $key;
//    }
//
//    try {
//        $response = $connection->request(
//            'post',
//            Urls::URL_TRANSACTION,
//            [
//                RequestOptions::BODY => json_encode(
//                    [
//                        'collections' => [
//                            'write' => array_unique($collections),
//                        ],
//                        'action' => sprintf("function () {var db = require('@arangodb').db;%s return {%s}}", $actions,
//                            implode(',', $return)),
//                    ]
//                ),
//            ]
//        );
//    } catch (\Throwable $e) {
//        $error = $e->getCode();
//        if (isset($onError[0][$error]) && method_exists($onError[0][$error][0], 'with')) {
//            $args = array_slice($onError[0][$error], 1);
//            $args[] = null;
//            throw call_user_func_array($onError[0][$error][0] . '::with', $args);
//        }
//        throw RuntimeException::fromServerException($e);
//    }
//
//    foreach ($batches as $key => $batch) {
//        checkResponse($response, $onError[$key] ?? null, $batches[$key], 'rId' . $key);
//    }
//}

function execute(ClientInterface $connection, ?array $onError, Type\Type ...$batches): void
{
    foreach ($batches as $key => $type) {
        $response = $connection->sendRequest($type->toRequest());
        checkResponse($response, $onError[$key] ?? null, $type);
    }
}

function checkResponse(ResponseInterface $response, ?array $onError, Type\Type $type, string $rId = null): void
{
    $httpCode = $response->getStatusCode();

    return;
    if ($httpCode < 200 || $httpCode > 300) {
        $error = $httpCode;
    } else {
//        $error = $type->checkResponse($response, $rId);
        // TODO
        return;
    }

    if ($error) {
        if (isset($onError[$error]) && \method_exists($onError[$error][0], 'with')) {
            $args = \array_slice($onError[$error], 1);
            $args[] = $response;
            throw \call_user_func_array($onError[$error][0] . '::with', $args);
        }
        throw RuntimeException::fromErrorResponse($response->getBody()->getContents(), $type);
    }
}

/**
 * @param ResponseInterface $response
 * @param string|null $key
 * @return string|bool|int|float
 */
function responseContentAsJson(ResponseInterface $response, string $key = null)
{
    $body = $response->getBody();

    if ($body instanceof VpackStream) {
        if ($key === null) {
            return $body->vpack()->toJson();
        }
        $value = $body->vpack()->access($key);

        if ($value instanceof Vpack) {
            $value = $value->toJson();
        }

        return $value;
    }
    if ($key === null) {
        return $body->getContents();
    }
    // TODO check key
    $value = \json_decode($body->getContents())->{$key};
    if (! \is_scalar($value)) {
        $value = \json_encode($value);
    }

    return $value;
}

/**
 * @param ResponseInterface $response
 * @param string|null $key
 * @return string|bool|int|float|array
 */
function responseContentAsArray(ResponseInterface $response, string $key = null)
{
    $body = $response->getBody();

    if ($body instanceof VpackStream) {
        if ($key === null) {
            return $body->vpack()->toArray();
        }
        $value = $body->vpack()->access($key);

        if ($value instanceof Vpack) {
            $value = $value->toArray();
        }

        return $value;
    }
    if ($key === null) {
        return $body->getContents();
    }
    // TODO check key
    return \json_decode($body->getContents(), true)[$key] ?? null;
}

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
                    'allowUserKeys' => false,
                    'type' => 'autoincrement',
                    'increment' => 1,
                    'offset' => 1,
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
