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

namespace Prooph\EventStore\ArangoDb\Type;

use ArangoDBClient\HttpHelper;
use ArangoDBClient\HttpResponse;
use ArangoDBClient\Urls;

final class TruncateCollection implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * Inspects response
     *
     * @var callable
     */
    private $inspector;

    private function __construct(string $collectionName, callable $inspector = null)
    {
        $this->collectionName = $collectionName;
        $this->inspector = $inspector ?: function (HttpResponse $response, string $rId = null) {
            if ($rId) {
                return null;
            }

            return strpos($response->getBody(), '"error":false') === false ? 422 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#truncate-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/DatabaseMethods.html#truncate
     *
     * @param string $collectionName
     * @return TruncateCollection
     */
    public static function with(string $collectionName): TruncateCollection
    {
        return new self($collectionName);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#truncate-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/DatabaseMethods.html#truncate
     *
     * @param string $collectionName
     * @param callable $inspector Inspects result, signature is (HttpResponse $response, string $rId = null)
     * @return TruncateCollection
     */
    public static function withInspector(
        string $collectionName,
        callable $inspector
    ): TruncateCollection {
        return new self($collectionName, $inspector);
    }

    public function checkResponse(HttpResponse $response, string $rId = null): ?int
    {
        return ($this->inspector)($response, $rId);
    }

    public function collectionName(): string
    {
        return $this->collectionName;
    }

    public function toHttp(): iterable
    {
        return $this->buildAppendBatch(
            HttpHelper::METHOD_PUT,
            Urls::URL_COLLECTION . '/' . $this->collectionName . '/truncate',
            []
        );
    }

    public function toJs(): string
    {
        return 'var rId = db._truncate("' . $this->collectionName . '");';
    }
}
