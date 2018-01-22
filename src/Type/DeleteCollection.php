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

use ArangoDBClient\HttpHelper;
use ArangoDb\Response;
use ArangoDBClient\Urls;

final class DeleteCollection implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * @var array
     */
    private $options;

    /**
     * Inspects response
     *
     * @var callable
     */
    private $inspector;

    private function __construct(string $collectionName, array $options = [],
        callable $inspector = null)
    {
        $this->collectionName = $collectionName;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            if ($rId) {
                return null;
            }

            return strpos($response->getBody(), '"error":false') === false ? 422 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#drops-a-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/CollectionMethods.html#drop
     *
     * @param string $collectionName
     * @param array $options
     * @return DeleteCollection
     */
    public static function with(string $collectionName, array $options = []): DeleteCollection
    {
        return new self($collectionName, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#drops-a-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/CollectionMethods.html#drop
     *
     * @param string $collectionName
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return DeleteCollection
     */
    public static function withInspector(
        string $collectionName,
        callable $inspector,
        array $options = []
    ): DeleteCollection {
        return new self($collectionName, $options, $inspector);
    }

    public function checkResponse(Response $response, string $rId = null): ?int
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
            HttpHelper::METHOD_DELETE,
            Urls::URL_COLLECTION . '/' . $this->collectionName,
            $this->options
        );
    }

    public function toJs(): string
    {
        return 'var rId = db._drop("' . $this->collectionName . '", '
            . ($this->options ? json_encode($this->options) : '{}') . ');';
    }
}
