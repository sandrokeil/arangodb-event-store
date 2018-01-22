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

class CreateCollection implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $name;

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

    private function __construct(
        string $name,
        array $options = [],
        callable $inspector = null
    ) {
        $this->name = $name;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            if ($rId) {
                return null;
            }

            return strpos($response->getBody(), '"error":false') === false ? 422 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#create-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/DatabaseMethods.html#create
     *
     * @param string $collectionName
     * @param array $options
     * @return CreateCollection
     */
    public static function with(string $collectionName, array $options = []): CreateCollection
    {
        return new self($collectionName, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Collection/Creating.html#create-collection
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Collections/DatabaseMethods.html#create
     *
     * @param string $collectionName
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return CreateCollection
     */
    public static function withInspector(
        string $collectionName,
        callable $inspector,
        array $options = []
    ): CreateCollection {
        return new self($collectionName, $options, $inspector);
    }

    public function checkResponse(Response $response, string $rId = null): ?int
    {
        return ($this->inspector)($response, $rId);
    }

    public function collectionName(): string
    {
        return $this->name;
    }

    public function toHttp(): iterable
    {
        $options = $this->options;
        $options['name'] = $this->name;

        return $this->buildAppendBatch(
            HttpHelper::METHOD_POST,
            Urls::URL_COLLECTION,
            $options
        );
    }

    public function toJs(): string
    {
        return 'var rId = db._create("' . $this->name . '", ' . json_encode($this->options) . ');';
    }
}
