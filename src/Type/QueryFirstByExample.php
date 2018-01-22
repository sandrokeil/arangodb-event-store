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

final class QueryFirstByExample implements Type, HasResponse
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * @var array
     */
    private $example;

    /**
     * @var array
     */
    private $options;

    /**
     * @var string
     */
    private $result = '{}';

    /**
     * Inspects response
     *
     * @var callable
     */
    private $inspector;

    private function __construct(
        string $collectionName,
        array $example,
        array $options = [],
        callable $inspector = null
    ) {
        $this->collectionName = $collectionName;
        $this->example = $example;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            return null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/SimpleQuery/#find-documents-matching-an-example
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#first-example
     *
     * @param string $collectionName
     * @param array $example
     * @param array $options
     * @return QueryFirstByExample
     */
    public static function with(string $collectionName, array $example, array $options = []): QueryFirstByExample
    {
        return new self($collectionName, $example, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/SimpleQuery/#find-documents-matching-an-example
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#first-example
     *
     * @param string $collectionName
     * @param array $example
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return QueryFirstByExample
     */
    public static function withInspector(
        string $collectionName,
        array $example,
        callable $inspector,
        array $options = []
    ): QueryFirstByExample {
        return new self($collectionName, $example, $options, $inspector);
    }

    public function checkResponse(Response $response, string $rId = null): ?int
    {
        $this->result = $response->getBody();

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
            Urls::URL_EXAMPLE,
            array_merge(
                $this->options,
                [
                    'collection' => $this->collectionName,
                    'example' => $this->example,
                ]
            )
        );
    }

    public function toJs(): string
    {
        $args = '';

        foreach ($this->example as $field => $value) {
            $args .= $field . ', ' . $value;
        }

        return 'var rId = db.' . $this->collectionName . '.firstExample(' . $args . ');';
    }

    public function rawResult(): ?string
    {
        return $this->result === '{}' ? null : $this->result;
    }

    public function result()
    {
        return json_decode($this->result, true)['result'] ?? null;
    }
}
