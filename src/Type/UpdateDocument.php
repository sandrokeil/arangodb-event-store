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

final class UpdateDocument implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * @var string
     */
    private $id;

    /**
     * @var array
     */
    private $data;

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
        string $collectionName,
        string $id,
        array $data,
        array $options = [],
        callable $inspector = null
    ) {
        $this->collectionName = $collectionName;
        $this->data = $data;
        $this->id = $id;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            if ($rId) {
                return null;
            }

            return strpos($response->getBody(), '"error":false') === false
            && strpos($response->getBody(), '"_key":"') === false ? 422 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#update-document
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#update
     *
     * @param string $collectionName
     * @param string $id
     * @param array $data
     * @param array $options
     * @return UpdateDocument
     */
    public static function with(string $collectionName, string $id, array $data, array $options = []): UpdateDocument
    {
        return new self($collectionName, $id, $data, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#update-document
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#update
     *
     * @param string $collectionName
     * @param string $id
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return UpdateDocument
     */
    public static function withInspector(
        string $collectionName,
        string $id,
        callable $inspector,
        array $options = []
    ): UpdateDocument {
        return new self($collectionName, $id, $options, $inspector);
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
            HttpHelper::METHOD_PATCH,
            Urls::URL_DOCUMENT . '/' . $this->collectionName . '/' . $this->id,
            $this->data,
            $this->options
        );
    }

    public function toJs(): string
    {
        return 'var rId = db.' . $this->collectionName
            . '.update("' . $this->id . '", ' . json_encode($this->data) . ', ' . json_encode($this->options) . ');';
    }
}
