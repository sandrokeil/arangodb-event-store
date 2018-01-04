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

use ArangoDb\Response;
use Prooph\EventStore\ArangoDb\Exception\LogicException;

final class UpdateDocumentByExample implements Type
{
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
        array $example,
        array $data,
        array $options = [],
        callable $inspector = null
    ) {
        $this->collectionName = $collectionName;
        $this->data = $data;
        $this->example = $example;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            return strpos($response->getBody(), '"' . $rId . '"' . ':0') !== false ? 404 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#update-documents
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#update-by-example
     *
     * @param string $collectionName
     * @param array $example
     * @param array $data
     * @param array $options
     * @return UpdateDocumentByExample
     */
    public static function with(
        string $collectionName,
        array $example,
        array $data,
        array $options = []
    ): UpdateDocumentByExample {
        return new self($collectionName, $example, $data, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#update-documents
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#update-by-example
     *
     * @param string $collectionName
     * @param array $example
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return UpdateDocumentByExample
     */
    public static function withInspector(
        string $collectionName,
        array $example,
        callable $inspector,
        array $options = []
    ): UpdateDocumentByExample {
        return new self($collectionName, $example, $options, $inspector);
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
        throw new LogicException('Not possible at the moment, see ArangoDB docs');
    }

    public function toJs(): string
    {
        return 'var rId = db.' . $this->collectionName
            . '.updateByExample(' . json_encode($this->example) . ', ' . json_encode($this->data) . ', ' . json_encode($this->options) . ');';
    }
}
