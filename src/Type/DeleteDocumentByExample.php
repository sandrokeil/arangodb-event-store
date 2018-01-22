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

final class DeleteDocumentByExample implements Type
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
     * Inspects response
     *
     * @var callable
     */
    private $inspector;

    private function __construct(string $collectionName, array $example, array $options = [],
        callable $inspector = null)
    {
        $this->collectionName = $collectionName;
        $this->example = $example;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            if ($rId) {
                return null;
            }

            return strpos($response->getBody(), '"deleted":0') !== false ? 404 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#removes-multiple-documents
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#remove-by-example
     *
     * @param string $collectionName
     * @param array $example
     * @param array $options
     * @return DeleteDocumentByExample
     */
    public static function with(string $collectionName, array $example, array $options = []): DeleteDocumentByExample
    {
        return new self($collectionName, $example, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#removes-multiple-documents
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#remove-by-example
     *
     * @param string $collectionName
     * @param array $example
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return DeleteDocumentByExample
     */
    public static function withInspector(
        string $collectionName,
        array $example,
        callable $inspector,
        array $options = []
    ): DeleteDocumentByExample {
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
        return $this->buildAppendBatch(
            HttpHelper::METHOD_PUT,
            Urls::URL_REMOVE_BY_EXAMPLE,
            [
                'collection' => $this->collectionName,
                'example' => $this->example,
            ],
            $this->options
        );
    }

    public function toJs(): string
    {
        $options = ! empty($this->options['waitForSync']) ? ', true' : ', false';

        if (! empty($this->options['limit'])) {
            $options .= ', ' . (int) $this->options['limit'];
        }

        return 'var rId = db.' . $this->collectionName . '.removeByExample(' . json_encode($this->example) . $options . ');';
    }
}
