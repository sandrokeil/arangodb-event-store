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
use Prooph\EventStore\ArangoDb\JsonIterator;

final class InsertDocument implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $collectionName;

    /**
     * @var iterable
     */
    private $streamEvents;

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
        iterable $streamEvents,
        array $options = [],
        callable $inspector = null
    ) {
        $this->collectionName = $collectionName;
        $this->streamEvents = $streamEvents;
        $this->options = $options;
        $this->inspector = $inspector ?: function (Response $response, string $rId = null) {
            if (null === $rId) {
                return null;
            }

            return strpos($response->getBody(), '"' . $rId . '"' . ':0') !== false
                    || strpos($response->getBody(), '"' . $rId . '"' . ':[{"error":true') !== false
                ? 404 : null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#insert
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#create-document
     *
     * @param string $collectionName
     * @param iterable $docs
     * @param array $options
     * @return InsertDocument
     */
    public static function with(string $collectionName, iterable $docs, array $options = []): InsertDocument
    {
        return new self($collectionName, $docs, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/Manual/DataModeling/Documents/DocumentMethods.html#insert
     * @see https://docs.arangodb.com/3.2/HTTP/Document/WorkingWithDocuments.html#create-document
     *
     * @param string $collectionName
     * @param iterable $docs
     * @param callable $inspector Inspects result, signature is (Response $response, string $rId = null)
     * @param array $options
     * @return InsertDocument
     */
    public static function withInspector(
        string $collectionName,
        iterable $docs,
        callable $inspector,
        array $options = []
    ): InsertDocument {
        return new self($collectionName, $docs, $options, $inspector);
    }

    public function collectionName(): string
    {
        return $this->collectionName;
    }

    public function checkResponse(Response $response, string $rId = null): ?int
    {
        return ($this->inspector)($response, $rId);
    }

    public function toHttp(): iterable
    {
        foreach ($this->streamEvents as $streamEvent) {
            yield $this->buildAppendBatch(
                HttpHelper::METHOD_POST,
                Urls::URL_DOCUMENT . '/' . $this->collectionName,
                $streamEvent,
                $this->options
            );
        }
    }

    public function toJs(): string
    {
        if ($this->streamEvents instanceof JsonIterator) {
            return 'var rId = db.' . $this->collectionName
                . '.insert(' . $this->streamEvents->asJson() . ', ' .  json_encode($this->options) . ');';
        }

        return 'var rId = db.' . $this->collectionName
            . '.insert(' . json_encode($this->streamEvents) . ', ' .  json_encode($this->options) . ');';
    }

    public function count(): int
    {
        return count($this->streamEvents);
    }
}
