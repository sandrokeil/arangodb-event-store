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

namespace Prooph\EventStore\ArangoDb;

use ArangoDb\Connection;
use ArangoDb\Cursor;
use ArangoDb\Vpack;
use ArangoDBClient\Statement;
use ArangoDBClient\Urls;
use Assert\Assertion;
use Iterator;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\ArangoDb\Type\DeleteCollection;
use Prooph\EventStore\ArangoDb\Type\DeleteDocumentByExample;
use Prooph\EventStore\ArangoDb\Type\Type;
use Prooph\EventStore\ArangoDb\Type\UpdateDocumentByExample;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use function Prooph\EventStore\ArangoDb\Fn\execute;
use function Prooph\EventStore\ArangoDb\Fn\executeInTransaction;
use Prooph\EventStore\TransactionalEventStore;

final class EventStore implements ProophEventStore, TransactionalEventStore
{
    private const SORT_ASC = 'ASC';
    private const SORT_DESC = 'DESC';

    /**
     * @var Connection
     */
    private $connection;
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    /**
     * @var int
     */
    private $loadBatchSize;

    /**
     * @var string
     */
    private $eventStreamsCollection;

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     * @var bool
     */
    private $inTransaction = false;

    /**
     * @var array
     */
    private $writeCollections = [];

    /**
     * @var int
     */
    private $resultId = -1;

    /**
     * @var string
     */
    private $actions = '';

    /**
     * @var Type[]
     */
    private $nonTransactionActions = [];

    private $results = [];

    public function __construct(
        MessageFactory $messageFactory,
        Connection $connection,
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsCollection = 'event_streams',
        bool $disableTransactionHandling = false
    ) {
        Assertion::min($loadBatchSize, 1);

        $this->messageFactory = $messageFactory;
        $this->connection = $connection;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsCollection = $eventStreamsCollection;
        $this->disableTransactionHandling = $disableTransactionHandling;
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        executeInTransaction(
            $this->connection,
            [
                [
                    404 => [StreamNotFound::class, $streamName],
                ],
            ],
            UpdateDocumentByExample::with(
                $this->eventStreamsCollection,
                ['real_stream_name' => $streamName->toString()],
                ['metadata' => $newMetadata],
                ['mergeObjects' => false]
            )
        );
    }

    public function create(Stream $stream): void
    {
        $streamName = $stream->streamName();
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        if (!$this->inTransaction && !$this->disableTransactionHandling) {
            /* @var $type Type */
            foreach ($this->persistenceStrategy->createCollection($collectionName) as $type) {
                $item = $type->toHttp();
                $this->connection->{$item[0]}(
                    $item[1],
                    Vpack::fromArray($item[2]),
                    $item[3]
                );
            }

            $this->connection->post(
                Urls::URL_DOCUMENT . '/' .  $this->eventStreamsCollection,
                Vpack::fromArray($this->createEventStreamData($stream)),
                ['silent' => true]
            );

            $this->connection->post(
                Urls::URL_DOCUMENT . '/' . $collectionName,
                Vpack::fromArray($this->persistenceStrategy->prepareData($stream->streamEvents())),
                ['silent' => true]
            );
            return;
        }

        /* @var $type Type */
        foreach ($this->persistenceStrategy->createCollection($collectionName) as $type) {
            $this->nonTransactionActions[] = $type;
        }

        $this->actions = 'var rId' . ++$this->resultId . ' = db.' . $this->eventStreamsCollection
            . '.insert(' . json_encode([$this->createEventStreamData($stream)])
            . ', {"silent":true});';
        $this->results[] .= 'rId' . $this->resultId;
        $this->writeCollections[] = $this->eventStreamsCollection;

        $this->actions .= 'var rId' . ++$this->resultId . ' = db.' . $collectionName
            . '.insert(' . $this->persistenceStrategy->jsonIterator($stream->streamEvents())->asJson()
            . ', {"silent":true});';
        $this->results[] .= 'rId' . $this->resultId;
        $this->writeCollections[] = $collectionName;
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        if (!$this->inTransaction && !$this->disableTransactionHandling) {
            $this->connection->post(
                Urls::URL_DOCUMENT . '/' . $collectionName,
                Vpack::fromArray($this->persistenceStrategy->prepareData($streamEvents)),
                ['silent' => true]
            );
            return;
        }

        $this->writeCollections[] = $collectionName;
        $this->actions .= 'var rId' . ++$this->resultId . ' = db.' . $collectionName
            . '.insert(' . $this->persistenceStrategy->jsonIterator($streamEvents)->asJson()
            . ', {"silent":true});';
        $this->results[] .= 'rId' . $this->resultId;
    }

    public function delete(StreamName $streamName): void
    {
        execute(
            $this->connection,
            [
                [
                    404 => [StreamNotFound::class, $streamName],
                ],
                [
                    404 => [StreamNotFound::class, $streamName],
                ],
            ],
            DeleteDocumentByExample::with(
                $this->eventStreamsCollection, ['real_stream_name' => $streamName->toString()]
            ),
            DeleteCollection::with($this->persistenceStrategy->generateCollectionName($streamName))
        );
    }

    public function load(
        StreamName $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        return $this->loadBy(self::SORT_ASC, $streamName, $fromNumber, $count, $metadataMatcher);
    }

    public function loadReverse(
        StreamName $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): Iterator {
        return $this->loadBy(self::SORT_DESC, $streamName, $fromNumber, $count, $metadataMatcher);
    }

    private function loadBy(
        $dir,
        StreamName $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): StreamIterator {
        $fromNumber = $this->persistenceStrategy->padPosition(
            null === $fromNumber && $dir === self::SORT_DESC ? PHP_INT_MAX : $fromNumber
        );

        [$where, $values] = $this->createWhereClause($metadataMatcher);

        $filter = implode(' AND ', $where);

        if (count($where)) {
            $filter = ' AND ' . $filter;
        }

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = min($count, $this->loadBatchSize);
        }

        $aql = <<<'EOF'
FOR c IN  @@collection
FILTER c._key %op% @from %filter%
SORT c._key %DIR%
LIMIT @limit
RETURN {
    "no": c._key,
    "event_id": c.event_id,
    "event_name": c.event_name,
    "payload": c.payload,
    "metadata": c.metadata,
    "created_at": c.created_at
}
EOF;
        $collectionName = $this->persistenceStrategy->generateCollectionName($streamName);

        $cursor = $this->connection->query(
            Vpack::fromJson(json_encode(
                [
                    Statement::ENTRY_QUERY => str_replace(
                        ['%DIR%', '%op%', '%filter%'],
                        [$dir, $dir === self::SORT_ASC ? '>=' : '<=', $filter],
                        $aql
                    ),
                    Statement::ENTRY_BINDVARS => array_merge(
                        [
                            '@collection' => $collectionName,
                            'from' => (string)$fromNumber,
                            'limit' => $limit,
                        ],
                        $values),
                    Statement::ENTRY_BATCHSIZE => 1000,
                ]
            )),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_JSON,
            ]
        );

        try {
            return new StreamIterator(
                $cursor,
                $this->persistenceStrategy->offsetNumber(),
                $this->messageFactory
            );
        } catch (\Throwable $e) {
            if ($cursor->getResponse()->getHttpCode() === 404) {
                throw StreamNotFound::with($streamName);
            }
            throw $e;
        }
    }

    public function fetchStreamNames(
        ?string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        return $this->fetchStreamNamesBy(false, $filter, $metadataMatcher, $limit, $offset);
    }

    public function fetchStreamNamesRegex(
        string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array {
        return $this->fetchStreamNamesBy(true, $filter, $metadataMatcher, $limit, $offset);
    }

    private function fetchStreamNamesBy(
        bool $isRegex,
        ?string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ) {
        [$where, $values] = $this->createWhereClause($metadataMatcher);

        if ($isRegex) {
            if (empty($filter) || false === @preg_match("/$filter/", '')) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            $where[] = 'c.real_stream_name =~ @name';
            $values['name'] = $filter;
        } elseif (null !== $filter) {
            $where[] = 'c.real_stream_name == @name';
            $values['name'] = $filter;
        }

        $filter = implode(' AND ', $where);

        if (! empty($filter)) {
            $filter = 'FILTER ' . $filter;
        }

        $aql = <<<'EOF'
FOR c IN  @@collection
%filter%
SORT c.real_stream_name ASC
LIMIT @offset, @limit
RETURN {
    "real_stream_name": c.real_stream_name
}
EOF;

        $cursor = $this->connection->query(
            Vpack::fromJson(json_encode(
                [
                    Statement::ENTRY_QUERY => str_replace('%filter%', $filter, $aql),
                    Statement::ENTRY_BINDVARS => array_merge(
                        [
                            '@collection' => $this->eventStreamsCollection,
                            'offset' => $offset,
                            'limit' => $limit,
                        ],
                        $values
                    ),
                    Statement::ENTRY_BATCHSIZE => 1000,
                ]
            )),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );

        $streamNames = [];
        $cursor->rewind();

        while ($cursor->valid()) {
            $streamNames[] = new StreamName($cursor->current()['real_stream_name']);
            $cursor->next();
        }

        return $streamNames;
    }

    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->fetchCategoryNamesBy(false, $filter, $limit, $offset);
    }

    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->fetchCategoryNamesBy(true, $filter, $limit, $offset);
    }

    private function fetchCategoryNamesBy(
        bool $isRegex,
        ?string $filter,
        int $limit = 20,
        int $offset = 0
    ) {
        $values = [];
        if ($isRegex) {
            if (empty($filter) || false === @preg_match("/$filter/", '')) {
                throw new Exception\InvalidArgumentException('Invalid regex pattern given');
            }
            $where[] = 'c.real_stream_name =~ @name';
            $values['name'] = $filter;
        } elseif (null !== $filter) {
            $where[] = 'c.category == @name';
            $values['name'] = $filter;
        } else {
            $where[] = 'c.category != null';
        }

        $filter = implode(' AND ', $where);

        if (! empty($filter)) {
            $filter = 'FILTER ' . $filter;
        }

        $aql = <<<'EOF'
FOR c IN  @@collection
%filter%
COLLECT
  category = c.category
SORT category ASC
LIMIT @offset, @limit
RETURN {
    "category": category
}
EOF;
        $categories = [];

        $cursor = $this->connection->query(
            Vpack::fromJson(json_encode(
                [
                    Statement::ENTRY_QUERY => str_replace('%filter%', $filter, $aql),
                    Statement::ENTRY_BINDVARS => array_merge(
                        [
                            '@collection' => $this->eventStreamsCollection,
                            'offset' => $offset,
                            'limit' => $limit,
                        ],
                        $values
                    ),
                    Statement::ENTRY_BATCHSIZE => 1000,
                ]
            )),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );
        $cursor->rewind();

        while ($cursor->valid()) {
            $categories[] = new StreamName($cursor->current()['category']);
            $cursor->next();
        }

        return $categories;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        $aql = <<<'EOF'
FOR c IN @@collection 
    FILTER c.real_stream_name == @real_stream_name 
    RETURN {
        metadata: c.metadata
    }
EOF;

        $cursor = $this->connection->query(
            Vpack::fromJson(json_encode(
                [
                    Statement::ENTRY_QUERY => $aql,
                    Statement::ENTRY_BINDVARS => [
                        '@collection' => $this->eventStreamsCollection,
                        'real_stream_name' => $streamName->toString(),
                    ],
                ]
            )),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );

        $cursor->rewind();
        if (false === $cursor->valid() || ($stream = $cursor->current()) === null) {
            throw StreamNotFound::with($streamName);
        }

        return $cursor->current()['metadata'];
    }

    public function hasStream(StreamName $streamName): bool
    {
        $aql = <<<'EOF'
FOR c IN @@collection 
    FILTER c.real_stream_name == @real_stream_name 
    COLLECT WITH COUNT INTO number 
    RETURN {
        number: number
    }
EOF;

        $cursor = $this->connection->query(
            Vpack::fromJson(json_encode(
                [
                    Statement::ENTRY_QUERY => $aql,
                    Statement::ENTRY_BINDVARS => [
                        '@collection' => $this->eventStreamsCollection,
                        'real_stream_name' => $streamName->toString(),
                    ],
                ]
            )),
            [
                Cursor::ENTRY_TYPE => Cursor::ENTRY_TYPE_ARRAY,
            ]
        );
        $cursor->rewind();

        return 1 === ($cursor->current()['number'] ?? 0);
    }

    private function createEventStreamData(Stream $stream): array
    {
        $realStreamName = $stream->streamName()->toString();

        $pos = strpos($realStreamName, '-');

        $category = null;

        if (false !== $pos && $pos > 0) {
            $category = substr($realStreamName, 0, $pos);
        }

        $streamName = $this->persistenceStrategy->generateCollectionName($stream->streamName());
        $metadata = $stream->metadata();

        return [
            'real_stream_name' => $realStreamName,
            'stream_name' => $streamName,
            'metadata' => $metadata,
            'category' => $category,
        ];
    }

    private function createWhereClause(?MetadataMatcher $metadataMatcher): array
    {
        $where = [];
        $values = [];

        if (! $metadataMatcher) {
            return [
                $where,
                $values,
            ];
        }

        foreach ($metadataMatcher->data() as $key => $match) {
            /** @var FieldType $fieldType */
            $fieldType = $match['fieldType'];
            $field = $match['field'];
            /** @var Operator $operator */
            $operator = $match['operator'];
            $value = $match['value'];
            $parameters = [];

            if (is_array($value)) {
                foreach ($value as $k => $v) {
                    $parameters[] = '@metadata_' . $key . '_' . $k;
                }
            } else {
                $parameters = ['@metadata_' . $key];
            }

            $parameterString = implode(', ', $parameters);
            $operatorStringEnd = '';

            switch ($operator) {
                case Operator::REGEX():
                    $operatorString = '=~';
                    break;
                case Operator::EQUALS():
                    $operatorString = '=' . $operator->getValue();
                    break;
                case Operator::IN():
                    $operatorString = 'IN [';
                    $operatorStringEnd = ']';
                    break;
                case Operator::NOT_IN():
                    $operatorString = 'NOT IN [';
                    $operatorStringEnd = ']';
                    break;
                case Operator::LOWER_THAN():
                case Operator::LOWER_THAN_EQUALS():
                case Operator::GREATER_THAN():
                case Operator::GREATER_THAN_EQUALS():
                case Operator::NOT_EQUALS():
                    // performance tweak
                    if ($value === null) {
                        $where[] = $fieldType->is(FieldType::METADATA())
                            ? 'HAS(c.metadata, "' . $field . '")'
                            : 'HAS(c, "' . $field . '")';
                    } else {
                        $where[] = $fieldType->is(FieldType::METADATA())
                            ? 'c.metadata.' . $field . ' != null'
                            : 'c.' . $field . ' != null';
                    }

                    $operatorString = $operator->getValue();
                    break;
                default:
                    $operatorString = $operator->getValue();
                    break;
            }

            $where[] = $fieldType->is(FieldType::METADATA())
                ? "c.metadata.$field $operatorString $parameterString $operatorStringEnd"
                : "c.$field $operatorString $parameterString $operatorStringEnd";

            $value = (array) $value;

            foreach ($value as $k => $v) {
                $values[substr($parameters[$k], 1)] = $v;
            }
        }

        return [
            $where,
            $values,
        ];
    }

    public function beginTransaction(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if ($this->inTransaction) {
            throw new TransactionAlreadyStarted();
        }
        $this->inTransaction = true;
    }

    public function commit(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }
        if (!$this->inTransaction) {
            throw new TransactionNotStarted();
        }

        try {
            if (!empty($this->nonTransactionActions)) {
                foreach ($this->nonTransactionActions as $type) {
                    $item = $type->toHttp();
                    $this->connection->{$item[0]}(
                        $item[1],
                        Vpack::fromArray($item[2]),
                        $item[3]
                    );
                }
            }

            $this->connection->post(
                Urls::URL_TRANSACTION,
                Vpack::fromArray(
                        [
                            'collections' => [
                                'write' => array_keys(array_flip($this->writeCollections)),
                            ],
                            'action' => "function () {var db = require('@arangodb').db;" . $this->actions
                                . 'return {' . implode(',', $this->results) .'}}',
                        ]
                ));
            $this->inTransaction = false;
            $this->nonTransactionActions = [];
            $this->results = [];
            $this->resultId = -1;
        } catch (\Throwable $exception) {
        }
    }

    public function rollback(): void
    {
        if ($this->disableTransactionHandling) {
            return;
        }
        if (!$this->inTransaction) {
            throw new TransactionNotStarted();
        }

        $this->inTransaction = false;
        $this->nonTransactionActions = [];
        $this->results = [];
        $this->resultId = -1;
    }

    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    /**
     * @throws \Exception
     *
     * @return mixed
     */
    public function transactional(callable $callable)
    {
        $this->beginTransaction();

        try {
            $result = $callable($this);
            $this->commit();
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }

        return $result ?: true;
    }
}
