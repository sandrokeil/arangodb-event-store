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
use Prooph\EventStore\ArangoDb\Exception\LogicException;

final class DeleteDatabase implements Type
{
    use ToHttpTrait;

    /**
     * @var string
     */
    private $databaseName;

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
        string $databaseName,
        array $options = [],
        callable $inspector = null
    ) {
        $this->databaseName = $databaseName;
        $this->options = $options;
        $this->inspector = $inspector ?: function (HttpResponse $response, string $rId = null) {
            return null;
        };
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Database/DatabaseManagement.html#drop-database
     *
     * @param string $databaseName
     * @param array $options
     * @return DeleteDatabase
     */
    public static function with(string $databaseName, array $options = []): DeleteDatabase
    {
        return new self($databaseName, $options);
    }

    /**
     * @see https://docs.arangodb.com/3.2/HTTP/Database/DatabaseManagement.html#drop-database
     *
     * @param string $databaseName
     * @param callable $inspector Inspects result, signature is (HttpResponse $response, string $rId = null)
     * @param array $options
     * @return DeleteDatabase
     */
    public static function withInspector(
        string $databaseName,
        callable $inspector,
        array $options = []
    ): DeleteDatabase {
        return new self($databaseName, $options, $inspector);
    }

    public function checkResponse(HttpResponse $response, string $rId = null): ?int
    {
        return ($this->inspector)($response, $rId);
    }

    public function collectionName(): string
    {
        throw new LogicException('Not possible at the moment, see ArangoDB docs');
    }

    public function toHttp(): iterable
    {
        return $this->buildAppendBatch(
            HttpHelper::METHOD_DELETE,
            Urls::URL_DATABASE . '/' . $this->databaseName,
            $this->options
        );
    }

    public function toJs(): string
    {
        throw new LogicException('Not possible at the moment, see ArangoDB docs');
    }
}
