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

namespace Prooph\EventStore\ArangoDb\Guard;

use ArangoDb\Guard\Guard;
use Fig\Http\Message\StatusCodeInterface;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\StreamName;
use Psr\Http\Message\ResponseInterface;

class StreamExistsGuard implements Guard
{
    /**
     * Content id
     *
     * @var string
     */
    private $contentId;

    /**
     * @var StreamName
     */
    private $streamName;

    private function __construct(string $contentId, StreamName $streamName)
    {
        $this->contentId = $contentId;
        $this->streamName = $streamName;
    }

    public static function withStreamName(StreamName $streamName): self
    {
        return new self(\bin2hex(\random_bytes(4)), $streamName);
    }

    public function contentId(): ?string
    {
        return $this->contentId;
    }

    public function __invoke(ResponseInterface $response): void
    {
        if ($response->getStatusCode() === StatusCodeInterface::STATUS_CONFLICT) {
            throw StreamExistsAlready::with($this->streamName);
        }
    }
}
