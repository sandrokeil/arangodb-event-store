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
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\StreamName;
use Psr\Http\Message\ResponseInterface;

class StreamNotFoundGuard implements Guard
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

    /**
     * @var bool
     */
    private $isJs;

    private function __construct(string $contentId, StreamName $streamName, bool $isJs)
    {
        $this->contentId = $contentId;
        $this->streamName = $streamName;
        $this->isJs = $isJs;
    }

    public static function withStreamName(StreamName $streamName, bool $isJs = false): self
    {
        return new self(\bin2hex(\random_bytes(4)), $streamName, $isJs);
    }

    public function contentId(): ?string
    {
        return $this->contentId;
    }

    public function __invoke(ResponseInterface $response): void
    {
        if ($this->isJs) {
            if (\strpos($response->getBody()->getContents(), '"rId' . $this->contentId . '"' . ':0') !== false) {
                throw StreamNotFound::with($this->streamName);
            }
        }

        $httpStatusCode = $response->getStatusCode();

        if ($httpStatusCode === StatusCodeInterface::STATUS_NOT_FOUND) {
            throw StreamNotFound::with($this->streamName);
        }
        if ($httpStatusCode !== StatusCodeInterface::STATUS_ACCEPTED
            && $httpStatusCode !== StatusCodeInterface::STATUS_OK
        ) {
            throw RuntimeException::fromResponse($response);
        }
    }
}
