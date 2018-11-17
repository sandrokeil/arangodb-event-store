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
use Prooph\EventStore\ArangoDb\Exception\RuntimeException;
use Psr\Http\Message\ResponseInterface;

class HttpStatusCodeGuard implements Guard
{
    /**
     * Content id
     *
     * @var string
     */
    private $contentId;

    /**
     * HTTP status code
     *
     * @var int
     */
    private $httpStatusCode;

    private function __construct(int $httpStatusCode, ?string $contentId)
    {
        $this->contentId = $contentId;
        $this->httpStatusCode = $httpStatusCode;
    }

    public static function withContentId(int $httpStatusCode, string $contentId = null): self
    {
        if (null === $contentId) {
            $contentId = \bin2hex(\random_bytes(4));
        }

        return new self($httpStatusCode, $contentId);
    }

    public static function withoutContentId(int $httpStatusCode): self
    {
        return new self($httpStatusCode, null);
    }

    public function contentId(): ?string
    {
        return $this->contentId;
    }

    public function __invoke(ResponseInterface $response): void
    {
        if ($response->getStatusCode() === $this->httpStatusCode) {
            throw RuntimeException::fromErrorResponse($response);
        }
    }
}
