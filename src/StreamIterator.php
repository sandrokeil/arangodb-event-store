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

namespace Prooph\EventStore\ArangoDb;

use ArangoDb\Cursor;
use DateTimeImmutable;
use DateTimeZone;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

final class StreamIterator implements \Countable, \Iterator
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * Position offset
     *
     * @var int
     */
    private $positionOffset;

    /**
     * @var Cursor
     */
    private $cursor;

    public function __construct(
        Cursor $cursor,
        int $positionOffset,
        MessageFactory $messageFactory
    ) {
        $this->cursor = $cursor;
        $this->cursor->rewind();
        $this->positionOffset = $positionOffset;
        $this->messageFactory = $messageFactory;
    }

    public function current(): ?Message
    {
        $data = $this->cursor->current();

        if ($data === null) {
            return null;
        }
        $data = json_decode($data, true);

        $createdAt = $data['created_at'];

        if (strlen($createdAt) === 19) {
            $createdAt .= '.000';
        }

        $createdAt = DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $createdAt,
            new DateTimeZone('UTC')
        );

        if (! isset($data['metadata']['_position'])) {
            $data['metadata']['_position'] = ((int) $data['no']) - $this->positionOffset + 1;
        }

        return $this->messageFactory->createMessageFromArray(
            $data['event_name'],
            [
                'uuid' => $data['event_id'],
                'created_at' => $createdAt,
                'payload' => $data['payload'],
                'metadata' => $data['metadata'],
            ]
        );
    }

    public function count()
    {
        return $this->cursor->count();
    }

    public function next()
    {
        $this->cursor->next();
    }

    public function key()
    {
        return $this->cursor->key();
    }

    public function valid()
    {
        return $this->cursor->valid();
    }

    public function rewind()
    {
        $this->cursor->rewind();
    }
}
