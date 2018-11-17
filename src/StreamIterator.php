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

use ArangoDb\Statement;
use DateTimeImmutable;
use DateTimeZone;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

final class StreamIterator implements \Prooph\EventStore\StreamIterator\StreamIterator
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
     * @var Statement
     */
    private $cursor;

    /**
     * @var mixed
     */
    private $currentItem;

    /**
     * @var int
     */
    private $currentKey = -1;

    public function __construct(
        Statement $cursor,
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
        $this->currentItem = $this->cursor->current();

        if ($this->currentItem === null) {
            return null;
        }
        $this->currentItem = \json_decode($this->currentItem, true);
        $createdAt = $this->currentItem['created_at'];

        if (\strlen($createdAt) === 19) {
            $createdAt .= '.000';
        }

        $createdAt = DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $createdAt,
            new DateTimeZone('UTC')
        );

        if (! isset($this->currentItem['metadata']['_position'])) {
            $this->currentItem['metadata']['_position'] = ((int) $this->currentItem['no']) - $this->positionOffset + 1;
        }

        return $this->messageFactory->createMessageFromArray(
            $this->currentItem['event_name'],
            [
                'uuid' => $this->currentItem['event_id'],
                'created_at' => $createdAt,
                'payload' => $this->currentItem['payload'],
                'metadata' => $this->currentItem['metadata'],
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
        if ($this->currentItem !== null && isset($this->currentItem['no'])) {
            return ((int) $this->currentItem['no']) - $this->positionOffset + 1;
        }

        return null;
    }

    public function valid()
    {
        $valid = $this->cursor->valid();

        if ($valid) {
            $this->currentItem = $this->cursor->current();
        }

        return $valid;
    }

    public function rewind()
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            $this->cursor->rewind();
            $this->currentItem = null;
            $this->currentKey = -1;
        }
    }
}
