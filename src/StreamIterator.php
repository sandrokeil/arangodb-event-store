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

use ArangoDBClient\Cursor;
use DateTimeImmutable;
use DateTimeZone;
use IteratorIterator;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

final class StreamIterator extends IteratorIterator implements \Countable
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

    public function __construct(
        Cursor $cursor,
        int $positionOffset,
        MessageFactory $messageFactory
    ) {
        parent::__construct($cursor);
        $this->positionOffset = $positionOffset;
        $this->messageFactory = $messageFactory;
    }

    public function current(): ?Message
    {
        $data = $this->getInnerIterator()->current();

        if ($data === null) {
            return null;
        }

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
        return $this->getInnerIterator()->getCount();
    }
}
