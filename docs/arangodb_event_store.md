# prooph ArangoDB Event Store

The [ArangoDB](https://arangodb.com/) Event Store is an implementation of [prooph/event-store](https://github.com/prooph/event-store)
and is a replacement of our former MongoDB Event Store.

For a better understanding, we recommend to read the event-store docs, first.

## Event streams / projections collection

All known event streams are stored in an event stream collection, so with a simple collection lookup, you can find out what streams
are available in your store.

Same goes for the projections, all known projections are stored in a single collection, so you can see what projections are
available, and what their current state / stream positition / status is.

## Load batch size

When reading from an event streams with multiple aggregates (especially when using projections), you could end of with
millions of events loaded in memory. Therefor the arangodb-event-store will load events only in batches of 1000 by default.
You can change to value to something higher to achieve even more performance with higher memory usage, or decrease it
to reduce memory usage even more, with the drawback of having a not as good performance.

## ArangoDB Connection for event-store and projection manager

It is important to use the same database for event-store and projection manager, you could use distinct ArangoDB connections
if you want to, but they should be both connected to the same database. Otherwise you will run into issues, because the
projection manager needs to query the underlying database collection of the event-store for its querying API.
 
It's recommended to just use the same ArangoDB connection instance for both.

## Persistence Strategies

This component ships with 3 default persistence strategies:

- AggregateStreamStrategy
- SimpleStreamStrategy
- SingleStreamStrategy

All persistence strategies have the following in common:

The generated collection name for a given stream is:

`'c' . sha1($streamName->toString()`

so a sha1 hash of the stream name, prefixed with a `c` is the used collection name.
You can query the `event_streams` collection to get real stream name to stream name mapping.

You can implement your own persistence strategy by implementing the `Prooph\EventStore\ArangoDb\PersistenceStrategy` interface.

### AggregateStreamStrategy

This stream strategy should be used together with event-sourcing, if you use one stream per aggregate. For example, you have 2 instances of two
different aggregates named `user-123`, `user-234`, `todo-345` and `todo-456`, you would have 4 different event streams,
one for each aggregate.

This stream strategy is the most performant of all, but it will create a lot of database collections, which is something not
everyone likes (especially DB admins).

All needed database collections will be created automatically for you.

### SingleStreamStrategy

This stream strategy should be used together with event-sourcing, if you want to store all events of an aggregate type into a single stream, for example
`user-123` and `user-234` should both be stored into a stream called `user`.

You can also store all stream of all aggregate types into a single stream, for example your aggregates `user-123`,
`user-234`, `todo-345` and `todo-456` can all be stored into a stream called `event_stream`.

This stream strategy is slightly less performant then the aggregate stream strategy.

You need to setup the database collection yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_stream.php).

### SimpleStreamStrategy

This stream strategy is not meant to be used for event-sourcing. It will create simple event streams without any constraints
at all, so having two events of the same aggregate with the same version will not rise any error.

This is very useful for projections, where you copy events from one stream to another (the resulting stream may need to use
the simple stream strategy) or when you want to use the event-store outside the scope of event-sourcing.

You need to setup the database collection yourself when using this strategy. An example script to do that can be [found here](https://github.com/prooph/proophessor-do/blob/master/scripts/create_event_stream.php).

### Using custom stream strategies

When you query the event streams a lot, it might be a good idea to create your own stream strategy, so you can add
custom indexes to your database collections. When using with the MetadataMatcher, take care that you add the metadata
matches in the right order, so they can match your indexes.

### Disable transaction handling

It is not possible to disable the transaction handling because there is no manual rollback. Please read more about this
in the [ArangoDB docs](https://docs.arangodb.com/3.2/Manual/Transactions/TransactionInvocation.html).
