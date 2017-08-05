# arangodb-event-store

[![Build Status](https://travis-ci.org/prooph/arangodb-event-store.svg?branch=master)](https://travis-ci.org/prooph/arangodb-event-store)
[![Coverage Status](https://coveralls.io/repos/prooph/arangodb-event-store/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/arangodb-event-store?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

[ArangoDB](https://arangodb.com/) EventStore implementation for [prooph EventStore](https://github.com/prooph/event-store)

## Requirements

- PHP >= 7.1
- ArangoDB server version >= 3.0

## Setup

TBD

## Tests
If you want to run the unit tests locally you need [Docker](https://docs.docker.com/engine/installation/ "Install Docker") 
and [Docker Compose](https://docs.docker.com/compose/install/ "Install Docker Compose").

Install dependencies with:

```
$ docker run --rm -i -v $(pwd):/app prooph/composer:7.1 update -o
```

Start containers with
```
$ docker-compose up -d --no-recreate
```

Execute tests with

```
$ docker-compose run --rm php vendor/bin/phpunit`
```

