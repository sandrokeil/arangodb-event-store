FROM php:7.1-alpine

RUN buildDeps=' \
    bash \
    git \
    g++ \
    make \
    cmake \
    libstdc++ \
    libssl1.0 \
    boost-dev \
    boost-system \
    boost-thread \
    curl-dev \
    openssl \
    openssl-dev \
    libc-dev \
    pcre-dev \
    autoconf \
    ' \
    && apk add --update $buildDeps \
    && docker-php-source extract

ENV PHP_ARANGODB_DRIVER port-everything-to-zend
ENV CPP_VELOCYPACK master
ENV CPP_FUERTE master

RUN git clone --branch ${CPP_VELOCYPACK} https://github.com/arangodb/velocypack.git /tmp/velocypack \
    && cd /tmp/velocypack \
    && mkdir -p build \
    && cd build \
    && cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-fPIC \
    && make install

RUN git clone --branch ${CPP_FUERTE} https://github.com/arangodb/fuerte.git /tmp/fuerte \
    && cd /tmp/fuerte \
    && cmake -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-fPIC \
    && make install

RUN git clone --branch ${PHP_ARANGODB_DRIVER}  https://github.com/sandrokeil/arangodb-php-driver.git /tmp/arangodb-php-driver \
    && cd /tmp/arangodb-php-driver \
    && phpize  \
    && ./configure  \
    && make all -j4
# tests need a DB
#   && make test

RUN echo "extension=arangodb.so" > /usr/local/etc/php/conf.d/arangodb.ini \
    && mv /tmp/arangodb-php-driver/modules/arangodb.so $(php-config --extension-dir) \
    && rm -rf /tmp/*

# Copy custom configuration
COPY xdebug-cli.ini /usr/local/etc/php/conf.d

RUN pecl install xdebug

WORKDIR /app

# Set up the application directory
VOLUME ["/app"]
