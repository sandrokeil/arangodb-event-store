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
    curl-dev \
    openssl \
    openssl-dev \
    libc-dev \
    pcre-dev \
    autoconf \
    file \
    pkgconf \
    re2c \
    ' \
    && apk add --update $buildDeps \
    && pecl install xdebug \
    && docker-php-ext-configure pcntl --enable-pcntl \
    && docker-php-ext-install \
       pcntl

ENV PHP_CPP_VERSION master

    # PHP-CPP
RUN git clone --branch ${PHP_CPP_VERSION} https://github.com/CopernicaMarketingSoftware/PHP-CPP /tmp/php-cpp \
        && cd /tmp/php-cpp \
        && sed -i 's/`\${PHP_CONFIG} \-\-ldflags`//g' Makefile \
        && make \
        && make install

ENV ARANGODB_PHP_DRIVER master

RUN git clone --branch ${ARANGODB_PHP_DRIVER} https://github.com/sandrokeil/arangodb-php-driver /tmp/arangodb-php-driver \
    && cd /tmp/arangodb-php-driver \
    && sh get_fuerte \
    && cd /tmp/arangodb-php-driver/build && cmake .. -DPHPCPP_ARCH=x86_64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-fPIC \
    && make install \
    && rm -rf /tmp \
    && echo "extension=libarangodb.so" > /usr/local/etc/php/conf.d/arangodb.ini

RUN apk del --purge $buildDeps

# Copy custom configuration
COPY xdebug-cli.ini /usr/local/etc/php/conf.d/

WORKDIR /app

# Set up the application directory
VOLUME ["/app"]
