FROM node:8-alpine
MAINTAINER denic

RUN apk add --upgrade --no-cache  \
    alpine-sdk \
    libc6-compat \
    bash \
    curl \
    make \
    gcc \
    g++ \
    python \
    cyrus-sasl-dev \
    libressl2.5-libcrypto --repository http://dl-3.alpinelinux.org/alpine/edge/main/ --allow-untrusted \
    libressl2.5-libssl --repository http://dl-3.alpinelinux.org/alpine/edge/main/ --allow-untrusted \
    librdkafka-dev --repository http://dl-3.alpinelinux.org/alpine/edge/community/ --allow-untrusted \
    dumb-init --repository http://dl-3.alpinelinux.org/alpine/edge/community/ --allow-untrusted

WORKDIR /home/node/event-client
COPY . .

# Make sure node can load modules from /var/tmp/base/node_modules
# Setting NODE_ENV is necessary for "npm install" below.
ENV NODE_ENV=development
RUN npm config set unsafe-perm true
RUN npm install
