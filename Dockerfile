FROM node:8-alpine
MAINTAINER Suresh Varman

WORKDIR /var/tmp/base
COPY package.json .

# Change owner since COPY/ADD assignes UID/GID 0 to all copied content.
RUN apk add curl --no-cache

# Make sure node can load modules from /var/tmp/base/node_modules
# Setting NODE_ENV is necessary for "npm install" below.
ENV NODE_ENV=development NODE_PATH=/var/tmp/base/node_modules PATH=${PATH}:${NODE_PATH}/.bin
RUN yarn

WORKDIR /home/node/event-client
COPY . .
