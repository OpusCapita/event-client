FROM node:8-alpine
MAINTAINER kwierchris

RUN apk add curl

WORKDIR /home/node/event-client
COPY . .

# Make sure node can load modules from /var/tmp/base/node_modules
# Setting NODE_ENV is necessary for "npm install" below.
ENV NODE_ENV=development
RUN npm install
