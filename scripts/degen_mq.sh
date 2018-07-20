#!/bin/bash

DIR="${0%/*}"

echo "1. blocking mq"

$DIR/block_mq.sh
sleep 20

echo "2. stopping mq"
docker-compose stop rabbitmq
sleep 10


echo "2. starting mq"
docker-compose start rabbitmq
