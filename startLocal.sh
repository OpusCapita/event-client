docker ps -a | grep "event-client_main" | awk '{print $1}' | xargs docker rm -f

docker-compose run --service-ports main npm run mocha
