#!/bin/sh
set -e

echo "Login into Docker Hub ..."
docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD

docker build -t docker.io/streamzi/gw-http-source http-source
docker push docker.io/streamzi/gw-http-source

docker build -t docker.io/streamzi/gw-kafka-source kafka-source
docker push docker.io/streamzi/gw-kafka-source

docker build -t docker.io/streamzi/gw-http-sink http-sink
docker push docker.io/streamzi/gw-http-sink
