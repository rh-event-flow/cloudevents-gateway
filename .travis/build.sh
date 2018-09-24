#!/bin/sh


if [ "$TRAVIS_PULL_REQUEST" != "false" ] ; then
    echo "Building Pull Request - nothing to push"
else
    echo "Login into Docker Hub ..."
    echo "$DOCKER_PASSWORD" | docker login --username "$DOCKER_USERNAME" --password-stdin docker.io

    docker build -t docker.io/streamzi/cloudevents-gateway-http-source:latest http-source
    docker push docker.io/streamzi/cloudevents-gateway-http-source:latest

    docker build -t docker.io/streamzi/cloudevents-gateway-kafka-source:latest kafka-source
    docker push docker.io/streamzi/cloudevents-gateway-kafka-source:latest

    docker build -t docker.io/streamzi/cloudevents-gateway-http-sink:latest http-sink
    docker push docker.io/streamzi/cloudevents-gateway-http-sink:latest

fi
