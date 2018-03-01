#!/bin/bash

docker build -t test --target builder . || exit 1

# Run unit tests and publish test coverage to Coveralls.
docker run test go test -v ./... || exit 1
docker run test goveralls -repotoken $COVERALLS_TOKEN || exit 1

if [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $TRAVIS_BRANCH == "master" ]]; then
    docker login --username $DOCKER_HUB_USER --password $DOCKER_HUB_PASS || exit 1
    docker build -t fterrag/simple-sqsd:latest . || exit 1
    docker push fterrag/simple-sqsd:latest || exit 1
fi
