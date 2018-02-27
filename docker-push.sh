#!/bin/bash

# You must be logged in to a Docker registry (Docker Hub) use this script.

docker build -t fterrag/simple-sqsd:latest .
docker push fterrag/simple-sqsd:latest
