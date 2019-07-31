#!/bin/bash

DOCKER_IMAGE=docker-accumulo-muse
TAG=$(git rev-parse --short HEAD)
mvn clean package -DskipTests
cd docker
docker build -t $DOCKER_IMAGE:$TAG .
docker tag $DOCKER_IMAGE:$TAG $DOCKER_IMAGE:latest
