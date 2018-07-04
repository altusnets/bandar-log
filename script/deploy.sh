#!/bin/bash

set -e

if [ "$TRAVIS_BRANCH" == "master" ]; then
  echo "pushing bandar-log image to dockerhub..."
  docker login -u=$DOCKERHUB_UNAME -p=$DOCKERHUB_PASS
  sbt bandarlog/docker:publish
fi