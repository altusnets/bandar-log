#!/bin/bash

set -e

echo "pushing bandar-log image to dockerhub..."
docker login -u=$DOCKERHUB_UNAME -p=$DOCKERHUB_PASS
sbt bandarlog/docker:publish