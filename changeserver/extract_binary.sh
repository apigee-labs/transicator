#!/bin/bash

docker build -f Dockerfile.changeserver -t proxy-changeserver ..
CID=$(docker run -d proxy-changeserver sh)
docker cp ${CID}:/changeserver changeserver
docker rmi -f proxy-changeserver
