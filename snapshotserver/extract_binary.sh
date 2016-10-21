#!/bin/bash

docker build -f ../Dockerfile.snapshotserver -t proxy-snapshotserver ..
CID=$(docker run -d proxy-snapshotserver sh)
docker cp ${CID}:/snapshotserver snapshotserver
docker rmi -f proxy-snapshotserver
