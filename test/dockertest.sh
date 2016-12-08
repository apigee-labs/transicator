#!/bin/sh

# Copyright 2016 The Transicator Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ ! $TEST_PG_PW ]
then
  echo "TEST_PG_PW not set. Enter Postgres password:"
  read -s TEST_PG_PW
fi

if [ ! -d ./docker-test-reports ]
then
  mkdir ./docker-test-reports
fi

netName=transicator-tests-$$
dbName=transicator-test-pg-$$
testName=transicator-test-$$
ssName=snapshot-test-$$
csName=changeserver-test-$$
slotName=changeserver_test_slot

# Build postgresql image
docker build --tag ${dbName} ./pgoutput

# Launch it
docker run -d \
  --name ${dbName} \
  -e POSTGRES_PASSWORD=${TEST_PG_PW} \
  ${dbName}

# Build a test container
docker build --tag ${testName} -f ./test/Dockerfile .

# Run the unit tests in it
docker run -i \
  --name ${testName} \
  --link ${dbName}:postgres \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=postgres \
  ${testName} \
  /go/src/github.com/apigee-labs/transicator/test/container_test_script.sh

# Copy JUnit test files and rm container
docker cp ${testName}:/go/src/github.com/apigee-labs/transicator/test-reports/. ./docker-test-reports
docker rm ${testName}

# Build changeserver and snapshot server images
docker build --tag ${ssName} -f ./Dockerfile.snapshotserver .
docker build --tag ${csName} -f ./Dockerfile.changeserver .

if [[ -z "$TEST_PG_PW" ]]; then
    POSTGRES_URL=postgres://postgres@${dbName}/postgres
else
    POSTGRES_URL=postgres://postgres:${TEST_PG_PW}@${dbName}/postgres
fi

# Launch them
docker run -d \
  --name ${csName} \
  --link ${dbName}:postgres \
  -v ${PWD}/test/keys:/keys \
  ${csName} \
  -t 9443 --key /keys/clearkey.pem --cert /keys/clearcert.pem \
  -s ${slotName} -u $POSTGRES_URL

docker run -d \
  --name ${ssName} \
  --link ${dbName}:postgres \
  -v ${PWD}/test/keys:/keys \
  ${ssName} \
  -t 9444 --key /keys/clearkey.pem --cert /keys/clearcert.pem \
  -u $POSTGRES_URL

# Run tests of the combined servers and Postgres
docker run -i \
  --name ${testName} \
  --link ${dbName}:postgres \
  --link ${csName}:changeserver \
  --link ${ssName}:snapshotserver \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=postgres \
  -e CHANGE_HOST=changeserver \
  -e SNAPSHOT_HOST=snapshotserver \
  -e CHANGE_PORT=9443 \
  -e SNAPSHOT_PORT=9444 \
  ${testName} \
  /go/src/github.com/apigee-labs/transicator/test/combined_test_script.sh

docker cp ${testName}:/go/src/github.com/apigee-labs/transicator/test-reports/. ./docker-test-reports
docker rm ${testName}

echo "*** changeserver logs ***"
docker logs ${csName}
echo "*** snapshotserver logs ***"
docker logs ${ssName}
# Uncomment this to see a lot of logs...
#echo "*** PG logs ***"
#docker logs ${dbName}

# Clean up
docker rm -f ${csName}
docker rm -f ${ssName}
docker rm -f ${dbName}

# --no-prune here will leave intermediate images around, which speeds
# up rebuild on a developer box
RMCMD="docker rmi --no-prune"
if [ $# -ge 1 ]
then
  if [ $1 == "fullcleanup" ]
  then
    RMCMD="docker rmi"
  fi
fi
${RMCMD} ${testName} ${ssName} ${csName} ${dbName}
