#!/bin/sh

if [ ! $TEST_PG_PW ]
then
  echo "TEST_PG_PW not set. Enter Postgres password:"
  read -s TEST_PG_PW
fi

netName=transicator-tests-$$
dbName=transicator-test-pg-$$
testName=transicator-test-$$
ssName=snapshot-test-$$
csName=changeserver-test-$$
slotName=changeserver_test_slot

# Need a network for our tests
docker network create --driver bridge ${netName}

# Build postgresql image
docker build --tag ${dbName} ./pgoutput

# Launch it
docker run -d -P \
  --network ${netName} \
  --name ${dbName} \
  -e POSTGRES_PASSWORD=${TEST_PG_PW} \
  ${dbName}

# Build a test container
docker build --tag ${testName} -f ./test/Dockerfile .

# Run the unit tests in it
docker run --rm -it \
  --network ${netName} \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=${dbName} \
  ${testName} \
  /go/src/github.com/30x/transicator/test/container_test_script.sh

# Build changeserver and snapshot server images
docker build --tag ${ssName} -f ./snapshotserver/Dockerfile.snapshotserver .
docker build --tag ${csName} -f ./changeserver/Dockerfile.changeserver .

# Launch them
docker run -d \
  --name ${csName} \
  -P --network ${netName} \
  ${csName} \
  -s ${slotName} -u postgres://postgres:${TEST_PG_PW}@${dbName}/postgres

docker run -d \
  --name ${ssName} \
  -P --network ${netName} \
  ${ssName} \
  -u postgres://postgres:${TEST_PG_PW}@${dbName}/postgres

# Run tests of the combined servers and Postgres
docker run --rm -it \
  --network ${netName} \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=${dbName} \
  -e CHANGE_HOST=${csName} \
  -e SNAPSHOT_HOST=${ssName} \
  -e CHANGE_PORT=9000 \
  -e SNAPSHOT_PORT=9001 \
  ${testName} \
  /go/src/github.com/30x/transicator/test/combined_test_script.sh

echo "*** changeserver logs ***"
docker logs ${csName}
echo "*** snapshotserver logs ***"
docker logs ${ssName}

# Clean up
docker rm -f ${csName}
docker rm -f ${ssName}

#docker logs ${dbName}
docker rm -f ${dbName}

docker network rm ${netName}

docker rmi ${testName}
docker rmi ${ssName}
docker rmi ${csName}
docker rmi ${dbName}
