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
docker run --rm -it \
  --link ${dbName}:postgres \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=postgres \
  ${testName} \
  /go/src/github.com/30x/transicator/test/container_test_script.sh

# Build changeserver and snapshot server images
docker build --tag ${ssName} -f ./snapshotserver/Dockerfile.snapshotserver .
docker build --tag ${csName} -f ./changeserver/Dockerfile.changeserver .

# Launch them
docker run -d \
  --name ${csName} \
  --link ${dbName}:postgres \
  ${csName} \
  -s ${slotName} -u postgres://postgres:${TEST_PG_PW}@${dbName}/postgres

docker run -d \
  --name ${ssName} \
  --link ${dbName}:postgres \
  ${ssName} \
  -u postgres://postgres:${TEST_PG_PW}@${dbName}/postgres

# Run tests of the combined servers and Postgres
docker run --rm -it \
  --link ${dbName}:postgres \
  --link ${csName}:changeserver \
  --link ${ssName}:snapshotserver \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=postgres \
  -e CHANGE_HOST=changeserver \
  -e SNAPSHOT_HOST=snapshotserver \
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

# --no-prune here will leave intermediate images around, which speeds
# up rebuild on a developer box
RMOPT=--no-prune
if [ $1 == "fullcleanup" ]
then
  RMOPT=
fi
docker rmi $(RMOPT) ${testName} ${ssName} ${csName} ${dbName}
