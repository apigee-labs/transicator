#!/bin/sh

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
  /go/src/github.com/30x/transicator/test/container_test_script.sh

# Copy JUnit test files and rm container
docker cp ${testName}:/go/src/github.com/30x/transicator/test-reports/. ./docker-test-reports
docker rm ${testName}

# Build changeserver and snapshot server images
docker build --tag ${ssName} -f ./Dockerfile.snapshotserver .
docker build --tag ${csName} -f ./Dockerfile.changeserver .

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
docker run -i \
  --name ${testName} \
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

docker cp ${testName}:/go/src/github.com/30x/transicator/test-reports/. ./docker-test-reports
docker rm ${testName}

echo "*** changeserver logs ***"
docker logs ${csName}
echo "*** snapshotserver logs ***"
docker logs ${ssName}
echo "*** PG logs ***"
docker logs ${dbName}

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
