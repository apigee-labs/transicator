#!/bin/sh

if [ ! $TEST_PG_PW ]
then
  echo "TEST_PG_PW not set. Enter Postgres password:"
  read -s TEST_PG_PW
fi

netName=transicator-tests-$$
dbName=transicator-test-pg-$$
testName=transicator-test-$$

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

# Run the tests on it
docker run --rm -it \
  --network ${netName} \
  -e PGPASSWORD=${TEST_PG_PW} \
  -e DBHOST=${dbName} \
  ${testName}

# Clean up
#docker logs transicator-test-db
docker rm -f ${dbName}

docker network rm ${netName}

docker rmi ${testName}
docker rmi ${dbName}
