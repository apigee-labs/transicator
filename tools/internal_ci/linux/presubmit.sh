#!/bin/sh

BUILDROOT=${BUILDROOT:-github/transicator}
export BUILDROOT

# Make a random password for the database we'll be testing with
TEST_PG_PW=`openssl rand -hex 18`
export TEST_PG_PW

# Choose a port for PG that probably won't be used.
pgPort=54321

# Make a temporary GOPATH to build in
gobase=`mktemp -d`
base=${gobase}/src/github.com/apigee-labs/transicator
GOPATH=${gobase}
export GOPATH

base=${GOPATH}/src/github.com/apigee-labs/transicator
mkdir -p ${base}
(cd ${BUILDROOT}; tar cf - .) | (cd ${base}; tar xf -)

finalResult=0

(cd ${base}; make presubmit)
finalResult=$?

if [ $finalResult -eq 0 ]
then
  dbName=transicator-pg-$$
  # Build and launch postgresql image
  (cd ${base}; docker build --tag ${dbName} ./pgoutput)
  docker run -d \
    --name ${dbName} \
    -e POSTGRES_PASSWORD=${TEST_PG_PW} \
    -p ${pgPort}:5432 \
    ${dbName}

  # Give PG a few seconds to start
  sleep 5

  TEST_PG_URL=postgres://postgres:${TEST_PG_PW}@localhost:${pgPort}/postgres
  export TEST_PG_URL
  (cd ${base}; make tests)
  finalResult=$?

  #docker logs ${dbName}
  docker rm -f ${dbName}
  docker rmi ${dbName}
fi

rm -rf ${gobase}

exit ${finalResult}
