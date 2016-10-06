#!/bin/sh

# This is the test script that runs on the actual container

TEST_PG_URL=postgres://postgres:${PGPASSWORD}@${DBHOST}/postgres
export TEST_PG_URL

(cd ./test; go test)
if [ $? -eq 0 ]
then
  echo "** SUCCESS **"
else
  echo "** FAILURE **"
fi
