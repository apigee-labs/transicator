#!/bin/sh

# This is the test script that runs on the actual container

TEST_PG_URL=postgres://postgres:${PGPASSWORD}@${DBHOST}/postgres
export TEST_PG_URL

if [ ! -d test-reports ]
then
  mkdir test-reports
fi
go test ./test
if [ $? -eq 0 ]
then
  echo "** SUCCESS **"
else
  echo "** FAILURE **"
fi
