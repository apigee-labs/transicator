#!/bin/sh

# This is the test script that runs on the actual container

if [[ -z "${PGPASSWORD}" ]]; then
    TEST_PG_URL=postgres://postgres@${DBHOST}/postgres
else
    TEST_PG_URL=postgres://postgres:${PGPASSWORD}@${DBHOST}/postgres
fi
export TEST_PG_URL

if [ ! -d test-reports ]
then
  mkdir test-reports
fi
go test \
  ./common ./storage ./pgclient \
  ./replication ./snapshotserver ./changeserver
if [ $? -eq 0 ]
then
  echo "** SUCCESS **"
else
  echo "** FAILURE **"
fi
