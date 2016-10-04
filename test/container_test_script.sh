#!/bin/sh

# This is the test script that runs on the actual container

TEST_PG_URL=postgres://postgres:${PGPASSWORD}@${DBHOST}/postgres
export TEST_PG_URL

go test \
  ./common ./storage ./pgclient \
  ./replication ./snapshotserver ./changeserver
if [ $? -eq 0 ]
then
  echo "** SUCCESS **"
else
  echo "** FAILURE **"
fi
