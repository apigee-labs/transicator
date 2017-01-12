#!/bin/sh

# This script runs the tests in "dockerTest" on the local box without
# Docker. It's handy for debugging the test itself.

if [ -z  ${TEST_PG_URL} ]
then
  echo "TEST_PG_URL" must be set
  exit 2
fi

../bin/snapshotserver -u ${TEST_PG_URL} -t 12123 --key ./keys/clearkey.pem --cert ./keys/clearcert.pem &
ssPid=$!

../bin/changeserver -u ${TEST_PG_URL} -t 12124 -s unittestslot -d ./data --key ./keys/clearkey.pem --cert ./keys/clearcert.pem &
csPid=$!

CHANGE_HOST=localhost CHANGE_PORT=12124 \
SNAPSHOT_HOST=localhost SNAPSHOT_PORT=12123 \
go test

curl http://localhost:12124/markdown

kill ${csPid}
kill ${ssPid}
rm -rf ./data
