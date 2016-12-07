#!/bin/sh

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
