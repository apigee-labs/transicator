#!/bin/bash

# Copyright 2016 The Transicator Authors
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

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

TEST_PG_URL=postgres://postgres:${PGPASSWORD}@${DBHOST}/postgres
export TEST_PG_URL

if [ ! -d test-reports ]
then
  mkdir test-reports
fi

error=0
TEST_COVERAGE_FILENAME=${TEST_COVERAGE_FILENAME:-coverage.txt}
TEST_COVERAGE_OUTPUT=${DIR}/../test-reports/${TEST_COVERAGE_FILENAME}
TEST_MODULES="./test"
if [[ "${TEST_COVERAGE}" == "true" ]]; then
  if [ ! -f "${TEST_COVERAGE_OUTPUT}" ]; then
    echo "mode: count" | tee ${TEST_COVERAGE_OUTPUT}
  fi
  for package in ${TEST_MODULES}; do
    go test -coverprofile=profile.out -covermode=count $package
    result=$?
    if [ $result -ne 0 ]; then
      error=$result
    fi
    if [ -f profile.out ]; then
      tail -n+2 profile.out >> ${TEST_COVERAGE_OUTPUT}
      rm profile.out
    fi
  done
else
  go test ${TEST_MODULES}
fi

if [ $error -eq 0 ]; then
  echo "** SUCCESS **"
else
  echo "** FAILURE **"
fi
exit $error
