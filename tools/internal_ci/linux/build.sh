#!/bin/sh

set -x

if [ ! -d $GOPATH ]
then
  echo "GOPATH ($GOPATH) is not set"
  exit 2
fi

BUILDROOT=${BUILDROOT:-git/transicator}
GLIDE=${GLIDE:-glide}

# Copy our code there, minus any vendor dependencies
base=${GOPATH}/src/github.com/apigee-labs/transicator
mkdir -p ${base}
(cd ${BUILDROOT}; tar cf - .) | (cd ${base}; tar xf -)

if [ ! -d ${base}/bin ]
then
  mkdir ${base}/bin
fi

if [ $CLEANVENDOR ]
then
  # Clean vendors
  rm -rf ${base}/vendor/*
  (cd ${base}; ${GLIDE} install)
fi

finalResult=0

# Now the build should succeed
(cd ${base}; go build -o bin/snapshotserver ./cmd/snapshotserver)
if [ $? -ne 0 ]
then
  echo "snapshot server build failed"
  finalResult=2
fi

(cd ${base}; go build -o bin/changeserver ./changeserver)
if [ $? -ne 0 ]
then
  echo "changeserver build failed"
  finalResult=2
fi

exit $finalResult
