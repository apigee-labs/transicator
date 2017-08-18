#!/bin/sh

BUILDROOT=${BUILDROOT:-github/transicator}
export BUILDROOT

# Make a temporary GOPATH to build in
gobase=`mktemp -d`
base=${gobase}/src/github.com/apigee-labs/transicator
GOPATH=${gobase}
export GOPATH

CLEANVENDOR=1
export CLEANVENDOR
${BUILDROOT}/tools/ci/linux/build.sh

if [ ! -d bin ]
then
  mkdir bin
fi
cp ${base}/bin/snapshotserver bin
cp ${base}/bin/changeserver bin

rm -rf ${gobase}
