#!/bin/sh

# Make a random password for the database we'll be testing with
TEST_PG_PW=`openssl rand -hex 18`
export TEST_PG_PW

BUILDROOT=${BUILDROOT:-git/transicator}
(cd ${BUILDROOT}; make dockerTests)

for n in ${BUILDROOT}/docker-test-reports/*.xml
do
  bn=`basename $n .xml`
  mv $n ${BUILDROOT}/docker-test-reports/${bn}_sponge_log.xml
done
