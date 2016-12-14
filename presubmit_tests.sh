#!/bin/sh

lc=`gofmt -l . | wc -l`
if [ $lc -gt  0 ]
then
  echo "** go fmt required for $(PWD) **"
  exit 2
fi

go vet
if [ $? -ne 0 ]
then
  echo "** go vet failed for $(PWD) **"
  exit 3
fi
