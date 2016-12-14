#!/bin/sh

failed=0

lc=`ls *.go 2>/dev/null | wc -l`
if [ $lc -gt 0 ]
then
  lc=`gofmt -l . | wc -l`
  if [ $lc -gt  0 ]
  then
    echo "** go fmt required:"
    gofmt -l .
    failed=1
  fi

  go vet
  if [ $? -ne 0 ]
  then
    echo "** go vet failed"
    failed=1
  fi
fi

shopt -s nullglob
for f in *.go *.[ch]
do
  lc=`egrep -c 'Copyright [0-9]+ The Transicator Authors|Apache License' $f`
  if [ $lc -lt 2 ]
  then
    echo "** $f is missing a license header"
    failed=1
  fi
done
shopt -u nullglob

if [ $failed -gt 0 ]
then
  exit 2
fi

