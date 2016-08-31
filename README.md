# Transicator

This is suite of programs, plus a Postgres plugin, that allows us to
use the Postgres "logical decoding" feature to distribute a consistent
set of changes from Postgres to many consumers.


# Developer Setup
## Prerequisites
### Install postgres
```
brew info postgresql
```
### Configure postgres
```

```


## Build and install PG logical replication output plugin
```
cd 30x/transicator/pgoutput/
make install
```

## Build replication client
```
cd 30x/transicator/
glide install
cd pgclient/
go build
cd ../replclient/

```