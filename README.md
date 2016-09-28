# Transicator

This is suite of programs, plus a Postgres plugin, that allows us to
use the Postgres "logical decoding" feature to distribute a consistent
set of changes from Postgres to many consumers.

# Overview

Transicator is designed to distribute changes to a Postgres database to a large
number of clients via an API.

There are three components to Transicator:

## Postgres Logical Replication

The "logical replication" feature of Postgres (version 9.4 and higher) does the
heavy lifting of ordering changes from every committed transaction into
commit order and publishing them to a logical replication slot.

Transicator adds a plugin to Postgres that formats these changes in JSON so
that we can easily consume them.

## Change Server

The goal of Transicator is to distribute logical changes to thousands of
occassionally-connected clients. Postgres is not designed to handle replication
consumers at this scale, and it does it using a complex protocol.

The "changeserver" is a server that consumes the stream of logical changes
directly from Postgres using the streaming replication protocol, and makes them
available via an HTTP API. Changes in changeserver are ordered by the order
in which they appeared in the logical replication stream, and are further
segregated by "scope."

changeserver is designed to handle thousands of clients waiting for the next
change via "long polling." This way, we can scale up the number of consumers
for the set of changes by adding changeservers.

## Snapshot Server

The changeserver would be impractical if it had to store every change for
every scope since the beginning of time. So, the snapshot server lets
clients get an initial snapshot of the data by requesting the contents of
the database for a particular set of scopes at a particular point in time.

# API Specifications:

* changeserver [API](http://playground.apistudio.io/b1c7fe2f-8138-425a-8e3b-aca9f867e35a/docs/) and [Swagger spec](./changeserver/api.yaml)

# API Example

For instance, using the changeserver a client may issue the following
API call:

    GET http://changeserver/changes?scope=foo&block=10"

This call asks for all the changes for the scope "foo" starting at the beginning
of the sequence. If there are any changes at all, it will receive something
like this:

    {
    "changes": [
    {
      "operation": 1,
      "table": "public.scope",
      "sequence": "0.26d85c0.0",
      "commitSequence": 40732096,
      "changeSequence": 40731888,
      "commitIndex": 0,
      "txid": 1982,
      "newRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "val": {
          "value": "foo",
          "type": 1043
        }
      }
    },
    {
      "operation": 1,
      "table": "public.scope",
      "sequence": "0.26d8748.0",
      "commitSequence": 40732488,
      "changeSequence": 40732360,
      "commitIndex": 0,
      "txid": 1984,
      "newRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "val": {
          "value": "bar",
          "type": 1043
        }
      }
      }
      ]
      }

This change indicates that there were two changes made since that time,
denoting to inserts to the table called "public.scope".

After processing those two changes, the client would call the
change server again with a value of "since" equal to the "sequence"
value of the last change that it received, like this:

    GET http://changeserver/changes?scope=foo&block=60&since=0.26d8748.0

If there are more changes in the database since that sequence, they
will be immediately returned. Otherwise, the API call will block for up
to 60 seconds, and return immediately if more changes are available.
Otherwise, after 60 seconds it will return with an empty change list.

By "long polling" in this way, the client can reliably process changes,
in order. Furthermore, if the client is disconnected in any way, it can
resume processing changes right where it left off.

# Postgres Usage Notes

## URLs

Both of the servers in this project use a "URL" to connect to Postgres.
These URLs are designed to be similar to those used in Postgres JDBC drivers.
They look like the following:

    postgres://USER:PASS@HOST:PORT/DATABASE?ssl=SSL

The values are as follows:

* USER: The postgres user name.
* PASS: The password for that user. It is likely that this is required but
it may not be.    
* HOST: The hostname or IP address of the database server.
* PORT: The port of the database. Defaults to 5432.
* DATABASE: The name of the database to connect to. Note that logical
replication slots are per-database, so only changes to this database will
be replicated.
* SSL: If the database is configured to support SSL, then adding "ssl=true"
will cause the server to attempt an SSL connection.

# Developer Setup
## Prerequisites
### Install postgres
```
brew info postgresql
```
### Configure postgres
```

```
### Install leveldb

    brew install leveldb

## Build and install PG logical replication output plugin
```
cd 30x/transicator/pgoutput/
make install
```

## Build changeserver and snapshotserver (non Docker):

    make

## Build Docker Containers for Changeserver, snapshotserver and postgres

```
    cd 30x/transicator/
    make docker
```
Optionally, you can also build docker inside the respective modules.
For example:

```
    cd 30x/transicator/changeserver
    make docker
```

This will build the docker containers in their respective directories,
which then can be run locally.

## Run them as docker containers:

The value of POSTGRES_URL is described above.

The SLOT_NAME is a unique name for the replication slot used by the
changeserver. Each changeserver *must* have a unique slot name -- otherwise
there will be replication errors and data loss. In addition, if a
changeserver is removed, its slot must be removed from the database,
or the database will grow its transaction log forever and not clean it
up, and eventually fail.

    cd  30x/transicator/changeserver
    docker run --rm -it changeserver -u POSTGRES_URL -s SLOT_NAME
    cd 30x/transicator/snapshotserver
    docker run --rm -it snapshotserver -u POSTGRES_URL

## Run the docker container in E2E

On your laptop, install the SSL Keys to access the E2E.

```
    https://docs.google.com/document/d/1_Sz_duPEKhhnJRJ_YQ5J7NeF5mtDHc1eSPN88RM0UCg/edit
```

Ensure, binary image (postgres, changeserver or snapshotserver) is docker promoted in E2E,
by building the source code in E2E.

```
    http://jenkins-hackathon.aeip.apigee.net/job/build-go-ci/
```

The services for Postgres, Snapshotserver and Changeserver are alreay running and can
be verified by:

```
   kubectl get services
```
The pods (for  Postgres, Snapshotserver and Changeserver) themselves can be started
and stopped. The Cluster IP address should be reachable between pods.

```
cd  30x/transicator/changeserver
kubectl delete -f k8sdev/deployment.yaml
kubectl create -f k8sdev/deployment.yaml
```

```
cd  30x/transicator/snapshotserver
kubectl delete -f k8sdev/deployment.yaml
kubectl create -f k8sdev/deployment.yaml
```

```
cd  30x/transicator/pgoutput
kubectl delete -f k8sdev/deployment.yaml
kubectl create -f k8sdev/deployment.yaml
```

## To delete a replication slot:

    select * from pg_drop_replication_slot('SLOT_NAME');
