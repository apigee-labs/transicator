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

The snapshot server and change server operate on the principle of a "scope."
A scope is simply a unique identifier for a particular set of records in the
database.

In order for transicator to work, we need to identify records by scope. We do
this by adding a "text" column named "_apid_scope" to every table that we
wish to replicate using transicator.

# API Specifications:

* changeserver [API](./changeserver/changeserver-api.html) and [OpenAPI spec](./changeserver/api.yaml)
* snapshot server [API](./snapshotserver/snapshot-api.html) and [OpenAPI spec](./snapshotserver/snapshot-api.yaml)

# Quick Start with Docker

First, start a Postgres server that has the Transicator plugin installed:

    docker run -p 5432:5432 --name pg -d \
    -e POSTGRES_PASSWORD=changeme thirtyx/transicator-postgres

Now you have a Postgres server running on port 5432 of whatever machine you
have Docker on. For instance, assuming that Docker runs on localhost, verify
that you can reach it. (BTW, you set the password above to be "changeme".")

````
psql -W -h localhost postgres postgres
Type "help" for help.

postgres=# select * from now();
              now
-------------------------------
 2016-11-01 22:20:11.092593+00
(1 row)
````

Looks great -- control-D to quit, BTW.

Now, start a snapshot server running on port 9001:

    docker run -d -p 9001:9001 thirtyx/transicator-snapshot \
    -u postgres://postgres:changeme@localhost/postgres

You can test quickly -- this API call should return "303 See Other":

    curl -v http://localhost:9001/snapshots?scope=foo

Finally, start a change server running on port 9000:

    docker run -d -p 9000:9000 thirtyx/transicator-changeserver \
    -u postgres://postgres:changeme@localhost/postgres -s testslot

This API call will tell you that the database is empty:

    curl http://localhost:9000/changes

# Complete API Example

Now that the servers are running, you can test the API.

To get started, imagine that we have a database that contains some tables
that have an _apid_scope column, and that there are some rows in those tables
that have the value "foo" for _apid_scope. For instance, let's create a table
and insert some values:

````
psql -W -h localhost postgres postgres
postgres=# create table test
    (id varchar primary key,
    val integer,
    _apid_scope varchar);
CREATE TABLE
postgres=# alter table test replica identity full;
ALTER TABLE
postgres=# insert into test values ('one', 1, 'foo');
INSERT 0 1
postgres=# insert into test values('two', 2, 'bar');
INSERT 0 1
postgres=# insert into test values('three', '3', 'foo');
INSERT 0 1
````

## Getting a snapshot

We can now get a snapshot in JSON format using the snapshot server:

````
$ curl -H "Accept: application/json" -L http://localhost:9001/snapshots?scope=foo

{"snapshotInfo":"229444:229444:","timestamp":"2016-10-13T17:42:12.171306-07:00",
"tables":[{"name":"public.snapshot_test","rows":[]},{"name":"scope","rows":[
{"_apid_scope":{"value":"foo","type":1043},"val":{"value":"bar","type":1043}}]},
{"name":"public.developer","rows":[]},{"name":"app","rows":[]},
{"name":"public.test","rows":[{"_apid_scope":{"value":"foo","type":1043},
"id":{"value":"one","type":1043},"val":{"value":"1","type":23}},
{"_apid_scope":{"value":"foo","type":1043},"id":{"value":"three","type":1043},
"val":{"value":"3","type":23}}]}]}
````

(In the example above, we use the "Accept" header because the snapshot server
can return different formats. We also use the "-L" argument to curl because the
API above actually redirects to a different URI.

The returned snapshot contains data that is consistent at a certain point
in time from a Postgres perspective. We can now use the change server to see
what has changed since that time

## Getting the first changes

The first time the changeserver is used, we do not know where to begin.
However, the snapshot contains the "snapshotInfo" field, which tells us which
Postgres transactions were visible when the snapshot was created.

At this point, using the changeserver, a client may issue the following
API call:

    curl -H "Accept: application/json" "http://localhost:9000/changes?snapshot=229444:229444:&scope=foo&block=10"

This call asks for all the changes for the scope "foo" starting from the first
change that was not visible in the sequence. If no changes appear for 10 seconds,
it will return with something like this:

````
{
  "lastSequence": "0.1390d838.0",
  "firstSequence": "",
  "changes": null
}
````

## Getting the next changes

Once the first set of changes has been retrieved, the "lastSequence" parameter
tells us where in the change stream we left off. We can now use this
in another API call to wait for more changes:

    curl -H "Accept: application/json" "http://changeserver/changes?since=0.1390d838.0&scope=foo&block=10"

This call won't result in any changes either but it will return us another sequence
if anything changed in the database.

Let's try again (with a longer timeout this time):

    curl -H "Accept: application/json" "http://changeserver/changes?since=0.1390d838.0&scope=foo&block=60"

and while we're waiting, let's use another window to insert something to the database:

````
postgres=# insert into test values('four', 4, 'foo');
INSERT 0 1
````

as soon as the change was committed to the database, a change
should come back from the API call.

````
{
  "lastSequence": "0.13923950.0",
  "firstSequence": "",
  "changes": [
    {
      "operation": 1,
      "table": "public.test",
      "sequence": "0.13923950.0",
      "commitSequence": 328350032,
      "changeSequence": 328349576,
      "commitIndex": 0,
      "txid": 229444,
      "newRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "id": {
          "value": "four",
          "type": 1043
        },
        "val": {
          "value": "4",
          "type": 23
        }
      }
    }
  ]
}
````

A successful user of the change server should now use the "lastSequence"
from this call and keep on making changes. For instance, experiment
with "update" and "delete" and see what happens. Experiment with transactions.
Committed transactions will only appear in the change log all at once
when the transactions commit. Rolled back transactions will never appear.

For instance:

    curl -H "Accept: application/json" "http://localhost:9000/changes?scope=foo&bock=60&since=0.1392ed18.0"

and then...

````
postgres=# begin;
BEGIN
postgres=# update test set val = 999 where id= 'one';
UPDATE 1
postgres=# delete from test where id = 'three';
DELETE 1
postgres=# commit;
COMMIT
````

Should result in:

````
{
  "lastSequence": "0.1392efb8.0",
  "firstSequence": "",
  "changes": [
    {
      "operation": 2,
      "table": "public.test",
      "sequence": "0.1392efb8.0",
      "commitSequence": 328396728,
      "changeSequence": 328396496,
      "commitIndex": 0,
      "txid": 229449,
      "newRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "id": {
          "value": "one",
          "type": 1043
        },
        "val": {
          "value": "999",
          "type": 23
        }
      },
      "oldRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "id": {
          "value": "one",
          "type": 1043
        },
        "val": {
          "value": "1",
          "type": 23
        }
      }
    }
  ]
}
````

and then another call should immediately give us:

````
{
  "lastSequence": "0.1392efb8.1",
  "firstSequence": "",
  "changes": [
    {
      "operation": 3,
      "table": "public.test",
      "sequence": "0.1392efb8.1",
      "commitSequence": 328396728,
      "changeSequence": 328396648,
      "commitIndex": 1,
      "txid": 229449,
      "oldRow": {
        "_apid_scope": {
          "value": "foo",
          "type": 1043
        },
        "id": {
          "value": "three",
          "type": 1043
        },
        "val": {
          "value": "3",
          "type": 23
        }
      }
    }
  ]
}
````

(What happened here? The change was propagated to us as soon as it appeared
in the database. The second change was available, though, and came through
right away.)

## Replication Settings

If, in the example above, no information was delivered on the "delete"
or "update" operations, it may be the replication settings for Postgres.
By default, Postgres only delivers the primary key on a deleted or updated
row, and that means that the change server does not see the "_apid_scope"
column.

The way to fix this is to change the table settings in Postgres so that
each row is delivered to the logical replication plugin. The command
looks like this:

    alter table NAME replica identity full

## Client best practices

So to sum it up, clients of transicator should do the following:

1) Download a snapshot for the scopes in which they are interested

2) Store the snapshot somewhere locally

3) Use the "snapshotInfo" field of the snapshot to request changes since the
beginning of time that were not visible at the time of the snapshot.

4) Use the "lastSequence" field on that API response, and all others,
and NOT the "snapshot" field, to get changes since the last set of
changes that the client saw.

5) Use the "block" parameter so that changes will immediately be delivered
to the client, and to avoid a huge number of API calls.

## Alternate Encodings

The JSON encoding of changes and snapshots is fine, but it has a limitation
in that all data values are converted into strings. This is especially a
problem if the database contains binary data ("bytea" fields in Postgres.)

As an alternate encoding, the snapshot server and change server can
return data encoded in protocol buffers if the Accept header is
set as follows:

    Accept: application/transicator+protobuf

The resulting protocol buffers can be decoded using the "common" package
in this project. To decode a change list, use:

    common.UnmarshalChangeListProto(buf []byte)

and to decode a snapshot, use:

    common.UnmarshalSnapshotProto(r io.Reader)

Finally, however, this leaves one last problem -- the snapshot may be very
large, and the "UnmarshalSnapshotProto" has to bring it all in memory at
once in order to create the "Snapshot" object.

For this problem, create a SnapshotReader using:

    common.CreateSnapshotReader(r io.Reader)

A SnapshotReader allows you to read the snapshot one table and row at a time.
The best practice is to loop through the results of the reader and insert
data into the database as you read it from the snapshot reader. That way
snapshots may be of any size and there will be no memory issues.

(Furthermore, snapshots produced in protobuf format are also produced in a
streaming way on the snapshot server, whereas JSON snapshots are not.
So, there are advantages to using protobuf-format snapshots whenever
possible.)

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

You can build and test transicator on OS X or Linux. You can also build
and test Docker containers.

Since the changeserver uses LevelDB as a native C library, Go's cross-compilation
does not work for this project. To build for Linux, you have to build on Linux
(or inside a Docker container).

## Build and Install on OS X
### Install postgres
```
brew info postgresql
```
### Configure postgres
```

```
### Install leveldb and other dependencies

    brew install leveldb
    brew install protobuf
    brew install protobuf-c

## Build and install PG logical replication output plugin

```
    cd 30x/transicator/pgoutput/
    make install
```

## Build changeserver and snapshotserver (non Docker):

```
    cd 30x/transicator/
    make
```

## Test on OS X

    make tests

This will run the tests in all the modules.
The environment variable TEST_PG_URL must be set in order to point the tests
to the right Postgres setup.

## Test on Docker

    make dockerTests

This will build Docker containers for Postgres, changeserver, and snapshotserver,
launch them, and then launch another container to run the tests.
This will work on any host that supports Docker -- there is no need to
install anything else in order to run these tests.

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

```
    cd  30x/transicator/changeserver
    docker run --rm -it changeserver -u POSTGRES_URL -s SLOT_NAME
    cd 30x/transicator/snapshotserver
    docker run --rm -it snapshotserver -u POSTGRES_URL
```

## To delete a replication slot:

    select * from pg_drop_replication_slot('SLOT_NAME');
