# Transicator Stress Tests

These tests are designed to allow us to load up Transicator, break various
components, and see what breaks.

The tests will use a database schema designed as follows:

```
create table stress_table (
  id integer primary key,
  group integer not null,
  sequence integer not null,
  content varchar,
  end bool,
  _change_selector varchar not null
);
```

In order to ensure that the server receives what it expects, in order, the
client will be constructed to send groups of messages.

## Test Jobs

Each "job" is a goroutine...

### Sender

The sender job sends groups of records as described above by inserting,
updating, and deleting records in the table described above. All the records
in each batch will have the same group id. The client will perform a mix of
inserts, deletes, and updates containing random "content" for the specified
set of selectors.

Periodically, the sender will increment the "group" id and send an "end"
record. It will notify the main of this fact for flow control.

### Receiver

The receiver will retrieve a snapshot for a particular selector, and then
apply all changes that come to the replication scheme to the database.
The receiver will also periodically throw away its database, fetch a
new snapshot, and fetch a new set of changes.

When a receiver sees an "end" record it will notify the main for flow control.

### Verifier

Once the senders and receivers have stopped, the verifier will inspect the
contents of the Postgres database and also inspect the contents of the
SQLite databases to see if they match.

### Main

The main will start the sender and receivers. It will also direct the
senders to send special messages at the end so that we know that everything
has finished.

Eventually, the main wil use the Docker SDK to stop the database so that we
can see what affect the Transicator database has on the SDK.
