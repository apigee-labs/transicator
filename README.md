# Transicator

This is suite of programs, plus a Postgres plugin, that allows us to
use the Postgres "logical decoding" feature to distribute a consistent
set of changes from Postgres to many consumers.


# Developer Setup

## Prerequisites

This assumes you have build tools available on your mac.  If you do not, you can install them with the following command

```
xcode-select --install
```

### Install postgres

```
brew install postgresql
```

### Configure postgres

Make a local postgres workspace, so you can test and not modify the default install

```
mkdir -p ~/dev/
cp -r /usr/local/var/postgres ~/dev/postgres
```

Modify ~/dev/postgres/postgresql.conf and add the following to the end of the file

```
#------------------------------------------------------------------------------
# CUSTOMIZED OPTIONS
#------------------------------------------------------------------------------

# Add settings for extensions here

#Props to document as required
wal_level=logical
max_wal_senders = 2
max_replication_slots = 2

```

Modify ~/dev/postgres/pg_hba.conf and add the following configuration to the end of the file.  Note this is for development only.  Production usage will want to evalute this security. 

```

#Added for replication to trust all
local   replication     all                                trust
host    replication     all        127.0.0.1/32            trust
host    replication     all        ::1/128                 trust
```


## Build and install PG logical replication output plugin
```
cd 30x/transicator/pgoutput/
make install
```

Now run postgres to ensure everything is working.  No errors = good news

```
postgres -D ~/dev/postgres
```

Ensure postgres is running by connecting with a PG SQL client.

```
psql postgres
select user from pg_stat_activity;
```

Once you've successfully been returned a user, you can exist the PGSQL client.  If you're unable to query PG, you have a problem with your installation.  

Make a note of this username, you will need it later.


## Build replication client
```
cd 30x/transicator/
glide install
cd replclient/
go build

```

## Test the replciation client
```
./replclient postgres://[your postgres username from select statement]@localhost:5432/postgres myslot
```

If successful, the replclient will stay connected to postgres, and you will see output similar to the following in your postgres log windows.

```
LOG:  logical decoding found consistent point at 0/1737238
DETAIL:  There are no running transactions.
LOG:  exported logical decoding snapshot: "000003AA-1" with 0 transaction IDs
LOG:  starting logical decoding for slot "myslot"
DETAIL:  streaming transactions committing after 0/1737270, reading WAL from 0/1737238
LOG:  logical decoding found consistent point at 0/1737238
DETAIL:  There are no running transactions.
```

Now you're ready to test the stream.  To do so create a test table in the pgsql client session

```sql
CREATE TABLE test_table(
   id text,
   tenant_id text,
   PRIMARY KEY( id )
);


ALTER TABLE test_table add column created timestamp with time zone;

ALTER TABLE test_table alter created set default now();
```

Now insert 2 test records

```sql
INSERT INTO test_table (id, tenant_id) VALUES ('foo', 'tenant1');
INSERT INTO test_table (id, tenant_id) VALUES ('bar', 'tenant1');
```

You should not see the commits stream across stdout in the replclient runtime


## Running the snapshot client

Create a table_catalog table.  This tells the snapshot client which tables should be included in the snapshot.

``` sql
CREATE TABLE table_catalog (
  table_name text
);
```

Insert the test_table into the table catalog for snapshot

```sql
INSERT INTO table_catalog (table_name) VALUES ('test_table');
```

Now from the terminal, insert test records.  Re-enter this a few times, say 10

```
X=$((X+1)); echo $X; psql postgres -c "insert into test_table values('$X', 'tenant2')"
```

Now, if you run the snapshot client for tenant2, you should be able to receive all of it's snapshot data


```
cd 30x/transicator/snapclient
go run snapclient.go "postgres://[your postgres username from select statement]@localhost:5432/postgres" tenant2 ./data
```

You should now have downloaded a snapshot in CSV format to the data directory.  You can view the output by 

```
cat data/tenant2.csv
```