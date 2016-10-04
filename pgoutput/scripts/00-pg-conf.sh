#!/bin/sh

# Replace replication settings in postgresql.conf with what we need
sed -ibak -f /docker-entrypoint-initdb.d/pgconf.sed $PGDATA/postgresql.conf

echo "*** New Replication Settings ***"
grep -e replication -e wal $PGDATA/postgresql.conf
echo "*******"

# Replace pg_hba.conf with one that is secure.
cp /docker-entrypoint-initdb.d/pg_hba.conf $PGDATA
