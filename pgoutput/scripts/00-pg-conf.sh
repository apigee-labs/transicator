/*
Copyright 2016 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#!/bin/sh

# BEGIN DEV SETUP

# Replace replication settings in postgresql.conf with what we need
sed -ibak -f /docker-entrypoint-initdb.d/pgconf.sed $PGDATA/postgresql.conf

echo "*** New Replication Settings ***"
grep -e replication -e wal $PGDATA/postgresql.conf
echo "*******"

# Replace pg_hba.conf with one that is secure.
cp /docker-entrypoint-initdb.d/pg_hba.conf $PGDATA

# END DEV SETUP
