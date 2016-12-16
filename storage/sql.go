/*
Copyright 2016 The Transicator Authors

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
package storage

const createTableSQL = `
pragma journal_mode=WAL;

create table if not exists transicator_entries
(scope text not null,
 lsn integer not null,
 ix integer not null,
 ts integer,
 data blob,
 primary key(scope, lsn, ix)
) without rowid;

create index if not exists transicator_sequence
on transicator_entries
(lsn, ix);
`

const insertSQL = `
insert or replace into transicator_entries (scope, lsn, ix, ts, data)
values (?, ?, ?, ?, ?)
`

const readRangeSQL = `
select lsn, ix, data from transicator_entries
where scope = ? and ((lsn > ?) or (lsn == ? and ix >= ?))
order by lsn, ix
`

const readFirstSQL = `
select lsn, ix from transicator_entries order by lsn asc, ix asc limit 1
`

const readLastSQL = `
select lsn, ix from transicator_entries order by lsn desc, ix desc limit 1
`

const purgeByTimeSQL = `
delete from transicator_entries where ts < ?
`
