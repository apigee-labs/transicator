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
values (?, ?, ?, datetime('now'), ?)
`

const readRangeSQL = `
select lsn, ix, data from transicator_entries
where scope = ? and lsn >= ? and ix >= ?
order by lsn, ix
limit ?
`

const readFirstSQL = `
select lsn, ix from transicator_entries order by lsn asc, ix asc limit 1
`

const readLastSQL = `
select lsn, ix from transicator_entries order by lsn desc, ix desc limit 1
`

const deleteEntrySQL = `
delete from transicator_entries where scope = ? and lsn = ? and ix = ?
`
