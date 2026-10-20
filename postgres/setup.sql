-- Case B test: keyless table with duplicate rows (PostgreSQL 16, wal_level = logical)
-- Run against a fresh database named caseb:  psql -d caseb -v identity=default -f setup.sql
-- The replication slot is created BEFORE the table, so the change stream covers
-- the table from creation and the oracle can replay it exactly.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'caseb';
SELECT 'slot created at ' || lsn FROM pg_create_logical_replication_slot('caseb', 'test_decoding');
-- No primary key, no unique index. "C" collation so 'a' and 'A' stay distinct.
CREATE TABLE t (v varchar(8) COLLATE "C" NOT NULL);
-- identity=full changes the table's replica identity: a table-level change,
-- which the Case B constraints count as a schema change.
SELECT CASE WHEN :'identity' = 'full' THEN 'ALTER TABLE t REPLICA IDENTITY FULL' ELSE 'SELECT 1' END \gexec
SELECT relreplident FROM pg_class WHERE relname = 't';
