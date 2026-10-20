-- Case B identifier inventory, SQL Server. Manual, three windows (A, B, C).
-- Question: does any identifier the READER can see about its own snapshot
-- also appear on the LOG side (CDC or the transaction log) for the writer?
-- If nothing matches, the reader cannot tell which logged changes it saw.
-- Record every value in the runbook's table. Run on the test container only.

-- ===== Window A (writer): start a change and leave it open =====
USE caseb;
BEGIN TRAN;
DELETE TOP (1) FROM dbo.t WHERE v = 'A';
SELECT transaction_id, transaction_sequence_num, transaction_is_snapshot
FROM sys.dm_tran_current_transaction;                       -- record A1, A2
SELECT [Transaction ID], [Xact ID], [Current LSN], Operation
FROM sys.fn_dblog(NULL, NULL)
WHERE Operation = 'LOP_BEGIN_XACT' AND SPID = @@SPID;       -- record A3 (pick the newest row)
-- (if [Xact ID] errors on your version, remove it from the column list)

-- ===== Window B (reader): open a snapshot while A is still open =====
USE caseb;
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRAN;
SELECT COUNT(*) FROM dbo.t WHERE v = 'A';                   -- snapshot starts here
SELECT transaction_id, transaction_sequence_num, first_snapshot_sequence_num,
       last_transaction_sequence_num
FROM sys.dm_tran_current_transaction;                       -- record B1..B4
SELECT ts.snapshot_sequence_num
FROM sys.dm_tran_transactions_snapshot AS ts
JOIN sys.dm_tran_current_transaction AS c
  ON ts.transaction_sequence_num = c.transaction_sequence_num;  -- record B5 (A should be listed)
-- leave B open

-- ===== Window A: commit =====
COMMIT;

-- ===== Window C: after ~5 seconds, look at the log side =====
USE caseb;
SELECT TOP (5) start_lsn, tran_id, tran_begin_time, tran_end_time
FROM cdc.lsn_time_mapping ORDER BY start_lsn DESC;          -- record C1 (the row for A)
SELECT TOP (5) __$start_lsn, __$seqval, __$operation, v
FROM cdc.dbo_t_CT ORDER BY __$start_lsn DESC;               -- record C2
-- Replace the literal below with A3 ([Transaction ID]) and inspect every column:
-- SELECT * FROM sys.fn_dblog(NULL, NULL) WHERE [Transaction ID] = '0000:00000000';

-- ===== Window B: finish =====
COMMIT;

-- Conclusion for this step:
--   Does B5 (or B1..B4) equal any of A1/A2, and does that same value appear in
--   C1, C2 or the fn_dblog rows for A3?  YES = a native link exists (document
--   it and test it at scale).  NO = no documented link on this version.
