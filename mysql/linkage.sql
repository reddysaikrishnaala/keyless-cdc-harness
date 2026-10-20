-- Case B identifier inventory, MySQL. Manual, two windows (A, B), as root.
-- Question: does any identifier the READER can see about its own snapshot
-- also appear in the binlog for the writer? If nothing matches, the reader
-- cannot tell which logged changes it saw.

-- ===== Window A (writer): start a change and leave it open =====
USE caseb;
START TRANSACTION;
DELETE FROM t WHERE v = 'A' LIMIT 1;
SELECT trx_id FROM information_schema.INNODB_TRX
WHERE trx_mysql_thread_id = CONNECTION_ID();               -- record A1 (InnoDB trx id)

-- ===== Window B (reader): open a snapshot while A is still open =====
USE caseb;
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT COUNT(*) FROM t WHERE v = 'A';
SELECT trx_id, trx_started FROM information_schema.INNODB_TRX
WHERE trx_mysql_thread_id = CONNECTION_ID();               -- record B1
SHOW ENGINE INNODB STATUS\G
-- In TRANSACTIONS, find this connection; record any "read view" line (B2).
-- performance_schema has no InnoDB id to join on: TRX_ID there is documented as unused.

-- ===== Window A: commit, then find its binlog events =====
COMMIT;
SELECT @@GLOBAL.gtid_executed;                              -- newest GTID = A's (A2)
SHOW BINARY LOGS;                                           -- note the last file name
-- From a shell:  mysqlbinlog -vv <datadir>/<last binlog file> | tail -40
-- Record A's GTID line and its "Xid = ..." value (A3).

-- ===== Window B: finish =====
COMMIT;

-- Conclusion for this step:
--   Is A1 (InnoDB trx id, which read views use) equal to anything in the
--   binlog for A (GTID A2, Xid A3, last_committed/sequence_number)?
--   Verified on 8.0.46: trx_id 240713 vs Xid 362412 - no match.
