-- Case B test: keyless table with duplicate rows (MariaDB 10.5+ / 11.x)
-- Server must run with: log-bin, binlog_format=ROW, binlog_row_image=FULL,
-- binlog_row_metadata=FULL (so the binary log carries column names).
-- Run RESET MASTER first so the binary log covers the table from creation.
DROP DATABASE IF EXISTS caseb;
CREATE DATABASE caseb;
-- No primary key, no unique index. Binary collation so 'a' and 'A' stay distinct.
CREATE TABLE caseb.t (
  v VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL
) ENGINE=InnoDB;
CREATE USER IF NOT EXISTS 'caseb'@'%' IDENTIFIED BY 'caseb';
GRANT SELECT, INSERT, UPDATE, DELETE ON caseb.* TO 'caseb'@'%';
-- Reading the binary log (oracle) and SHOW MASTER STATUS (watermarks).
GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'caseb'@'%';
