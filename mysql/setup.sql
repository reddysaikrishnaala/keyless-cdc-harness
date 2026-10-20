-- Case B test: keyless table with duplicate rows (MySQL 8.0/8.4)
-- Server must run with: gtid_mode=ON, enforce_gtid_consistency=ON,
-- binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL
DROP DATABASE IF EXISTS caseb;
CREATE DATABASE caseb;
-- No primary key, no unique index. Binary collation so 'a' and 'A' stay distinct.
CREATE TABLE caseb.t (
  v VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL
) ENGINE=InnoDB;
CREATE USER IF NOT EXISTS 'caseb'@'%' IDENTIFIED BY 'caseb';
GRANT SELECT, INSERT, UPDATE, DELETE ON caseb.* TO 'caseb'@'%';
-- Replication privileges let oracle.py read the binlog; BACKUP_ADMIN lets probe M2 read performance_schema.log_status.
GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN ON *.* TO 'caseb'@'%';
GRANT SELECT ON performance_schema.* TO 'caseb'@'%';
