-- Case B test: keyless heap with duplicate rows (SQL Server 2019/2022)
-- Run as sa in a FRESH container (see the runbook). SQL Server Agent must be
-- running (MSSQL_AGENT_ENABLED=true) or CDC captures nothing.
CREATE DATABASE caseb;
GO
ALTER DATABASE caseb SET RECOVERY SIMPLE;
-- Needed for snapshot-isolation chunk reads (a database option, not a table change).
ALTER DATABASE caseb SET ALLOW_SNAPSHOT_ISOLATION ON;
GO
USE caseb;
GO
-- Heap: no primary key, no unique index. Binary collation so 'a' and 'A' stay distinct.
CREATE TABLE dbo.t (
  v VARCHAR(8) COLLATE Latin1_General_BIN2 NOT NULL
);
GO
EXEC sys.sp_cdc_enable_db;
GO
-- No key, so no net-changes function; capture instance is dbo_t.
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo', @source_name = N't',
  @role_name = NULL, @supports_net_changes = 0;
GO
-- The capture job keeps its default 5-second polling interval; oracle.py waits for it to go quiet.
SELECT name, is_cdc_enabled, snapshot_isolation_state_desc FROM sys.databases WHERE name = 'caseb';
GO
