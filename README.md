# keyless-cdc-harness

Test harness for **exact change data capture (CDC) of keyless tables with identical duplicate rows**, as studied in:

> S. Ala *et al.*, "Exact Change Data Capture for Keyless Tables with Duplicate Rows: An Indistinguishability Argument and an Empirical Study of MySQL, MariaDB, PostgreSQL and SQL Server," manuscript, 2026.

## The question

A CDC backfill reads a live table in chunks, without locks, while the change log keeps moving. Watermark methods (e.g. DBLog, Debezium incremental snapshots) reconcile each chunk with the log by **primary key**. A keyless table can hold several identical rows, so the replica must reproduce a *multiset*. Without a key, the backfill must know exactly which logged transactions each chunk read already contains.

This harness tests whether **MySQL 8.0 and 8.4**, **MariaDB 11.4**, **SQL Server 2022** and **PostgreSQL 16** expose that information through documented interfaces. It works under four fixed constraints: no source schema change, no locks, no frozen copy, and no long transaction.

## How it works

| Part | What it does |
|---|---|
| `*/workload.py` | Loads 20 copies of each of 5 values into a keyless table, then runs 8 writer threads doing single-copy inserts, deletes and updates. With `--control-races`, also runs the **positive control**: it pauses writers between transactions so the recorded position is exact, and the scorer must then report 0 errors. |
| `*/probe.py` | Reader "races": low watermark → chunk read (counts per value) → high watermark. Probe modes also record the log position they claim the read saw. |
| `*/oracle.py` | Replays the change stream (MySQL and MariaDB binlog / SQL Server CDC / PostgreSQL logical decoding) in commit order to get the exact multiset at every position, and checks the replay against the live table. |
| `score.py` | For each race, checks "assume low", "assume high" and "use the probe position" against the oracle. It also flags reads that match no commit-order prefix. |
| `sqlserver/linkage.py` | Identifier inventory: what a snapshot reader can see about concurrent writers (including `sys.dm_tran_current_snapshot`, `sys.dm_tran_transactions_snapshot` and an observer session) versus what appears in CDC and the log. |

Probe modes:
- **MySQL:** `plain`; `m1`, which reads `@@GLOBAL.gtid_executed` inside `START TRANSACTION WITH CONSISTENT SNAPSHOT`; `m2`, which reads `performance_schema.log_status`.
- **MariaDB:** `plain`; `pos`, which reads `SHOW MASTER STATUS` inside `START TRANSACTION WITH CONSISTENT SNAPSHOT`; `snap`, which reads `Binlog_snapshot_file` / `Binlog_snapshot_position`, documented as the binary-log position matching the consistent snapshot.
- **SQL Server**, all under `SNAPSHOT` isolation: `plain`; `s1`, which reads the log end before the count; `s2`, which reads it after.
- **PostgreSQL**, all under `REPEATABLE READ`: `plain`; `pos1`, which reads the WAL insert LSN as the first statement and then counts; `pos2`, which counts first; `set`, which counts and then reads `pg_current_snapshot()`, the documented set of in-progress transaction IDs, scored against the transaction IDs in the change stream.

## Requirements

- Python 3.10+ and `pip install -r requirements.txt`. The scripts look for a virtual environment at `~/caseb-venv` first.
- Docker for MySQL, MariaDB and SQL Server. On Apple Silicon, SQL Server runs as `linux/amd64` under emulation.
- PostgreSQL 16 with `wal_level = logical` and a free replication slot. `run_postgres.sh` connects to `127.0.0.1:5433` as `postgres` by default (override with `PGHOST`, `PGPORT`, `PGUSER`).
- The SQL Server harness reads the log end through the undocumented `sys.fn_dblog`, for test instrumentation only.

## Running

```bash
# SQL Server 2022 in Docker (Agent + CDC enabled automatically)
bash run_sqlserver.sh            # 1 shakedown run + identifier check
bash run_sqlserver.sh 3          # 3 full runs (as in the paper) + identifier check
bash run_sqlserver.sh linkage    # identifier check only (~10 min)

# SQL Server on Windows (local Developer edition instance) - see README_windows.txt
powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password '...' -Runs 3

# MySQL in Docker
bash run_mysql.sh                          # MySQL 8.0, 3 runs
IMG=mysql:8.4 TAG=my84 bash run_mysql.sh   # MySQL 8.4 LTS, 3 runs

# MariaDB 11.4 in Docker
bash run_mariadb.sh              # 3 runs: plain, pos and snap probes
SNAPONLY=1 bash run_mariadb.sh   # 3 longer runs of only the snapshot-position probe (0, 2 and 10 ms gaps)

# PostgreSQL 16 (running server, wal_level = logical)
IDENTITY=default RUNS=1 SECS=30 bash run_postgres.sh   # keyless table as is (no schema change)
IDENTITY=full RUNS=3 bash run_postgres.sh              # REPLICA IDENTITY FULL (a table-level change)
```

Results are written to `runs/`.

> **Status of the run scripts.** `run_sqlserver.sh` produced the SQL Server results below. The paper's MySQL results were produced with the same Python scripts and settings, run by hand. `run_mysql.sh` packages those steps: MySQL 8.0 with GTIDs and row-based binlog, `RESET MASTER` before setup, 8 writers, 90 s runs, 1,500 control races, 2,000 races per probe mode, a 20 ms settle delay, and 2 ms gap variants in run 3. `run_mysql.sh` produced the MySQL 8.4 results below, `run_mariadb.sh` the MariaDB results, and `run_postgres.sh` the PostgreSQL results. `run_sqlserver_windows.ps1` has not yet been run.

## Results in this repository

`results/` holds the raw outputs behind the paper's tables. `oracle.json` files are gzipped; run `gunzip -k` before re-scoring.

```bash
gunzip -k results/mysql/run1/oracle.json.gz
cat results/mysql/run1/p_*.jsonl > /tmp/all.jsonl
python score.py --oracle results/mysql/run1/oracle.json --probes /tmp/all.jsonl --min-racy 1000
```

| Folder | Engine | Contents |
|---|---|---|
| `results/mysql/run1..3` | MySQL 8.0.46 (Ubuntu build, Linux x86-64) | Table I |
| `results/sqlserver/run1..3` | SQL Server 2022 CU27 (16.0.4305.5), Linux container under x86-64 emulation | Table II |
| `results/sqlserver/linkage*.txt` | same | Table III (identifier linkage, observer session, `dm_tran_current_snapshot`) |
| `results/mysql84/run1..3` | MySQL 8.4.11 (Docker, Apple Silicon) | Same probes as MySQL 8.0 |
| `results/mariadb/run1..3` | MariaDB 11.4.13 (Docker, Apple Silicon) | plain, pos and snap probes |
| `results/mariadb/snaponly-run1..3` | same | Snapshot-position probe only, 0/2/10 ms gaps |
| `results/postgres/pg-default-run1` | PostgreSQL 16.15 (Linux x86-64), default replica identity | Change stream cannot be replayed |
| `results/postgres/pg-full-run1..3` | PostgreSQL 16.15, `REPLICA IDENTITY FULL` | Position and snapshot-set probes |

Headline numbers, summed over three runs:
- **Positive control:** 0 errors in every run on every engine.
- **MySQL:** the `gtid_executed` probe was wrong in 10.6% of racing reads and the `log_status` probe in 75.8%.
- **SQL Server:** the log-end probe was wrong in 67.7% of racing reads when read before the snapshot read and 36.3% when read after. 74 of 6,272 racing snapshot reads (about 1 in 85) saw commits out of CDC commit order. Both documented per-snapshot views returned no rows.
- **MySQL 8.4.11:** same picture as 8.0: `gtid_executed` wrong in 12.3% and `log_status` in 92.9% of racing reads; 0 control errors in 4,500.
- **MariaDB 11.4.13:** 0 control errors in 9,000. `SHOW MASTER STATUS` inside the snapshot was wrong in 44.3% of racing reads. The documented snapshot position (`Binlog_snapshot_position`) was right in almost every read but **wrong in 40 of 52,489** (0.076%): 5 of 20,999 with no gap, 19 of 16,492 with a 2 ms gap, 16 of 14,998 with a 10 ms gap. Each error was off by 5 to 57 commits.
- **PostgreSQL, default replica identity (no schema change):** the change stream recorded 41,289 deletes and 29,635 updates of the keyless table without the old row value, so it cannot say which value they removed.
- **PostgreSQL, `REPLICA IDENTITY FULL`:** 0 control errors in 4,500 reads. The WAL-position probes were wrong in 74.4% and 94.3% of racing reads. The documented snapshot set (`pg_current_snapshot()`) was wrong in **0 of 7,498** racing reads.

## Limitations

- SQL Server was tested on Linux under emulation, not on Windows.
- The MariaDB snapshot-position errors are rare and have not been traced to a cause; they were reproduced in four separate server instances.
- PostgreSQL's documentation does not state explicitly that `pg_current_snapshot()` returns the transaction's own snapshot under `REPEATABLE READ`; the zero-error result supports it empirically. Subtransactions are not covered by the workload.
- The scorer explains at most one out-of-order commit per read automatically. One read in SQL Server run 3 needed two and was checked by hand, and the scorer marks that run INVALID by its strict rule.
- A finite stress test can reject a probe but cannot prove one correct.

## License

MIT. See `LICENSE`.
