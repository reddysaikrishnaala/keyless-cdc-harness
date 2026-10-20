# keyless-cdc-harness

Test harness for **exact change data capture (CDC) of keyless tables with identical duplicate rows**, as studied in:

> S. Ala *et al.*, "Exact Change Data Capture for Keyless Tables with Duplicate Rows: An Indistinguishability Argument and an Empirical Study of MySQL and SQL Server," manuscript, 2026.

## The question

A CDC backfill reads a live table in chunks, without locks, while the change log keeps moving. Watermark methods (e.g. DBLog, Debezium incremental snapshots) reconcile each chunk with the log by **primary key**. A keyless table can hold several identical rows, so the replica must reproduce a *multiset*. Without a key, the backfill must know exactly which logged transactions each chunk read already contains.

This harness tests whether stock **MySQL 8.0** and **SQL Server 2022** expose that information through documented interfaces. It works under four fixed constraints: no source schema change, no locks, no frozen copy, and no long transaction.

## How it works

| Part | What it does |
|---|---|
| `*/workload.py` | Loads 20 copies of each of 5 values into a keyless table, then runs 8 writer threads doing single-copy inserts, deletes and updates. With `--control-races`, also runs the **positive control**: it pauses writers between transactions so the recorded position is exact, and the scorer must then report 0 errors. |
| `*/probe.py` | Reader "races": low watermark → chunk read (counts per value) → high watermark. Probe modes also record the log position they claim the read saw. |
| `*/oracle.py` | Replays the change stream (MySQL binlog / SQL Server CDC) in commit order to get the exact multiset at every position, and checks the replay against the live table. |
| `score.py` | For each race, checks "assume low", "assume high" and "use the probe position" against the oracle. It also flags reads that match no commit-order prefix. |
| `sqlserver/linkage.py` | Identifier inventory: what a snapshot reader can see about concurrent writers (including `sys.dm_tran_current_snapshot`, `sys.dm_tran_transactions_snapshot` and an observer session) versus what appears in CDC and the log. |

Probe modes:
- **MySQL:** `plain`; `m1`, which reads `@@GLOBAL.gtid_executed` inside `START TRANSACTION WITH CONSISTENT SNAPSHOT`; `m2`, which reads `performance_schema.log_status`.
- **SQL Server**, all under `SNAPSHOT` isolation: `plain`; `s1`, which reads the log end before the count; `s2`, which reads it after.

## Requirements

- Python 3.10+ and `pip install -r requirements.txt`. The scripts look for a virtual environment at `~/caseb-venv` first.
- Docker. On Apple Silicon, SQL Server runs as `linux/amd64` under emulation.
- The SQL Server harness reads the log end through the undocumented `sys.fn_dblog`, for test instrumentation only.

## Running

```bash
# SQL Server 2022 in Docker (Agent + CDC enabled automatically)
bash run_sqlserver.sh            # 1 shakedown run + identifier check
bash run_sqlserver.sh 3          # 3 full runs (as in the paper) + identifier check
bash run_sqlserver.sh linkage    # identifier check only (~10 min)

# SQL Server on Windows (local Developer edition instance) - see README_windows.txt
powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password '...' -Runs 3

# MySQL 8.0 in Docker
bash run_mysql.sh                # 3 runs (as in the paper)
```

Results are written to `runs/`.

> **Status of the run scripts.** `run_sqlserver.sh` produced the SQL Server results below. The paper's MySQL results were produced with the same Python scripts and settings, run by hand. `run_mysql.sh` packages those steps: MySQL 8.0 with GTIDs and row-based binlog, `RESET MASTER` before setup, 8 writers, 90 s runs, 1,500 control races, 2,000 races per probe mode, a 20 ms settle delay, and 2 ms gap variants in run 3. It has passed a syntax check but has **not yet been run end-to-end**. `run_sqlserver_windows.ps1` has also not yet been run.

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

Headline numbers, summed over three runs:
- **Positive control:** 0 errors in 4,500 MySQL and 3,600 SQL Server control races.
- **MySQL:** the `gtid_executed` probe was wrong in 10.6% of racing reads and the `log_status` probe in 75.8%.
- **SQL Server:** the log-end probe was wrong in 67.7% of racing reads when read before the snapshot read and 36.3% when read after. 74 of 6,272 racing snapshot reads (about 1 in 85) saw commits out of CDC commit order. Both documented per-snapshot views returned no rows.

## Limitations

- SQL Server was tested on Linux under emulation, not on Windows.
- The scorer explains at most one out-of-order commit per read automatically. One read in SQL Server run 3 needed two and was checked by hand, and the scorer marks that run INVALID by its strict rule.
- A finite stress test can reject a probe but cannot prove one correct.

## AI assistance

This harness and its documentation were written with the assistance of Claude (Anthropic). The authors defined the problem and its constraints and ran the SQL Server experiments.

## License

MIT. See `LICENSE`.
