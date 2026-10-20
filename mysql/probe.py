#!/usr/bin/env python3
"""Case B reader probes for MySQL. Run while workload.py is writing.

Each race: read a low watermark, do one short chunk read (the whole small
table, counted per value), read a high watermark. Probe modes also record the
position they claim the chunk saw. Nothing is written to the source and no
lock is requested. (The positive control lives in workload.py --control-races.)

Modes
  plain  autocommit SELECT ... GROUP BY. Step 1: shows the ambiguity.
  m1     START TRANSACTION WITH CONSISTENT SNAPSHOT, then read
         @@GLOBAL.gtid_executed as the claimed position, then count.
  m2     Same, but read performance_schema.log_status (needs BACKUP_ADMIN).

    python mysql/probe.py --mode plain --races 3000 --out probes.jsonl
    python mysql/probe.py --mode m1 --races 3000 --gap-ms 2 --out probes.jsonl

--gap-ms sleeps between opening the snapshot and reading the position. It
widens a race that already exists so it shows up in fewer runs; it never
creates a race that is not there.

--settle-ms (default 20) waits before reading the high watermark, because a
commit can be visible to readers before its GTID is in gtid_executed.
--label records a different mode name, e.g. m1-gap2, so runs stay separate.
"""
import argparse
import json
import time

import pymysql

COUNT_SQL = "SELECT v, COUNT(*) FROM t GROUP BY v"


def gtid_executed(cur):
    cur.execute("SELECT @@GLOBAL.gtid_executed")
    return cur.fetchone()[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="caseb")
    ap.add_argument("--password", default="caseb")
    ap.add_argument("--mode", choices=["plain", "m1", "m2"], required=True)
    ap.add_argument("--races", type=int, default=3000)
    ap.add_argument("--gap-ms", type=float, default=0.0)
    # A commit can become visible to readers slightly before its GTID appears in
    # @@GLOBAL.gtid_executed, so a high watermark read immediately after the chunk
    # can be too low. Waiting before reading it keeps it an upper bound; score.py
    # still checks every race ("inconsistent_read") and rejects the run if not.
    ap.add_argument("--settle-ms", type=float, default=20.0)
    ap.add_argument("--label", default=None, help="mode name to record (default: --mode)")
    ap.add_argument("--out", default="probes.jsonl")
    a = ap.parse_args()

    conn = pymysql.connect(host=a.host, port=a.port, user=a.user, password=a.password,
                           database="caseb", autocommit=True)
    cur = conn.cursor()
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    with open(a.out, "a") as f:
        for i in range(a.races):
            low = gtid_executed(cur)
            probe = None
            if a.mode == "plain":
                cur.execute(COUNT_SQL)
                rows = cur.fetchall()
            else:
                cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                if a.gap_ms:
                    time.sleep(a.gap_ms / 1000)
                if a.mode == "m1":
                    probe = gtid_executed(cur)
                else:
                    cur.execute("SELECT LOCAL FROM performance_schema.log_status")
                    probe = json.loads(cur.fetchone()[0])["gtid_executed"]
                cur.execute(COUNT_SQL)
                rows = cur.fetchall()
                cur.execute("COMMIT")
            if a.settle_ms:
                time.sleep(a.settle_ms / 1000)
            high = gtid_executed(cur)
            f.write(json.dumps({"i": i, "mode": a.label or a.mode, "low": low, "high": high,
                                "probe": probe, "counts": {v: n for v, n in rows}}) + "\n")
    print(f"wrote {a.races} races to {a.out}")


if __name__ == "__main__":
    main()
