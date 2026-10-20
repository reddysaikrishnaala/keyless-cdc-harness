#!/usr/bin/env python3
"""Case B reader probes for SQL Server. Run while workload.py is writing.

Each race: read a low watermark (log end), do one short snapshot-isolation
chunk read (the whole small table, counted per value), read a high watermark.
Probe modes also record the position they claim the chunk saw. Nothing is
written to the source and no lock is requested.

Modes
  plain  SNAPSHOT transaction, count. Step 1: shows the ambiguity.
  s1     SNAPSHOT transaction: read the log end, then count. The snapshot
         starts at the first statement that reads data, i.e. the count.
  s2     SNAPSHOT transaction: count, then read the log end.

    python sqlserver/probe.py --password '...' --mode plain --races 3000 --out probes.jsonl
    python sqlserver/probe.py --password '...' --mode s1 --races 3000 --gap-ms 2 --out probes.jsonl

--gap-ms sleeps between the probe read and the count (s1) or the count and the
probe read (s2). It widens an existing race; it never creates one.
--settle-ms (default 20) waits after the low watermark and before the high
one, so both stay valid bounds; score.py rejects the run if they are not.
"""
import argparse
import json
import time

from common import add_conn_args, check_lsn_format, connect, end_lsn

COUNT_SQL = "SELECT v, COUNT(*) FROM dbo.t GROUP BY v"


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--mode", choices=["plain", "s1", "s2"], required=True)
    ap.add_argument("--races", type=int, default=3000)
    ap.add_argument("--gap-ms", type=float, default=0.0)
    ap.add_argument("--settle-ms", type=float, default=20.0)
    ap.add_argument("--label", default=None, help="mode name to record (default: --mode)")
    ap.add_argument("--out", default="probes.jsonl")
    a = ap.parse_args()

    conn = connect(a)
    cur = conn.cursor()
    check_lsn_format(cur, a.lsn_source)
    gap = a.gap_ms / 1000
    with open(a.out, "a") as f:
        for i in range(a.races):
            low = end_lsn(cur, a.lsn_source)
            time.sleep(a.settle_ms / 1000)
            probe = None
            cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            cur.execute("BEGIN TRAN")
            if a.mode == "s1":
                probe = end_lsn(cur, a.lsn_source)
                if gap:
                    time.sleep(gap)
            cur.execute(COUNT_SQL)                       # snapshot starts here
            rows = cur.fetchall()
            if a.mode == "s2":
                if gap:
                    time.sleep(gap)
                probe = end_lsn(cur, a.lsn_source)
            cur.execute("COMMIT")
            time.sleep(a.settle_ms / 1000)
            high = end_lsn(cur, a.lsn_source)
            f.write(json.dumps({"i": i, "mode": a.label or a.mode, "low": low, "high": high,
                                "probe": probe, "counts": {v: n for v, n in rows}}) + "\n")
    print(f"wrote {a.races} races to {a.out}")


if __name__ == "__main__":
    main()
