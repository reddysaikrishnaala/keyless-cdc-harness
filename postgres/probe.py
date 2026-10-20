#!/usr/bin/env python3
"""Case B reader probes for PostgreSQL. Run while workload.py is writing.

Each race: read a low watermark (WAL insert LSN), do one short chunk read (the
whole small table, counted per value), read a high watermark. Nothing is
written to the source and no lock is requested.

Modes
  plain  autocommit SELECT ... GROUP BY. Shows the ambiguity.
  pos1   REPEATABLE READ: read the WAL insert LSN (this first statement fixes
         the snapshot), then count. Claimed position = that LSN.
  pos2   REPEATABLE READ: count, then read the WAL insert LSN.
  set    REPEATABLE READ: count, then pg_current_snapshot(). The claim is the
         documented snapshot (xmin:xmax:xip list); score.py checks it against
         the transaction ids in the change stream.

--gap-ms sleeps between the first and second statement. --settle-ms (default
20) waits before the high watermark.

    python postgres/probe.py --mode set --races 2000 --out probes.jsonl
"""
import argparse
import json
import time

from common import COUNT_SQL, add_conn_args, connect, norm_lsn, wal_lsn


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--mode", choices=["plain", "pos1", "pos2", "set"], required=True)
    ap.add_argument("--races", type=int, default=2000)
    ap.add_argument("--gap-ms", type=float, default=0.0)
    ap.add_argument("--settle-ms", type=float, default=20.0)
    ap.add_argument("--label", default=None)
    ap.add_argument("--out", default="probes.jsonl")
    a = ap.parse_args()

    conn = connect(a)
    cur = conn.cursor()
    gap = a.gap_ms / 1000
    with open(a.out, "a") as f:
        for i in range(a.races):
            low = wal_lsn(cur)
            probe = snap = None
            if a.mode == "plain":
                cur.execute(COUNT_SQL)
                rows = cur.fetchall()
            else:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                if a.mode == "pos1":
                    cur.execute("SELECT pg_current_wal_insert_lsn()")
                    probe = norm_lsn(cur.fetchone()[0])
                    if gap:
                        time.sleep(gap)
                    cur.execute(COUNT_SQL)
                    rows = cur.fetchall()
                else:
                    cur.execute(COUNT_SQL)
                    rows = cur.fetchall()
                    if gap:
                        time.sleep(gap)
                    if a.mode == "pos2":
                        cur.execute("SELECT pg_current_wal_insert_lsn()")
                        probe = norm_lsn(cur.fetchone()[0])
                    else:
                        cur.execute("SELECT pg_current_snapshot()::text")
                        snap = cur.fetchone()[0]
                cur.execute("COMMIT")
            if a.settle_ms:
                time.sleep(a.settle_ms / 1000)
            high = wal_lsn(cur)
            rec = {"i": i, "mode": a.label or a.mode, "low": low, "high": high,
                   "probe": probe, "counts": {v: n for v, n in rows}}
            if snap is not None:
                rec["snap"] = snap
            f.write(json.dumps(rec) + "\n")
    print(f"wrote {a.races} races to {a.out}")


if __name__ == "__main__":
    main()
