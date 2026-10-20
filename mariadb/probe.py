#!/usr/bin/env python3
"""Case B reader probes for MariaDB. Run while workload.py is writing.

Each race: read a low watermark, do one short chunk read (the whole small
table, counted per value), read a high watermark. Probe modes also record the
position they claim the chunk saw. Nothing is written to the source and no
lock is requested. (The positive control lives in workload.py --control-races.)

Modes
  plain  autocommit SELECT ... GROUP BY. Shows the ambiguity.
  pos    START TRANSACTION WITH CONSISTENT SNAPSHOT, then SHOW MASTER STATUS
         (the server's current position) as the claimed position, then count.
  snap   Same, but read Binlog_snapshot_file / Binlog_snapshot_position, which
         MariaDB documents as the position matching the consistent snapshot.

    python mariadb/probe.py --mode snap --races 2000 --out probes.jsonl

--gap-ms sleeps between opening the snapshot and reading the position. It
widens a race that already exists so it shows up in fewer runs; it never
creates a race that is not there.

--settle-ms (default 20) waits before reading the high watermark, because a
commit can be visible to readers before the binary-log position moves past it.
--label records a different mode name, e.g. snap-gap2, so runs stay separate.
"""
import argparse
import json
import time

from common import COUNT_SQL, add_conn_args, connect, master_pos, snapshot_pos



def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--mode", choices=["plain", "pos", "snap"], required=True)
    ap.add_argument("--races", type=int, default=3000)
    ap.add_argument("--gap-ms", type=float, default=0.0)
    # A commit can become visible to readers slightly before SHOW MASTER STATUS
    # moves past it, so a high watermark read immediately after the chunk
    # can be too low. Waiting before reading it keeps it an upper bound; score.py
    # still checks every race ("inconsistent_read") and rejects the run if not.
    ap.add_argument("--settle-ms", type=float, default=20.0)
    ap.add_argument("--label", default=None, help="mode name to record (default: --mode)")
    ap.add_argument("--out", default="probes.jsonl")
    a = ap.parse_args()

    conn = connect(a, autocommit=True)
    cur = conn.cursor()
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    with open(a.out, "a") as f:
        for i in range(a.races):
            low = master_pos(cur)
            probe = None
            if a.mode == "plain":
                cur.execute(COUNT_SQL)
                rows = cur.fetchall()
            else:
                cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                if a.gap_ms:
                    time.sleep(a.gap_ms / 1000)
                probe = master_pos(cur) if a.mode == "pos" else snapshot_pos(cur)
                cur.execute(COUNT_SQL)
                rows = cur.fetchall()
                cur.execute("COMMIT")
            if a.settle_ms:
                time.sleep(a.settle_ms / 1000)
            high = master_pos(cur)
            f.write(json.dumps({"i": i, "mode": a.label or a.mode, "low": low, "high": high,
                                "probe": probe, "counts": {v: n for v, n in rows}}) + "\n")
    print(f"wrote {a.races} races to {a.out}")


if __name__ == "__main__":
    main()
