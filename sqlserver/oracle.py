#!/usr/bin/env python3
"""Build the ground-truth oracle for SQL Server from CDC.

Waits for the capture job to go quiet, reads every change to dbo.t since CDC
was enabled (the table was empty then), groups changes by commit LSN and
records each transaction's net change per value. Run after the workload and
probes have stopped.

    python sqlserver/oracle.py --password '...' --out oracle.json
"""
import argparse
import json
import time
from collections import defaultdict

from common import add_conn_args, connect, lsn_bin

CHANGES_SQL = """
SELECT __$start_lsn, __$seqval, __$operation, v
FROM cdc.fn_cdc_get_all_changes_dbo_t(
       sys.fn_cdc_get_min_lsn('dbo_t'), sys.fn_cdc_get_max_lsn(), N'all update old')
ORDER BY __$start_lsn, __$seqval, __$operation
"""
SIGN = {1: -1, 2: +1, 3: -1, 4: +1}   # delete, insert, update (before), update (after)


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--out", default="oracle.json")
    ap.add_argument("--quiet-polls", type=int, default=3)
    a = ap.parse_args()

    conn = connect(a)
    cur = conn.cursor()

    # 1. Wait until the capture job has caught up: max captured LSN unchanged
    #    for --quiet-polls polls, 3 seconds apart, after the writers stopped.
    last, same = None, 0
    while same < a.quiet_polls:
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        m = cur.fetchone()[0]
        same = same + 1 if m == last else 0
        last = m
        time.sleep(3)

    # 2. Read and group the changes.
    cur.execute(CHANGES_SQL)
    txns, idx = [], {}
    for start_lsn, _seq, op, v in cur.fetchall():
        k = lsn_bin(start_lsn)
        if k not in idx:
            idx[k] = len(txns)
            txns.append({"id": k, "d": defaultdict(int)})
        txns[idx[k]]["d"][v] += SIGN[op]
    for t in txns:
        t["d"] = {k: v for k, v in t["d"].items() if v}

    # 3. Self-check: replay must equal the live table.
    final = defaultdict(int)
    for t in txns:
        for v, d in t["d"].items():
            final[v] += d
    cur.execute("SELECT v, COUNT(*) FROM dbo.t GROUP BY v")
    live = {v: n for v, n in cur.fetchall()}
    bad = {v: (final.get(v, 0), live.get(v, 0)) for v in set(final) | set(live)
           if final.get(v, 0) != live.get(v, 0)}
    if bad:
        raise SystemExit(f"ORACLE INVALID: replay != live table for {bad} "
                         "(CDC not enabled before the load, capture not finished, "
                         "or writers still running)")

    json.dump({"engine": "sqlserver", "txns": txns}, open(a.out, "w"))
    print(f"oracle: {len(txns)} transactions changed dbo.t -> {a.out}")


if __name__ == "__main__":
    main()
