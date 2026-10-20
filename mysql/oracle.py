#!/usr/bin/env python3
"""Build the ground-truth oracle for MySQL from the binlog.

Reads every binlog event since the server started, groups row events by GTID,
and records each transaction's net change per value of caseb.t. Run after the
workload and probes have finished.

    python mysql/oracle.py --out oracle.json
"""
import argparse
import json
from collections import defaultdict

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="caseb")
    ap.add_argument("--password", default="caseb")
    ap.add_argument("--out", default="oracle.json")
    a = ap.parse_args()

    stream = BinLogStreamReader(
        connection_settings={"host": a.host, "port": a.port, "user": a.user, "passwd": a.password},
        server_id=4242, blocking=False, resume_stream=False,
        only_events=[GtidEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])

    txns, cur = [], None
    for ev in stream:
        if isinstance(ev, GtidEvent):
            cur = {"id": ev.gtid, "d": defaultdict(int)}
            txns.append(cur)
            continue
        if cur is None or ev.schema != "caseb" or ev.table != "t":
            continue
        for row in ev.rows:
            if isinstance(ev, WriteRowsEvent):
                cur["d"][row["values"]["v"]] += 1
            elif isinstance(ev, DeleteRowsEvent):
                cur["d"][row["values"]["v"]] -= 1
            else:
                cur["d"][row["before_values"]["v"]] -= 1
                cur["d"][row["after_values"]["v"]] += 1
    stream.close()

    for t in txns:
        t["d"] = {k: v for k, v in t["d"].items() if v}

    # Self-check: replaying every transaction must give the table as it is now
    # (run this only after the workload and probes have stopped). A mismatch
    # means the binlog did not cover the table from creation; see RESET step.
    final = defaultdict(int)
    for t in txns:
        for v, d in t["d"].items():
            final[v] += d
    conn = pymysql.connect(host=a.host, port=a.port, user=a.user, password=a.password,
                           database="caseb")
    with conn.cursor() as cur:
        cur.execute("SELECT v, COUNT(*) FROM t GROUP BY v")
        live = {v: n for v, n in cur.fetchall()}
    conn.close()
    bad = {v: (final.get(v, 0), live.get(v, 0)) for v in set(final) | set(live)
           if final.get(v, 0) != live.get(v, 0)}
    if bad:
        raise SystemExit(f"ORACLE INVALID: replay != live table for {bad} "
                         "(binlog not reset before setup, or writers still running)")

    json.dump({"engine": "mysql", "txns": txns}, open(a.out, "w"))
    touched = sum(1 for t in txns if t["d"])
    print(f"oracle: {len(txns)} transactions, {touched} changed caseb.t -> {a.out}")


if __name__ == "__main__":
    main()
