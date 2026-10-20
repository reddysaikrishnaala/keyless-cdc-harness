#!/usr/bin/env python3
"""Build the ground-truth oracle for MariaDB from the binary log.

Reads every binlog event since RESET MASTER, groups row events into
transactions (MariaDB GTID event ... XID event), and records each
transaction's net change per value of caseb.t, keyed by the binary-log
position just after its commit. Run after the workload and probes finish.

    python mariadb/oracle.py --out oracle.json
"""
import argparse
import json
from collections import defaultdict

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import MariadbGtidEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from common import add_conn_args, connect, norm_pos


def val(values):
    """The value of column v. If the binlog lacks column names (binlog_row_metadata
    not FULL), the library labels columns by position; the table has only one."""
    if "v" in values:
        return values["v"]
    if len(values) != 1:
        raise SystemExit(f"cannot identify column v in {values}; set binlog_row_metadata=FULL")
    return next(iter(values.values()))


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--out", default="oracle.json")
    a = ap.parse_args()

    stream = BinLogStreamReader(
        connection_settings={"host": a.host, "port": a.port, "user": a.user, "passwd": a.password},
        server_id=4243, blocking=False, resume_stream=False, is_mariadb=True,
        only_events=[MariadbGtidEvent, XidEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent])

    txns, cur = [], None
    for ev in stream:
        if isinstance(ev, MariadbGtidEvent):
            cur = defaultdict(int)
            continue
        if isinstance(ev, XidEvent):
            if cur is not None:
                txns.append({"id": norm_pos(stream.log_file, ev.packet.log_pos),
                             "d": {k: v for k, v in cur.items() if v}})
            cur = None
            continue
        if cur is None or ev.schema != "caseb" or ev.table != "t":
            continue
        for row in ev.rows:
            if isinstance(ev, WriteRowsEvent):
                cur[val(row["values"])] += 1
            elif isinstance(ev, DeleteRowsEvent):
                cur[val(row["values"])] -= 1
            else:
                cur[val(row["before_values"])] -= 1
                cur[val(row["after_values"])] += 1
    stream.close()

    ids = [t["id"] for t in txns]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        raise SystemExit("commit positions are not strictly increasing; scoring assumes they are")

    final = defaultdict(int)
    for t in txns:
        for v, d in t["d"].items():
            final[v] += d
    conn = connect(a)
    with conn.cursor() as c:
        c.execute("SELECT v, COUNT(*) FROM t GROUP BY v")
        live = {v: n for v, n in c.fetchall()}
    conn.close()
    bad = {v: (final.get(v, 0), live.get(v, 0)) for v in set(final) | set(live)
           if final.get(v, 0) != live.get(v, 0)}
    if bad:
        raise SystemExit(f"ORACLE INVALID: replay != live table for {bad} "
                         "(binary log not reset before setup, or writers still running)")

    json.dump({"engine": "mariadb", "txns": txns}, open(a.out, "w"))
    touched = sum(1 for t in txns if t["d"])
    print(f"oracle: {len(txns)} transactions, {touched} changed caseb.t -> {a.out}")


if __name__ == "__main__":
    main()
