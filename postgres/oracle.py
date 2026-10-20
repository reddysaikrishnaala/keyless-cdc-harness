#!/usr/bin/env python3
"""Ground-truth oracle for the PostgreSQL Case B test.

Reads the logical replication slot created by setup.sql (test_decoding, with
transaction ids), replays every committed transaction on public.t in commit
order, and writes {"engine": "postgres", "txns": [{"id": commit_lsn, "xid": n,
"d": {value: net change}}]}. It then checks the replay against the live table.

If a DELETE or UPDATE carries no old row data (replica identity DEFAULT on a
table without a primary key), the change stream cannot say which value was
removed, so no replay is possible: the oracle reports that and exits non-zero.
"""
import argparse
import json
import re
import sys
from collections import defaultdict

from common import add_conn_args, connect, norm_lsn

VAL = r"v\[character varying\]:'([^']*)'"
INS = re.compile(r"^table public\.t: INSERT: " + VAL + "$")
DEL = re.compile(r"^table public\.t: DELETE: " + VAL + "$")
UPD = re.compile(r"^table public\.t: UPDATE: old-key: " + VAL + " new-tuple: " + VAL + "$")


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--out", default="oracle.json")
    a = ap.parse_args()

    conn = connect(a)
    cur = conn.cursor()
    txns, cur_d, cur_xid = [], None, None
    blind = defaultdict(int)          # changes with no old row data
    while True:
        cur.execute("SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_changes("
                    "'caseb', NULL, 200000, 'include-xids', '1', 'skip-empty-xacts', '1')")
        rows = cur.fetchall()
        if not rows:
            break
        for lsn, xid, data in rows:
            if data.startswith("BEGIN"):
                cur_d, cur_xid = defaultdict(int), int(xid)
            elif data.startswith("COMMIT"):
                txns.append({"id": norm_lsn(lsn), "xid": cur_xid, "d": {k: v for k, v in cur_d.items() if v}})
                cur_d = None
            elif data.startswith("table public.t:"):
                if m := INS.match(data):
                    cur_d[m.group(1)] += 1
                elif m := DEL.match(data):
                    cur_d[m.group(1)] -= 1
                elif m := UPD.match(data):
                    cur_d[m.group(1)] -= 1
                    cur_d[m.group(2)] += 1
                elif "(no-tuple-data)" in data or ": UPDATE: v[" in data:
                    blind["DELETE" if "DELETE" in data else "UPDATE"] += 1
                else:
                    sys.exit(f"unparsed change: {data}")

    print(f"decoded {len(txns)} transactions")
    if blind:
        print(f"ORACLE IMPOSSIBLE: {blind['DELETE']} DELETEs and {blind['UPDATE']} UPDATEs carry no old row "
              "data (replica identity DEFAULT on a keyless table); the change stream cannot say which "
              "value they removed")
        sys.exit(2)
    ids = [t["id"] for t in txns]
    if ids != sorted(ids) or len(set(ids)) != len(ids):
        sys.exit("commit LSNs are not strictly increasing; scoring assumes they are")

    final = defaultdict(int)
    for t in txns:
        for v, d in t["d"].items():
            final[v] += d
    cur.execute("SELECT v, COUNT(*) FROM t GROUP BY v")
    live = {v: n for v, n in cur.fetchall()}
    replay = {v: n for v, n in final.items() if n}
    if replay != live:
        sys.exit(f"ORACLE INVALID: replay {replay} != live table {live}")
    print(f"oracle self-check passed: replay matches live table {live}")
    json.dump({"engine": "postgres", "txns": txns}, open(a.out, "w"))


if __name__ == "__main__":
    main()
