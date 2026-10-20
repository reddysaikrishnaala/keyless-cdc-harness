#!/usr/bin/env python3
"""Concurrent writers for the Case B test on SQL Server.

Same workload as mysql/workload.py: 20 identical copies of each value, then
single-copy inserts, deletes and updates (some two per transaction) until
--seconds elapse. CDC records every change, so the oracle can replay them.

    python sqlserver/workload.py --password '<sa password>' --seconds 120 --threads 8

Positive control (--control-races N): a reader thread records N races, mode
"ctl". For each race it reads the low watermark while writers run, pauses the
writers between transactions, waits until none has a transaction open, reads
the log end as the claimed position, takes the snapshot-isolation count,
resumes the writers and reads the high watermark. With no writer
mid-transaction the claimed position is exact by construction, so score.py
must report 0 probe errors for "ctl"; otherwise the harness is broken.

    python sqlserver/workload.py --password '...' --seconds 120 --control-races 1500 --control-out ctl.jsonl
"""
import argparse
import json
import random
import threading
import time

import pymssql

from common import add_conn_args, check_lsn_format, connect, end_lsn

VALUES = ["A", "B", "C", "D", "E"]


class Gate:
    """Lets the control thread pause writers between transactions."""

    def __init__(self):
        self.cv = threading.Condition()
        self.open = True
        self.active = 0

    def enter(self):
        with self.cv:
            while not self.open:
                self.cv.wait()
            self.active += 1

    def leave(self):
        with self.cv:
            self.active -= 1
            self.cv.notify_all()

    def close_and_drain(self):
        with self.cv:
            self.open = False
            while self.active:
                self.cv.wait()

    def reopen(self):
        with self.cv:
            self.open = True
            self.cv.notify_all()


def one_op(cur, rnd):
    op = rnd.random()
    v = rnd.choice(VALUES)
    if op < 0.4:
        cur.execute("INSERT INTO dbo.t (v) VALUES (%s)", (v,))
    elif op < 0.75:
        cur.execute("DELETE TOP (1) FROM dbo.t WHERE v = %s", (v,))          # removes ONE copy
    else:
        w = rnd.choice([x for x in VALUES if x != v])
        cur.execute("UPDATE TOP (1) dbo.t SET v = %s WHERE v = %s", (w, v))  # moves ONE copy


def writer(a, stop, gate, counter, seed):
    rnd = random.Random(seed)
    conn = connect(a, autocommit=False)
    cur = conn.cursor()
    n = 0
    while not stop.is_set():
        gate.enter()
        try:
            for _ in range(2 if rnd.random() < 0.2 else 1):
                one_op(cur, rnd)
            conn.commit()
            n += 1
        except pymssql.Error:
            conn.rollback()  # deadlock victim (1205) or similar: retry
        finally:
            gate.leave()
    counter.append(n)
    conn.close()


def control(a, gate, done):
    conn = connect(a)
    cur = conn.cursor()
    with open(a.control_out, "a") as f:
        for i in range(a.control_races):
            low = end_lsn(cur, a.lsn_source)
            time.sleep(a.settle_ms / 1000)
            gate.close_and_drain()                   # no writer mid-transaction now
            try:
                probe = end_lsn(cur, a.lsn_source)
                cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
                cur.execute("BEGIN TRAN")
                cur.execute("SELECT v, COUNT(*) FROM dbo.t GROUP BY v")  # snapshot starts here
                rows = cur.fetchall()
                cur.execute("COMMIT")
            finally:
                gate.reopen()
            time.sleep(a.settle_ms / 1000)
            high = end_lsn(cur, a.lsn_source)
            f.write(json.dumps({"i": i, "mode": "ctl", "low": low, "high": high,
                                "probe": probe, "counts": {v: n for v, n in rows}}) + "\n")
    conn.close()
    done.set()


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--skip-load", action="store_true")
    ap.add_argument("--control-races", type=int, default=0)
    ap.add_argument("--control-out", default="ctl.jsonl")
    ap.add_argument("--settle-ms", type=float, default=20.0)
    a = ap.parse_args()

    conn = connect(a, autocommit=False)
    cur = conn.cursor()
    check_lsn_format(cur, a.lsn_source)
    if not a.skip_load:
        for v in VALUES:
            cur.executemany("INSERT INTO dbo.t (v) VALUES (%s)", [(v,)] * 20)
            conn.commit()
    conn.close()

    stop, gate, counter = threading.Event(), Gate(), []
    ts = [threading.Thread(target=writer, args=(a, stop, gate, counter, i)) for i in range(a.threads)]
    for t in ts:
        t.start()
    done = threading.Event()
    if a.control_races:
        threading.Thread(target=control, args=(a, gate, done), daemon=True).start()
    time.sleep(a.seconds)
    if a.control_races and not done.is_set():
        print("WARNING: control races did not finish within --seconds; "
              "races after the writers stop are not racy")
        done.wait()
    stop.set()
    for t in ts:
        t.join()
    print(f"committed {sum(counter)} writer transactions")
    if a.control_races:
        print(f"wrote {a.control_races} control races to {a.control_out}")


if __name__ == "__main__":
    main()
