#!/usr/bin/env python3
"""Case B identifier inventory for SQL Server, automated (same steps as linkage.sql).

Window A (writer) deletes one row and stays open. Window B (reader) opens a
SNAPSHOT transaction while A is open. A commits, then CDC and the log are read.
Prints every identifier from both sides and which ones are equal. This only
gathers facts; it does not test or propose a replication method.

    python sqlserver/linkage.py --password '...'
"""
import argparse
import time

from common import add_conn_args, connect


def rows(cur, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def show(title, rs):
    print(f"\n--- {title} ---")
    if not rs:
        print("(no rows)")
    for r in rs:
        print({k: (v.hex() if isinstance(v, (bytes, bytearray)) else v)
               for k, v in r.items() if v not in (None, "")})


def forms(v):
    """Every plain text form of a value, for equality checks across sources."""
    out = set()
    if v is None:
        return out
    if isinstance(v, (bytes, bytearray)):
        h = bytes(v).hex()
        out |= {h, h.lstrip("0") or "0"}
        try:
            out.add(str(int(h, 16)))
        except ValueError:
            pass
    else:
        s = str(v).strip()
        out.add(s.lower())
        if s.isdigit():
            out |= {s, format(int(s), "x")}
            if len(s) >= 16:   # decimal-form LSN: VLF * 10^15 + block * 10^5 + slot
                n = int(s)
                h = "%08x%08x%04x" % (n // 10**15, (n // 10**5) % 10**10, n % 10**5)
                out |= {h, h.lstrip("0") or "0"}
        if ":" in s:
            out.add(s.replace(":", "").lower().lstrip("0") or "0")
    return {x for x in out if x not in ("", "0")}


def run_once(a, n):
    conn_a = connect(a, autocommit=False)            # writer, implicit transaction
    A = conn_a.cursor()
    B = connect(a).cursor()                           # reader

    A.execute("DELETE TOP (1) FROM dbo.t")
    a_cur = rows(A, "SELECT transaction_id, transaction_sequence_num, transaction_is_snapshot "
                    "FROM sys.dm_tran_current_transaction")
    a_db = rows(A, "SELECT database_transaction_begin_lsn, database_transaction_last_lsn "
                   "FROM sys.dm_tran_database_transactions "
                   "WHERE transaction_id = CURRENT_TRANSACTION_ID() AND database_id = DB_ID()")
    a_log = rows(A, "SELECT [Transaction ID], [Current LSN], Operation FROM sys.fn_dblog(NULL, NULL) "
                    "WHERE Operation = 'LOP_BEGIN_XACT' AND SPID = @@SPID")
    a_xid = a_log[-1]["Transaction ID"] if a_log else None

    B.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
    B.execute("BEGIN TRAN")
    B.execute("SELECT COUNT(*) FROM dbo.t")          # snapshot starts here
    B.fetchall()
    b_cur = rows(B, "SELECT transaction_id, transaction_sequence_num, first_snapshot_sequence_num, "
                    "last_transaction_sequence_num FROM sys.dm_tran_current_transaction")
    b_snap_all = rows(B, "SELECT * FROM sys.dm_tran_transactions_snapshot")
    b_cursnap = rows(B, "SELECT * FROM sys.dm_tran_current_snapshot")   # inside B's snapshot
    b_active = rows(B, "SELECT * FROM sys.dm_tran_active_snapshot_database_transactions")
    b_spid = rows(B, "SELECT @@SPID AS spid")[0]["spid"]

    # Observer: a third session looks at the same views while B's snapshot and A are both open.
    C = connect(a).cursor()
    o_sess = rows(C, f"SELECT session_id, transaction_isolation_level, open_transaction_count "
                     f"FROM sys.dm_exec_sessions WHERE session_id IN ({b_spid}, @@SPID)")
    o_snap = rows(C, "SELECT * FROM sys.dm_tran_transactions_snapshot")
    o_active = rows(C, "SELECT * FROM sys.dm_tran_active_snapshot_database_transactions")
    B.execute("SELECT COUNT(*) FROM dbo.t")          # second read in B, then look again
    B.fetchall()
    b_cursnap2 = rows(B, "SELECT * FROM sys.dm_tran_current_snapshot")
    o_snap2 = rows(C, "SELECT * FROM sys.dm_tran_transactions_snapshot")
    C.connection.close()

    conn_a.commit()
    B.execute("COMMIT")
    time.sleep(8)                                    # let CDC capture A

    c_map = rows(B, "SELECT TOP (3) start_lsn, tran_id, tran_begin_time, tran_end_time "
                    "FROM cdc.lsn_time_mapping ORDER BY start_lsn DESC")
    c_ct = rows(B, "SELECT TOP (3) __$start_lsn, __$seqval, __$operation, v "
                   "FROM cdc.dbo_t_CT ORDER BY __$start_lsn DESC")
    c_log = rows(B, f"SELECT * FROM sys.fn_dblog(NULL, NULL) WHERE [Transaction ID] = '{a_xid}'") if a_xid else []

    print(f"\n================ SAMPLE {n} ================")
    print("=== READER-VISIBLE (window B) ===")
    show("B own snapshot: sys.dm_tran_current_transaction", b_cur)
    show("B own snapshot: sys.dm_tran_transactions_snapshot (all rows)", b_snap_all)
    show("B own snapshot: sys.dm_tran_current_snapshot", b_cursnap)
    show("B own snapshot: sys.dm_tran_current_snapshot after B's second read", b_cursnap2)
    show("B server-wide view: sys.dm_tran_active_snapshot_database_transactions", b_active)
    print(f"\n=== OBSERVER (separate session, while B's snapshot is open; B is session {b_spid}) ===")
    show("O: sys.dm_exec_sessions (isolation 5 = SNAPSHOT)", o_sess)
    show("O: sys.dm_tran_transactions_snapshot", o_snap)
    show("O: sys.dm_tran_active_snapshot_database_transactions", o_active)
    show("O: sys.dm_tran_transactions_snapshot after a second read in B", o_snap2)
    print("\n=== WRITER, seen live (window A) ===")
    show("A: sys.dm_tran_current_transaction", a_cur)
    show("A: sys.dm_tran_database_transactions", a_db)
    show("A: fn_dblog LOP_BEGIN_XACT", a_log[-1:] if a_log else [])
    print("\n=== LOG SIDE after commit (window C) ===")
    show("C: cdc.lsn_time_mapping (newest 3)", c_map)
    show("C: cdc.dbo_t_CT (newest 3)", c_ct)
    show(f"C: fn_dblog rows for Transaction ID {a_xid}", c_log)

    # Writer identifiers vs (1) the reader's own snapshot metadata, (2) the
    # server-wide view the reader can query, (3) the log side.
    a_ids = {}
    for r in a_cur + a_db:
        for k, v in r.items():
            if k != "transaction_is_snapshot" and v is not None:
                a_ids[f"A.{k}"] = forms(v)
    b_own_txn = {str(b_cur[0]["transaction_id"]), str(b_cur[0]["transaction_sequence_num"])} if b_cur else set()
    own = set()
    for r in b_cur:
        for k in ("first_snapshot_sequence_num", "last_transaction_sequence_num"):
            own |= forms(r.get(k))
    b_xsn = b_cur[0]["transaction_sequence_num"] if b_cur else None
    for r in b_snap_all + o_snap + o_snap2:
        if r.get("transaction_sequence_num") == b_xsn:
            own |= forms(r.get("snapshot_sequence_num"))
    for r in b_cursnap + b_cursnap2:
        own |= forms(r.get("transaction_sequence_num"))
    server = set()
    for r in b_active:
        for k in ("transaction_id", "transaction_sequence_num", "commit_sequence_num"):
            v = r.get(k)
            if v is not None and str(v) not in b_own_txn:
                server |= forms(v)
    log_vals = {}
    for name, rs in (("cdc.lsn_time_mapping", c_map[:1]), ("cdc.change_table", c_ct[:1]),
                     ("fn_dblog(undocumented)", c_log)):
        for r in rs:
            for k, v in r.items():
                for f in forms(v):
                    log_vals.setdefault(f, set()).add(f"{name}.{k}")

    print("\n=== SNAPSHOT VIEW DIAGNOSIS ===")
    print(f"rows seen by B itself={len(b_snap_all)}; by observer={len(o_snap)}; "
          f"by observer after B's second read={len(o_snap2)}")
    b_rows = [r for r in o_snap + o_snap2 if r.get("transaction_sequence_num") == b_xsn]
    a_xsn = a_cur[0]["transaction_sequence_num"] if a_cur else None
    print(f"rows for B's snapshot (XSN {b_xsn})={len(b_rows)}; "
          f"lists writer A's XSN {a_xsn}={any(r.get('snapshot_sequence_num') == a_xsn for r in b_rows)}")
    cs = sorted({r.get("transaction_sequence_num") for r in b_cursnap + b_cursnap2})
    print(f"dm_tran_current_snapshot inside B: rows={len(b_cursnap)} (after 2nd read {len(b_cursnap2)}); "
          f"XSNs={cs}; lists writer A's XSN {a_xsn}={a_xsn in cs}")
    print("\n=== MATCHES ===")
    for k, fs in a_ids.items():
        in_log = sorted({src for f in fs for src in log_vals.get(f, ())})
        print(f"{k}: in B's own snapshot metadata={bool(fs & own)}; "
              f"in server-wide view={bool(fs & server)}; on log side={in_log or 'no'}")


def config(a):
    cur = connect(a).cursor()
    show("Server and database settings", rows(cur,
        "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(32)) AS version, "
        "CAST(SERVERPROPERTY('ProductUpdateLevel') AS varchar(16)) AS cu, "
        "d.snapshot_isolation_state_desc, d.is_read_committed_snapshot_on, d.is_cdc_enabled, "
        "HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS view_server_state, "
        "HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE') AS view_server_perf_state "
        "FROM sys.databases d WHERE d.name = DB_NAME()"))
    show("Host platform: sys.dm_os_host_info", rows(cur,
        "SELECT host_platform, host_distribution, host_release, host_service_pack_level, "
        "CAST(SERVERPROPERTY('Edition') AS varchar(64)) AS edition, "
        "CAST(SERVERPROPERTY('ProductUpdateReference') AS varchar(32)) AS kb FROM sys.dm_os_host_info"))
    cur.connection.close()


def main():
    ap = argparse.ArgumentParser()
    add_conn_args(ap)
    ap.add_argument("--repeat", type=int, default=3)
    a = ap.parse_args()
    config(a)
    for n in range(1, a.repeat + 1):
        run_once(a, n)
    print("\nThese are facts about identifiers only. A writer identifier links the two sides when it is")
    print("visible to the reader AND appears on the log side; whether a link holds under load is untested.")


if __name__ == "__main__":
    main()
