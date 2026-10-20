"""Shared helpers for the SQL Server half of the Case B test.

Positions are commit LSNs written as 20 lowercase hex digits, the same layout
as CDC's binary(10) __$start_lsn (4-byte VLF, 4-byte block, 2-byte slot), so
score.py can compare them as strings.
"""
import re
import sys

import pymssql

LSN_TEXT = re.compile(r"^([0-9A-Fa-f]{8}):([0-9A-Fa-f]{8}):([0-9A-Fa-f]{4})$")


def add_conn_args(ap):
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default="1433")
    ap.add_argument("--user", default="sa")
    ap.add_argument("--password", required=True)
    ap.add_argument("--lsn-source", choices=["dblog", "logstats"], default="dblog",
                    help="where the harness reads the current end of the log "
                         "(test instrumentation only, never part of a solution)")


def connect(a, autocommit=True):
    return pymssql.connect(server=a.host, port=str(a.port), user=a.user,
                           password=a.password, database="caseb", autocommit=autocommit)


def norm_text_lsn(s):
    """'0000002b:000001f0:0003' -> '0000002b000001f00003'. Fails loudly on any other format."""
    m = LSN_TEXT.match((s or "").strip())
    if not m:
        sys.exit(f"Unexpected LSN text format: {s!r}. Use --lsn-source dblog, "
                 "or extend norm_text_lsn() after checking the format by hand.")
    return "".join(m.groups()).lower()


def lsn_bin(b):
    """CDC binary(10) LSN -> 20 lowercase hex digits."""
    return bytes(b).hex()


def end_lsn(cur, source):
    """LSN of the last record in the caseb transaction log."""
    if source == "dblog":
        # Undocumented but long-standing; fine for a test harness.
        cur.execute("SELECT MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL)")
    else:
        cur.execute("SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID())")
    return norm_text_lsn(cur.fetchone()[0])


def check_lsn_format(cur, source):
    """Sanity check: CDC's highest captured LSN must not be after the log end."""
    e = end_lsn(cur, source)
    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    m = cur.fetchone()[0]
    if m is not None and lsn_bin(m) > e:
        sys.exit(f"LSN formats disagree: CDC max {lsn_bin(m)} > log end {e} ({source}).")
    return e
