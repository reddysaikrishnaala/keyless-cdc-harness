"""Shared helpers for the PostgreSQL half of the Case B test.

Positions are WAL LSNs written as 16 lowercase hex digits ("0/191BA18" ->
"000000000191ba18"), so score.py can compare them as strings.
"""
import psycopg


def add_conn_args(ap):
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5433)
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--password", default="")
    ap.add_argument("--dbname", default="caseb")


def connect(a, autocommit=True):
    return psycopg.connect(host=a.host, port=a.port, user=a.user, password=a.password or None,
                           dbname=a.dbname, autocommit=autocommit)


def norm_lsn(s):
    hi, lo = str(s).split("/")
    return "%08x%08x" % (int(hi, 16), int(lo, 16))


def wal_lsn(cur):
    cur.execute("SELECT pg_current_wal_insert_lsn()")
    return norm_lsn(cur.fetchone()[0])


COUNT_SQL = "SELECT v, COUNT(*) FROM t GROUP BY v"
