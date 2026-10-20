"""Shared helpers for the MariaDB half of the Case B test.

Positions are binary-log coordinates written as 18 digits: 6 for the log file
number, 12 for the byte position ("binlog.000003", 4567 -> "000003000000004567"),
so score.py can compare them as strings.
"""
import pymysql


def add_conn_args(ap):
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3308)
    ap.add_argument("--user", default="caseb")
    ap.add_argument("--password", default="caseb")


def connect(a, autocommit=False):
    return pymysql.connect(host=a.host, port=a.port, user=a.user, password=a.password,
                           database="caseb", autocommit=autocommit)


def norm_pos(log_file, pos):
    return "%06d%012d" % (int(str(log_file).rsplit(".", 1)[1]), int(pos))


def master_pos(cur):
    """Current end of the binary log (documented as the server's current position)."""
    cur.execute("SHOW MASTER STATUS")
    row = cur.fetchone()
    return norm_pos(row[0], row[1])


def snapshot_pos(cur):
    """Binlog_snapshot_file / Binlog_snapshot_position: documented to give the binary-log
    position that matches the current START TRANSACTION WITH CONSISTENT SNAPSHOT."""
    cur.execute("SHOW STATUS LIKE 'Binlog_snapshot_%'")
    st = {k.lower(): v for k, v in cur.fetchall()}
    return norm_pos(st["binlog_snapshot_file"], st["binlog_snapshot_position"])


COUNT_SQL = "SELECT v, COUNT(*) FROM t GROUP BY v"
