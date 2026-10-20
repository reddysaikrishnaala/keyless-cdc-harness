#!/usr/bin/env python3
"""Score Case B probe runs against the ground-truth oracle.

Usage:
    python score.py --oracle oracle.json --probes probes.jsonl [--min-racy 1000]

The oracle lists every committed transaction on the test table in commit
order, with its net change per row value. The table starts empty and the log
covers it from creation, so the oracle knows the exact count of every value at
every log position.

For each probe race the reader recorded:
    low    - a log position read just before the chunk read
    high   - a log position read just after the chunk read
    counts - the chunk's count per row value
    probe  - (probe modes only) the position the probe claims the chunk saw

The reader must report the exact count of every value at `high`. Three
strategies are scored:
    assume_low  - pretend the chunk saw the table exactly at `low`
    assume_high - pretend the chunk saw the table exactly at `high`
    probe       - use the probe's position
A strategy is right for a race only if every value matches the oracle at
`high`. A race is "racy" when committed changes to the table landed between
`low` and `high`; only racy races can expose the ambiguity.

out_of_order_read counts reads whose snapshot saw exactly one commit out of
log commit order (seen on SQL Server under concurrent commits). On PostgreSQL,
reads with several commits out of WAL order are counted as multi_reorder_read;
there the documented snapshot ("set" probe) and the control check them exactly. No single log
position describes such a read, so any position-based probe can be off by one
there. inconsistent_read counts reads nothing explains: those invalidate a run.
"""
import argparse
import bisect
import json
import sys
from collections import defaultdict


# ---------- positions -> number of visible transactions (a commit-order prefix) ----------

def parse_gtid_set(s):
    """MySQL GTID set text -> {uuid: [(start, end), ...]} with merged intervals."""
    out = defaultdict(list)
    s = "".join(s.split())
    if s:
        for part in s.split(","):
            uuid, *ranges = part.split(":")
            for r in ranges:
                a, _, b = r.partition("-")
                out[uuid.lower()].append((int(a), int(b or a)))
    for u, iv in out.items():
        iv.sort()
        merged = [list(iv[0])]
        for a, b in iv[1:]:
            if a <= merged[-1][1] + 1:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        out[u] = [tuple(m) for m in merged]
    return out


class Positions:
    """Maps an engine position to k = how many oracle transactions (in commit order) it covers."""

    def __init__(self, oracle):
        self.engine = oracle["engine"]
        self.ids = [t["id"] for t in oracle["txns"]]
        if self.engine == "mysql":
            uuids = {i.rsplit(":", 1)[0].lower() for i in self.ids}
            if len(uuids) > 1:
                sys.exit("oracle has GTIDs from more than one server; use a single test server")
            self.uuid = uuids.pop() if uuids else None
            self.gnos = [int(i.rsplit(":", 1)[1]) for i in self.ids]
            if self.gnos != sorted(self.gnos):
                sys.exit("binlog commit order differs from GTID order; scoring assumes they match")

    def k(self, pos):
        if self.engine in ("sqlserver", "postgres", "mariadb"):
            return bisect.bisect_right(self.ids, pos)
        gs = parse_gtid_set(pos).get(self.uuid, [])
        if not gs:
            return 0
        # Visible set must be a prefix of commit order: one interval starting at or before the first GTID.
        first = self.gnos[0] if self.gnos else 1
        if gs[0][0] > first or any(a <= g <= b for a, b in gs[1:] for g in self.gnos):
            return None  # gap: visible set is not a prefix
        return bisect.bisect_right(self.gnos, gs[0][1])


# ---------- scoring ----------

def build_prefix_counts(txns):
    cum = [dict()]
    cur = defaultdict(int)
    for t in txns:
        for v, d in t["d"].items():
            cur[v] += d
        cum.append(dict(cur))
    return cum


def same(a, b):
    return all(a.get(k, 0) == b.get(k, 0) for k in set(a) | set(b))


def diff(a, b):
    """a - b per value."""
    return {k: a.get(k, 0) - b.get(k, 0) for k in set(a) | set(b)}


def plus(a, b):
    return {k: a.get(k, 0) + b.get(k, 0) for k in set(a) | set(b)}


def one_commit_reordered(cum, txns, kl, kh, c, reach=60):
    """True if c equals some state in [kl, kh] with exactly one commit moved:
    a later commit already visible, or an earlier one not yet visible."""
    for k in range(kl, kh + 1):
        for j in range(k, min(len(txns), k + reach)):
            if txns[j]["d"] and same(plus(cum[k], txns[j]["d"]), c):
                return True
        for j in range(max(0, k - reach), k):
            if txns[j]["d"] and same(diff(cum[k], txns[j]["d"]), c):
                return True
    return False


def parse_snapshot(s):
    """PostgreSQL pg_snapshot text 'xmin:xmax:xip,xip' -> (xmin, xmax, set of in-progress xids)."""
    xmin, xmax, xip = s.split(":")
    return int(xmin), int(xmax), {int(x) for x in xip.split(",") if x}


class SnapshotSets:
    """For a PostgreSQL snapshot, the oracle transactions (commit index < kh) it does NOT see."""

    def __init__(self, txns):
        self.by_xid = sorted((t["xid"], i) for i, t in enumerate(txns))
        self.xids = [x for x, _ in self.by_xid]
        self.maxxid, m = [0], 0
        for t in txns:
            m = max(m, t["xid"])
            self.maxxid.append(m)

    def invisible(self, snap, kh):
        xmin, xmax, xip = parse_snapshot(snap)
        out = []
        j = bisect.bisect_left(self.xids, xmin)
        while j < len(self.xids) and self.xids[j] <= self.maxxid[kh]:
            x, i = self.by_xid[j]
            if i < kh and (x >= xmax or x in xip):
                out.append(i)
            j += 1
        return out


def score(oracle, races):
    P = Positions(oracle)
    txns = oracle["txns"]
    cum = build_prefix_counts(txns)
    S = SnapshotSets(txns) if oracle["engine"] == "postgres" else None
    stats = defaultdict(lambda: defaultdict(int))
    examples = defaultdict(list)

    for r in races:
        st = stats[r["mode"]]
        st["races"] += 1
        kl, kh = P.k(r["low"]), P.k(r["high"])
        if kl is None or kh is None or kl > kh:
            st["bad_watermarks"] += 1
            continue
        c = {k: int(v) for k, v in r["counts"].items()}
        truth_high = cum[kh]
        racy = any(txns[i]["d"] for i in range(kl, kh))
        st["racy"] += racy

        # The chunk must equal the table at some point inside the window. On SQL
        # Server a snapshot can see one commit out of CDC commit-LSN order; such
        # reads are counted as out_of_order_read, not as harness errors.
        if not any(same(cum[k], c) for k in range(kl, kh + 1)):
            if one_commit_reordered(cum, txns, kl, kh, c):
                st["out_of_order_read"] += 1
            elif S is not None:
                # PostgreSQL: visibility follows the running-transaction set, not WAL
                # order, so several commits can be out of order. Counted, not fatal;
                # the "set" probe and the control check these reads exactly.
                st["multi_reorder_read"] += 1
            else:
                st["inconsistent_read"] += 1

        estimates = {
            "assume_low": plus(c, diff(cum[kh], cum[kl])),
            "assume_high": c,
        }
        if r.get("probe") is not None:
            kp = P.k(r["probe"])
            if kp is None:
                st["probe_unusable"] += 1
                kp = kl
            kp = min(max(kp, kl), kh)
            estimates["probe"] = plus(c, diff(cum[kh], cum[kp]))
        if r.get("snap") is not None and S is not None:
            # Documented snapshot set: add exactly the changes committed by high that it did not see.
            est = dict(c)
            for i in S.invisible(r["snap"], kh):
                est = plus(est, txns[i]["d"])
            estimates["set"] = est

        for name, est in estimates.items():
            ok = same(est, truth_high)
            if racy and not ok:
                st[f"{name}_wrong_racy"] += 1
                if len(examples[(r["mode"], name)]) < 2:
                    examples[(r["mode"], name)].append({
                        "race": r.get("i"), "chunk_counts": c,
                        "estimate": {k: v for k, v in est.items() if v or k in c},
                        "truth_at_high": truth_high})
    return stats, examples


def verdict(st, has_probe, min_racy, key="probe"):
    if st["bad_watermarks"] or st["inconsistent_read"]:
        return "INVALID RUN - fix watermarks or isolation before trusting any result"
    if st["racy"] < min_racy:
        return f"NOT ENOUGH RACES ({st['racy']} racy < {min_racy}) - run longer, add writers or --gap-ms"
    if not has_probe:
        if st["assume_low_wrong_racy"] and st["assume_high_wrong_racy"]:
            return "AMBIGUITY REPRODUCED - neither naive assumption is safe"
        return "AMBIGUITY NOT SHOWN - one naive assumption was always right; raise write load"
    if st[f"{key}_wrong_racy"] == 0 and st["probe_unusable"] == 0:
        return "PROBE PASSED EMPIRICALLY - still needs a documented guarantee to count"
    return "PROBE FAILED - it does not identify the position the chunk saw"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle", required=True)
    ap.add_argument("--probes", required=True)
    ap.add_argument("--min-racy", type=int, default=1000)
    a = ap.parse_args()
    oracle = json.load(open(a.oracle))
    races = [json.loads(l) for l in open(a.probes) if l.strip()]
    stats, examples = score(oracle, races)
    if not stats:
        sys.exit("no probe races found")
    for mode, st in stats.items():
        has_probe = any(r.get("probe") is not None for r in races if r["mode"] == mode)
        has_set = any(r.get("snap") is not None for r in races if r["mode"] == mode)
        print(f"\n=== mode: {mode} ===")
        keys = ["races", "racy", "bad_watermarks", "inconsistent_read", "out_of_order_read",
                "assume_low_wrong_racy", "assume_high_wrong_racy"]
        if oracle["engine"] == "postgres":
            keys.insert(5, "multi_reorder_read")
        if has_probe:
            keys += ["probe_wrong_racy", "probe_unusable"]
        if has_set:
            keys += ["set_wrong_racy"]
        for k in keys:
            print(f"  {k:24s} {st[k]}")
        if has_probe:
            print(f"  VERDICT (position): {verdict(st, True, a.min_racy)}")
        if has_set:
            print(f"  VERDICT (snapshot set): {verdict(st, True, a.min_racy, key='set')}")
        if not has_probe and not has_set:
            print(f"  VERDICT: {verdict(st, False, a.min_racy)}")
        for (m, name), ex in examples.items():
            if m == mode and (name in ("probe", "set") or not (has_probe or has_set)):
                print(f"  example ({name} wrong): {json.dumps(ex[0])}")


if __name__ == "__main__":
    main()
