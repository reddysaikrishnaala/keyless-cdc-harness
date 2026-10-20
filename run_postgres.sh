#!/bin/bash
# Case B PostgreSQL test, start to finish, against a running PostgreSQL 16 server
# with wal_level = logical (default connection 127.0.0.1:5433, user postgres).
#   IDENTITY=default RUNS=1 bash run_postgres.sh   -> keyless table as is (no schema change)
#   IDENTITY=full    RUNS=3 bash run_postgres.sh   -> REPLICA IDENTITY FULL (a schema change under Case B rules)
# Results go to runs/pg-<identity>-run<N>/ (log: runs/pg-log.txt).
RUNS=${RUNS:-3}; IDENTITY=${IDENTITY:-full}
SECS=${SECS:-90}; CTL=${CTL:-1500}; PR=${PR:-2000}; PRGAP=${PRGAP:-1500}; MINR=${MINR:-1000}
export PGHOST=${PGHOST:-127.0.0.1} PGPORT=${PGPORT:-5433} PGUSER=${PGUSER:-postgres}

cd "$(dirname "$0")" || exit 1
if [ -x "$HOME/caseb-venv/bin/python" ]; then PY="$HOME/caseb-venv/bin/python"; else PY=python3; fi
mkdir -p runs
exec > >(tee -a runs/pg-log.txt) 2>&1
echo "===== $(date) : $RUNS run(s), identity=$IDENTITY, ${SECS}s each ====="
C=(--host "$PGHOST" --port "$PGPORT" --user "$PGUSER")

PIDS=()
for r in $(seq 1 "$RUNS"); do
  D=runs/pg-$IDENTITY-run$r; rm -rf "$D"; mkdir -p "$D"
  echo; echo "########## RUN $r ($IDENTITY) ##########"
  psql -d postgres -Atc "SELECT version()"
  psql -d postgres -qAtc "SELECT pg_drop_replication_slot('caseb') FROM pg_replication_slots WHERE slot_name='caseb'" >/dev/null
  psql -d postgres -qc "DROP DATABASE IF EXISTS caseb" -c "CREATE DATABASE caseb" || exit 1
  psql -d caseb -v ON_ERROR_STOP=1 -v identity="$IDENTITY" -Atf postgres/setup.sql || { echo "setup failed"; exit 1; }
  $PY postgres/workload.py "${C[@]}" --seconds "$SECS" --threads 8 \
    --control-races "$CTL" --control-out "$D/p_ctl.jsonl" > "$D/workload.log" 2>&1 & PIDS+=($!)
  sleep 3
  for m in plain pos1 pos2 set; do
    $PY postgres/probe.py "${C[@]}" --mode $m --races "$PR" --out "$D/p_$m.jsonl" & PIDS+=($!)
  done
  if [ "$r" = "3" ]; then
    $PY postgres/probe.py "${C[@]}" --mode pos1 --label pos1-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_pos1gap.jsonl" & PIDS+=($!)
    $PY postgres/probe.py "${C[@]}" --mode set --label set-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_setgap.jsonl" & PIDS+=($!)
  fi
  wait "${PIDS[@]}"; PIDS=()
  cat "$D/workload.log"
  if $PY postgres/oracle.py "${C[@]}" --out "$D/oracle.json" | tee "$D/oracle.txt"; [ "${PIPESTATUS[0]}" = "0" ]; then
    cat "$D"/p_*.jsonl > "$D/all.jsonl"
    $PY score.py --oracle "$D/oracle.json" --probes "$D/all.jsonl" --min-racy "$MINR" | tee "$D/score.txt"
  else
    echo "No oracle for this run, so no scoring (see oracle.txt)."
  fi
done
psql -d postgres -qAtc "SELECT pg_drop_replication_slot('caseb') FROM pg_replication_slots WHERE slot_name='caseb'" >/dev/null
echo; echo "DONE $(date). Results are in runs/"
