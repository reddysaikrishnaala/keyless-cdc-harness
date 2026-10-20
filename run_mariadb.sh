#!/bin/bash
# Case B MariaDB test, start to finish (same workload and settings as run_mysql.sh).
#   bash run_mariadb.sh          -> 3 runs (run 3 adds the 2 ms gap probes), MariaDB 11.4 in Docker
#   RUNS=1 bash run_mariadb.sh   -> 1 run
#   SNAPONLY=1 bash run_mariadb.sh -> 3 longer runs of only the snapshot-position probes
#                                     (no gap, 2 ms and 10 ms gaps; 5,000 races each), runs/mdbsnap-run<N>/
# To use an existing server instead of Docker (log-bin, binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL):
#   EXTERNAL=1 MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_ROOT_PASSWORD=... bash run_mariadb.sh
# Results go to runs/mdb-run<N>/ (log: runs/mdb-log.txt).
RUNS=${RUNS:-3}
if [ "$SNAPONLY" = "1" ]; then SECS=${SECS:-200}; PR=${PR:-5000}; fi
SECS=${SECS:-90}; CTL=${CTL:-1500}; PR=${PR:-2000}; PRGAP=${PRGAP:-1500}; MINR=${MINR:-1000}
IMG=${IMG:-mariadb:11.4}
HOST=${MYSQL_HOST:-127.0.0.1}; PORT=${MYSQL_PORT:-3308}; ROOTPW=${MYSQL_ROOT_PASSWORD:-casebroot}

cd "$(dirname "$0")" || exit 1
if [ -x "$HOME/caseb-venv/bin/python" ]; then PY="$HOME/caseb-venv/bin/python"; else PY=python3; fi
mkdir -p runs
exec > >(tee -a runs/mdb-log.txt) 2>&1
echo "===== $(date) : $RUNS run(s), ${SECS}s each ====="

if [ "$EXTERNAL" = "1" ]; then
  rootsql() { mariadb -h "$HOST" -P "$PORT" -u root -p"$ROOTPW" "$@"; }
else
  rootsql() { docker exec -i caseb-mariadb mariadb -u root -p"$ROOTPW" "$@"; }
fi

start_server() {
  [ "$EXTERNAL" = "1" ] && return 0
  docker rm -f caseb-mariadb >/dev/null 2>&1
  docker run -d --name caseb-mariadb -p "$PORT":3306 -e MARIADB_ROOT_PASSWORD="$ROOTPW" "$IMG" \
    --server-id=1 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL >/dev/null || exit 1
  echo "waiting for MariaDB to start..."
  for i in $(seq 1 60); do
    rootsql -e "SELECT 1" >/dev/null 2>&1 && break
    sleep 3
  done
  rootsql -e "SELECT 1" >/dev/null 2>&1 || { echo "MariaDB did not start"; docker logs caseb-mariadb | tail -20; exit 1; }
  sleep 5
}

setup() {
  rootsql -N -e "SELECT VERSION()"
  # The oracle replays the binary log from the start, so clear it before creating the table.
  rootsql -e "RESET MASTER" || exit 1
  rootsql < mariadb/setup.sql || { echo "setup.sql failed"; exit 1; }
}

C=(--host "$HOST" --port "$PORT")
PIDS=()
for r in $(seq 1 "$RUNS"); do
  D=runs/mdb${SNAPONLY:+snap}-run$r; rm -rf "$D"; mkdir -p "$D"
  echo; echo "########## RUN $r ##########"
  start_server; setup
  $PY mariadb/workload.py "${C[@]}" --seconds "$SECS" --threads 8 \
    --control-races "$CTL" --control-out "$D/p_ctl.jsonl" > "$D/workload.log" 2>&1 & PIDS+=($!)
  sleep 3
  if [ "$SNAPONLY" = "1" ]; then
    $PY mariadb/probe.py "${C[@]}" --mode snap --races "$PR" --out "$D/p_snap.jsonl" & PIDS+=($!)
    $PY mariadb/probe.py "${C[@]}" --mode snap --label snap-gap2 --gap-ms 2 --races "$PR" --out "$D/p_snapgap2.jsonl" & PIDS+=($!)
    $PY mariadb/probe.py "${C[@]}" --mode snap --label snap-gap10 --gap-ms 10 --races "$PR" --out "$D/p_snapgap10.jsonl" & PIDS+=($!)
  else
    $PY mariadb/probe.py "${C[@]}" --mode plain --races "$PR" --out "$D/p_plain.jsonl" & PIDS+=($!)
    $PY mariadb/probe.py "${C[@]}" --mode pos --races "$PR" --out "$D/p_pos.jsonl" & PIDS+=($!)
    $PY mariadb/probe.py "${C[@]}" --mode snap --races "$PR" --out "$D/p_snap.jsonl" & PIDS+=($!)
    if [ "$r" = "3" ]; then
      $PY mariadb/probe.py "${C[@]}" --mode pos --label pos-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_posgap.jsonl" & PIDS+=($!)
      $PY mariadb/probe.py "${C[@]}" --mode snap --label snap-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_snapgap.jsonl" & PIDS+=($!)
    fi
  fi
  wait "${PIDS[@]}"; PIDS=()
  cat "$D/workload.log"
  $PY mariadb/oracle.py "${C[@]}" --out "$D/oracle.json" || echo "ORACLE STEP FAILED"
  cat "$D"/p_*.jsonl > "$D/all.jsonl"
  $PY score.py --oracle "$D/oracle.json" --probes "$D/all.jsonl" --min-racy "$MINR" | tee "$D/score.txt"
done

[ "$EXTERNAL" = "1" ] || docker rm -f caseb-mariadb >/dev/null 2>&1
echo; echo "DONE $(date). Results are in runs/"
