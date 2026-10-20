#!/bin/bash
# Case B MySQL test, start to finish. Reproduces the settings behind the paper's Table I.
#   bash run_mysql.sh          -> 3 runs (run 3 adds the 2 ms gap probes), MySQL 8.0 in Docker
#   RUNS=1 bash run_mysql.sh   -> 1 run
# To use an existing server instead of Docker (it must run with the settings in mysql/setup.sql):
#   EXTERNAL=1 MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_ROOT_PASSWORD=... bash run_mysql.sh
# The paper's runs used MySQL 8.0.46. Results go to runs/my-run<N>/ (log: runs/my-log.txt).
RUNS=${RUNS:-3}
SECS=${SECS:-90}; CTL=${CTL:-1500}; PR=${PR:-2000}; PRGAP=${PRGAP:-1500}; MINR=${MINR:-1000}
IMG=${IMG:-mysql:8.0}
HOST=${MYSQL_HOST:-127.0.0.1}; PORT=${MYSQL_PORT:-3307}; ROOTPW=${MYSQL_ROOT_PASSWORD:-casebroot}

cd "$(dirname "$0")" || exit 1
if [ -x "$HOME/caseb-venv/bin/python" ]; then PY="$HOME/caseb-venv/bin/python"; else PY=python3; fi
mkdir -p runs
exec > >(tee -a runs/my-log.txt) 2>&1
echo "===== $(date) : $RUNS run(s), ${SECS}s each ====="

if [ "$EXTERNAL" = "1" ]; then
  rootsql() { mysql -h "$HOST" -P "$PORT" -u root -p"$ROOTPW" "$@"; }
else
  rootsql() { docker exec -i caseb-mysql mysql -u root -p"$ROOTPW" "$@"; }
fi

start_server() {
  [ "$EXTERNAL" = "1" ] && return 0
  docker rm -f caseb-mysql >/dev/null 2>&1
  docker run -d --name caseb-mysql -p "$PORT":3306 -e MYSQL_ROOT_PASSWORD="$ROOTPW" "$IMG" \
    --server-id=1 --log-bin=binlog --gtid-mode=ON --enforce-gtid-consistency=ON \
    --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL >/dev/null || exit 1
  echo "waiting for MySQL to start..."
  for i in $(seq 1 60); do
    rootsql -e "SELECT 1" >/dev/null 2>&1 && break
    sleep 3
  done
  rootsql -e "SELECT 1" >/dev/null 2>&1 || { echo "MySQL did not start"; docker logs caseb-mysql | tail -20; exit 1; }
  sleep 5
}

setup() {
  rootsql -N -e "SELECT VERSION()"
  # The oracle replays the binlog from the start, so clear it before creating the table.
  if rootsql -N -e "SELECT VERSION()" 2>/dev/null | grep -q '^8\.0\.'; then
    rootsql -e "RESET MASTER" || exit 1
  else
    rootsql -e "RESET BINARY LOGS AND GTIDS" || exit 1
  fi
  rootsql < mysql/setup.sql || { echo "setup.sql failed"; exit 1; }
}

C=(--host "$HOST" --port "$PORT")
PIDS=()
for r in $(seq 1 "$RUNS"); do
  D=runs/my-run$r; rm -rf "$D"; mkdir -p "$D"
  echo; echo "########## RUN $r ##########"
  start_server; setup
  $PY mysql/workload.py "${C[@]}" --seconds "$SECS" --threads 8 \
    --control-races "$CTL" --control-out "$D/p_ctl.jsonl" > "$D/workload.log" 2>&1 & PIDS+=($!)
  sleep 3
  $PY mysql/probe.py "${C[@]}" --mode plain --races "$PR" --out "$D/p_plain.jsonl" & PIDS+=($!)
  $PY mysql/probe.py "${C[@]}" --mode m1 --races "$PR" --out "$D/p_m1.jsonl" & PIDS+=($!)
  $PY mysql/probe.py "${C[@]}" --mode m2 --races "$PR" --out "$D/p_m2.jsonl" & PIDS+=($!)
  if [ "$r" = "3" ]; then
    $PY mysql/probe.py "${C[@]}" --mode m1 --label m1-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_m1gap.jsonl" & PIDS+=($!)
    $PY mysql/probe.py "${C[@]}" --mode m2 --label m2-gap2 --gap-ms 2 --races "$PRGAP" --out "$D/p_m2gap.jsonl" & PIDS+=($!)
  fi
  wait "${PIDS[@]}"; PIDS=()
  cat "$D/workload.log"
  $PY mysql/oracle.py "${C[@]}" --out "$D/oracle.json" || echo "ORACLE STEP FAILED"
  cat "$D"/p_*.jsonl > "$D/all.jsonl"
  $PY score.py --oracle "$D/oracle.json" --probes "$D/all.jsonl" --min-racy "$MINR" | tee "$D/score.txt"
done

[ "$EXTERNAL" = "1" ] || docker rm -f caseb-mysql >/dev/null 2>&1
echo; echo "DONE $(date). Results are in runs/"
