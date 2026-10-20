#!/bin/bash
# Case B SQL Server test, start to finish, in Docker on your Mac.
#   bash ~/caseb/run_sqlserver.sh       -> shakedown: 1 short run + identifier check (~15 min)
#   bash ~/caseb/run_sqlserver.sh 3     -> full: 3 runs (run 3 adds the 2 ms gap probe) + check (~40 min)
#   bash ~/caseb/run_sqlserver.sh linkage -> identifier check only, with the observer session (~10 min)
# Everything is saved under ~/caseb/runs/ (log: runs/ms-log.txt).
RUNS=${1:-1}
PW='Caseb#2026!'
IMG=mcr.microsoft.com/mssql/server:2022-latest
if [ "$RUNS" = "1" ]; then SECS=300; CTL=600; PR=800; MINR=500; else SECS=420; CTL=1200; PR=1500; MINR=1000; fi

cd "$(dirname "$0")" || exit 1
source "$HOME/caseb-venv/bin/activate" || { echo "Python environment ~/caseb-venv not found"; exit 1; }
mkdir -p runs
exec > >(tee -a runs/ms-log.txt) 2>&1
echo "===== $(date) : $RUNS run(s) ====="

SQLCMD=""
sq() { docker exec caseb-mssql $SQLCMD -S localhost -U sa -P "$PW" "$@"; }

start_server() {
  docker rm -f caseb-mssql >/dev/null 2>&1
  docker run -d --platform linux/amd64 --name caseb-mssql -p 1433:1433 -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$PW" \
    -e MSSQL_AGENT_ENABLED=true -e MSSQL_PID=Developer "$IMG" >/dev/null || exit 1
  if docker exec caseb-mssql test -x /opt/mssql-tools18/bin/sqlcmd; then
    SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C"; else SQLCMD="/opt/mssql-tools/bin/sqlcmd"; fi
  echo "waiting for SQL Server to start..."
  for i in $(seq 1 100); do
    sq -Q "SELECT 1" >/dev/null 2>&1 && { sq -h -1 -Q "SET NOCOUNT ON; SELECT @@VERSION"; return 0; }
    sleep 3
  done
  echo "SQL Server did not start. Last log lines:"; docker logs caseb-mssql 2>&1 | tail -30; exit 1
}

wait_agent() {
  echo "waiting for SQL Server Agent (needed by CDC)..."
  for i in $(seq 1 60); do
    st=$(sq -h -1 -W -Q "SET NOCOUNT ON; SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'" 2>/dev/null | tr -d '[:space:]')
    ag=$(sq -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name LIKE 'SQLAgent%'" 2>/dev/null | tr -d '[:space:]')
    if [ "$st" = "Running" ] || { [ -n "$ag" ] && [ "$ag" != "0" ]; }; then sleep 15; echo "Agent running"; return 0; fi
    sleep 5
  done
  echo "SQL Server Agent did not start (status: $st). Check MSSQL_AGENT_ENABLED."; exit 1
}

setup() {
  wait_agent
  # Stream the file in, so the container's own user creates and owns /tmp/setup.sql.
  docker exec -i caseb-mssql sh -c 'cat > /tmp/setup.sql' < sqlserver/setup.sql || { echo "could not copy setup.sql"; exit 1; }
  sq -b -i /tmp/setup.sql || { echo "setup.sql failed"; exit 1; }
  echo "--- CDC jobs ---"; sq -d caseb -Q "EXEC sys.sp_cdc_help_jobs"
  sleep 5
}

if [ "$RUNS" = "linkage" ]; then
  echo; echo "########## IDENTIFIER CHECK ONLY ##########"
  start_server; setup
  sq -d caseb -Q "SET NOCOUNT ON; INSERT INTO dbo.t (v) SELECT TOP (200) CHAR(65 + ABS(CHECKSUM(NEWID())) % 4) FROM sys.all_objects" || exit 1
  sleep 10
  python sqlserver/linkage.py --password "$PW" | tee runs/ms-linkage-observer.txt
  docker rm -f caseb-mssql >/dev/null 2>&1
  echo; echo "DONE $(date). Result: ~/caseb/runs/ms-linkage-observer.txt - tell Claude it finished."
  exit 0
fi

for r in $(seq 1 "$RUNS"); do
  D=runs/ms-run$r; rm -rf "$D"; mkdir -p "$D"
  echo; echo "########## RUN $r ##########"
  start_server; setup
  python sqlserver/workload.py --password "$PW" --seconds $SECS --threads 8 \
    --control-races $CTL --control-out $D/p_ctl.jsonl > $D/workload.log 2>&1 &
  sleep 3
  python sqlserver/probe.py --password "$PW" --mode plain --races $PR --out $D/p_plain.jsonl &
  python sqlserver/probe.py --password "$PW" --mode s1 --races $PR --out $D/p_s1.jsonl &
  python sqlserver/probe.py --password "$PW" --mode s2 --races $PR --out $D/p_s2.jsonl &
  if [ "$r" = "3" ]; then
    python sqlserver/probe.py --password "$PW" --mode s1 --label s1-gap2 --gap-ms 2 --races $PR --out $D/p_s1gap.jsonl &
  fi
  wait
  cat $D/workload.log
  python sqlserver/oracle.py --password "$PW" --out $D/oracle.json || echo "ORACLE STEP FAILED"
  cat $D/p_*.jsonl > $D/all.jsonl
  python score.py --oracle $D/oracle.json --probes $D/all.jsonl --min-racy $MINR | tee $D/score.txt
done

echo; echo "########## IDENTIFIER CHECK ##########"
python sqlserver/linkage.py --password "$PW" | tee runs/ms-linkage.txt
docker rm -f caseb-mssql >/dev/null 2>&1
echo; echo "DONE $(date). Results are in ~/caseb/runs - tell Claude it finished."
