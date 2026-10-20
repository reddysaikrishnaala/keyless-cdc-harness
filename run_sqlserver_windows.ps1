# Case B SQL Server test on a local Windows SQL Server 2022 instance.
# Same Python scripts, same settings and same order as run_sqlserver.sh (the Docker version on the Mac).
# Only the driver changes: there is no container, so each run drops and recreates the caseb database.
#
#   powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password '...'              # shakedown: 1 run + identifier check
#   powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password '...' -Runs 3      # full: 3 runs + identifier check
#   powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password '...' -Runs linkage  # identifier check only
#
# Needs: SQL Server 2022 Developer (not Express - Express has no Agent, so no CDC), SQL Server Agent running,
# SQL authentication enabled, a sysadmin login (default sa), sqlcmd on PATH, Python venv at $HOME\caseb-venv.
param(
  [Parameter(Mandatory = $true)][string]$Password,
  [string]$Runs = "1",
  [string]$Server = "127.0.0.1",
  [string]$Port = "1433",
  [string]$User = "sa"
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Py = Join-Path $HOME "caseb-venv\Scripts\python.exe"
if (-not (Test-Path $Py)) { Write-Host "Python environment $HOME\caseb-venv not found"; exit 1 }
New-Item -ItemType Directory -Force -Path runs | Out-Null
Start-Transcript -Append -Path "runs\ms-win-log.txt" | Out-Null
Write-Host "===== $(Get-Date) : $Runs run(s) ====="

if ($Runs -eq "1") { $SECS = 300; $CTL = 600; $PR = 800; $MINR = 500 }
else { $SECS = 420; $CTL = 1200; $PR = 1500; $MINR = 1000 }

$Conn = @("--host", $Server, "--port", $Port, "--user", $User, "--password", $Password)

# sqlcmd: newer versions need -C to trust the local self-signed certificate; older ones reject -C.
if (-not (Get-Command sqlcmd -ErrorAction SilentlyContinue)) { Write-Host "sqlcmd not found on PATH"; exit 1 }
$SqlTrust = @("-C")
try { & sqlcmd -S "$Server,$Port" -U $User -P $Password -C -h -1 -Q "SELECT 1" *> $null } catch { }
if ($LASTEXITCODE -ne 0) { $SqlTrust = @() }
function Sq([string[]]$a) {
  & sqlcmd -S "$Server,$Port" -U $User -P $Password @SqlTrust @a
  if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed: $a" }
}
function SqVal([string]$q) { ((Sq @("-h", "-1", "-W", "-Q", "SET NOCOUNT ON; $q")) -join "").Trim() }

# Quote one argument for Start-Process (it joins arguments with spaces).
function Q([string]$s) { '"' + ($s -replace '"', '\"') + '"' }
function StartPy([string[]]$a, [string]$log) {
  $argline = ($a | ForEach-Object { Q $_ }) -join " "
  if ($log) {
    Start-Process -FilePath $Py -ArgumentList $argline -NoNewWindow -PassThru `
      -RedirectStandardOutput $log -RedirectStandardError "$log.err"
  } else {
    Start-Process -FilePath $Py -ArgumentList $argline -NoNewWindow -PassThru
  }
}

function Check-Server {
  Write-Host (SqVal "SELECT @@VERSION")
  $eng = SqVal "SELECT CAST(SERVERPROPERTY('EngineEdition') AS int)"
  if ($eng -eq "4") { Write-Host "This is SQL Server Express: no Agent, so CDC cannot run. Use Developer edition."; exit 1 }
  Write-Host "waiting for SQL Server Agent (needed by CDC)..."
  for ($i = 0; $i -lt 60; $i++) {
    $st = SqVal "SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'"
    if ($st -eq "Running") { Start-Sleep 15; Write-Host "Agent running"; return }
    Start-Sleep 5
  }
  Write-Host "SQL Server Agent is not running (status: $st). Start it in services.msc or SQL Server Configuration Manager."; exit 1
}

# Stands in for the fresh container: drop caseb (and its CDC jobs) so setup.sql starts from nothing.
function Reset-Db {
  $exists = SqVal "SELECT COUNT(*) FROM sys.databases WHERE name = 'caseb'"
  if ($exists -ne "0") {
    Write-Host "dropping existing caseb database"
    Sq @("-b", "-Q", "IF EXISTS (SELECT 1 FROM sys.databases WHERE name='caseb' AND is_cdc_enabled=1) EXEC caseb.sys.sp_cdc_disable_db; ALTER DATABASE caseb SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE caseb;") | Out-Host
  }
}

function Setup {
  Reset-Db
  Sq @("-b", "-i", "sqlserver\setup.sql") | Out-Host
  Write-Host "--- CDC jobs ---"; Sq @("-d", "caseb", "-Q", "EXEC sys.sp_cdc_help_jobs") | Out-Host
  Start-Sleep 5
}

Check-Server

if ($Runs -eq "linkage") {
  Write-Host ""; Write-Host "########## IDENTIFIER CHECK ONLY ##########"
  Setup
  Sq @("-d", "caseb", "-Q", "SET NOCOUNT ON; INSERT INTO dbo.t (v) SELECT TOP (200) CHAR(65 + ABS(CHECKSUM(NEWID())) % 4) FROM sys.all_objects") | Out-Host
  Start-Sleep 10
  & $Py sqlserver\linkage.py @Conn | Tee-Object -FilePath "runs\win-linkage-observer.txt"
  Write-Host ""; Write-Host "DONE $(Get-Date). Result: runs\win-linkage-observer.txt"
  Stop-Transcript | Out-Null
  exit 0
}

for ($r = 1; $r -le [int]$Runs; $r++) {
  $D = "runs\win-run$r"
  if (Test-Path $D) { Remove-Item -Recurse -Force $D }
  New-Item -ItemType Directory -Force -Path $D | Out-Null
  Write-Host ""; Write-Host "########## RUN $r ##########"
  Setup
  $procs = @()
  $procs += StartPy (@("sqlserver\workload.py") + $Conn + @("--seconds", "$SECS", "--threads", "8",
      "--control-races", "$CTL", "--control-out", "$D\p_ctl.jsonl")) "$D\workload.log"
  Start-Sleep 3
  foreach ($m in @("plain", "s1", "s2")) {
    $procs += StartPy (@("sqlserver\probe.py") + $Conn + @("--mode", $m, "--races", "$PR", "--out", "$D\p_$m.jsonl")) $null
  }
  if ($r -eq 3) {
    $procs += StartPy (@("sqlserver\probe.py") + $Conn + @("--mode", "s1", "--label", "s1-gap2", "--gap-ms", "2",
        "--races", "$PR", "--out", "$D\p_s1gap.jsonl")) $null
  }
  foreach ($p in $procs) { $p.WaitForExit() }
  Get-Content "$D\workload.log", "$D\workload.log.err" -ErrorAction SilentlyContinue | Out-Host
  & $Py sqlserver\oracle.py @Conn --out "$D\oracle.json"
  if ($LASTEXITCODE -ne 0) { Write-Host "ORACLE STEP FAILED" }
  # Concatenate with Python so the file stays plain UTF-8 without a byte-order mark.
  & $Py -c "import glob,sys; d=sys.argv[1]; open(d+'/all.jsonl','w').write(''.join(open(f).read() for f in sorted(glob.glob(d+'/p_*.jsonl'))))" $D
  & $Py score.py --oracle "$D\oracle.json" --probes "$D\all.jsonl" --min-racy $MINR | Tee-Object -FilePath "$D\score.txt"
}

Write-Host ""; Write-Host "########## IDENTIFIER CHECK ##########"
& $Py sqlserver\linkage.py @Conn | Tee-Object -FilePath "runs\win-linkage.txt"
Write-Host ""; Write-Host "DONE $(Get-Date). Results are in runs\ (log: runs\ms-win-log.txt)."
Stop-Transcript | Out-Null
