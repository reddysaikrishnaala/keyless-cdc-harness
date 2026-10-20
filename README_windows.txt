Case B kit - Windows SQL Server run
====================================
1. SQL Server 2022 Developer edition (free; NOT Express - it has no Agent, so no CDC).
   Patch to CU27 if you can (build 16.0.4305.5) so it matches the Mac run.
   During install: Mixed Mode authentication, set the sa password. Default instance, port 1433.
2. Services (services.msc): start "SQL Server Agent (MSSQLSERVER)" and set it to Automatic.
   SQL Server Configuration Manager > Network Configuration > Protocols > enable TCP/IP, restart SQL Server.
3. sqlcmd: comes with SQL Server tools; check with  sqlcmd -?  in PowerShell.
4. Python 3.13 from python.org, then in PowerShell:
     py -3.13 -m venv $HOME\caseb-venv
     & $HOME\caseb-venv\Scripts\python.exe -m pip install pymssql
5. Unzip this kit, open PowerShell in the caseb folder, then:
     powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password 'YOUR_SA_PASSWORD' -Runs linkage
     powershell -ExecutionPolicy Bypass -File .\run_sqlserver_windows.ps1 -Password 'YOUR_SA_PASSWORD' -Runs 3
   Use a dedicated test instance: each run drops and recreates a database named caseb.
6. Send back the whole runs\ folder (zip it).
