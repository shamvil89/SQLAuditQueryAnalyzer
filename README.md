# SQL Server Audit AI Analyzer

Offline, CPU-only analyzer for SQL Server audit CSV files. It identifies write operations against user tables and ignores temporary tables whose names start with `#`.

The analyzer is intended for work servers where audit data must stay local. It does not call any cloud AI service and does not require a GPU.

## What It Finds

Flagged user-table write operations:

```sql
DELETE FROM dbo.Customers WHERE CustomerId = 10;
INSERT INTO sales.Orders (Id, Amount) VALUES (1, 99.00);
UPDATE hr.Employee SET Salary = 100 WHERE EmployeeId = 5;
MERGE dbo.Inventory AS target USING dbo.Stage AS source ON target.Id = source.Id;
TRUNCATE TABLE dbo.StageOrders;
SELECT * INTO dbo.ArchiveOrders FROM dbo.Orders;
```

Ignored examples:

```sql
SELECT * FROM dbo.Customers;
INSERT INTO #Temp VALUES (1);
UPDATE ##Work SET Value = 2;
DELETE FROM tempdb..#AuditScratch;
GRANT UPDATE ON dbo.Customer TO app_role;
REVOKE DELETE ON dbo.Customer FROM cleanup_role;
```

Permission changes such as `GRANT`, `REVOKE`, and `DENY` are included in training as non-write examples. They are not treated as table data writes.

## Project Files

- `audit_ai/analyze_sql_audit.py` - command-line analyzer and bundled lightweight model training examples
- `install_dependencies.ps1` - dependency bootstrap script for new Windows servers
- `deploy.ps1` - one-time deployment and validation script
- `requirements.txt` - future Python package dependency list
- `models/sql_write_model.json` - generated local model file
- `samples/sample_audit.csv` - sample audit input
- `tests/test_analyze_sql_audit.py` - verification tests
- `SKILL.md` - detailed operating guidance for the AI analyst

## Fresh Server Setup

From the cloned repo folder, run:

```powershell
powershell -ExecutionPolicy Bypass -File ".\deploy.ps1"
```

`deploy.ps1` will:

- Check and install dependencies
- Create `.venv`
- Recreate `.venv` if it was copied from another server or points to a missing Python path
- Train the bundled offline model
- Run tests
- Analyze every `.csv` file in `samples`
- Create validation output in `out` for each sample CSV

Validation output files look like:

```text
out\findings_deployment_check_<sample_name>_<timestamp>.csv
out\summary_deployment_check_<sample_name>_<timestamp>.json
```

## Dependency Installer

Run this only if you want to bootstrap dependencies separately:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1"
```

If Python 3.10+ is missing, the script silently installs Python 3.12 for the current user, updates PATH, sets Python environment variables, creates `.venv`, and installs packages from `requirements.txt` if any are listed.

Current project dependency note: the analyzer uses only the Python standard library today, so `requirements.txt` is intentionally empty except for comments.

## Machine-Wide Python Install

For all-users Python installation, open PowerShell as Administrator:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -InstallScope Machine
```

This updates machine-level PATH and environment variables.

## No-Internet Server Setup

Download the Python Windows installer on another machine, copy it to the server, then run:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -PythonInstallerPath C:\Installers\python-3.12.10-amd64.exe
```

Then deploy:

```powershell
powershell -ExecutionPolicy Bypass -File ".\deploy.ps1"
```

## Analyze A CSV

After deployment:

```powershell
.\.venv\Scripts\python.exe .\audit_ai\analyze_sql_audit.py .\audit.csv --out-dir .\out
```

Output files:

```text
out\findings_<timestamp>.csv
out\summary_<timestamp>.json
```

For larger files, the analyzer prints progress every 500 rows by default:

```powershell
.\.venv\Scripts\python.exe .\audit_ai\analyze_sql_audit.py .\audit.csv --out-dir .\out --progress-every 250
```

To disable progress messages:

```powershell
.\.venv\Scripts\python.exe .\audit_ai\analyze_sql_audit.py .\audit.csv --out-dir .\out --no-progress
```

## Explicit Column Mapping

Use explicit mapping when your CSV headers are unusual:

```powershell
.\.venv\Scripts\python.exe .\audit_ai\analyze_sql_audit.py .\audit.csv `
  --query-column TextData `
  --user-column LoginName `
  --datetime-column EventTime `
  --server-column ServerName `
  --database-column DatabaseName `
  --out-dir .\out
```

The analyzer can auto-detect common headers such as:

- Query text: `query`, `statement`, `sql_text`, `command`, `tsql`, `textdata`
- Username: `username`, `user`, `login`, `loginname`, `principal`
- Date/time: `datetime`, `event_time`, `timestamp`, `start_time`
- Server: `servername`, `server`, `host`
- Database: `database`, `database_name`, `dbname`

## Output Columns

`findings_<timestamp>.csv` includes:

- `source_row`
- `username`
- `event_datetime`
- `server_name`
- `database_name`
- `operation`
- `target_table`
- `confidence`
- `reason`
- `query`

`summary_<timestamp>.json` includes total rows, total findings, and counts by operation, user, database, and server.

## Retrain Model

Training examples are bundled in `audit_ai/analyze_sql_audit.py`. To regenerate the local model:

```powershell
.\.venv\Scripts\python.exe .\audit_ai\analyze_sql_audit.py --train-model
```

The model stays local at:

```text
models\sql_write_model.json
```

## Run Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

## Troubleshooting

If PowerShell blocks scripts:

```powershell
powershell -ExecutionPolicy Bypass -File ".\deploy.ps1"
```

If machine-level installation fails, rerun PowerShell as Administrator or use user-scope install:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -InstallScope User
```

If an old output file is open in Excel, rerun the analyzer. New runs use timestamped output filenames, so they should not overwrite locked files.

If Python installation appears stuck, leave it for a few minutes. The dependency script now prints progress every 15 seconds and writes the installer log here:

```text
.deps\python-install.log
```

To change the installer timeout:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -InstallTimeoutSeconds 1200
```

To print the installer log while it runs:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -PrintInstallerLog
```

To control how many log lines are printed:

```powershell
powershell -ExecutionPolicy Bypass -File ".\install_dependencies.ps1" -PrintInstallerLog -InstallerLogTailLines 150
```

## Important Notes

This tool is for audit triage. It gives an explainable finding with operation, target table, confidence score, and original query text. High-risk findings, especially broad `DELETE`, `UPDATE`, `MERGE`, and `TRUNCATE` operations, should still be reviewed manually.
