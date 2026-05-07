# SQL Server Audit Write Operation Analyst

## Purpose

Use this skill to analyze SQL Server audit CSV exports offline and identify write operations against user tables. The analyst focuses on operations such as `DELETE`, `INSERT`, `UPDATE`, `MERGE`, and `TRUNCATE`, while ignoring SQL Server temporary tables whose table names begin with `#`.

The workflow is designed for work servers that only have CPU and RAM. It does not require a GPU, cloud calls, or an internet connection at runtime.

## What The AI Should Detect

Flag a row when the audited SQL statement performs a write operation against a non-temporary user table.

Included write operations:

- `DELETE FROM dbo.TableName`
- `INSERT INTO dbo.TableName (...)`
- `UPDATE dbo.TableName SET ...`
- `MERGE dbo.TableName AS target ...`
- `TRUNCATE TABLE dbo.TableName`
- `SELECT ... INTO dbo.TableName ...`

Ignored operations:

- Writes to local temporary tables, for example `#Temp`, `[ #Temp ]`, or `tempdb..#Temp`
- Writes to global temporary tables, for example `##Temp`
- Reads only, for example `SELECT * FROM dbo.Users`
- Metadata statements and maintenance statements unless they contain a supported write pattern

## Offline AI Model Strategy

Use a hybrid offline approach:

1. A deterministic SQL normalizer strips comments and string literals so SQL keywords inside comments or quoted text do not trigger false positives.
2. A lightweight local classifier scores the statement using bundled examples. The classifier is CPU-only, uses simple token and character features, and is stored locally after first training.
3. A rule-based extractor identifies the write operation and target table name for explainability.
4. A final policy layer ignores any target table whose base name starts with `#`.

This approach keeps resource usage low and makes every finding auditable. The AI score is supporting evidence; the extracted operation and target table are the primary explanation.

## Expected CSV Inputs

The CSV can contain any column names, but the analyzer works best when columns resemble:

- Query text: `query`, `statement`, `sql_text`, `command`, `tsql`, `textdata`
- Username: `username`, `user`, `login`, `loginname`, `principal`
- Date/time: `datetime`, `event_time`, `timestamp`, `start_time`
- Server: `servername`, `server`, `host`
- Database: `database`, `database_name`, `dbname`

If names are different, pass explicit column mappings with command-line options.

## Output

The analyzer writes:

- A finding CSV with every suspicious write operation
- A JSON summary with totals, counts by operation, users, databases, and servers

Each finding should include:

- Source row number
- Username
- Event datetime
- Server name
- Database name
- Operation
- Target table
- AI confidence score
- Reason
- Original query text

## Recommended Workflow

1. Deploy once with `deploy.ps1`.
2. Copy SQL Server audit CSV exports into a local folder.
3. Run the analyzer against each CSV.
4. Review findings by highest confidence and by risky operations such as `DELETE`, `TRUNCATE`, and broad `UPDATE`.
5. Preserve the original CSV and output files together for audit traceability.

## Analyst Rules

- Always keep processing offline.
- Never send audit rows, usernames, queries, or server names to a remote service.
- Prefer explicit column mappings when CSV headers are ambiguous.
- Treat the output as triage evidence, not a final compliance verdict.
- Review high-risk findings manually, especially wide writes with no obvious `WHERE` clause.
- Record the analyzer version and command used when sharing results.

## Edge Cases To Watch

- Dynamic SQL such as `EXEC('DELETE FROM dbo.Users')` may be detected if the literal contains the statement, but nested construction can be ambiguous.
- Stored procedure calls are not expanded unless the audit row includes the procedure body or statement text.
- Multi-statement batches may produce one finding per detected write target.
- SQL dialect quirks can require local tuning. Add known local patterns to the bundled examples or tests before relying on them operationally.

