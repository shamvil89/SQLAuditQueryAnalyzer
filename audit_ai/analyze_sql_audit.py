#!/usr/bin/env python
"""Offline SQL Server audit CSV analyzer.

The model is intentionally small: it trains from bundled examples and stores
token weights in JSON. Rules extract the operation/table for explainability.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = ROOT / "models" / "sql_write_model.json"


QUERY_COLUMNS = ("query", "statement", "sql_text", "sqltext", "command", "tsql", "textdata", "sql")
USER_COLUMNS = ("username", "user", "login", "loginname", "principal", "server_principal_name")
DATETIME_COLUMNS = ("datetime", "event_time", "timestamp", "start_time", "eventtime", "date")
SERVER_COLUMNS = ("servername", "server", "host", "server_name")
DATABASE_COLUMNS = ("database", "database_name", "dbname", "database_name")


TRAINING_EXAMPLES: Sequence[Tuple[str, int]] = (
    ("DELETE FROM dbo.Customers WHERE CustomerId = 42", 1),
    ("delete c from sales.Customer c where c.Id = 1", 1),
    ("INSERT INTO dbo.Orders (Id, Amount) VALUES (1, 10)", 1),
    ("insert sales.OrderLine select * from dbo.StageOrderLine", 1),
    ("UPDATE hr.Employee SET Title = 'Lead' WHERE EmployeeId = 9", 1),
    ("update dbo.Account set Balance = Balance - 1 where AccountId = 4", 1),
    ("MERGE dbo.Inventory AS target USING dbo.InventoryStage AS source ON target.Id = source.Id WHEN MATCHED THEN UPDATE SET Qty = source.Qty", 1),
    ("TRUNCATE TABLE dbo.LoadStage", 1),
    ("SELECT * INTO dbo.ArchiveOrders FROM dbo.Orders WHERE OrderDate < '2020-01-01'", 1),
    ("SELECT * FROM dbo.Customers WHERE CustomerId = 42", 0),
    ("EXEC dbo.GetCustomers @CustomerId = 42", 0),
    ("CREATE INDEX IX_Customer_Name ON dbo.Customer(Name)", 0),
    ("ALTER TABLE dbo.Customer ADD Notes nvarchar(100)", 0),
    ("INSERT INTO #Temp VALUES (1)", 0),
    ("UPDATE ##Scratch SET Value = 2", 0),
    ("DELETE FROM tempdb..#AuditScratch WHERE Id = 1", 0),
    ("SELECT 'DELETE FROM dbo.Customer' AS ExampleText", 0),
    ("-- UPDATE dbo.Customer SET Name = 'x'\nSELECT 1", 0),
    ("MERGE INTO dbo.Customer AS target USING dbo.CustomerStage AS source ON target.CustomerId = source.CustomerId WHEN MATCHED THEN UPDATE SET Name = source.Name WHEN NOT MATCHED THEN INSERT (CustomerId, Name) VALUES (source.CustomerId, source.Name);", 1),
    ("MERGE sales.OrderTotals target USING sales.OrderTotalsStage source ON target.OrderId = source.OrderId WHEN MATCHED THEN UPDATE SET target.Total = source.Total;", 1),
    ("MERGE dbo.Inventory WITH (HOLDLOCK) AS t USING dbo.InventoryDelta AS s ON t.Sku = s.Sku WHEN MATCHED THEN UPDATE SET Qty = s.Qty WHEN NOT MATCHED BY TARGET THEN INSERT (Sku, Qty) VALUES (s.Sku, s.Qty);", 1),
    ("MERGE [warehouse].[FactSales] AS tgt USING [stage].[FactSales] AS src ON tgt.SaleId = src.SaleId WHEN NOT MATCHED THEN INSERT (SaleId, Amount) VALUES (src.SaleId, src.Amount);", 1),
    ("MERGE INTO audit.LoginRollup AS target USING audit.LoginRollupInput AS source ON target.LoginName = source.LoginName WHEN MATCHED THEN UPDATE SET LastSeen = source.LastSeen;", 1),
    ("MERGE dbo.AccountBalance AS target USING (SELECT AccountId, SUM(Amount) AS Amount FROM dbo.TransactionStage GROUP BY AccountId) AS source ON target.AccountId = source.AccountId WHEN MATCHED THEN UPDATE SET Balance = source.Amount;", 1),
    ("MERGE reporting.MonthlyRevenue target USING #MonthlyRevenue source ON target.MonthKey = source.MonthKey WHEN MATCHED THEN UPDATE SET Revenue = source.Revenue;", 1),
    ("MERGE dbo.Product AS target USING dbo.ProductLoad AS source ON target.ProductCode = source.ProductCode WHEN NOT MATCHED BY SOURCE THEN UPDATE SET IsActive = 0;", 1),
    ("MERGE dbo.PriceList target USING dbo.PriceListImport source ON target.PriceListId = source.PriceListId WHEN MATCHED AND target.Price <> source.Price THEN UPDATE SET Price = source.Price;", 1),
    ("MERGE crm.Contact AS tgt USING crm.ContactImport AS src ON tgt.Email = src.Email WHEN NOT MATCHED BY TARGET THEN INSERT (Email, FullName, CreatedAt) VALUES (src.Email, src.FullName, SYSUTCDATETIME());", 1),
    ("WITH stale AS (SELECT CustomerId FROM dbo.Customer WHERE IsDeleted = 1) DELETE FROM dbo.CustomerArchive WHERE CustomerId IN (SELECT CustomerId FROM stale);", 1),
    ("WITH ranked AS (SELECT Id, ROW_NUMBER() OVER (PARTITION BY Email ORDER BY CreatedAt DESC) AS rn FROM dbo.Customer) DELETE FROM dbo.Customer WHERE Id IN (SELECT Id FROM ranked WHERE rn > 1);", 1),
    ("WITH recent_orders AS (SELECT OrderId FROM sales.Orders WHERE OrderDate >= DATEADD(day, -30, SYSUTCDATETIME())) UPDATE sales.Orders SET Reviewed = 1 WHERE OrderId IN (SELECT OrderId FROM recent_orders);", 1),
    ("WITH source_rows AS (SELECT * FROM stage.EmployeeImport WHERE BatchId = 77) INSERT INTO hr.Employee (EmployeeId, FullName) SELECT EmployeeId, FullName FROM source_rows;", 1),
    ("WITH bad_rows AS (SELECT InvoiceId FROM dbo.Invoice WHERE Status = 'Error') DELETE FROM dbo.InvoiceLine WHERE InvoiceId IN (SELECT InvoiceId FROM bad_rows);", 1),
    ("WITH totals AS (SELECT AccountId, SUM(Amount) AS TotalAmount FROM dbo.Payment GROUP BY AccountId) UPDATE dbo.Account SET LastPaymentTotal = totals.TotalAmount FROM dbo.Account INNER JOIN totals ON dbo.Account.AccountId = totals.AccountId;", 1),
    ("WITH payload AS (SELECT CAST(@json AS nvarchar(max)) AS JsonBody) INSERT INTO dbo.ApiPayloadLog (JsonBody, CreatedAt) SELECT JsonBody, SYSUTCDATETIME() FROM payload;", 1),
    ("WITH expired AS (SELECT SessionId FROM dbo.UserSession WHERE ExpiresAt < SYSUTCDATETIME()) DELETE FROM dbo.UserSession WHERE SessionId IN (SELECT SessionId FROM expired);", 1),
    ("WITH cte AS (SELECT Id FROM dbo.JobQueue WHERE Status = 'Ready') UPDATE dbo.JobQueue SET Status = 'Running' WHERE Id IN (SELECT TOP (10) Id FROM cte ORDER BY Id);", 1),
    ("WITH source AS (SELECT CustomerId, Segment FROM stage.CustomerSegment) MERGE dbo.CustomerSegment AS target USING source ON target.CustomerId = source.CustomerId WHEN MATCHED THEN UPDATE SET Segment = source.Segment;", 1),
    ("UPDATE dbo.Customer SET LastLoginAt = (SELECT MAX(LoginAt) FROM audit.LoginEvent WHERE audit.LoginEvent.CustomerId = dbo.Customer.CustomerId) WHERE EXISTS (SELECT 1 FROM audit.LoginEvent WHERE audit.LoginEvent.CustomerId = dbo.Customer.CustomerId);", 1),
    ("DELETE FROM dbo.CartItem WHERE CartId IN (SELECT CartId FROM dbo.Cart WHERE UpdatedAt < DATEADD(day, -90, GETDATE()));", 1),
    ("INSERT INTO dbo.SecurityEvent (UserName, EventName, EventTime) SELECT UserName, 'PasswordChanged', SYSUTCDATETIME() FROM dbo.Users WHERE PasswordChangedAt > @lastRun;", 1),
    ("UPDATE sales.Invoice SET Status = 'Paid' WHERE InvoiceId IN (SELECT InvoiceId FROM sales.Payment WHERE ClearedAt IS NOT NULL);", 1),
    ("DELETE FROM audit.RawEvent WHERE EventId IN (SELECT EventId FROM audit.RawEvent WHERE CreatedAt < DATEADD(month, -13, SYSUTCDATETIME()));", 1),
    ("INSERT INTO reporting.CustomerSnapshot (CustomerId, SnapshotDate, Balance) SELECT c.CustomerId, CONVERT(date, GETDATE()), a.Balance FROM dbo.Customer c JOIN dbo.Account a ON c.CustomerId = a.CustomerId;", 1),
    ("UPDATE dbo.Subscription SET RenewalDate = DATEADD(year, 1, RenewalDate) WHERE SubscriptionId IN (SELECT SubscriptionId FROM dbo.Payment WHERE PaymentStatus = 'Settled');", 1),
    ("DELETE FROM dbo.Notification WHERE NotificationId IN (SELECT TOP (500) NotificationId FROM dbo.Notification WHERE IsRead = 1 ORDER BY CreatedAt);", 1),
    ("INSERT INTO dbo.ExportQueue (EntityName, EntityId) SELECT 'Order', OrderId FROM sales.Orders WHERE Status = 'ReadyForExport';", 1),
    ("UPDATE dbo.Device SET LastHeartbeatAt = h.LastSeen FROM dbo.Device d INNER JOIN stage.DeviceHeartbeat h ON d.DeviceKey = h.DeviceKey;", 1),
    ("BEGIN TRANSACTION; UPDATE dbo.Account SET Balance = Balance - 100 WHERE AccountId = 10; UPDATE dbo.Account SET Balance = Balance + 100 WHERE AccountId = 20; COMMIT;", 1),
    ("BEGIN TRY INSERT INTO dbo.AuditTrail (ActionName, ActorName) VALUES ('ManualAdjustment', SYSTEM_USER); END TRY BEGIN CATCH INSERT INTO dbo.ErrorLog (ErrorNumber, ErrorMessage) VALUES (ERROR_NUMBER(), ERROR_MESSAGE()); END CATCH;", 1),
    ("IF EXISTS (SELECT 1 FROM dbo.Customer WHERE CustomerId = @CustomerId) UPDATE dbo.Customer SET ModifiedAt = SYSUTCDATETIME() WHERE CustomerId = @CustomerId ELSE INSERT INTO dbo.Customer (CustomerId, CreatedAt) VALUES (@CustomerId, SYSUTCDATETIME());", 1),
    ("WHILE EXISTS (SELECT 1 FROM dbo.WorkQueue WHERE Status = 'Done') DELETE TOP (1000) FROM dbo.WorkQueue WHERE Status = 'Done';", 1),
    ("UPDATE TOP (100) dbo.EmailQueue SET Status = 'Sending' WHERE Status = 'Pending' AND NextAttemptAt <= SYSUTCDATETIME();", 1),
    ("DELETE TOP (250) FROM dbo.EventBuffer WHERE CreatedAt < DATEADD(hour, -6, SYSUTCDATETIME());", 1),
    ("INSERT dbo.ImportError (BatchId, RowNumber, ErrorText) SELECT BatchId, RowNumber, ErrorText FROM stage.ImportError WHERE BatchId = @BatchId;", 1),
    ("TRUNCATE TABLE stage.CustomerImport;", 1),
    ("TRUNCATE TABLE [stage].[OrderImport];", 1),
    ("SELECT CustomerId, Name, Email INTO dbo.CustomerBackup FROM dbo.Customer WHERE IsActive = 1;", 1),
    ("SELECT * INTO reporting.OrderAuditSnapshot FROM sales.Orders WHERE ModifiedAt >= DATEADD(day, -1, SYSUTCDATETIME());", 1),
    ("INSERT INTO dbo.CustomerNote (CustomerId, NoteText) VALUES (@CustomerId, @NoteText);", 1),
    ("INSERT INTO [dbo].[LedgerEntry] ([AccountId], [Amount], [CreatedAt]) VALUES (@AccountId, @Amount, SYSUTCDATETIME());", 1),
    ("UPDATE [dbo].[LedgerEntry] SET [ReversedAt] = SYSUTCDATETIME() WHERE [LedgerEntryId] = @LedgerEntryId;", 1),
    ("DELETE FROM [dbo].[LedgerEntry] WHERE [LedgerEntryId] = @LedgerEntryId;", 1),
    ("INSERT INTO dbo.JsonAudit (Payload) SELECT value FROM OPENJSON(@payload);", 1),
    ("UPDATE dbo.Profile SET PreferencesJson = JSON_MODIFY(PreferencesJson, '$.theme', 'dark') WHERE UserId = @UserId;", 1),
    ("DELETE FROM dbo.ProfileToken WHERE TokenHash = HASHBYTES('SHA2_256', @token);", 1),
    ("INSERT INTO dbo.BatchResult (BatchId, SuccessCount, FailureCount) SELECT @BatchId, SUM(CASE WHEN Status = 'S' THEN 1 ELSE 0 END), SUM(CASE WHEN Status = 'F' THEN 1 ELSE 0 END) FROM stage.BatchRow;", 1),
    ("UPDATE dbo.Customer SET RiskScore = r.Score FROM dbo.Customer c CROSS APPLY dbo.CalculateRisk(c.CustomerId) r WHERE c.CustomerId = dbo.Customer.CustomerId;", 1),
    ("DELETE FROM dbo.OrphanRecord WHERE NOT EXISTS (SELECT 1 FROM dbo.ParentRecord p WHERE p.ParentId = dbo.OrphanRecord.ParentId);", 1),
    ("INSERT INTO dbo.PermissionAudit (PrincipalName, PermissionName, CapturedAt) SELECT grantee_principal_id, permission_name, SYSUTCDATETIME() FROM sys.database_permissions;", 1),
    ("UPDATE dbo.UserPreference SET Value = source.Value FROM dbo.UserPreference target INNER JOIN stage.UserPreference source ON target.UserId = source.UserId AND target.PreferenceKey = source.PreferenceKey;", 1),
    ("DELETE FROM sales.OrderLine WHERE OrderId = @OrderId AND ProductId IN (SELECT ProductId FROM dbo.DiscontinuedProduct);", 1),
    ("INSERT INTO archive.OrderLine SELECT * FROM sales.OrderLine WHERE OrderDate < '2022-01-01';", 1),
    ("UPDATE dbo.Membership SET IsActive = 0, DeactivatedAt = SYSUTCDATETIME() WHERE UserId IN (SELECT UserId FROM dbo.Users WHERE LockedOut = 1);", 1),
    ("DELETE FROM dbo.ApiKey WHERE UserId = @UserId AND ExpiresAt < SYSUTCDATETIME();", 1),
    ("MERGE dbo.UserRole AS t USING (VALUES (@UserId, @RoleId)) AS s(UserId, RoleId) ON t.UserId = s.UserId AND t.RoleId = s.RoleId WHEN NOT MATCHED THEN INSERT (UserId, RoleId) VALUES (s.UserId, s.RoleId);", 1),
    ("WITH changes AS (SELECT * FROM stage.AddressChange WHERE BatchId = @BatchId) UPDATE dbo.Address SET Line1 = changes.Line1, City = changes.City FROM dbo.Address INNER JOIN changes ON dbo.Address.AddressId = changes.AddressId;", 1),
    ("WITH to_archive AS (SELECT TOP (1000) * FROM dbo.WebhookEvent WHERE Processed = 1 ORDER BY CreatedAt) INSERT INTO archive.WebhookEvent SELECT * FROM to_archive;", 1),
    ("WITH to_archive AS (SELECT TOP (1000) EventId FROM dbo.WebhookEvent WHERE Processed = 1 ORDER BY CreatedAt) DELETE FROM dbo.WebhookEvent WHERE EventId IN (SELECT EventId FROM to_archive);", 1),
    ("EXEC dbo.ReportOnlyProcedure @FromDate = '2026-01-01', @ToDate = '2026-01-31';", 0),
    ("SELECT CustomerId, Name FROM dbo.Customer WHERE ModifiedAt > @since;", 0),
    ("SELECT o.OrderId, SUM(ol.Amount) FROM sales.Orders o JOIN sales.OrderLine ol ON o.OrderId = ol.OrderId GROUP BY o.OrderId;", 0),
    ("WITH totals AS (SELECT CustomerId, COUNT(*) AS OrderCount FROM sales.Orders GROUP BY CustomerId) SELECT * FROM totals WHERE OrderCount > 10;", 0),
    ("WITH active_users AS (SELECT UserId FROM dbo.Users WHERE IsActive = 1) SELECT COUNT(*) FROM active_users;", 0),
    ("SELECT * FROM dbo.Customer WHERE EXISTS (SELECT 1 FROM sales.Orders WHERE sales.Orders.CustomerId = dbo.Customer.CustomerId);", 0),
    ("SELECT TOP (100) * FROM audit.LoginEvent ORDER BY LoginAt DESC;", 0),
    ("SELECT 'UPDATE dbo.Customer SET Name = ''Test''' AS ExampleSql;", 0),
    ("PRINT 'DELETE FROM dbo.Customer WHERE CustomerId = 1';", 0),
    ("RAISERROR('INSERT INTO dbo.Customer failed validation', 16, 1);", 0),
    ("CREATE TABLE dbo.NewAuditTable (AuditId int NOT NULL, CreatedAt datetime2 NOT NULL);", 0),
    ("ALTER TABLE dbo.Customer ADD LastReviewedAt datetime2 NULL;", 0),
    ("DROP TABLE dbo.OldScratchTable;", 0),
    ("CREATE INDEX IX_Order_CustomerId ON sales.Orders(CustomerId);", 0),
    ("DROP INDEX IX_Order_CustomerId ON sales.Orders;", 0),
    ("CREATE OR ALTER VIEW reporting.ActiveCustomer AS SELECT CustomerId, Name FROM dbo.Customer WHERE IsActive = 1;", 0),
    ("CREATE OR ALTER PROCEDURE dbo.UpdateCustomerReport AS SELECT CustomerId FROM dbo.Customer;", 0),
    ("GRANT SELECT ON dbo.Customer TO reporting_reader;", 0),
    ("GRANT INSERT ON dbo.Customer TO data_loader;", 0),
    ("GRANT UPDATE ON sales.Orders TO order_editor;", 0),
    ("GRANT DELETE ON dbo.Customer TO cleanup_job;", 0),
    ("GRANT EXECUTE ON OBJECT::dbo.RebuildCustomerSummary TO app_role;", 0),
    ("GRANT CONTROL ON SCHEMA::sales TO sales_admin;", 0),
    ("GRANT VIEW DEFINITION ON DATABASE::SalesDb TO auditor_role;", 0),
    ("GRANT SELECT, INSERT, UPDATE ON dbo.Customer TO crm_app;", 0),
    ("REVOKE SELECT ON dbo.Customer FROM reporting_reader;", 0),
    ("REVOKE INSERT ON dbo.Customer FROM data_loader;", 0),
    ("REVOKE UPDATE ON sales.Orders FROM order_editor;", 0),
    ("REVOKE DELETE ON dbo.Customer FROM cleanup_job;", 0),
    ("REVOKE EXECUTE ON OBJECT::dbo.RebuildCustomerSummary FROM app_role;", 0),
    ("DENY DELETE ON dbo.Customer TO contractor_role;", 0),
    ("DENY UPDATE ON sales.Orders TO readonly_role;", 0),
    ("DENY INSERT ON dbo.Payment TO support_reader;", 0),
    ("ALTER ROLE db_datareader ADD MEMBER report_user;", 0),
    ("ALTER ROLE db_datawriter DROP MEMBER legacy_loader;", 0),
    ("CREATE USER audit_reader FOR LOGIN audit_reader;", 0),
    ("DROP USER temp_contractor;", 0),
    ("ALTER AUTHORIZATION ON SCHEMA::reporting TO dbo;", 0),
    ("EXEC sp_addrolemember 'db_datareader', 'report_user';", 0),
    ("EXEC sp_droprolemember 'db_datawriter', 'old_loader';", 0),
    ("BACKUP DATABASE SalesDb TO DISK = 'D:\\Backup\\SalesDb.bak';", 0),
    ("RESTORE VERIFYONLY FROM DISK = 'D:\\Backup\\SalesDb.bak';", 0),
    ("DBCC CHECKDB('SalesDb') WITH NO_INFOMSGS;", 0),
    ("UPDATE STATISTICS dbo.Customer WITH FULLSCAN;", 0),
    ("DECLARE @sql nvarchar(max) = N'DELETE FROM dbo.Customer WHERE CustomerId = 1'; SELECT @sql;", 0),
    ("INSERT INTO #CustomerStage (CustomerId, Name) SELECT CustomerId, Name FROM dbo.Customer;", 0),
    ("UPDATE #CustomerStage SET Name = 'Masked' WHERE CustomerId = 10;", 0),
    ("DELETE FROM #CustomerStage WHERE CustomerId = 10;", 0),
    ("MERGE #CustomerStage AS target USING dbo.Customer AS source ON target.CustomerId = source.CustomerId WHEN MATCHED THEN UPDATE SET Name = source.Name;", 0),
    ("TRUNCATE TABLE #CustomerStage;", 0),
    ("SELECT * INTO #RecentOrders FROM sales.Orders WHERE OrderDate > DATEADD(day, -7, GETDATE());", 0),
    ("INSERT INTO ##GlobalStage VALUES (1, 'abc');", 0),
    ("UPDATE tempdb..#Work SET Processed = 1;", 0),
    ("DELETE FROM tempdb..##GlobalWork WHERE Processed = 1;", 0),
)


@dataclass
class WriteMatch:
    operation: str
    table_name: str
    confidence: float
    reason: str


def strip_comments_and_literals(sql: str) -> str:
    result: List[str] = []
    i = 0
    length = len(sql)
    in_line_comment = False
    in_block_comment = False
    in_single = False
    in_double = False
    in_bracket = False

    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if ch in "\r\n":
                in_line_comment = False
                result.append(ch)
            else:
                result.append(" ")
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                result.extend("  ")
                i += 2
            else:
                result.append(" ")
                i += 1
            continue

        if in_single:
            if ch == "'" and nxt == "'":
                result.extend("  ")
                i += 2
                continue
            if ch == "'":
                in_single = False
            result.append(" ")
            i += 1
            continue

        if in_double:
            if ch == '"':
                in_double = False
            result.append(" ")
            i += 1
            continue

        if in_bracket:
            result.append(ch)
            if ch == "]":
                in_bracket = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            result.extend("  ")
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            result.extend("  ")
            i += 2
            continue
        if ch == "'":
            in_single = True
            result.append(" ")
            i += 1
            continue
        if ch == '"':
            in_double = True
            result.append(" ")
            i += 1
            continue
        if ch == "[":
            in_bracket = True
            result.append(ch)
            i += 1
            continue

        result.append(ch)
        i += 1

    return "".join(result)


def normalize_identifier(identifier: str) -> str:
    parts = [part.strip() for part in identifier.split(".") if part.strip()]
    cleaned = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        cleaned.append(part.strip())
    return ".".join(cleaned)


def base_table_name(identifier: str) -> str:
    cleaned = normalize_identifier(identifier)
    if not cleaned:
        return ""
    return cleaned.split(".")[-1].strip()


def is_temp_table(identifier: str) -> bool:
    return base_table_name(identifier).startswith("#")


IDENTIFIER = r"(?:\[[^\]]+\]|[#A-Za-z_][\w#$@]*|\.)+(?:\s*\.\s*(?:\[[^\]]+\]|[#A-Za-z_][\w#$@]*))*"
NON_TABLE_KEYWORDS = {"SET", "FROM", "WHERE", "USING", "ON", "WHEN", "THEN", "VALUES", "SELECT"}
WRITE_PATTERNS: Sequence[Tuple[str, re.Pattern[str]]] = (
    ("DELETE", re.compile(rf"\bDELETE\s+(?:TOP\s*\([^)]+\)\s*)?(?:FROM\s+)?(?P<table>{IDENTIFIER})", re.IGNORECASE)),
    ("INSERT", re.compile(rf"\bINSERT\s+(?:INTO\s+)?(?P<table>{IDENTIFIER})", re.IGNORECASE)),
    ("UPDATE", re.compile(rf"\bUPDATE\s+(?:TOP\s*\([^)]+\)\s*)?(?P<table>{IDENTIFIER})\s+\bSET\b", re.IGNORECASE)),
    ("MERGE", re.compile(rf"\bMERGE\s+(?:INTO\s+)?(?P<table>{IDENTIFIER})", re.IGNORECASE)),
    ("TRUNCATE", re.compile(rf"\bTRUNCATE\s+TABLE\s+(?P<table>{IDENTIFIER})", re.IGNORECASE)),
    ("SELECT_INTO", re.compile(rf"\bSELECT\b[\s\S]*?\bINTO\s+(?P<table>{IDENTIFIER})\s+\bFROM\b", re.IGNORECASE)),
)


def extract_write_targets(sql: str) -> List[Tuple[str, str]]:
    normalized_sql = strip_comments_and_literals(sql)
    matches: List[Tuple[int, str, str]] = []
    for operation, pattern in WRITE_PATTERNS:
        for match in pattern.finditer(normalized_sql):
            table = normalize_identifier(match.group("table"))
            if table and table.upper() not in NON_TABLE_KEYWORDS:
                matches.append((match.start(), operation, table))
    matches.sort(key=lambda item: item[0])
    return [(operation, table) for _, operation, table in matches]


def features(sql: str) -> Counter:
    text = strip_comments_and_literals(sql).lower()
    tokens = re.findall(r"[#a-z_][\w#$@]*", text)
    feats = Counter(f"tok:{token}" for token in tokens)
    for first, second in zip(tokens, tokens[1:]):
        feats[f"bi:{first}_{second}"] += 1
    for keyword in ("delete", "insert", "update", "merge", "truncate", "into", "from", "set", "where"):
        feats[f"has:{keyword}"] = 1 if keyword in tokens else 0
    return feats


def train_model(path: Path = MODEL_PATH) -> Dict[str, object]:
    positive_docs = [features(sql) for sql, label in TRAINING_EXAMPLES if label == 1]
    negative_docs = [features(sql) for sql, label in TRAINING_EXAMPLES if label == 0]
    vocab = sorted(set().union(*(doc.keys() for doc in positive_docs + negative_docs)))
    pos_totals = Counter()
    neg_totals = Counter()
    for doc in positive_docs:
        pos_totals.update(doc)
    for doc in negative_docs:
        neg_totals.update(doc)

    alpha = 1.0
    pos_token_total = sum(pos_totals.values()) + alpha * len(vocab)
    neg_token_total = sum(neg_totals.values()) + alpha * len(vocab)
    weights = {}
    for feat in vocab:
        pos_prob = (pos_totals[feat] + alpha) / pos_token_total
        neg_prob = (neg_totals[feat] + alpha) / neg_token_total
        weights[feat] = math.log(pos_prob / neg_prob)

    prior = math.log(len(positive_docs) / len(negative_docs))
    model = {"prior": prior, "weights": weights, "version": 1}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model, indent=2, sort_keys=True), encoding="utf-8")
    return model


def load_model(path: Path = MODEL_PATH) -> Dict[str, object]:
    if not path.exists():
        return train_model(path)
    return json.loads(path.read_text(encoding="utf-8"))


def score_sql(sql: str, model: Dict[str, object]) -> float:
    score = float(model["prior"])
    weights: Dict[str, float] = model["weights"]  # type: ignore[assignment]
    for feat, value in features(sql).items():
        score += weights.get(feat, 0.0) * value
    return 1.0 / (1.0 + math.exp(-score))


def analyze_query(sql: str, model: Optional[Dict[str, object]] = None) -> List[WriteMatch]:
    if model is None:
        model = load_model()
    confidence = score_sql(sql, model)
    findings = []
    for operation, table in extract_write_targets(sql):
        if is_temp_table(table):
            continue
        findings.append(
            WriteMatch(
                operation=operation,
                table_name=table,
                confidence=round(max(confidence, 0.70), 4),
                reason=f"{operation} targets non-temporary table {table}",
            )
        )
    return findings


def normalized_header(header: str) -> str:
    return re.sub(r"[^a-z0-9]", "", header.lower())


def choose_column(headers: Sequence[str], candidates: Sequence[str], explicit: Optional[str]) -> Optional[str]:
    if explicit:
        if explicit not in headers:
            raise ValueError(f"Column '{explicit}' was not found. Available columns: {', '.join(headers)}")
        return explicit
    normalized = {normalized_header(header): header for header in headers}
    for candidate in candidates:
        key = normalized_header(candidate)
        if key in normalized:
            return normalized[key]
    for header in headers:
        h = normalized_header(header)
        if any(normalized_header(candidate) in h for candidate in candidates):
            return header
    return None


def analyze_csv(args: argparse.Namespace) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    model = load_model()
    input_path = Path(args.csv_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")

    with input_path.open("r", encoding=args.encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV file has no header row.")
        headers = reader.fieldnames
        query_col = choose_column(headers, QUERY_COLUMNS, args.query_column)
        if not query_col:
            raise ValueError("Could not find a query column. Pass --query-column.")
        user_col = choose_column(headers, USER_COLUMNS, args.user_column)
        datetime_col = choose_column(headers, DATETIME_COLUMNS, args.datetime_column)
        server_col = choose_column(headers, SERVER_COLUMNS, args.server_column)
        database_col = choose_column(headers, DATABASE_COLUMNS, args.database_column)

        findings: List[Dict[str, str]] = []
        total_rows = 0
        for total_rows, row in enumerate(reader, start=1):
            query = row.get(query_col, "") or ""
            for match in analyze_query(query, model):
                findings.append(
                    {
                        "source_row": str(total_rows),
                        "username": row.get(user_col, "") if user_col else "",
                        "event_datetime": row.get(datetime_col, "") if datetime_col else "",
                        "server_name": row.get(server_col, "") if server_col else "",
                        "database_name": row.get(database_col, "") if database_col else "",
                        "operation": match.operation,
                        "target_table": match.table_name,
                        "confidence": f"{match.confidence:.4f}",
                        "reason": match.reason,
                        "query": query,
                    }
                )

    findings_path = out_dir / f"findings_{run_id}.csv"
    fieldnames = [
        "source_row",
        "username",
        "event_datetime",
        "server_name",
        "database_name",
        "operation",
        "target_table",
        "confidence",
        "reason",
        "query",
    ]
    with findings_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(findings)

    summary = {
        "input_file": str(input_path),
        "total_rows": total_rows,
        "findings": len(findings),
        "by_operation": dict(Counter(finding["operation"] for finding in findings)),
        "by_user": dict(Counter(finding["username"] for finding in findings if finding["username"])),
        "by_database": dict(Counter(finding["database_name"] for finding in findings if finding["database_name"])),
        "by_server": dict(Counter(finding["server_name"] for finding in findings if finding["server_name"])),
        "output_findings_csv": str(findings_path),
    }
    summary_path = out_dir / f"summary_{run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return findings, summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline SQL Server audit CSV write-operation analyzer.")
    parser.add_argument("csv_file", nargs="?", help="Path to SQL Server audit CSV file.")
    parser.add_argument("--out-dir", default="out", help="Directory for findings.csv and summary.json.")
    parser.add_argument("--encoding", default="utf-8-sig", help="Input CSV encoding.")
    parser.add_argument("--query-column", help="Column containing SQL query text.")
    parser.add_argument("--user-column", help="Column containing username/login.")
    parser.add_argument("--datetime-column", help="Column containing event datetime.")
    parser.add_argument("--server-column", help="Column containing server name.")
    parser.add_argument("--database-column", help="Column containing database name.")
    parser.add_argument("--run-id", help="Optional output file suffix. Defaults to current timestamp.")
    parser.add_argument("--train-model", action="store_true", help="Train the bundled lightweight offline model and exit.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.train_model:
        train_model()
        print(f"Model trained: {MODEL_PATH}")
        return 0
    if not args.csv_file:
        parser.error("csv_file is required unless --train-model is used")
    findings, summary = analyze_csv(args)
    print(f"Analyzed {summary['total_rows']} rows")
    print(f"Findings: {len(findings)}")
    print(f"Findings CSV: {summary['output_findings_csv']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
