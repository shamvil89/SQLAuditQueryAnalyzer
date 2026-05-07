from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "samples" / "sample_audit_6mb_2500.csv"
TARGET_BYTES = 6 * 1024 * 1024
ROW_COUNT = 2500

FIELDNAMES = [
    "Max Timestamp",
    "DB User Name",
    "Server Host Name",
    "Analyzed Client IP",
    "Server IP",
    "Database",
    "Original SQL",
]


def base_sql(index: int) -> str:
    table_suffix = f"{index:04d}"
    patterns = [
        f"UPDATE dbo.Customer SET Name = 'Customer {index}', ModifiedAt = SYSUTCDATETIME() WHERE CustomerId = {index};",
        f"DELETE FROM sales.Orders WHERE OrderId = {100000 + index} AND Status = 'Cancelled';",
        f"INSERT INTO #TempAudit VALUES ({index}, 'temporary work row');",
        f"SELECT CustomerId, Name, Email FROM dbo.Customer WHERE CustomerId = {index};",
        (
            "WITH ready_jobs AS ("
            f"SELECT TOP (10) Id FROM dbo.JobQueue WHERE Status = 'Ready' AND BatchId = {index} ORDER BY Id"
            ") UPDATE dbo.JobQueue SET Status = 'Running', StartedAt = SYSUTCDATETIME() "
            "WHERE Id IN (SELECT Id FROM ready_jobs);"
        ),
        (
            "MERGE dbo.Inventory AS target USING dbo.InventoryStage AS source "
            "ON target.Sku = source.Sku "
            "WHEN MATCHED THEN UPDATE SET Qty = source.Qty, ModifiedAt = SYSUTCDATETIME() "
            "WHEN NOT MATCHED THEN INSERT (Sku, Qty, CreatedAt) VALUES (source.Sku, source.Qty, SYSUTCDATETIME());"
        ),
        "GRANT UPDATE ON dbo.Customer TO app_role;",
        "REVOKE DELETE ON dbo.Customer FROM cleanup_role;",
        f"TRUNCATE TABLE stage.CustomerLoad_{table_suffix};",
        f"SELECT * INTO reporting.CustomerSnapshot_{table_suffix} FROM dbo.Customer WHERE IsActive = 1;",
    ]
    return patterns[index % len(patterns)]


def long_context(index: int, length: int) -> str:
    fragment = (
        f" /* audit context row={index}; application=SqlAuditAnalyzerLoadTest; "
        "module=nightly-compliance-review; ticket=ACL-2026-LOAD; "
        "details=synthetic sample data used to validate offline CPU-only processing; */"
    )
    repeats = (length // len(fragment)) + 1
    return (fragment * repeats)[:length]


def make_rows(padding_length: int):
    for index in range(1, ROW_COUNT + 1):
        sql = base_sql(index) + long_context(index, padding_length)
        yield {
            "Max Timestamp": f"08-05-2026 {10 + (index // 60):02d}:{index % 60:02d}:00",
            "DB User Name": f"user_{index % 25:02d}",
            "Server Host Name": f"sql-prod-{(index % 4) + 1:02d}",
            "Analyzed Client IP": f"10.10.{20 + (index % 10)}.{10 + (index % 200)}",
            "Server IP": f"10.10.1.{20 + (index % 4)}",
            "Database": ["Sales", "Finance", "HR", "Audit"][index % 4],
            "Original SQL": sql,
        }


def write_file(padding_length: int) -> int:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(make_rows(padding_length))
    return OUTPUT.stat().st_size


def main() -> int:
    low = 0
    high = 5000
    best_size = 0
    best_padding = 0
    while low <= high:
        mid = (low + high) // 2
        size = write_file(mid)
        if size < TARGET_BYTES:
            best_size = size
            best_padding = mid
            low = mid + 1
        else:
            high = mid - 1

    final_size = write_file(best_padding)
    while final_size < TARGET_BYTES:
        best_padding += 1
        final_size = write_file(best_padding)

    print(f"Created: {OUTPUT}")
    print(f"Rows: {ROW_COUNT}")
    print(f"Size: {final_size} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

