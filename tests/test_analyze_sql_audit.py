import argparse
import csv
import json
import unittest
from pathlib import Path

from audit_ai.analyze_sql_audit import TRAINING_EXAMPLES, analyze_csv, analyze_query, train_model


class AnalyzeSqlAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        train_model()

    def test_detects_user_table_writes(self):
        cases = [
            ("DELETE FROM dbo.Customers WHERE Id = 1", "DELETE", "dbo.Customers"),
            ("INSERT INTO [sales].[Orders] (Id) VALUES (1)", "INSERT", "sales.Orders"),
            ("UPDATE hr.Employee SET Title = 'Lead' WHERE Id = 2", "UPDATE", "hr.Employee"),
            ("MERGE INTO dbo.Inventory AS target USING dbo.Stage AS source ON target.Id = source.Id WHEN MATCHED THEN UPDATE SET Qty = source.Qty", "MERGE", "dbo.Inventory"),
            ("TRUNCATE TABLE dbo.LoadStage", "TRUNCATE", "dbo.LoadStage"),
            ("SELECT * INTO dbo.ArchiveOrders FROM dbo.Orders", "SELECT_INTO", "dbo.ArchiveOrders"),
        ]
        for sql, operation, table in cases:
            with self.subTest(sql=sql):
                findings = analyze_query(sql)
                self.assertEqual(len(findings), 1)
                self.assertEqual(findings[0].operation, operation)
                self.assertEqual(findings[0].table_name, table)

    def test_training_set_has_expanded_examples(self):
        self.assertGreaterEqual(len(TRAINING_EXAMPLES), 118)
        self.assertTrue(any("MERGE" in sql.upper() for sql, _ in TRAINING_EXAMPLES))
        self.assertTrue(any("WITH " in sql.upper() for sql, _ in TRAINING_EXAMPLES))
        self.assertTrue(any("GRANT " in sql.upper() for sql, _ in TRAINING_EXAMPLES))
        self.assertTrue(any("REVOKE " in sql.upper() for sql, _ in TRAINING_EXAMPLES))

    def test_ignores_temp_tables_and_read_only_queries(self):
        cases = [
            "INSERT INTO #Temp VALUES (1)",
            "UPDATE ##Scratch SET Value = 2",
            "DELETE FROM tempdb..#AuditScratch WHERE Id = 1",
            "SELECT * FROM dbo.Customers",
            "SELECT 'DELETE FROM dbo.Customers' AS TextOnly",
            "-- DELETE FROM dbo.Customers\nSELECT 1",
        ]
        for sql in cases:
            with self.subTest(sql=sql):
                self.assertEqual(analyze_query(sql), [])

    def test_analyze_csv_writes_outputs(self):
        tmp_path = Path(__file__).resolve().parents[1] / ".test_tmp"
        tmp_path.mkdir(exist_ok=True)
        try:
            csv_path = tmp_path / "audit.csv"
            out_dir = tmp_path / "out"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["EventTime", "LoginName", "ServerName", "DatabaseName", "TextData"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "EventTime": "2026-05-08 10:00:00",
                        "LoginName": "alice",
                        "ServerName": "sql-prod-01",
                        "DatabaseName": "Sales",
                        "TextData": "UPDATE dbo.Customer SET Name = 'A' WHERE Id = 1",
                    }
                )
                writer.writerow(
                    {
                        "EventTime": "2026-05-08 10:01:00",
                        "LoginName": "bob",
                        "ServerName": "sql-prod-01",
                        "DatabaseName": "Sales",
                        "TextData": "INSERT INTO #Temp VALUES (1)",
                    }
                )

            args = argparse.Namespace(
                csv_file=str(csv_path),
                out_dir=str(out_dir),
                encoding="utf-8",
                query_column=None,
                user_column=None,
                datetime_column=None,
                server_column=None,
                database_column=None,
                run_id="unit_test",
            )
            findings, summary = analyze_csv(args)
            self.assertEqual(len(findings), 1)
            self.assertEqual(findings[0]["username"], "alice")
            self.assertEqual(findings[0]["operation"], "UPDATE")
            self.assertTrue((out_dir / "findings_unit_test.csv").exists())
            self.assertTrue((out_dir / "summary_unit_test.json").exists())
            saved_summary = json.loads((out_dir / "summary_unit_test.json").read_text(encoding="utf-8"))
            self.assertEqual(saved_summary["findings"], 1)
            self.assertEqual(summary["total_rows"], 2)
        finally:
            for path in sorted(tmp_path.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmp_path.exists():
                tmp_path.rmdir()


if __name__ == "__main__":
    unittest.main()
