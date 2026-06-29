import unittest
import pandas as pd
import sqlite3
import os
import json
from agent.auto_cross_field_rules import generate_auto_cross_field_rules
from agent.profiling.assessment_orchestrator import _same_dataset_representation
from agent.session_store import _connect, save_pipeline_run, get_pipeline_runs_for_datasets

class TestAuditFixes(unittest.TestCase):
    def test_same_dataset_representation_optimized(self):
        # Create identical dataframes to test same representation
        df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        
        # Should return True since schemas match, high overlap, and values match
        self.assertTrue(_same_dataset_representation(df1, df2))

        # Different schemas or id missing
        df_no_id = pd.DataFrame({"key": [1, 2, 3]})
        self.assertFalse(_same_dataset_representation(df1, df_no_id))

        # Low overlap
        df_low_overlap = pd.DataFrame({"id": [10, 20, 30], "value": ["a", "b", "c"]})
        self.assertFalse(_same_dataset_representation(df1, df_low_overlap))

    def test_auto_cross_field_rules_no_duplicates(self):
        # Assessment containing two date columns
        assessment = {
            "datasets": {
                "sales": {
                    "columns": {
                        "start_date": {"semantic_type": "date"},
                        "end_date": {"semantic_type": "date"}
                    }
                }
            }
        }
        rules = generate_auto_cross_field_rules(assessment)
        
        # Verify it generates a single rule (c1, c2) or (c2, c1), but not duplicate pairs
        date_rules = [r for r in rules if r["type"] == "date_order"]
        self.assertEqual(len(date_rules), 1)

    def test_session_store_thread_local_and_sql_filter(self):
        # Verify ConnectionProxy behaves correctly
        conn = _connect()
        self.assertTrue(hasattr(conn, "cursor"))
        
        # Run a test session store pipeline run write & retrieve
        session_id = "test_audit_session_unique_123"
        run_id = save_pipeline_run(
            session_id=session_id,
            dataset_names=["orders_dataset", "customers_dataset"],
            schema_hash="abc",
            dq_score=95,
            dq_issue_count=0
        )
        self.assertTrue(run_id > 0)
        
        # Retrieve using SQL-side filter
        runs = get_pipeline_runs_for_datasets(["orders_dataset"], limit=5)
        self.assertTrue(len(runs) >= 1)
        # Find our specific run
        matched_run = next((r for r in runs if r["session_id"] == session_id), None)
        self.assertIsNotNone(matched_run)
        self.assertIn("orders_dataset", matched_run["dataset_names"])

        # No match should return empty list
        no_runs = get_pipeline_runs_for_datasets(["non_existent_dataset"])
        # Our specific run shouldn't be matched
        matched_bad = next((r for r in no_runs if r["session_id"] == session_id), None)
        self.assertIsNone(matched_bad)
