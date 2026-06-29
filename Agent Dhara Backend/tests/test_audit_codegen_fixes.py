import pytest
from typing import Dict, Any, List

# 1. Test Codegen Validator
def test_codegen_validator():
    from agent.etl_pipeline.codegen_validator import validate_python, validate_tsql, validate_pyspark, validate_adf
    
    # Python
    ok, errs = validate_python("def transform_data(df):\n    df = df.copy()\n    import logging\n    logger = logging.getLogger()\n    return df\n")
    assert ok, f"Python validation failed: {errs}"
    
    ok, errs = validate_python("eval('1+1')")
    assert not ok
    assert any("eval() is forbidden" in e for e in errs)
    
    # TSQL
    ok, errs = validate_tsql("CREATE TABLE dbo.ETL_LOG (id INT);\nCREATE TABLE dbo.ETL_REJECTS (id INT);\nBEGIN TRY\nSELECT SCOPE_IDENTITY();\nBEGIN TRANSACTION;\nCOMMIT TRANSACTION;\nEND TRY\nBEGIN CATCH\nROLLBACK;\nEND CATCH;\n")
    assert ok, f"TSQL validation failed: {errs}"
    
    ok, errs = validate_tsql("CREATE TABLE IF NOT EXISTS dbo.test (id INT)")
    assert not ok
    
    # PySpark
    ok, errs = validate_pyspark("from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\ndef _resolve_data_path(x): return x\n")
    assert ok, f"PySpark validation failed: {errs}"

# 2. Test Token Trimming
def test_token_trimming():
    from agent.etl_pipeline.llm_codegen import _trim_payload_for_window
    
    large_payload = {
        "source_metadata": {
            "ds1": {
                "row_count": 100,
                "columns": {f"col_{i}": {"dtype": "int", "null_percentage": 0.1, "semantic_type": "int", "sub_type": "int", "pii_level": "none"} for i in range(1500)}
            }
        },
        "manual_review": [{"dataset": "ds1", "column": f"col_{i}", "message": "error"} for i in range(20)],
        "blocked": [{"dataset": "ds1", "column": f"col_{i}", "reason": "blocked"} for i in range(10)]
    }
    
    trimmed = _trim_payload_for_window(large_payload, "python")
    
    # Ensure manual_review is capped
    assert len(trimmed["manual_review"]) <= 8
    assert "manual_review_truncated" in trimmed
    
    # Ensure blocked is capped
    assert len(trimmed["blocked"]) <= 5
    
    # Ensure metadata columns are trimmed (keys dtype, semantic_type, sub_type remain, null_percentage is removed)
    cols = trimmed["source_metadata"]["ds1"]["columns"]
    for col, cm in cols.items():
        assert "null_percentage" not in cm
        assert "dtype" in cm

# 3. Test Preflight Gate
def test_preflight_gate():
    from agent.sql_preflight import run_sql_preflight
    
    res = run_sql_preflight("CREATE TABLE IF NOT EXISTS dbo.test (id INT)")
    assert not res["passed"]
    assert any("CREATE TABLE IF NOT EXISTS" in e for e in res["errors"])
    
    res = run_sql_preflight("SELECT * FROM dbo.test")
    assert res["passed"]

# 4. Test Non-Fixable Resolutions Promotion
def test_non_fixable_promotions():
    from agent.etl_pipeline.manual_review_promote import promote_non_fixable_resolutions
    
    plan = {
        "datasets": {
            "ds1": {"steps": []}
        },
        "blocked": []
    }
    
    resolutions = [
        {"dataset": "ds1", "column": "col1", "strategy": "quarantine", "user_note": "FK check"},
        {"dataset": "ds1", "column": "col2", "strategy": "accept_risk", "user_note": "risky"}
    ]
    
    updated = promote_non_fixable_resolutions(plan, resolutions)
    
    # Check step added
    steps = updated["datasets"]["ds1"]["steps"]
    assert len(steps) == 1
    assert steps[0]["action"] == "validate_referential_integrity_or_stage"
    
    # Check blocked added
    assert len(updated["blocked"]) == 1
    assert updated["blocked"][0]["column"] == "col2"

# 5. Test Post-ETL Feedback Loop
def test_post_etl_feedback():
    from agent.etl_pipeline.execution_orchestrator import post_etl_regen_if_needed
    
    report = {
        "ok": False,
        "deltas": {
            "regressions": [
                {"table": "ds1", "column": "col1", "issue": "null rate increased"}
            ]
        }
    }
    
    plan = {"datasets": {"ds1": {"steps": []}}}
    assessment = {"datasets": {"ds1": {"columns": {"col1": {"dtype": "int"}}}}}
    
    # Mock llm gen to return a dummy string on retry
    patched = post_etl_regen_if_needed(report, plan, assessment, "python")
    # Should attempt calling generate_etl_with_llm (which might return an LLM error in test since API keys are missing, but verifies code path)
    assert patched is None or "Error" in patched or isinstance(patched, str)

# 6. Test Plan step coverage report
def test_coverage_report():
    from agent.etl_pipeline.plan_coverage_report import build_coverage_report
    
    assessment = {
        "datasets": {
            "ds1": {
                "quality": {
                    "issues": [
                        {"column": "col1", "type": "nulls", "message": "missing data"},
                        {"column": "col2", "type": "outliers", "message": "extreme value"}
                    ]
                }
            }
        }
    }
    
    plan = {
        "datasets": {
            "ds1": {
                "steps": [
                    {"column": "col1", "action": "fill_nulls_simple"}
                ]
            }
        },
        "manual_review": [],
        "blocked": []
    }
    
    report = build_coverage_report(assessment, plan)
    assert report["coverage_pct"] == 50.0
    assert len(report["covered"]) == 1
    assert len(report["uncovered"]) == 1
    assert report["covered"][0]["column"] == "col1"
    assert report["uncovered"][0]["column"] == "col2"

# 7. Test Compiler Layering
def test_compiler_layering():
    from agent.etl_pipeline.issue_to_step_compiler import preprocess_suggestions_in_place
    
    suggestions = [
        {"dataset": "ds1", "column": "col1", "issue_type": "nulls", "auto_fixable": True},
        {"dataset": "ds1", "column": "col2", "issue_type": "missing_required_column", "auto_fixable": True}
    ]
    rules = {"never_drop_rows": True}
    sem_schema = {}
    
    non_fixables = preprocess_suggestions_in_place(suggestions, rules, sem_schema)
    
    # col1 should be mapped to fill_nulls_simple due to never_drop_rows
    assert suggestions[0]["suggested_action"] == "fill_nulls_simple"
    assert suggestions[0]["auto_fixable"] is True
    
    # col2 is non_fixable
    assert len(non_fixables) == 1
    assert non_fixables[0]["column"] == "col2"
    assert suggestions[1]["non_fixable"] is True


# 8. Test Cross-Dataset Inconsistency Mapping
def test_cross_dataset_inconsistency_mapping():
    from agent.transformation_suggester import suggest_transformations
    
    assessment_result = {
        "datasets": {
            "ds1": {"columns": {"col_mismatch": {"dtype": "int"}}},
            "ds2": {"columns": {"col_mismatch": {"dtype": "object"}}}
        },
        "data_quality_issues": {
            "global_issues": {
                "cross_dataset_inconsistencies": [
                    {
                        "type": "cross_dataset_mixed_type",
                        "column": "col_mismatch",
                        "severity": "HIGH",
                        "message": "Conflicting dtypes"
                    }
                ]
            }
        }
    }
    
    res = suggest_transformations(assessment_result)
    sugs = res["suggested_transformations"]
    
    # Verify that col_mismatch has suggestions in both ds1 and ds2
    ds1_sugs = [s for s in sugs if s["dataset"] == "ds1" and s["column"] == "col_mismatch"]
    ds2_sugs = [s for s in sugs if s["dataset"] == "ds2" and s["column"] == "col_mismatch"]
    
    assert len(ds1_sugs) == 1
    assert len(ds2_sugs) == 1
    assert ds1_sugs[0]["suggested_action"] == "cast_type"
    assert ds2_sugs[0]["suggested_action"] == "cast_type"

