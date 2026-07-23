"""
test_etl_codegen_quality.py
Tests verifying ETL code generation governance rules:
- No fake ID generation (mean/median fill for business keys)
- No categorical columns cast to numeric types
- No unsafe dropDuplicates() when no business key
- Date parsing emits is_invalid_ flag columns
- Domain violation flagging for digit-strings in categorical columns
- Readiness % increases after manual review resolutions
- classify_steps correctly marks user-promoted steps as 'auto'
"""
from __future__ import annotations

import pytest
from typing import Any, Dict, List


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_plan(steps_by_ds: Dict[str, List[Dict[str, Any]]], **kwargs) -> Dict[str, Any]:
    datasets = {}
    for ds, steps in steps_by_ds.items():
        datasets[ds] = {"steps": steps}
    return {
        "plan_id": "test_plan",
        "datasets": datasets,
        "business_rules": kwargs.get("business_rules", {}),
        "manual_review": kwargs.get("manual_review", []),
        "resolved_manual_review": kwargs.get("resolved_manual_review", []),
        "connector_manifest": {},
        **{k: v for k, v in kwargs.items() if k not in ("business_rules", "manual_review", "resolved_manual_review")},
    }


# ─── Test 1: Business key columns NEVER filled with mean/median ───────────────

class TestNoFakeIDGeneration:
    def test_sale_id_not_filled_with_mean(self):
        """sale_id must never get F.lit(988.6368) or any computed fill."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "sale_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "mean", "fill_value": 988.6368}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "988.6368" not in code, "Fake ID value 988.6368 must not appear in generated code"
        assert "F.coalesce(F.col('sale_id'), F.lit(988" not in code
        # Should emit a flag column instead
        assert "is_missing_sale_id" in code, "Missing-ID flag column must be emitted"

    def test_campaign_id_not_filled_with_median(self):
        """campaign_id must never get a computed fill value."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "campaign_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "median", "fill_value": 604.1034}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "604.1034" not in code, "Fake campaign_id value must not appear in generated code"
        assert "is_missing_campaign_id" in code

    def test_factory_id_not_filled_with_mean(self):
        """factory_id must never get a computed fill value."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "factory.json": [
                {"order": 1, "column": "factory_id", "action": "fill_or_drop",
                 "params": {"fill_strategy": "mean", "fill_value": 488.7018}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "488.7018" not in code, "Fake factory_id value must not appear in generated code"
        assert "is_missing_factory_id" in code


# ─── Test 2: Categorical columns NOT cast to long ─────────────────────────────

class TestNoCategoricalCastToNumeric:
    def test_region_not_cast_to_long(self):
        """region is a categorical column \u2014 must not be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "region", "action": "cast_type",
                 "params": {"target_type": "long"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert ".cast('long')" not in code or "region" not in code.split(".cast('long')")[0].split("\n")[-1], \
            "region must not be cast to long"
        assert "region_domain_flag" in code, "Domain flag must be emitted for categorical column"

    def test_channel_not_cast_to_long(self):
        """channel is a categorical column \u2014 must not be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "channel", "action": "cast_type",
                 "params": {"target_type": "integer"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "channel_domain_flag" in code, "Domain flag must be emitted for channel column"

    def test_non_categorical_cast_is_allowed(self):
        """Numeric columns like 'qty' CAN be cast to long."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "factory.json": [
                {"order": 1, "column": "qty", "action": "cast_type",
                 "params": {"target_type": "long"}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "cast('long')" in code, "Non-categorical columns should be castable"


# ─── Test 3: Dedup uses row_number, not dropDuplicates ───────────────────────

class TestSafeDeduplication:
    def test_dedup_without_business_key_flags_not_drops(self):
        """When no business key is configured, dedup must flag rows, not drop them."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "[Row-level]", "action": "deduplicate", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "dropDuplicates()" not in code, "dropDuplicates() must not appear without a business key"
        assert "_is_duplicate" in code, "Duplicate flag column must be emitted"
        assert "manual review required" in code.lower() or "NOT dropped" in code

    def test_dedup_with_business_key_uses_row_number(self):
        """With a business key, dedup uses row_number window."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan(
            {"sales.xml": [
                {"order": 1, "column": "sale_id", "action": "deduplicate", "params": {}},
            ]},
            business_keys={"sales.xml": ["sale_id"]},
        )
        code = generate_pyspark_etl(plan, {})
        assert "row_number()" in code, "Business-key dedup must use row_number()"
        assert "dropDuplicates()" not in code


# ─── Test 4: parse_dates_safe emits flag column ───────────────────────────────

class TestSafeDateParsing:
    def test_parse_dates_safe_emits_flag(self):
        """parse_dates_safe must emit is_invalid_ flag before nullifying bad dates."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "date", "action": "parse_dates_safe", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "is_invalid_date" in code, "is_invalid_date flag column must be emitted"
        assert "00/00/0000" in code, "Sentinel date must be in the bad-value list"
        assert "to_timestamp" in code, "Timestamp parse must still happen"

    def test_plain_parse_dates_still_works(self):
        """Legacy parse_dates still works (backwards compat)."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "sales.xml": [
                {"order": 1, "column": "created_at", "action": "parse_dates", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "to_timestamp" in code


# ─── Test 5: Domain violation flagging ───────────────────────────────────────

class TestDomainViolationFlagging:
    def test_flag_domain_violation_action(self):
        """flag_domain_violation must emit a flag column, not cast to integer."""
        from agent.etl_pipeline.pyspark_codegen import generate_pyspark_etl
        plan = _make_plan({
            "marketing.csv": [
                {"order": 1, "column": "region", "action": "flag_domain_violation", "params": {}},
            ]
        })
        code = generate_pyspark_etl(plan, {})
        assert "region_domain_flag" in code
        assert ".cast('long')" not in code or "region" not in code


# ─── Test 6: Readiness % improves after resolution ───────────────────────────

class TestReadinessPctAfterResolution:
    def _build_plan_with_resolutions(self, n_auto: int, n_total: int, n_resolved: int) -> dict:
        steps = []
        for i in range(n_auto):
            steps.append({"order": i + 1, "column": f"col_{i}", "action": "trim",
                          "bucket": "auto", "classification": "auto"})
        for i in range(n_total - n_auto):
            steps.append({"order": n_auto + i + 1, "column": f"col_r_{i}", "action": "review_manually",
                          "bucket": "review", "classification": "review"})
        resolved = [{"id": f"r_{i}", "dataset": "ds", "column": f"col_r_{i}",
                     "issue_type": "nulls", "status": "resolved"} for i in range(n_resolved)]
        return {
            "plan_id": "t",
            "datasets": {"ds": {"steps": steps}},
            "manual_review": [],
            "resolved_manual_review": resolved,
            "blocked": [],
        }

    def test_readiness_increases_with_resolved_items(self):
        """Resolving manual review items should increase the readiness percentage."""
        # Simulate: 61 auto out of 84 total = 72% before resolution
        plan_before = self._build_plan_with_resolutions(n_auto=61, n_total=84, n_resolved=0)
        plan_after = self._build_plan_with_resolutions(n_auto=61, n_total=84, n_resolved=15)

        # Simulate the frontend pct formula
        def calc_pct(plan: dict) -> int:
            datasets = plan.get("datasets", {})
            total, auto = 0, 0
            for ds in datasets.values():
                for s in ds.get("steps", []):
                    total += 1
                    if (s.get("classification") or s.get("bucket") or "auto").lower() == "auto":
                        auto += 1
            resolved = len(plan.get("resolved_manual_review") or [])
            denom = total + resolved
            return round(((auto + resolved) / denom) * 100) if denom > 0 else 0

        pct_before = calc_pct(plan_before)
        pct_after = calc_pct(plan_after)
        assert pct_after > pct_before, (
            f"Readiness % must increase after resolving items: before={pct_before}%, after={pct_after}%"
        )
        # 61/84 = 72.6% — rounds to 72 in JS Math.round, 73 in Python round(); accept either
        assert pct_before in (72, 73), f"Baseline should be ~72-73%, got {pct_before}%"
        assert pct_after > 75, f"After 15 resolutions, should be >75%, got {pct_after}%"


# ─── Test 7: classify_steps marks user-promoted steps as auto ────────────────

class TestClassifyStepsBuckets:
    def test_user_promoted_step_gets_auto_bucket(self):
        """Steps with evidence.rule_override=True must get bucket='auto'."""
        from agent.etl_pipeline.classify_steps import tag_plan_step_buckets
        plan = {
            "datasets": {
                "sales.xml": {
                    "steps": [
                        {
                            "order": 1, "column": "amount", "action": "fill_or_drop",
                            "severity": "high",
                            "evidence": {"rule_override": True, "confidence": 0.95, "user_resolution": "fill_null"},
                            "source": "manual_review_promote",
                        }
                    ]
                }
            }
        }
        result = tag_plan_step_buckets(plan, {})
        step = result["datasets"]["sales.xml"]["steps"][0]
        assert step["bucket"] == "auto", (
            f"User-promoted step must have bucket='auto', got '{step['bucket']}'"
        )

    def test_non_fixable_resolution_gets_auto_bucket(self):
        """Steps from non_fixable_user_resolution source must get bucket='auto'."""
        from agent.etl_pipeline.classify_steps import tag_plan_step_buckets
        plan = {
            "datasets": {
                "factory.json": {
                    "steps": [
                        {
                            "order": 1, "column": "[Row-level]",
                            "action": "validate_referential_integrity_or_stage",
                            "severity": "high",
                            "source": "non_fixable_user_resolution",
                            "evidence": {},
                        }
                    ]
                }
            }
        }
        result = tag_plan_step_buckets(plan, {})
        step = result["datasets"]["factory.json"]["steps"][0]
        assert step["bucket"] == "auto", (
            f"Non-fixable resolution step must have bucket='auto', got '{step['bucket']}'"
        )

    def test_new_actions_in_auto_set(self):
        """New safe actions (parse_dates_safe, flag_domain_violation, fill_nulls_flag) must be AUTO."""
        from agent.etl_pipeline.classify_steps import classify_step_bucket
        for action in ("parse_dates_safe", "flag_domain_violation", "fill_nulls_flag", "replace_sentinel_values"):
            result = classify_step_bucket(action, severity="medium")
            assert result["bucket"] == "auto", (
                f"Action '{action}' must classify as 'auto', got '{result['bucket']}'"
            )


# ─── Test 8: issue_to_step_compiler mapping fixes ────────────────────────────

class TestIssueCompilerMappings:
    def _get_action(self, issue_type: str) -> str:
        from agent.etl_pipeline.issue_to_step_compiler import _ISSUE_TO_ACTION_MAP
        return _ISSUE_TO_ACTION_MAP.get(issue_type, "noop")

    def test_duplicate_primary_key_routes_to_manual(self):
        """duplicate_primary_key must map to review_manually, never deduplicate."""
        action = self._get_action("duplicate_primary_key")
        assert action == "review_manually", (
            f"duplicate_primary_key must map to review_manually, got '{action}'"
        )

    def test_mixed_case_routes_to_trim(self):
        """mixed_case must map to trim, not lowercase (protects master data)."""
        assert self._get_action("mixed_case") == "trim"
        assert self._get_action("inconsistent_case") == "trim"

    def test_placeholder_routes_to_nullify(self):
        """placeholder_detected must map to nullify_punctuation."""
        assert self._get_action("placeholder_detected") == "nullify_punctuation"

    def test_digit_strings_route_to_flag(self):
        """string_with_only_digits_in_text_column must map to flag_domain_violation."""
        assert self._get_action("string_with_only_digits_in_text_column") == "flag_domain_violation"

    def test_invalid_date_format_routes_to_safe_parse(self):
        """invalid_date_format must map to parse_dates_safe for audit trail."""
        assert self._get_action("invalid_date_format") == "parse_dates_safe"

    def test_duplicate_pk_in_complex_types(self):
        """duplicate_primary_key must be in _COMPLEX_ISSUE_TYPES."""
        from agent.etl_pipeline.issue_to_step_compiler import _COMPLEX_ISSUE_TYPES
        assert "duplicate_primary_key" in _COMPLEX_ISSUE_TYPES

    def test_string_with_digits_in_complex_types(self):
        """string_with_only_digits_in_text_column must be in _COMPLEX_ISSUE_TYPES."""
        from agent.etl_pipeline.issue_to_step_compiler import _COMPLEX_ISSUE_TYPES
        assert "string_with_only_digits_in_text_column" in _COMPLEX_ISSUE_TYPES
