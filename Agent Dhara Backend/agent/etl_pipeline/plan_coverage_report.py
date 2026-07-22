"""
Plan step coverage report: cross-references assessment issues against plan steps.
"""
from typing import Any, Dict, List

RESOLVING_ACTIONS = {
    "whitespace": {"trim", "regex_replace"},
    "nulls": {"fill_or_drop", "fill_nulls_simple"},
    "invalid_date_format": {"parse_dates"},
    "ancient_dates": {"parse_dates"},
    "invalid_email": {"sanitize_email", "regex_replace"},
    "pii_email": {"sanitize_email", "regex_replace"},
    "proactive_sanitize_email": {"sanitize_email", "regex_replace"},
    "invalid_phone": {"normalize_phone", "hash_phone", "mask_phone", "regex_replace"},
    "pii_phone": {"normalize_phone", "hash_phone", "mask_phone", "regex_replace"},
    "invalid_numeric": {"coerce_numeric", "cast_type"},
    "mixed_types": {"coerce_numeric", "cast_type"},
    "integer_stored_as_float": {"cast_type"},
    "negative_values": {"clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers", "range_clip"},
    "custom_range": {"clip_or_flag", "flag_outliers", "clip_outliers", "cap_outliers", "range_clip"},
    "suspicious_zero": {"zero_to_null", "replace_sentinel_values"},
    "duplicate_rows": {"deduplicate"},
    "duplicate_primary_key": {"deduplicate"},
    "duplicate_insensitive_values": {"deduplicate", "lowercase"},
    "sentinel_numeric_value": {"replace_sentinel_values", "zero_to_null"},
    "punctuation_only_value": {"nullify_punctuation"},
    "dummy_dates": {"nullify_dummy_dates"},
    "binary_like_column": {"standardize_boolean"},
    "boolean_inconsistency": {"standardize_boolean"},
    "casing_inconsistency": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "mixed_casing": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "inconsistent_casing": {"lowercase", "uppercase", "capitalize", "titlecase", "trim"},
    "outliers": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "outlier": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
    "outlier_values": {"flag_outliers", "clip_outliers", "cap_outliers", "range_clip", "clip_or_flag"},
}

def build_coverage_report(assessment: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares the quality issues identified in the assessment against the steps
    and manual review/blocked items defined in the ETL plan, validating action-level coverage.
    """
    import re
    covered = []
    uncovered = []
    
    def _clean_ds(ds):
        d = str(ds or "").strip().lower()
        if not d or d == "global":
            return "_global"
        for ext in (".csv", ".tsv", ".json", ".parquet", ".xml", ".xlsx", ".xls", ".jsonl"):
            if d.endswith(ext):
                d = d[:-len(ext)]
                break
        return re.sub(r"[^a-z0-9_]", "", d)

    def _clean_col(col):
        c = str(col or "").strip()
        return c.lower() if c else "*"

    # 1. Collect all columns with steps in the plan mapped to their actions
    planned_steps_map = {}
    datasets = (plan or {}).get("datasets") or {}
    for ds_name, ds_block in datasets.items():
        for step in (ds_block.get("steps") or []):
            col = step.get("column")
            act = step.get("action")
            if col and act:
                key = (_clean_ds(ds_name), _clean_col(col))
                planned_steps_map.setdefault(key, set()).add(str(act).lower())
                
    # 2. Collect columns in manual review, blocked, non_fixable
    mr_cols = set()
    manual_review = (plan or {}).get("manual_review") or []
    for item in manual_review:
        mr_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))
            
    blocked_cols = set()
    blocked = (plan or {}).get("blocked") or []
    for item in blocked:
        blocked_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))

    nf_cols = set()
    non_fixable = (plan or {}).get("non_fixable") or []
    for item in non_fixable:
        nf_cols.add((_clean_ds(item.get("dataset")), _clean_col(item.get("column"))))
            
    # 3. Iterate over quality issues in the assessment
    ass_datasets = (assessment or {}).get("datasets") or {}
    total_issues = 0
    
    for ds_name, ds_meta in ass_datasets.items():
        dq_issues = list((ds_meta.get("quality") or {}).get("issues") or [])
        legacy_issues = (assessment or {}).get("data_quality_issues", {}).get("datasets", {}).get(ds_name, {}).get("issues", [])
        if legacy_issues:
            dq_issues.extend(legacy_issues)
            
        for issue in dq_issues:
            col = issue.get("column")
            it = issue.get("type")
            total_issues += 1
            
            issue_detail = {
                "dataset": ds_name,
                "column": col or "",
                "issue_type": it,
                "message": issue.get("message"),
                "severity": issue.get("severity")
            }
            
            issue_key = (_clean_ds(ds_name), _clean_col(col))
            
            is_covered = (
                issue_key in mr_cols or 
                issue_key in blocked_cols or 
                issue_key in nf_cols
            )
            
            if not is_covered:
                actions_for_col = planned_steps_map.get(issue_key, set())
                resolving = RESOLVING_ACTIONS.get(it) or set()
                if actions_for_col & resolving:
                    is_covered = True
                elif not resolving and actions_for_col:
                    is_covered = True
            
            if is_covered:
                covered.append(issue_detail)
            else:
                uncovered.append(issue_detail)
                
    coverage_pct = 100.0
    if total_issues > 0:
        coverage_pct = round((len(covered) / total_issues) * 100, 2)
        
    return {
        "covered": covered,
        "uncovered": uncovered,
        "coverage_pct": coverage_pct
    }
