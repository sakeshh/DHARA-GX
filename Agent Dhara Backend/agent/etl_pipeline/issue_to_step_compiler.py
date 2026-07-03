"""
Issue-to-Step Compiler: 3-pass layering engine translating data quality issues
to rule-validated, semantics-refined ETL plan steps, manual review queues, and non-fixable items.
"""
from typing import Any, Dict, List, Tuple
import logging

logger = logging.getLogger("agent.etl_pipeline.issue_to_step_compiler")

from agent.transformation_suggester import ISSUE_TO_ACTION
_ISSUE_TO_ACTION_MAP = dict(ISSUE_TO_ACTION)
_ISSUE_TO_ACTION_MAP.update({
    "nulls": "fill_or_drop",
    "null_values": "fill_or_drop",
    "type_mismatch": "cast_type",
    "invalid_type": "cast_type",
    "outliers": "clip_or_flag",
    "numeric_outliers": "clip_or_flag",
    "duplicate": "deduplicate",
    "duplicates": "deduplicate",
    "near_duplicate_rows": "deduplicate",
    "numeric_outliers_iqr": "clip_or_flag",
    "duplicate_primary_key": "deduplicate",
    "invalid_gstin": "regex_replace",
    "invalid_pan": "regex_replace",
    "invalid_aadhaar": "regex_replace",
    "invalid_ifsc": "regex_replace",
    "invalid_cin": "regex_replace",
    "invalid_url": "regex_replace",
    "repeated_token_in_string": "regex_replace",
    "encoding_corruption": "regex_replace",
    "sentinel_numeric_value": "zero_to_null",
    "string_length_outlier": "flag_outliers",
    "custom_rule_violation": "review_manually",
})


# These issues CANNOT be resolved by any ETL action — must be acknowledged
_NON_FIXABLE_ISSUE_TYPES = frozenset({
    "missing_required_column",
    "very_wide_table",
    "empty_dataset",
    "orphan_foreign_keys",          # FK violation — needs source fix
    "schema_mismatch",              # target schema incompatible
    "encoding_corruption",          # when regex cannot recover original
    "referential_integrity_violation",
})

# These CAN be partially addressed but carry high business risk — need explicit acknowledgment
_COMPLEX_ISSUE_TYPES = frozenset({
    "custom_rule_violation",
    "business_key_duplicate",
    "duplicate_primary_key",        # when multi-column PK is involved
    "dq_gate_warning",
    "high_null_percentage",         # >50% null — may indicate structural problem
    "dominant_value_skew",          # may indicate sentinel fill
    "multivariate_outliers",        # cross-column — needs domain judgment
})


def compile_issues_to_steps(
    suggestions: List[Dict[str, Any]],
    rules: Dict[str, Any],
    sem_schema: Dict[str, Any]
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    datasets_steps = {}
    manual_review = []
    
    never_drop_rows = bool(rules.get("never_drop_rows"))
    outlier_strategy = str(rules.get("outlier_strategy") or "flag").lower().strip()
    non_nullable_cols = [str(x).lower().strip() for x in (rules.get("non_nullable") or [])]
    
    from agent.etl_pipeline.manual_review_catalog import enrich_manual_review_item, manual_review_item_id
    
    # Process suggestions
    for sug in suggestions:
        ds = sug.get("dataset") or "global"
        col = sug.get("column") or "*"
        it = sug.get("issue_type") or "unknown"
        action = sug.get("suggested_action")
        
        # Original manual intent preservation flag
        was_manual = (sug.get("auto_fixable") is False) or (sug.get("suggested_action") == "review_manually")
        
        # 1. PASS 1: Issue Baseline Mapping
        if not action or action == "noop":
            action = _ISSUE_TO_ACTION_MAP.get(it) or "noop"
            
        # If still unmapped or review manually, mark as review_manually
        if action == "review_manually" or action == "noop":
            action = "review_manually"
            
        note = f"Baseline issue mapping for {it}"
        params = {}
        
        # 2. PASS 2: Business Rules Overrides
        if never_drop_rows and action == "fill_or_drop":
            action = "fill_nulls_simple"
            note = f"never_drop_rows override: changed from fill_or_drop to fill_nulls_simple"
            params["fill_strategy"] = "value"
            params["fill_value"] = None
            
        if action == "clip_or_flag":
            if outlier_strategy == "clip":
                action = "clip_outliers"
            elif outlier_strategy == "cap":
                action = "cap_outliers"
            else:
                action = "flag_outliers"
            note = f"outlier_strategy={outlier_strategy} override"
            
        col_full_key = f"{ds}.{col}".lower()
        col_short_key = col.lower()
        
        # Non-nullable override
        is_non_nullable = col_short_key in non_nullable_cols or col_full_key in non_nullable_cols
        if is_non_nullable and action == "fill_or_drop":
            action = "fill_nulls_simple"
            note = f"non_nullable override: using fill-only instead of drop/fill choice"
            params["fill_strategy"] = "value"
            params["fill_value"] = None

        # 3. PASS 3: Semantic Refinement
        sem_desc = sem_schema.get(f"{ds}.{col}") or {}
        sem_type = str(sem_desc.get("semantic_type") or "").lower().strip()
        sub_type = str(sem_desc.get("sub_type") or "").lower().strip()
        pii_level = str(sem_desc.get("pii_level") or "").lower().strip()
        
        if sem_type == "date" and action in ("cast_type", "noop", "review_manually"):
            action = "parse_dates"
            note = "Semantic refinement: parse_dates for date semantic_type"
            
        if sem_type == "email" and action in ("trim", "lowercase", "noop", "review_manually"):
            action = "sanitize_email"
            note = "Semantic refinement: sanitize_email for email semantic_type"
            
        if sem_type == "phone" and action in ("trim", "noop", "review_manually"):
            if pii_level == "high" or pii_level == "medium":
                action = "hash_phone"
                note = "Semantic refinement: hash_phone for high/medium PII phone number"
            else:
                action = "normalize_phone"
                note = "Semantic refinement: normalize_phone for phone semantic_type"
                
        if sub_type == "currency" and action in ("clip_or_flag", "clip_outliers", "review_manually"):
            action = "range_clip"
            params["lower_bound"] = 0.0
            note = "Semantic refinement: range_clip (lower_bound=0) for currency sub_type"
            
        if sub_type == "boolean_int" and action in ("coerce_numeric", "review_manually"):
            action = "standardize_boolean"
            note = "Semantic refinement: standardize_boolean for boolean_int sub_type"
            
        # Enforce manual intent preservation if semantic confidence is low
        if was_manual and action != "review_manually":
            sem_confidence = float(sem_desc.get("confidence", 0.0))
            if sem_confidence < 0.85:
                action = "review_manually"
                note = f"Preserved manual review: semantic confidence {sem_confidence} < 0.85"
            
        # Determine auto_fixable
        # Risky steps: drop_column, exclude_column, clip_outliers/cap_outliers, or review_manually
        is_risky = action in ("drop_column", "exclude_column", "clip_outliers", "cap_outliers", "review_manually")
        
        auto_fixable = sug.get("auto_fixable", False)
        if is_risky:
            auto_fixable = False
            
        # Special case: if it is an LLM-inferred suggestion, check confidence
        from agent.etl_pipeline.rule_provenance import RuleProvenance
        is_llm_inferred = sug.get("llm_recommendation") is not None and sug.get("provenance") != RuleProvenance.AUTO_DETECTED
        if is_llm_inferred:
            confidence = sug.get("llm_confidence") or 1.0
            if confidence < 0.65:
                auto_fixable = False
            
        # Save step or route to manual_review/non_fixable
        if auto_fixable and action != "review_manually":
            # Add to steps
            datasets_steps.setdefault(ds, [])
            max_order = max((s.get("order", 0) for s in datasets_steps[ds]), default=0)
            
            datasets_steps[ds].append({
                "order": max_order + 1,
                "column": col,
                "action": action,
                "bucket": "cleanse",
                "phase": sug.get("phase", "cleanse"),
                "priority": 80,
                "params": params,
                "note": f"{sug.get('message') or note} ({note})",
                "source": "issue_to_step_compiler",
                "auto_fixable": True,
                "source_issue_type": it,
                "severity": sug.get("severity") or "medium",
                "estimated_affected_rows": sug.get("row_count_affected"),
                "llm_recommendation": sug.get("llm_recommendation"),
                "message": sug.get("message"),
            })
        else:
            # Determine risk tier
            if it in _NON_FIXABLE_ISSUE_TYPES:
                risk_tier = "non_fixable"
            elif it in _COMPLEX_ISSUE_TYPES or (
                sug.get("severity", "").lower() == "high" and not auto_fixable
            ):
                risk_tier = "complex"
            else:
                risk_tier = "standard"

            mr_item = {
                "id": manual_review_item_id(ds if ds != "global" else None, col, it),
                "dataset": ds if ds != "global" else None,
                "column": col,
                "issue_type": it,
                "risk_tier": risk_tier,          # ← NEW unified field
                "severity": sug.get("severity") or "medium",
                "message": sug.get("message") or f"Review required for {it}",
                "guidance": sug.get("manual_guidance") or f"Configure action for {col}",
                "suggested_action": action,
                "auto_fixable": False,
            }
            if "llm_recommendation" in sug:
                mr_item["llm_recommendation"] = sug["llm_recommendation"]
            if "llm_confidence" in sug:
                mr_item["llm_confidence"] = sug["llm_confidence"]

            # ALL tiers go to manual_review — no separate non_fixable list
            manual_review.append(enrich_manual_review_item(
                mr_item,
                llm_recommendation=mr_item.get("llm_recommendation")
            ))
            
    return datasets_steps, manual_review


def preprocess_suggestions_in_place(
    suggestions: List[Dict[str, Any]],
    rules: Dict[str, Any],
    sem_schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Modifies suggestions in-place to apply the 3-pass compiler logic.
    Returns the list of non_fixable issues.
    """
    never_drop_rows = bool(rules.get("never_drop_rows"))
    outlier_strategy = str(rules.get("outlier_strategy") or "flag").lower().strip()
    non_nullable_cols = [str(x).lower().strip() for x in (rules.get("non_nullable") or [])]
    
    non_fixables = []

    for sug in suggestions:
        ds = sug.get("dataset") or "global"
        col = sug.get("column") or "*"
        it = sug.get("issue_type") or "unknown"
        action = sug.get("suggested_action")
        
        # Original manual intent preservation flag
        was_manual = (sug.get("auto_fixable") is False) or (sug.get("suggested_action") == "review_manually")
        
        # 1. PASS 1: Issue Baseline Mapping
        if not action or action == "noop":
            action = _ISSUE_TO_ACTION_MAP.get(it) or "noop"
            
        if action == "review_manually" or action == "noop":
            action = "review_manually"
            
        # 2. PASS 2: Business Rules Overrides
        if never_drop_rows and action == "fill_or_drop":
            action = "fill_nulls_simple"
            
        if action == "clip_or_flag":
            if outlier_strategy == "clip":
                action = "clip_outliers"
            elif outlier_strategy == "cap":
                action = "cap_outliers"
            else:
                action = "flag_outliers"
            
        col_full_key = f"{ds}.{col}".lower()
        col_short_key = col.lower()
        
        is_non_nullable = col_short_key in non_nullable_cols or col_full_key in non_nullable_cols
        if is_non_nullable and action == "fill_or_drop":
            action = "fill_nulls_simple"

        # 3. PASS 3: Semantic Refinement
        sem_desc = sem_schema.get(f"{ds}.{col}") or {}
        sem_type = str(sem_desc.get("semantic_type") or "").lower().strip()
        sub_type = str(sem_desc.get("sub_type") or "").lower().strip()
        pii_level = str(sem_desc.get("pii_level") or "").lower().strip()
        
        if sem_type == "date" and action in ("cast_type", "noop", "review_manually"):
            action = "parse_dates"
            
        if sem_type == "email" and action in ("trim", "lowercase", "noop", "review_manually"):
            action = "sanitize_email"
            
        if sem_type == "phone" and action in ("trim", "noop", "review_manually"):
            if pii_level == "high" or pii_level == "medium":
                action = "hash_phone"
            else:
                action = "normalize_phone"
                
        if sub_type == "currency" and action in ("clip_or_flag", "clip_outliers", "review_manually"):
            action = "range_clip"
            
        if sub_type == "boolean_int" and action in ("coerce_numeric", "review_manually"):
            action = "standardize_boolean"
            
        # Enforce manual intent preservation if semantic confidence is low
        if was_manual and action != "review_manually":
            sem_confidence = float(sem_desc.get("confidence", 0.0))
            if sem_confidence < 0.85:
                action = "review_manually"
            
        # Determine auto_fixable
        is_risky = action in ("drop_column", "exclude_column", "clip_outliers", "cap_outliers", "review_manually")
        
        auto_fixable = sug.get("auto_fixable", False)
        if is_risky:
            auto_fixable = False
            
        confidence = sug.get("llm_confidence") or 1.0
        if confidence < 0.65:
            auto_fixable = False
            
        sug["suggested_action"] = action
        sug["auto_fixable"] = auto_fixable
        
        # Check non_fixable
        is_non_fixable = it in _NON_FIXABLE_ISSUE_TYPES
        if is_non_fixable:
            sug["non_fixable"] = True
            sug["risk_tier"] = "non_fixable"
            from agent.etl_pipeline.manual_review_catalog import manual_review_item_id
            nf_item = {
                "id": manual_review_item_id(ds if ds != "global" else None, col, it),
                "dataset": ds if ds != "global" else None,
                "column": col,
                "issue_type": it,
                "risk_tier": "non_fixable",
                "severity": sug.get("severity") or "medium",
                "message": sug.get("message") or f"Manual review required for {it}",
                "guidance": sug.get("manual_guidance") or f"Configure action for {col}",
                "suggested_action": action,
                "auto_fixable": False
            }
            if "llm_recommendation" in sug:
                nf_item["llm_recommendation"] = sug["llm_recommendation"]
            if "llm_confidence" in sug:
                nf_item["llm_confidence"] = sug["llm_confidence"]
            non_fixables.append(nf_item)
            
    return non_fixables
