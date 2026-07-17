from __future__ import annotations

import re
import os
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from agent.etl_pipeline.codegen_policy import plan_policy_block
from agent.etl_pipeline.codegen_shared import outlier_multiplier, step_params
from agent.etl_pipeline.join_emitters import (
    emit_pyspark_joins,
    emit_pyspark_load,
    emit_pyspark_output_contract,
    emit_pyspark_write_outputs,
)
from agent.etl_pipeline.io_snippets import (
    pyspark_iqr_bounds_helper,
    pyspark_prefix_non_key_columns_helper,
    pyspark_production_helpers,
    resolve_path_pyspark_helper,
    resolve_path_fabric_pyspark_helper,
)


def _safe(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    return (s or "dataset").strip("_")


def _emit_fill_spark(col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    strat = params.get("fill_strategy")
    fval = params.get("fill_value")
    dtype_str = f"dict({df}.dtypes)[{c}]"
    
    if strat == "median":
        if fval is not None:
            return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({fval}).cast({dtype_str})))"]
        return [
            f"_med = {df}.select(F.percentile_approx(F.col({c}), 0.5).alias('m')).first()['m']",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit(_med).cast({dtype_str})))",
        ]
    if strat == "mean":
        if fval is not None:
            return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({fval}).cast({dtype_str})))"]
        return [
            f"_avg = {df}.select(F.avg(F.col({c}).cast('double')).alias('m')).first()['m']",
            f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit(_avg).cast({dtype_str})))",
        ]
    if strat == "value" and fval is not None:
        if str(fval).strip().lower() in ("none", "null", ""):
            return [
                f"# Warning: fill_value is empty/null/none for column {col}; keeping original values as-is",
            ]
        return [f"{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit({repr(fval)}).cast({dtype_str})))"]
    if strat == "value":
        is_text_safe = not any(k in str(col).lower() for k in ("id", "key", "code", "ref", "date", "time", "email", "phone", "num", "val", "amount", "price", "credit"))
        if is_text_safe:
            return [
                f"# WARNING: fill_value is None for column {col} — using empty string default",
                f'{df} = {df}.withColumn({c}, F.coalesce(F.col({c}), F.lit("").cast({dtype_str})))'
            ]
        else:
            return [
                f"# WARNING: fill_value is None for column {col} — defaulting to None to prevent invalid data/casts; keeping original values as-is",
            ]
    # Default fallback to satisfy validation
    return [
        f"# Warning: no fill_strategy for {col}; keeping original values as-is",
    ]


def _emit_outliers_spark(action: str, col: str, df: str, params: Dict[str, Any]) -> List[str]:
    c = repr(str(col))
    flag_col = repr(f"{col}_outlier_flagged")
    mult = outlier_multiplier(params)
    method = params.get("outlier_method") or (
        "clip" if action == "clip_outliers" else "cap" if action == "cap_outliers" else "flag"
    )
    lines = [
        f"_stats, _iqr, _lower, _upper = _iqr_bounds({df}, {c}, multiplier={mult})",
    ]
    if method == "clip":
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when(F.col({c}) < F.lit(_lower), F.lit(_lower))"
            f" .when(F.col({c}) > F.lit(_upper), F.lit(_upper))"
            f" .otherwise(F.col({c})))"
        )
    elif method == "cap":
        med = params.get("fill_value")
        if med is not None:
            lines.append(f"_median = {med}")
        else:
            lines.append(f"_median = float(_stats['median']) if _stats is not None and _stats['median'] is not None else 0.0")
        lines.append(
            f"{df} = {df}.withColumn({c},"
            f" F.when((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper)), F.lit(_median))"
            f" .otherwise(F.col({c})))"
        )
    else:
        lines.append(
            f"{df} = {df}.withColumn({flag_col},"
            f" ((F.col({c}) < F.lit(_lower)) | (F.col({c}) > F.lit(_upper))) & F.col({c}).isNotNull())"
        )
    return lines


def _emit_spark(action: str, col: str | None, df: str, step_meta: Optional[Dict[str, Any]] = None) -> List[str]:
    params = step_params(step_meta)
    act = (action or "").lower()
    if act == "deduplicate":
        if not col or str(col).lower() in ("row-level", "[row-level]"):
            return [f"{df} = {df}.dropDuplicates()"]
        else:
            return [f"{df} = {df}.dropDuplicates([{repr(str(col))}])"]
    c = repr(str(col))
    if act == "trim":
        return [f'{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast("string")))']
    if act in ("fill_or_drop", "fill_nulls_simple"):
        return _emit_fill_spark(col, df, params)
    if act == "coerce_numeric":
        is_key = any(k in str(col).lower() for k in ("id", "key", "code", "ref"))
        if is_key:
            return [
                f"# Coerce numeric on key: keep as string to prevent loss of leading zeros or alphanumeric chars",
                f"{df} = {df}.withColumn({c}, F.trim(F.col({c}).cast('string')))"
            ]
        return [f"{df} = {df}.withColumn({c}, F.col({c}).cast('double'))"]
    if act == "zero_to_null":
        return [f"{df} = {df}.withColumn({c}, F.when((F.col({c}) == 0) | (F.col({c}) == '0'), F.lit(None)).otherwise(F.col({c})))"]
    if act == "cast_type":
        target_type = params.get("target_type") or params.get("cast_to") or "long"
        is_key = any(k in str(col).lower() for k in ("id", "key", "code", "ref"))
        is_int_target = any(it in str(target_type).lower() for it in ("long", "int", "integer"))
        if is_key and is_int_target:
            return [
                f"{df} = {df}.withColumn({c}, "
                f"F.when(F.col({c}).cast('string').rlike(r'^\\d+$'), F.col({c}).cast({repr(target_type)}))"
                f".otherwise(F.lit(None).cast({repr(target_type)})))"
            ]
        return [f"{df} = {df}.withColumn({c}, F.col({c}).cast({repr(target_type)}))"]
    if act == "parse_dates":
        return [f"{df} = {df}.withColumn({c}, F.to_timestamp(F.col({c})))"]
    if act == "sanitize_email":
        return [
            f'{df} = {df}.withColumn({c}, F.lower(F.trim(F.col({c}).cast("string"))))',
            f"{df} = {df}.withColumn({c}, F.when(F.col({c}).contains('@'), F.col({c})).otherwise(None))",
        ]
    if act == "normalize_phone":
        return [f'{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""))']
    if act == "hash_phone":
        return [
            f"# Privacy: one-way hash (params.privacy=hash)",
            f"{df} = {df}.withColumn({c}, F.sha2(F.col({c}).cast('string'), 256))",
        ]
    if act == "mask_phone":
        return [
            f"# Privacy: reversible mask (params.privacy=mask)",
            f'{df} = {df}.withColumn({c}, F.concat(F.lit("***"), F.substring(F.regexp_replace(F.col({c}).cast("string"), "\\\\D", ""), -4, 4)))',
        ]
    if act == "lowercase":
        return [f'{df} = {df}.withColumn({c}, F.lower(F.col({c}).cast("string")))']
    if act == "uppercase":
        return [f'{df} = {df}.withColumn({c}, F.upper(F.col({c}).cast("string")))']
    if act in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
        return _emit_outliers_spark(act, col, df, params)
    if act == "standardize_boolean":
        return [
            f'{df} = {df}.withColumn({c}, F.when(F.lower(F.col({c}).cast("string")).isin("1","true","yes","y","t"), F.lit(1)).otherwise(F.lit(0)))'
        ]
    if act == "nullify_punctuation":
        return [
            f"# Nullify punctuation-only strings",
            f"{df} = {df}.withColumn({c}, F.when(~F.col({c}).cast('string').rlike('[a-zA-Z0-9]'), F.lit(None)).otherwise(F.col({c})))"
        ]
    if act == "nullify_dummy_dates":
        col_clean = _safe(col)
        return [
            f"# Nullify dummy dates (e.g. 1900-01-01)",
            f"{df} = {df}.withColumn('_tmp_date_{col_clean}', F.to_date(F.col({c})))",
            f"{df} = {df}.withColumn({c}, F.when((F.col('_tmp_date_{col_clean}').eqNullSafe(F.lit('1900-01-01'))) | ((F.month(F.col('_tmp_date_{col_clean}')) == 1) & (F.dayofmonth(F.col('_tmp_date_{col_clean}')) == 1)), F.lit(None)).otherwise(F.col({c})))",
            f"{df} = {df}.drop('_tmp_date_{col_clean}')"
        ]
    if act == "range_clip":
        lo = params.get("min_value") or params.get("lower_bound") or 0
        hi = params.get("max_value") or params.get("upper_bound")
        if hi is not None:
            cond_str = f"(F.col({c}) < {lo}) | (F.col({c}) > {hi})"
        else:
            cond_str = f"F.col({c}) < {lo}"
        return [f"{df} = {df}.withColumn({c}, F.when({cond_str}, F.lit(None)).otherwise(F.col({c})))"]
    if act == "regex_replace":
        pattern = params.get("regex_pattern") or params.get("pattern") or r"[^\w\s]"
        replacement = params.get("replacement") or ""
        pattern_esc = pattern.replace("\\", "\\\\")
        return [f"{df} = {df}.withColumn({c}, F.regexp_replace(F.col({c}).cast('string'), {repr(pattern_esc)}, {repr(replacement)}))"]
    if act == "replace_values":
        mapping = params.get("replace_values") or {}
        if mapping:
            col_expr = f"F.col({c})"
            for old_v, new_v in mapping.items():
                col_expr = f"F.when(F.col({c}) == F.lit({repr(old_v)}), F.lit({repr(new_v)})).otherwise({col_expr})"
            return [f"{df} = {df}.withColumn({c}, {col_expr})"]
        return [f"# replace_values on {col}: no mapping provided"]
    if act in ("drop_column", "exclude_column"):
        return [f"{df} = {df}.drop({c})"]
    if act == "noop":
        return [f"# Column {col}: no transform"]
    if act == "validate_referential_integrity_or_stage":
        rel_ds = params.get("related_dataset") or "?"
        rel_col = params.get("related_column") or "?"
        mode = params.get("enforcement_mode") or "flag"
        fk_action = params.get("fk_action") or "flag"
        
        lines = [
            f"# Referential integrity check: {col} -> {rel_ds}.{rel_col} (action={fk_action}, mode={mode})",
            f"if all_dfs is not None and {repr(rel_ds)} in all_dfs:",
            f"    _parent_df = all_dfs[{repr(rel_ds)}]",
            f"    if {repr(rel_col)} in _parent_df.columns:",
            f"        _parent_keys = _parent_df.select(F.col({repr(rel_col)}).alias('_parent_key')).filter(F.col('_parent_key').isNotNull()).distinct()",
            f"        {df} = {df}.join(_parent_keys, F.col({repr(col)}) == _parent_keys['_parent_key'], 'left')",
            f"        _orphan_count = {df}.filter(F.col('_parent_key').isNull() & F.col({repr(col)}).isNotNull()).count()",
            f"        if _orphan_count > 0:",
            f"            logger.warning(f'Found {{_orphan_count}} orphan values in {col} referencing {rel_ds}.{rel_col}')",
        ]
        if fk_action == "reject_orphans":
            lines.extend([
                f"            # Action: reject_orphans",
                f"            {df} = {df}.filter(F.col('_parent_key').isNotNull() | F.col({repr(col)}).isNull())",
                f"            logger.info(f'Dropped {{_orphan_count}} orphan rows')",
            ])
        elif fk_action == "null_fill_fk":
            lines.extend([
                f"            # Action: null_fill_fk",
                f"            {df} = {df}.withColumn({repr(col)}, F.when(F.col('_parent_key').isNull() & F.col({repr(col)}).isNotNull(), F.lit(None)).otherwise(F.col({repr(col)})))",
                f"            logger.info(f'Null-filled {{_orphan_count}} orphan values')",
            ])
        elif fk_action == "create_unknown_dim_record":
            lines.extend([
                f"            # Action: create_unknown_dim_record",
                f"            _orphans = {df}.filter(F.col('_parent_key').isNull() & F.col({repr(col)}).isNotNull()).select(F.col({repr(col)}).alias({repr(rel_col)})).distinct()",
                f"            _new_rows = _orphans",
                f"            for _c in _parent_df.columns:",
                f"                if _c != {repr(rel_col)}:",
                f"                    _new_rows = _new_rows.withColumn(_c, F.lit(None).cast(dict(_parent_df.dtypes)[_c]))",
                f"            all_dfs[{repr(rel_ds)}] = _parent_df.unionByName(_new_rows)",
                f"            logger.info(f'Created unknown dimension records in {rel_ds}')",
            ])
        else:
            lines.extend([
                f"            # Action: flag / warn only",
                f"            pass",
            ])
        lines.extend([
            f"        {df} = {df}.drop('_parent_key')",
            f"else:",
            f"    logger.warning(f'Skipped referential integrity check for {col} -> {rel_ds}.{rel_col} (parent dataset not loaded)')",
        ])
        return lines
    if act == "review_manually":
        return [
            f"# MANUAL REVIEW REQUIRED: {col}",
            f"logger.warning('Column {col} requires manual review — excluded from auto-clean')",
        ]
    return [f"# Unsupported in pyspark template v1: {act} on {col}"]


def _emit_valid_values_spark(df: str, ds_name: str, rules: Dict[str, Any]) -> List[str]:
    vv = rules.get("valid_values") or {}
    if not vv:
        return []
    never_drop = bool(rules.get("never_drop_rows"))
    lines: List[str] = []
    for col, allowed in vv.items():
        c = repr(str(col))
        sid = _safe(col)
        allowed_lit = repr([str(v).lower() for v in allowed])
        if never_drop:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _bad = ~F.lower(F.col({c}).cast('string')).isin({allowed_lit}) & F.col({c}).isNotNull()",
                f"    {df} = {df}.withColumn({c}, F.when(_bad, F.lit(None)).otherwise(F.col({c})))",
            ])
        else:
            lines.extend([
                f"if {c} in {df}.columns:",
                f"    _before = {df}.count()",
                f"    {df} = {df}.filter(F.lower(F.col({c}).cast('string')).isin({allowed_lit}) | F.col({c}).isNull())",
                f"    logger.info('valid_values {ds_name}.{col}: dropped %d rows', _before - {df}.count())",
            ])
    return lines


def generate_pyspark_etl(plan: Dict[str, Any], assessment: Dict[str, Any]) -> str:
    _ = assessment
    plan_id = str(plan.get("plan_id") or "unknown")
    rules = plan.get("business_rules") or {}
    never_drop = bool(rules.get("never_drop_rows"))
    rel = plan.get("relationships") or {}
    joins = rel.get("joins") or []
    join_strategy = str(joins[0].get("join_type") or "left") if joins else "none"

    policy = plan_policy_block(plan).replace("\n", "\n# ")
    lines: List[str] = [
        '"""',
        f"PySpark ETL — plan_id={plan_id}",
        "Generated by: Agent Dhara",
        "Policy:",
        policy,
        '"""',
        "from __future__ import annotations",
        "",
        "import logging",
        "import os",
        "from typing import Optional",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import DataFrame",
        "",
        "logging.basicConfig(level=logging.INFO)",
        "logger = logging.getLogger('agent_dhara')",
        "",
    ]
    notes = str(rules.get("notes") or "").strip()
    if notes:
        lines.extend(["# Business notes:", "# " + notes.replace("\n", "\n# "), ""])

    manifest = plan.get("connector_manifest") or {}
    use_fabric = plan.get("execution_target") == "fabric" or any(
        ent.get("source_type") == "fabric_files_zone"
        for ent in (manifest.get("datasets") or {}).values()
    )
    
    if manifest.get("datasets"):
        if use_fabric:
            ws_id = os.getenv("FABRIC_WORKSPACE_ID") or ""
            lh_id = os.getenv("FABRIC_LAKEHOUSE_ID") or os.getenv("FABRIC_LAKEHOUSE_NAME") or ""
            if lh_id and not (len(lh_id) == 36 and lh_id.count("-") == 4):
                try:
                    from agent.fabric_api_client import FabricAPIClient
                    client = FabricAPIClient()
                    resolved = client.resolve_lakehouse_id_by_name(ws_id, lh_id)
                    if resolved:
                        lh_id = resolved
                except Exception:
                    pass
            lines.append(resolve_path_fabric_pyspark_helper(ws_id, lh_id))
        else:
            lines.append(resolve_path_pyspark_helper())
        lines.append("")
        lines.append(pyspark_production_helpers())
        lines.append("")
        lines.append(pyspark_iqr_bounds_helper())
        lines.append("")
        lines.append(pyspark_prefix_non_key_columns_helper())
        lines.append("")

    for ds_name, block in (plan.get("datasets") or {}).items():
        fn = f"transform_{_safe(ds_name)}"
        lines.append(f"def {fn}(df: DataFrame, all_dfs: Optional[dict] = None) -> DataFrame:")
        var = "out"
        lines.append(f"    {var} = df")

        # Manual review warnings in code (B3)
        for mr in plan.get("manual_review") or []:
            if mr.get("dataset") == ds_name:
                col = mr.get("column")
                if col:
                    lines.append(f"    logger.warning('Column {col} requires manual review — skipping automation')")

        for st in sorted(block.get("steps") or [], key=lambda x: int(x.get("order") or 0)):
            action = str(st.get("action") or "")
            col = st.get("column")
            lines.append(f"    # Step: {action} on {col}")
            for sl in _emit_spark(action, col, var, step_meta=st):
                lines.append(f"    {sl}")
        for sl in _emit_valid_values_spark(var, ds_name, rules):
            lines.append(f"    {sl}")
        for col in rules.get("non_nullable") or []:
            lines.append(f'    _warn_nulls_in_columns({var}, [{col!r}], "{ds_name}")')
        lines.append(f"    return {var}")
        lines.append("")

    lines.append("DATASETS = " + repr(list((plan.get("datasets") or {}).keys())))
    lines.append("")

    non_nullable = [str(c) for c in (rules.get("non_nullable") or []) if c]
    if manifest.get("datasets") or rel.get("joins"):
        for sl in emit_pyspark_output_contract(plan, manifest):
            lines.append(sl)
        lines.append("def run_pipeline(spark):")
        lines.append("    dfs = {}")
        for sl in emit_pyspark_load(plan, manifest):
            lines.append(f"    {sl}")
        for ds_name in (plan.get("datasets") or {}):
            fn = f"transform_{_safe(ds_name)}"
            lines.append(f'    if "{ds_name}" in dfs:')
            lines.append(f'        dfs["{ds_name}"] = {fn}(dfs["{ds_name}"], dfs)')
            if non_nullable:
                lines.append(f'        _warn_nulls_in_columns(dfs["{ds_name}"], {non_nullable!r}, "{ds_name}")')
            lines.append(f'        _log_row_count(dfs["{ds_name}"], "{ds_name}")')
        for sl in emit_pyspark_joins(plan):
            lines.append(f"    {sl}")
        for sl in emit_pyspark_write_outputs(plan, manifest):
            lines.append(f"    {sl}")
        lines.append("    return dfs, OUTPUT_PATHS")
        lines.append("")
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("try:")
        lines.append("    spark  # type: ignore[name-defined]")
        lines.append("except NameError:")
        lines.append("    from pyspark.sql import SparkSession")
        lines.append('    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("_dfs, _paths = run_pipeline(spark)")
    else:
        lines.append("# Fabric notebooks pre-inject 'spark'; fall back to creating one for local dev.")
        lines.append("try:")
        lines.append("    spark  # type: ignore[name-defined]")
        lines.append("except NameError:")
        lines.append("    from pyspark.sql import SparkSession")
        lines.append('    spark = SparkSession.builder.appName("AgentDharaETL").getOrCreate()')
        lines.append("logger.info('Empty pipeline executed successfully')")

    return "\n".join(lines)
