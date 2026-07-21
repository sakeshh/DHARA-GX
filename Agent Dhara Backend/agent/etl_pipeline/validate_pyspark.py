"""
PySpark ETL validation: syntax + no pandas + plan column references + I/O sanity.
"""
from __future__ import annotations

import ast
import re
from typing import Any, Dict, List, Set, Tuple

from agent.etl_pipeline.validate_python import validate_etl_python_source


def _plan_columns(plan: Dict[str, Any]) -> Set[str]:
    cols: Set[str] = set()
    for block in (plan.get("datasets") or {}).values():
        for st in (block or {}).get("steps") or []:
            c = st.get("column")
            if c:
                cols.add(str(c))
    return cols


def _check_spark_session_import(source: str, tree: ast.AST) -> List[str]:
    errs: List[str] = []
    uses_session = "SparkSession" in source and (
        "SparkSession.builder" in source or "SparkSession(" in source
    )
    if not uses_session:
        return errs
    imported = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("pyspark.sql"):
            for n in node.names or []:
                if n.name == "SparkSession":
                    imported = True
        elif isinstance(node, ast.Import):
            for n in node.names or []:
                if (n.name or "").endswith("SparkSession") or n.name == "pyspark.sql":
                    imported = True
    if not imported:
        errs.append(
            "SparkSession is used but not imported — add: from pyspark.sql import SparkSession"
        )
    return errs


def _check_pyspark_imports(source: str, tree: ast.AST) -> List[str]:
    errs: List[str] = []
    
    # Check DataFrame import
    if "DataFrame" in source:
        imported = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "") in ("pyspark.sql", "pyspark.sql.dataframe", "pyspark.sql.types"):
                for n in node.names or []:
                    if n.name in ("DataFrame", "*"):
                        imported = True
            elif isinstance(node, ast.Import):
                for n in node.names or []:
                    if n.name == "pyspark.sql" or n.name == "pyspark":
                        imported = True
        if not imported:
            errs.append("DataFrame is referenced but not imported — add: from pyspark.sql import DataFrame")
            
    # Check functions as F import
    if "F." in source:
        imported = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "") in ("pyspark.sql", "pyspark.sql.functions"):
                for n in node.names or []:
                    if (n.name == "functions" and n.asname == "F") or (n.name == "F") or n.name == "*":
                        imported = True
            elif isinstance(node, ast.Import):
                for n in node.names or []:
                    if (n.name == "pyspark.sql.functions" and n.asname == "F") or (n.name == "pyspark.sql.functions" and n.name == "F"):
                        imported = True
        if not imported:
            errs.append("functions as F is referenced but not imported — add: from pyspark.sql import functions as F")
            
    return errs


def _check_never_drop_rows(source: str, plan: Dict[str, Any] | None) -> List[str]:
    if not plan:
        return []
    rules = plan.get("business_rules") or {}
    if not rules.get("never_drop_rows"):
        return []
    errs: List[str] = []
    low = source.lower()
    if re.search(r'how\s*=\s*["\']inner["\']', low) or re.search(
        r'\.join\s*\([^)]*how\s*=\s*["\']inner["\']', low, re.I
    ):
        errs.append(
            "never_drop_rows: do not use inner join — write per-dataset outputs or use left join only"
        )
    if re.search(r"\.dropna\s*\(\s*\)", source) or re.search(
        r"\.dropna\s*\(\s*subset\s*=", source
    ):
        errs.append("never_drop_rows: do not use dropna() — use fill/coalesce instead")
    
    import ast
    try:
        tree = ast.parse(source)
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                child.parent = parent
                
        lines = source.splitlines()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in ("filter", "where"):
                    curr = node
                    is_assigned_or_returned = False
                    while hasattr(curr, "parent"):
                        curr = curr.parent
                        if isinstance(curr, (ast.Assign, ast.Return)):
                            is_assigned_or_returned = True
                            break
                    if is_assigned_or_returned:
                        line = lines[node.lineno - 1]
                        if not any(x in line for x in ("== 1", "==1", "row_number", "_rn", "isNotNull")):
                            errs.append(f"never_drop_rows: generic filtering/selection using .{node.func.attr}() is restricted unless approved: {line.strip()}")
    except Exception:
        pass
        
    return errs


def _check_bare_deduplicate_spark(source: str, plan: Dict[str, Any] | None) -> List[str]:
    if not plan:
        return []
    errs: List[str] = []
    has_keys = bool(plan.get("business_keys"))
    has_row_level = False
    for ds_name, block in (plan.get("datasets") or {}).items():
        steps = (block or {}).get("steps") or []
        for s in steps:
            if s.get("action") == "deduplicate":
                col = str(s.get("column") or "").lower().strip()
                if col in ("row-level", "[row-level]"):
                    has_row_level = True
                else:
                    has_keys = True
    if has_keys and not has_row_level:
        if "dropDuplicates()" in source.replace(" ", "") or "drop_duplicates()" in source.replace(" ", ""):
            errs.append("Reject bare dropDuplicates(): key-aware deduplication must partition or subset when near duplicates are possible")
    return errs


def _check_row_count_logging_spark(source: str) -> List[str]:
    if "def transform" not in source:
        return []
    errs: List[str] = []
    if "log_row_count" not in source and "logger.info" not in source and "count()" not in source:
        errs.append("Row-count logging is required before and after major stages")
    return errs


def _check_resolve_helper_defined(source: str) -> Tuple[List[str], List[str]]:
    """Returns (hard_errors, warnings)"""
    if "_resolve_data_path(" not in source:
        return [], []
    if re.search(r"def\s+_resolve_data_path\s*\(", source):
        return _check_resolve_helper_quality(source), []
    # Not defined but used — only warn if abfss:// paths exist, hard error if no path at all
    if "abfss://" in source:
        return [], ["Advisory: _resolve_data_path used but not defined — ensure abfss:// paths are complete"]
    return ["_resolve_data_path() is used but not defined — add helper or use full abfss:// paths"], []


def _check_resolve_helper_quality(source: str) -> List[str]:
    """Reject stub helpers that cannot resolve Azure blob paths.
    Accepts:
      - Full abfss:// URL built from AZURE_STORAGE_ACCOUNT + DHARA_BLOB_CONTAINER
      - DHARA_BLOB_BASE_PATH / DHARA_BLOB_MOUNT env-based paths
      - /lakehouse/default prefix (Fabric native notebooks)
      - onelake.dfs.fabric.microsoft.com (Fabric OneLake)
      - Files/ or Tables/ relative paths (Fabric Lakehouse)
    """
    errs: List[str] = []
    # Catch bare `return f"abfss://{location}"` stubs (no account/container)
    if re.search(
        r'return\s+f?["\']abfss://\{location\}["\']',
        source,
        re.I,
    ) or re.search(r'return\s+f?["\']abfss://\{loc\}["\']', source, re.I):
        errs.append(
            "Incomplete _resolve_data_path: must use AZURE_STORAGE_ACCOUNT + "
            "DHARA_BLOB_CONTAINER (abfss://container@account.dfs.core.windows.net/...) "
            "or DHARA_BLOB_BASE_PATH — not abfss://{location} only"
        )
    if "_resolve_data_path" in source and "def _resolve_data_path" in source:
        body = source.split("def _resolve_data_path", 1)[-1][:2000]
        # Accepted patterns — any of these makes the helper valid
        _VALID_PATTERNS = (
            "/lakehouse/default",
            "onelake.dfs.fabric.microsoft.com",
            "AZURE_STORAGE_ACCOUNT",
            "dfs.core.windows.net",
            "DHARA_BLOB_BASE_PATH",
            "DHARA_BLOB_CONTAINER",
            "DHARA_BLOB_MOUNT",
        )
        has_valid = any(p in body for p in _VALID_PATTERNS)
        # Only fail if abfss:// appears as a construction target (not passthrough) without valid context
        if re.search(r"abfss://", body, re.I) and not has_valid:
            # Allow if it's only used in a startswith() passthrough check (not building a URL)
            passthrough_only = bool(
                re.search(r'startswith\s*\(\s*[(\[]?\s*["\']abfss://', body, re.I)
            ) and not re.search(r'f["\']abfss://', body, re.I)
            if not passthrough_only:
                errs.append(
                    "_resolve_data_path must build full abfss URLs with storage account and container, "
                    "use DHARA_BLOB_BASE_PATH, or return /lakehouse/default paths for Fabric"
                )
    return errs


def _check_dead_join_variables(source: str) -> List[str]:
    """Flag join results that are assigned but never written or returned."""
    errs: List[str] = []
    for m in re.finditer(r"(\w+)\s*=\s*[^=\n]*?\.join\s*\(", source):
        var = m.group(1)
        if var in ("dfs", "spark", "df"):
            continue
        uses = len(re.findall(rf"\b{re.escape(var)}\b", source))
        if uses <= 1:
            errs.append(
                f"Dead join: `{var}` is assigned from .join() but never written or returned — "
                "write it (e.g. joined_*.parquet) or remove the join"
            )
    return errs


def _check_io_antipatterns(source: str, plan: Dict[str, Any] | None) -> List[str]:
    errs: List[str] = []
    low = source.lower()

    # .xml read as csv
    if re.search(r'read\.csv\s*\([^)]*\.xml', low, re.I) or re.search(
        r'\.csv\s*\(\s*r?["\'][^"\']*\.xml', low, re.I
    ):
        errs.append("Do not use read.csv for .xml files — use spark-xml or manifest read_snippet_pyspark")

    if re.search(r'write\.csv\s*\([^)]*\.xml', low, re.I):
        errs.append("Do not use write.csv for .xml output paths — use parquet/json per manifest")

    manifest = (plan or {}).get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    for ds_name, ent in m_ds.items():
        if not isinstance(ent, dict):
            continue
        if ent.get("format") == "xml" and "read.csv" in source and ds_name in source:
            errs.append(f"Dataset '{ds_name}' is XML — remove read.csv usage for this dataset")

    # Bare filename loads without resolve helper when manifest has blob sources
    if m_ds and any(
        isinstance(e, dict) and e.get("source_type") == "blob_storage" for e in m_ds.values()
    ):
        if "_resolve_data_path" not in source and not re.search(
            r"abfss://|wasbs://", source, re.I
        ):
            if re.search(r'read\.(json|csv)\s*\(\s*r?["\'][\w./-]+\.(json|csv|xml)', source, re.I):
                errs.append(
                    "Blob sources detected — use _resolve_data_path(manifest location) "
                    "or abfss:// paths from connector_manifest, not bare filenames"
                )

    return errs


def _check_approx_quantile_usage(source: str) -> List[str]:
    errs = []
    if "approxQuantile" in source and "_iqr_bounds" not in source:
        errs.append("use _iqr_bounds helper instead of df.approxQuantile()[index] — unsafe on null columns")
    return errs


def _check_datasets_declaration(source: str) -> List[str]:
    errs = []
    if "DATASETS" not in source or not re.search(r"\bDATASETS\s*=\s*\[", source):
        errs.append("DATASETS = [...] declaration is missing at the module level")
    return errs


def _check_iqr_bounds_unpacking(source: str) -> List[str]:
    errs = []
    # Exclude the function definition itself to find if it is actually called
    called_source = source.replace("def _iqr_bounds", "")
    if "_iqr_bounds" in called_source:
        if not re.search(r"(\w+)\s*,\s*(\w+)\s*,\s*(\w+)\s*,\s*(\w+)\s*=\s*_iqr_bounds", source):
            errs.append("outlier bounds: always unpack all 4 variables from _iqr_bounds(df, col) helper: stats, iqr, lower, upper = _iqr_bounds(df, col)")
    return errs


def _check_semantic_rules(source: str) -> List[str]:
    errs = []
    if "contains('@')" in source or 'contains("@")' in source:
        # Check if email cleansing is happening in the same block/context
        if "email" in source.lower():
            errs.append("Semantic error: naive contains('@') used for email validation. Use rlike with proper email regex instead.")
    return errs


def _check_undefined_function_calls(tree: ast.AST) -> List[str]:
    """Flag calls to transform_* functions that are not defined in the source script."""
    defined_funcs: Set[str] = set()
    called_funcs: List[Tuple[str, int]] = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            defined_funcs.add(node.name)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_funcs.append((node.func.id, getattr(node, "lineno", 0)))
                
    errs: List[str] = []
    for func_name, lineno in called_funcs:
        if func_name.startswith("transform_") and func_name not in defined_funcs:
            errs.append(
                f"Undefined function '{func_name}' called at line {lineno} — "
                f"defined functions: {sorted(list(defined_funcs))}"
            )
    return errs


def validate_pyspark_source(
    source: str,
    plan: Dict[str, Any] | None = None,
) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not source or not source.strip():
        return False, ["empty source"]

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return False, [f"syntax: {e.msg} at line {e.lineno}"]

    ok_py, py_errs = validate_etl_python_source(source, plan)
    if not ok_py:
        errs.extend(
            e
            for e in py_errs
            if not e.startswith("disallowed import: os")
            and "disallowed import from 'os'" not in e
        )

    low = source.lower()
    if re.search(r"\bimport\s+pandas\b", low) or re.search(r"\bfrom\s+pandas\b", low):
        errs.append("PySpark script must not import pandas")
    if "pd." in source and "pyspark" not in low:
        errs.append("pandas-style pd.* usage detected — use pyspark.sql.functions")

    has_spark = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names or []:
                if (n.name or "").startswith("pyspark"):
                    has_spark = True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").startswith("pyspark"):
                has_spark = True

    if not has_spark and "SparkSession" not in source:
        errs.append("expected pyspark import or SparkSession usage")

    unsupported = re.findall(r"# Unsupported in pyspark template v1: (.+)", source)
    if unsupported:
        errs.append(f"Unsupported template actions detected: {', '.join(unsupported)}")

    resolve_errs, resolve_warnings = _check_resolve_helper_defined(source)
    errs.extend(resolve_errs)
    errs.extend(_check_dead_join_variables(source))
    errs.extend(_check_io_antipatterns(source, plan))
    errs.extend(_check_spark_session_import(source, tree))
    errs.extend(_check_pyspark_imports(source, tree))
    errs.extend(_check_undefined_function_calls(tree))
    errs.extend(_check_never_drop_rows(source, plan))
    errs.extend(_check_bare_deduplicate_spark(source, plan))
    errs.extend(_check_row_count_logging_spark(source))
    errs.extend(_check_approx_quantile_usage(source))
    errs.extend(_check_datasets_declaration(source))
    errs.extend(_check_iqr_bounds_unpacking(source))
    errs.extend(_check_semantic_rules(source))

    if plan:
        allowed = _plan_columns(plan)
        if allowed:
            quoted = set(re.findall(r"['\"]([a-zA-Z_][\w]*)['\"]", source))
            suspicious = [c for c in quoted if c.endswith("_outlier_flagged")]
            for c in suspicious:
                base = c.replace("_outlier_flagged", "")
                if base not in allowed and c not in allowed:
                    errs.append(f"column '{c}' not in plan — verify spelling")

    if errs:
        return False, errs
    return True, []
