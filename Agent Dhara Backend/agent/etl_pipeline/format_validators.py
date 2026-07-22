"""
format_validators.py - Pre-codegen validation for dataset blob formats and options.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List
from agent.etl_pipeline.format_capabilities import PYSPARK_FORMAT_CAPABILITIES, get_capability

logger = logging.getLogger("agent.format_validators")

def validate_dataset_format_entry(entry: Dict[str, Any]) -> None:
    """
    Validates format availability and format-specific configuration options.
    Raises ValueError for unsupported formats or missing required parameters.
    """
    fmt = str(entry.get("format") or "csv").strip().lower()
    if fmt in ("sql_table", "jdbc"):
        return
    
    cap = get_capability(fmt)
    if not cap or not cap.get("supported"):
        raise ValueError(
            f"Unsupported PySpark format for blob source: '{fmt}'. "
            f"Supported formats: {sorted(PYSPARK_FORMAT_CAPABILITIES.keys())}"
        )
        
    if fmt == "xml":
        options = entry.get("options") or {}
        row_tag = options.get("row_tag") or options.get("rowTag")
        if not row_tag:
            logger.warning(f"Dataset '{entry.get('location')}' (XML) missing options.row_tag; defaulting to 'row'.")
            
    if fmt in ("xlsx", "xls"):
        options = entry.setdefault("options", {})
        if "sheet_name" not in options and "sheet" not in options:
            options["sheet_name"] = 0

def validate_plan_formats(plan: Dict[str, Any]) -> List[str]:
    """
    Scans a plan's datasets and returns warning strings for conditional or package-dependent formats.
    """
    warnings: List[str] = []
    datasets = (plan.get("connector_manifest") or {}).get("datasets") or plan.get("datasets") or {}
    for ds_name, ds_info in datasets.items():
        if isinstance(ds_info, dict):
            fmt = str(ds_info.get("format") or "").strip().lower()
            cap = get_capability(fmt)
            if cap and not cap.get("fabric_ready"):
                pkg = cap.get("requires_package")
                warnings.append(
                    f"Dataset '{ds_name}' uses format '{fmt}' which requires package '{pkg}' attached to your Fabric Spark environment."
                )
    return warnings
