"""
Deterministic file readers dispatcher for PySpark ETL.
"""
from __future__ import annotations
from typing import Any, Dict

def read_csv(path_expr: str) -> str:
    return f"spark.read.option('header', 'true').option('inferSchema', 'true').csv({path_expr})"

def read_tsv(path_expr: str) -> str:
    return f"spark.read.option('header', 'true').option('inferSchema', 'true').option('delimiter', '\\t').csv({path_expr})"

def read_json(path_expr: str) -> str:
    return f"spark.read.json({path_expr})"

def read_parquet(path_expr: str) -> str:
    return f"spark.read.parquet({path_expr})"

def read_xml(path_expr: str, row_tag: str = "row") -> str:
    return (
        f'spark.read.format("com.databricks.spark.xml")'
        f'.option("rowTag", "{row_tag}").load({path_expr})'
    )

def read_excel(path_expr: str) -> str:
    return f'spark.read.format("com.crealytics.spark.excel").option("header", "true").load({path_expr})'

DISPATCH = {
    "csv": read_csv,
    "tsv": read_tsv,
    "json": read_json,
    "parquet": read_parquet,
    "xml": read_xml,
    "xlsx": read_excel,
    "xls": read_excel,
}

def get_pyspark_read_snippet(path_expr: str, file_format: str, options: Dict[str, Any] | None = None) -> str:
    fmt = str(file_format).strip().lower()
    options = options or {}
    
    if fmt == "xml":
        row_tag = options.get("row_tag") or options.get("rowTag") or "row"
        return read_xml(path_expr, row_tag=row_tag)
        
    dispatcher = DISPATCH.get(fmt)
    if dispatcher:
        return dispatcher(path_expr)
        
    # Default fallback
    return read_csv(path_expr)
