"""
Post-generation structural validators for each codegen engine.
Returns (ok: bool, errors: list[str])
"""
from __future__ import annotations

import ast
import json
import re
from typing import Tuple, List, Dict, Any, Callable

def validate_python(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str]]:
    errors = []
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append(f"SyntaxError line {e.lineno}: {e.msg}")
        return False, errors
    
    required_patterns = [
        (r"def transform_", "Missing transform_<dataset> function"),
        (r"logging\.getLogger", "Missing logger"),
        (r"df\.copy\(\)", "Missing df.copy() — mutation safety"),
    ]
    for pattern, msg in required_patterns:
        if not re.search(pattern, code):
            errors.append(msg)
    
    # Forbidden patterns
    forbidden = [
        (r"\beval\b", "eval() is forbidden"),
        (r"\bexec\b", "exec() is forbidden"),
        (r"\bos\.system\b", "os.system() is forbidden"),
        (r"pd\.read_csv\(['\"].*\.xml", "read_csv on .xml file — use read_xml"),
    ]
    for pattern, msg in forbidden:
        if re.search(pattern, code):
            errors.append(msg)
            
    if never_drop_rows:
        if ".dropna(" in code:
            errors.append("Validation failed: '.dropna()' is forbidden when never_drop_rows is enabled.")
        if ".drop(" in code and ("axis=0" in code or "index=" in code):
            errors.append("Validation failed: Row dropping '.drop()' is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors

def validate_tsql(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str]]:
    errors = []
    upper = code.upper()
    
    # Must-have structural elements
    if "CREATE TABLE IF NOT EXISTS" in upper:
        errors.append("Invalid T-SQL: CREATE TABLE IF NOT EXISTS (use sys.tables guard)")
    if "CREATE OR REPLACE" in upper:
        errors.append("Invalid T-SQL: CREATE OR REPLACE is MySQL syntax")
    if "AUTO_INCREMENT" in upper:
        errors.append("Invalid T-SQL: AUTO_INCREMENT is MySQL syntax, use IDENTITY")
    if "ON CONFLICT" in upper:
        errors.append("Invalid T-SQL: ON CONFLICT is PostgreSQL syntax")
    if "SERIAL" in upper and "SERIAL" not in upper.replace("SERIAL", ""):
        errors.append("Possible SERIAL usage — invalid in T-SQL, use IDENTITY(1,1)")
    
    required = [
        ("ETL_LOG", "Missing dbo.etl_log DDL"),
        ("ETL_REJECTS", "Missing dbo.etl_rejects DDL"),
        ("SCOPE_IDENTITY", "Missing SCOPE_IDENTITY() for @run_id capture"),
        ("BEGIN TRY", "Missing BEGIN TRY block"),
        ("BEGIN CATCH", "Missing BEGIN CATCH block"),
        ("ROLLBACK", "Missing ROLLBACK in CATCH block"),
    ]
    for keyword, msg in required:
        if keyword not in upper:
            errors.append(msg)
            
    if never_drop_rows:
        if "DELETE FROM" in upper or "TRUNCATE TABLE" in upper:
            errors.append("Validation failed: DELETE/TRUNCATE is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors

def validate_pyspark(code: str, never_drop_rows: bool = False) -> Tuple[bool, List[str]]:
    errors = []
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors.append(f"SyntaxError line {e.lineno}: {e.msg}")
        return False, errors
    
    if "spark.read.csv" in code and ".xml" in code:
        errors.append("read.csv on .xml file — use XML format reader")
    if "_resolve_data_path" not in code:
        errors.append("Missing _resolve_data_path helper")
    if "SparkSession" not in code:
        errors.append("Missing SparkSession creation")
        
    if never_drop_rows:
        if ".dropna(" in code or "dropna" in code:
            errors.append("Validation failed: '.dropna()' is forbidden when never_drop_rows is enabled.")
            
    return len(errors) == 0, errors

def validate_adf(obj: dict, never_drop_rows: bool = False) -> Tuple[bool, List[str]]:
    errors = []
    if not isinstance(obj, dict):
        errors.append("ADF output is not a JSON object")
        return False, errors
    if "flows" not in obj and "pipeline" not in obj:
        errors.append("ADF output missing 'flows' or 'pipeline' key")
        
    if never_drop_rows:
        # Check if ADF has any delete or filter transforms that discard rows
        flows = obj.get("flows") or []
        for flow in flows:
            for transform in flow.get("transformations") or []:
                if transform.get("type") in ("filter", "alterRow"):
                    # Check if delete is enabled on alterRow
                    props = transform.get("typeProperties") or {}
                    if props.get("delete") or transform.get("type") == "filter":
                        errors.append(f"Validation failed: transformation '{transform.get('name')}' uses filtering/deletion which is forbidden when never_drop_rows is enabled.")
                        
    return len(errors) == 0, errors

VALIDATORS: Dict[str, Callable[[Any], Tuple[bool, List[str]]]] = {
    "python": validate_python,
    "sql-tsql": validate_tsql,
    "sql-ansi": validate_tsql,  # same guard set
    "pyspark": validate_pyspark,
    "adf": validate_adf,
}

def get_validator(engine_key: str) -> Callable[[Any], Tuple[bool, List[str]]]:
    val = VALIDATORS.get(engine_key)
    if not val:
        raise ValueError(f"Unrecognized codegen engine key: '{engine_key}'. Valid engines: {list(VALIDATORS.keys())}")
    return val
