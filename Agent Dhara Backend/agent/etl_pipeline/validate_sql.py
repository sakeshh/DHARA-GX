from __future__ import annotations

import re
from typing import List, Tuple

_DANGEROUS = [
    (r"\bdrop\s+table\s+(?!.*\b\w*(?:_clean|_transformed|_stg|temp_|_temp)\b|.*#)", "contains DROP TABLE on non-staging/clean/transformed table — remove for safety"),
    (r"\btruncate\s+table\s+(?!.*\b\w*(?:_clean|_transformed|_stg|temp_|_temp)\b|.*#)", "contains TRUNCATE TABLE on non-staging/clean/transformed table — remove for safety"),
    (r"\bdelete\s+from\s+(?!.*\b\w*(?:_clean|_transformed|_stg|_dedup|temp_|etl_log|cte|_temp)\b|.*#)", "contains DELETE FROM on non-staging/clean/transformed table — remove for safety"),
    (r"\bxp_cmdshell\b", "contains xp_cmdshell — system command execution not allowed"),
    (r"\bopenrowset\b", "contains OPENROWSET — external data source access not allowed in execution mode"),
]


def _bracket_balance(source: str) -> List[str]:
    issues: List[str] = []
    depth = 0
    for ch in source:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                issues.append("unbalanced parentheses")
                break
    if depth != 0:
        issues.append("unclosed parentheses")
    return issues


def _has_fake_default_value(sql: str) -> bool:
    low = sql.lower()
    banned = ["'99999'", "'10120631.5'", "'1900-01-01'", "'19000101'"]
    for b in banned:
        start = 0
        while True:
            idx = low.find(b, start)
            if idx == -1:
                break
            # Skip if the line contains references to seeding metadata table etl_invalid_values
            line_start = low.rfind("\n", 0, idx) + 1
            line_end = low.find("\n", idx)
            if line_end == -1:
                line_end = len(low)
            line_content = low[line_start:line_end]
            if "etl_invalid_values" in line_content:
                start = idx + len(b)
                continue

            # Check downstream text for 'then null' within 150 characters
            downstream = low[idx + len(b):idx + len(b) + 150]
            if "then null" in downstream:
                then_idx = downstream.find("then null")
                if ";" not in downstream[:then_idx]:
                    start = idx + len(b)
                    continue
            return True
    return False


def validate_sql_basic_dict(source: str) -> dict:
    """Parse SQL and return structured validation result."""
    if not source or not source.strip():
        return {"valid": False, "error": "Empty SQL", "issues": ["empty sql"], "warnings": []}

    issues: List[str] = []
    warnings: List[str] = []
    low = source.lower()
    for pattern, msg in _DANGEROUS:
        if re.search(pattern, low):
            for line in low.splitlines():
                stripped = line.strip()
                if re.search(pattern, stripped) and not stripped.startswith("--"):
                    issues.append(msg)
                    break

    try:
        import sqlparse  # type: ignore

        parsed = sqlparse.parse(source)
        if not parsed or not parsed[0].tokens:
            return {"valid": False, "error": "Empty or unparseable SQL", "issues": ["Empty or unparseable SQL"], "warnings": []}
    except ImportError:
        pass  # structural checks only when sqlparse unavailable
    except Exception as e:
        return {"valid": False, "error": f"SQL validation error: {str(e)}", "issues": [str(e)], "warnings": []}

    issues.extend(_bracket_balance(source))
    tx_issues, tx_warnings = _tsql_transaction_blocks(source)
    issues.extend(tx_issues)
    warnings.extend(tx_warnings)

    # Strict DQ Rules Validation (Advisory Warnings)
    # 1. Reject pipeline defined but not used
    # Only flag if etl_rejects is referenced INSIDE stored procedure bodies
    # (beyond the shared infrastructure CREATE TABLE block).
    if "etl_rejects" in low:
        # Strip the shared infrastructure CREATE TABLE block for etl_rejects
        low_no_infra = re.sub(
            r"if\s+object_id\s*\(\s*'dbo\.etl_rejects'.*?end\s*;\s*go",
            "", low, flags=re.DOTALL | re.IGNORECASE
        )
        # Check if etl_rejects is still referenced (e.g. in procedure comments or statements)
        if "etl_rejects" in low_no_infra:
            pattern_rejects = r"insert\s+into\s+(?:dbo\s*\.\s*)?\[?etl_rejects\]?"
            if not re.search(pattern_rejects, low_no_infra):
                warnings.append("etl_rejects table is defined but never inserted into (reject pipeline not used)")
        
    # 2. Fake default values
    if _has_fake_default_value(source):
        warnings.append("contains hardcoded fake default values ('99999', '10120631.5', or '1900-01-01')")
        
    # 3. Wrong deduplication ordering/columns
    if re.search(r"over\s*\([^\)]*etl_created_at", low):
        warnings.append("deduplication partitions or orders by etl_created_at instead of a business column")
        
    # 4. SELECT DISTINCT * used for dedup
    if "select distinct *" in low:
        warnings.append("contains SELECT DISTINCT * instead of key-aware CTE deduplication")
        
    # 5. Non-production safe SELECT INTO
    into_matches = re.finditer(r"\bselect\b(?:(?!insert|update|delete|create|procedure|\bgo\b|declare|begin|end|commit|rollback)\b[\s\S])*?\binto\s+([\w\.\_\[\]#]+)", low)
    for match in into_matches:
        tbl = match.group(1).strip("[]")
        if not tbl.startswith("#") and not any(x in tbl for x in ("temp_", "staging", "log", "watermark", "reject")):
            warnings.append(f"contains SELECT INTO on clean/joined table '{tbl}' instead of CREATE VIEW or INSERT INTO")

    # 6. Destructive multi-column NULL update (data wipe pattern)
    if re.search(r"set\s+[\w\.\_\[\]#]+\s*=\s*null\s*,\s*[\w\.\_\[\]#]+\s*=\s*null", low):
        warnings.append("contains destructive multi-column NULL update statement (data wipe pattern)")

    # 7. Redundant/double casting
    if re.search(r"cast\(\s*(?:try_)?cast\(", low) or re.search(r"try_cast\(\s*(?:try_)?cast\(", low):
        warnings.append("contains redundant double CAST statements (e.g. CAST(CAST(...)))")
    if re.search(r"lower\(\s*cast\(\s*(?:ltrim|rtrim|replace|lower|upper|coalesce)", low):
        warnings.append("contains redundant nested CAST operations inside LOWER/LTRIM string wrappers")
        
    # 8. Email validation constraint check
    if "email" in low:
        if not any(pat in low for pat in ("%_@_%._%", "%_@_%._%")):
            warnings.append("Email column detected but missing format check constraint (e.g. Email LIKE '%_@_%._%')")
            
    # 9. Phone normalization & validation check
    if "phone" in low:
        if "replace" not in low:
            warnings.append("Phone column detected but missing symbol cleaning operations (nested REPLACE for spaces/dashes)")
        if not any(x in low for x in ("len(", "length(", "[^0-9]")):
            warnings.append("Phone column detected but missing validation checks (length >= 7 or only numeric digits)")

    # 10. Date parsing checks for OrderDate / CreatedDate
    if "orderdate" in low or "createddate" in low:
        if not any(x in low for x in ("try_convert(", "try_cast(", "to_date(", "to_datetime(")):
            warnings.append("Date columns detected but missing TRY_CAST/TRY_CONVERT date parsing or validation")

    # 11. Empty column wildcard check (symptom of empty metadata)
    if "select *, @run_id" in low or "select *, @batch_id" in low or "select *," in low:
        warnings.append("contains empty column wildcard select list (symptom of empty metadata)")

    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
    }


def _tsql_transaction_blocks(source: str) -> Tuple[List[str], List[str]]:
    """
    Returns (hard_issues, warnings).
    hard_issues  → block execution (genuine SQL Server parse errors)
    warnings     → advisory only, do not block execution
    """
    issues: List[str] = []
    warnings: List[str] = []
    low = source.lower()

    has_begin_try   = "begin try"    in low
    has_end_catch   = "end catch"    in low
    has_begin_tran  = "begin tran"   in low or "begin transaction" in low
    has_commit      = "commit"       in low or "commit transaction" in low
    has_rollback    = "rollback"     in low or "rollback transaction" in low

    # HARD BLOCK: SQL Server will fail to parse this
    if has_begin_try and not has_end_catch:
        issues.append("BEGIN TRY without matching END CATCH — SQL Server will reject this script")

    # ADVISORY: valid but worth flagging
    if has_begin_tran and not has_commit:
        warnings.append("BEGIN TRANSACTION without COMMIT TRANSACTION — verify rollback is intentional")
    if (has_commit or has_rollback) and not has_begin_tran:
        warnings.append("COMMIT/ROLLBACK TRANSACTION found without BEGIN TRANSACTION")

    return issues, warnings


def validate_sql_basic(source: str) -> Tuple[bool, List[str]]:
    result = validate_sql_basic_dict(source)
    if result.get("valid"):
        return True, []
    errs = list(result.get("issues") or [])
    if result.get("error") and not errs:
        errs.append(str(result["error"]))
    return False, errs


def validate_for_execution(sql: str) -> dict:
    """
    Stricter pre-execution gate on top of validate_sql_basic_dict().
    Additionally checks:
    - SQL is not empty after stripping comments
    - No raw credential patterns (password=, pwd=, connectionstring= in non-comment lines)
    - No EXEC xp_cmdshell
    - No OPENROWSET or BULK INSERT pointing to external paths
    - No USE [database] switching mid-script
    Returns {"valid": bool, "issues": list[str], "warnings": list[str]}
    """
    issues: list[str] = []
    warnings: list[str] = []

    if not sql or not sql.strip():
        return {"valid": False, "issues": ["SQL script is empty"], "warnings": []}

    # Strip single-line comments (-- comment)
    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
    # Strip multi-line comments (/* comment */)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)

    if not cleaned.strip():
        return {"valid": False, "issues": ["SQL script is empty after stripping comments"], "warnings": []}

    cleaned_lower = cleaned.lower()

    # Raw credential patterns
    if re.search(r"\b(password|pwd|connectionstring)\s*=", cleaned_lower):
        issues.append("contains raw credential patterns (password, pwd, or connectionstring)")

    # System command execution
    if re.search(r"\bxp_cmdshell\b", cleaned_lower):
        issues.append("contains xp_cmdshell — system command execution not allowed")

    # OPENROWSET or BULK INSERT
    if re.search(r"\bopenrowset\b", cleaned_lower):
        issues.append("contains OPENROWSET — external data source access not allowed in execution mode")
    if re.search(r"\bbulk\s+insert\b", cleaned_lower):
        issues.append("contains BULK INSERT — external data source access not allowed in execution mode")

    # DB switching switching mid-script
    if re.search(r"\buse\s+\[?\w+\]?\b", cleaned_lower):
        issues.append("contains USE statement — database switching not allowed mid-script")

    # Basic validations
    basic_res = validate_sql_basic_dict(sql)
    if not basic_res.get("valid", False):
        for iss in basic_res.get("issues", []):
            if iss not in issues:
                issues.append(iss)
        if basic_res.get("error") and basic_res["error"] not in issues:
            issues.append(basic_res["error"])

    # Also collect warnings
    for warn in basic_res.get("warnings", []):
        if warn not in warnings:
            warnings.append(warn)

    return {
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
    }
