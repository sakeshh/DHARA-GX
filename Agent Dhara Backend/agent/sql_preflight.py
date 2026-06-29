"""
Preflight validation and linting for generated SQL code.
Uses sqlfluff as an optional dependency for checking dialect correctness.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger("agent.sql_preflight")


def lint_generated_sql(sql: str, dialect: str = "tsql") -> Dict[str, Any]:
    """
    Lints the generated SQL using sqlfluff if available.
    Returns:
        Dict: {
            "ok": bool,
            "errors": List[Dict[str, Any]],
            "message": str
        }
    """
    if not sql or not sql.strip():
        return {"ok": True, "errors": [], "message": "SQL is empty."}

    try:
        import sqlfluff
    except ImportError:
        logger.debug("sqlfluff is not installed; skipping SQL linting preflight checks.")
        return {
            "ok": True,
            "errors": [],
            "message": "sqlfluff not installed. Linting skipped."
        }

    try:
        # sqlfluff.lint returns a list of violations (dicts)
        violations = sqlfluff.lint(sql, dialect=dialect)
        if not violations:
            return {"ok": True, "errors": [], "message": "SQL linting passed."}

        errors = []
        for v in violations:
            # Normalize violation keys
            errors.append({
                "line": v.get("line_no"),
                "column": v.get("line_pos"),
                "code": v.get("code"),
                "description": v.get("description")
            })

        return {
            "ok": len(errors) == 0,
            "errors": errors,
            "message": f"SQL linting failed with {len(errors)} violations."
        }
    except Exception as e:
        logger.warning("Error running sqlfluff lint: %s", e)
        return {
            "ok": True,
            "errors": [],
            "message": f"sqlfluff failed internally: {e}. SQL skipped."
        }
