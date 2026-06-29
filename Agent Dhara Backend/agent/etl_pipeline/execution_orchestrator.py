"""
ETL execution orchestrator for Agent Dhara.
Sits between etl_handlers.py code output and the Azure SQL executor.
Handles approval logic, calls executor, runs post-execution reconciliation,
and packages results for governance/report attachment.
"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from agent.etl_pipeline.validate_sql import validate_sql_basic
from agent.azure_sql_executor import (
    check_requires_approval,
    get_connection,
)
from agent.execution import execute_plan, ExecutionPlan, ExecutionEngine

logger = logging.getLogger("agent.etl_pipeline.execution_orchestrator")


def orchestrate_sql_execution(
    sql: str,
    *,
    session_id: str,
    run_id: str | None = None,
    approved: bool = False,
    dry_run: bool = False,
    connection_string: str | None = None,
    pre_execution_counts: dict | None = None,
    assessment: dict | None = None,
    timeout_s: int = 120,
) -> dict:
    """
    Coordinate SQL generation output -> approval gate -> execution -> reconciliation.
    """
    rid = run_id or str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).isoformat()

    # 1. Validate SQL with validate_sql_basic
    valid, errors = validate_sql_basic(sql)
    if not valid:
        return {
            "ok": False,
            "stage": "validation",
            "run_id": rid,
            "session_id": session_id,
            "approved": approved,
            "dry_run": dry_run,
            "validation_errors": errors,
            "requires_approval": False,
            "ops_found": [],
            "execution": {},
            "post_execution_summary": {
                "transaction_committed": False,
                "total_rows_affected": 0,
                "total_duration_ms": 0.0,
                "batch_count": 0,
                "row_deltas": {},
                "rollback_reason": "SQL basic validation failed",
            },
            "timestamp_utc": now_str,
        }

    # 2. Check approval requirement via check_requires_approval()
    app_check = check_requires_approval(sql)
    if app_check["requires_approval"] and not approved and not dry_run:
        return {
            "ok": False,
            "stage": "approval_required",
            "run_id": rid,
            "session_id": session_id,
            "approved": approved,
            "dry_run": dry_run,
            "validation_errors": [],
            "requires_approval": True,
            "ops_found": app_check["ops_found"],
            "execution": {},
            "post_execution_summary": {
                "transaction_committed": False,
                "total_rows_affected": 0,
                "total_duration_ms": 0.0,
                "batch_count": 0,
                "row_deltas": {},
                "rollback_reason": "Approval required",
            },
            "timestamp_utc": now_str,
        }

    # 3. Call execution registry
    plan = ExecutionPlan(
        plan_id=rid,
        engine=ExecutionEngine.AZURE_SQL,
        code=sql,
        requires_approval=app_check["requires_approval"],
        destructive_ops=app_check["ops_found"],
    )
    exec_result = execute_plan(
        plan,
        connection_string=connection_string,
        dry_run=dry_run,
        approved=approved,
        timeout_s=timeout_s,
        run_id=rid,
    )
    exec_res = {
        "ok": exec_result.ok,
        "run_id": exec_result.run_id,
        "total_rows_affected": exec_result.rows_affected,
        "total_duration_ms": exec_result.duration_ms,
        "transaction_committed": exec_result.committed,
        "rollback_reason": exec_result.error,
        "batches_run": len(exec_result.batch_results),
        "batch_results": exec_result.batch_results,
        "artifacts": exec_result.artifacts,
    }

    # 4. Build post_execution_summary
    committed = exec_res.get("transaction_committed", False)
    total_rows = exec_res.get("total_rows_affected", 0)
    duration = exec_res.get("total_duration_ms", 0.0)
    batch_count = exec_res.get("batches_run", 0)
    rollback_reason = exec_res.get("rollback_reason")

    row_deltas = {}
    post_validation_report = {"ok": True, "deltas": {"improvements": [], "regressions": []}}
    if pre_execution_counts and committed and not dry_run:
        table_names = list(pre_execution_counts.keys())
        post_execution_counts = build_pre_execution_counts(table_names, connection_string)
        for t in table_names:
            before = pre_execution_counts[t]
            after = post_execution_counts.get(t)
            if before is not None and after is not None:
                delta = after - before
            else:
                delta = None
            row_deltas[t] = {
                "before": before,
                "after": after,
                "delta": delta,
            }
        
        # Run post-ETL validation (Component 14)
        from agent.post_etl_validator import run_post_etl_validation
        try:
            post_validation_report = run_post_etl_validation(
                target_tables=table_names,
                connection_string=connection_string,
                pre_assessment=assessment
            )
        except Exception as e:
            logger.debug(f"Post-ETL validation run failed: {e}")

    post_execution_summary = {
        "transaction_committed": committed,
        "total_rows_affected": total_rows,
        "total_duration_ms": duration,
        "batch_count": batch_count,
        "row_deltas": row_deltas,
        "rollback_reason": rollback_reason,
        "post_etl_validation": post_validation_report,
    }

    return {
        "ok": exec_res.get("ok", False),
        "stage": "execution",
        "run_id": rid,
        "session_id": session_id,
        "approved": approved,
        "dry_run": dry_run,
        "validation_errors": [],
        "requires_approval": app_check["requires_approval"],
        "ops_found": app_check["ops_found"],
        "execution": exec_res,
        "post_execution_summary": post_execution_summary,
        "timestamp_utc": now_str,
    }


def _safe_bracket_quote(name: str) -> str:
    """
    Safely bracket-quote a table or schema name, escaping any ']' by doubling it.
    If the name has parts separated by '.', each part is quoted and escaped.
    """
    if not name:
        return ""
    parts = name.split(".")
    quoted_parts = []
    for part in parts:
        part = part.strip()
        if part.startswith("[") and part.endswith("]"):
            part = part[1:-1]
        escaped = part.replace("]", "]]")
        quoted_parts.append(f"[{escaped}]")
    return ".".join(quoted_parts)


def build_pre_execution_counts(
    table_names: list[str],
    connection_string: str | None = None,
) -> dict:
    """
    Run SELECT COUNT(*) FROM <table> for each table_name.
    Return {"table_name": count_or_None, ...}. Never raise.
    """
    counts = {}
    if not table_names:
        return counts

    conn = None
    try:
        conn = get_connection(connection_string)
        cursor = conn.cursor()
        for table in table_names:
            safe_table = _safe_bracket_quote(table)
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {safe_table}")
                row = cursor.fetchone()
                counts[table] = row[0] if row else None
            except Exception:
                counts[table] = None
    except Exception:
        for table in table_names:
            counts[table] = None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return counts
