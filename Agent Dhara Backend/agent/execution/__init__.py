from __future__ import annotations

from agent.execution.execution_target import ExecutionEngine, ExecutionPlan, ExecutionResult, Executor
from agent.execution.executor_registry import execute_plan, get_executor, register_executor
from agent.execution.azure_sql_executor_adapter import AzureSQLExecutorAdapter

# Auto-register Azure SQL executor adapter
register_executor(ExecutionEngine.AZURE_SQL, AzureSQLExecutorAdapter)

__all__ = [
    "ExecutionEngine",
    "ExecutionPlan",
    "ExecutionResult",
    "Executor",
    "register_executor",
    "get_executor",
    "execute_plan",
    "AzureSQLExecutorAdapter",
]
