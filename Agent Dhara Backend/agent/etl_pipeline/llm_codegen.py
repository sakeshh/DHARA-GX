"""
LLM-based ETL Code Generator.
Translates an approved ETL plan + assessment metadata into production-ready code
for Python, SQL, PySpark, and Azure Data Factory.
"""
from __future__ import annotations

import json
import os
import re
import time
import asyncio
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

# Per-plan-id locks — process-wide cross-thread coordination
_PLAN_LOCKS: dict[str, threading.Lock] = defaultdict(threading.Lock)
_PLAN_FAILURE_COUNTS: dict[str, int] = defaultdict(int)
_MAX_PLAN_FAILURES = 3  # after this, circuit-open for 5 min

try:
    from openai import AzureOpenAI, OpenAI, RateLimitError, APITimeoutError
except ImportError:
    AzureOpenAI = None
    OpenAI = None
    RateLimitError = None
    APITimeoutError = None

from agent.errors import ConnectorConfigError
from agent.model_config import load_llm_config, LLM_REQUEST_TIMEOUT, get_context_window
from agent.etl_pipeline.codegen_policy import llm_codegen_extra_context, plan_policy_block
from agent.etl_pipeline.io_snippets import (
    resolve_path_pyspark_helper,
    resolve_path_fabric_pyspark_helper,
)
from agent.etl_pipeline.payload_trimmer import trim_payload, _FLOOR_TRIM_CONFIG, _CODEGEN_TRIM_CONFIG
import logging
logger = logging.getLogger("agent.etl_pipeline.llm_codegen")

LLM_ERROR_PREFIX = "# Error"

class LLMInfraError(Exception):
    """Raised for LLM infrastructure errors like rate limits or timeouts."""
    pass

_CODE_CACHE = {}
_VALIDATOR_ACCEPTS_NDR: dict[str, bool] = {}
_FAILURE_CACHE: dict[str, tuple[str, float]] = {}  # key -> (error_msg, expiry_time)

def _is_failure_cached(cache_key: str) -> str | None:
    entry = _FAILURE_CACHE.get(cache_key)
    if entry and time.time() < entry[1]:
        return entry[0]  # return cached error
    return None

def _write_failure_cache(cache_key: str, error: str, ttl_seconds: int = 30):
    _FAILURE_CACHE[cache_key] = (error, time.time() + ttl_seconds)

_PYSPARK_REQUIRED_IMPORTS = [
    ("from pyspark.sql import SparkSession, DataFrame", ["SparkSession", "DataFrame"]),
    ("from pyspark.sql import functions as F", ["F."]),
    ("import logging", ["logging."]),
]

def _inject_pyspark_imports(code: str) -> str:
    """
    Ensure required PySpark module-level imports are present at the top of the file.
    If the LLM placed them inside function bodies, we inject them at the module level
    so that the import check always passes without needing a retry.
    """
    if not code or not code.strip():
        return code

    lines = code.splitlines(keepends=True)

    # Step 1: Determine the end of the module docstring block
    insert_at = 0
    i = 0
    if lines and lines[0].strip().startswith('"""'):
        # Multi-line or single-line module docstring
        if lines[0].strip().count('"""') >= 2 and len(lines[0].strip()) > 3:
            # Single-line docstring: e.g. """plan_id: ..."""
            insert_at = 1
            i = 1
        else:
            # Multi-line: scan forward until closing """
            i = 1
            while i < len(lines):
                if '"""' in lines[i]:
                    i += 1
                    break
                i += 1
            insert_at = i

    # Step 2: Skip any blank lines and comments immediately after the docstring
    while insert_at < len(lines):
        stripped = lines[insert_at].strip()
        if not stripped or stripped.startswith("#"):
            insert_at += 1
        else:
            break

    # Step 3: Check what is already in the header (first 40 lines after insert_at)
    header = "".join(lines[:min(insert_at + 40, len(lines))])

    to_inject = []
    # Only look at the true module-level header (lines before insert_at + 15 lines after)
    true_header = "".join(lines[:min(insert_at + 15, len(lines))])

    for import_line, triggers in _PYSPARK_REQUIRED_IMPORTS:
        # Only inject if any trigger token is referenced in the entire source
        if not any(t in code for t in triggers):
            continue
        # Check if this exact import statement is already in the header
        if import_line in true_header:
            continue
        to_inject.append(import_line + "\n")

    if not to_inject:
        return code

    injected = lines[:insert_at] + to_inject + ["\n"] + lines[insert_at:]
    return "".join(injected)


def _inject_pyspark_fabric_ids(code: str) -> str:
    """
    Ensure the fallback values in _resolve_data_path are correctly populated
    with the actual workspace_id and lakehouse_id from the environment.
    """
    if not code:
        return code
        
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
            
    # Locate ws_id = "" and lh_id = "" patterns and replace them
    code = re.sub(
        r'ws_id\s*=\s*["\']["\']',
        f'ws_id = "{ws_id}"',
        code
    )
    code = re.sub(
        r'lh_id\s*=\s*["\']["\']',
        f'lh_id = "{lh_id}"',
        code
    )
    return code



def _estimate_tokens(text: str | dict) -> int:
    if isinstance(text, dict):
        try:
            text = json.dumps(text, default=str)
        except Exception:
            text = str(text)
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text, disallowed_special=()))
    except (ImportError, Exception):
        is_json_like = text.lstrip().startswith(("{", "["))
        chars_per_token = 2 if is_json_like else 4
        return max(1, len(text) // chars_per_token)


# Actions the planner may emit — LLM must implement each one correctly for the target engine.
_PLAN_PARAMS = """
Each plan step includes a "params" dict — use it as the source of truth (not only evidence):
- params.fill_strategy: "mean" | "median" | "value" — for fill_or_drop / fill_nulls_simple
- params.fill_value: scalar when fill_strategy is "value" or precomputed mean/median
- params.outlier_method: "flag" | "clip" | "cap"
- params.outlier_iqr_multiplier: float (default 1.5)
- params.privacy: "hash" | "mask" | "exclude" for phone/privacy columns
- params.enforcement_mode: "flag" | "quarantine" for referential integrity steps
- params.execution_mode: "in_place" | "new_column" | "new_table"
"""

_PLAN_ACTIONS = """
Supported plan step actions (implement ALL steps in order per dataset):
- trim: strip whitespace on strings
- lowercase / uppercase: case normalization
- fill_or_drop / fill_nulls_simple: fill nulls (if never_drop_rows in business_rules, NEVER delete rows)
- cast_type: nullable integer use Int64 (pandas) / long (spark); preserve nulls
- coerce_numeric: safe numeric conversion
- parse_dates: safe datetime parsing
- sanitize_email: trim, lower, invalid emails -> null
- normalize_phone: digits only
- hash_phone: F.sha2(column.cast('string'), 256) for privacy (per business notes / manual_review)
- mask_phone: keep last 4 digits with *** prefix
- regex_replace: clean per plan note if present
- range_clip: bound numeric values (e.g. lower bound 0)
- clip_or_flag / flag_outliers: IQR-based outlier flag column (suffix _outlier_flagged)
- clip_outliers: IQR clip values to bounds
- cap_outliers: IQR replace outliers with median
- standardize_boolean: map yes/no/1/0/true/false to 0/1
- replace_values: map values per business_rules.valid_values when provided
- zero_to_null: replace 0 with null
- deduplicate: drop duplicate rows (subset column if column set, else full row)
- validate_referential_integrity_or_stage: emit validation/staging comments + checks, do not skip
"""

_BASE_RULES = """
UNIVERSAL RULES (mandatory):
1. Implement EVERY step in plan.datasets[*].steps in ascending "order". Do not skip or merge steps.
2. Read step["params"] for fill/outlier/privacy — match template codegen semantics.
3. Honor business_rules: never_drop_rows, required_columns, exclude_columns, non_nullable, valid_values, notes.
4. Preserve exact column name casing from the plan.
5. Add clear comments for manual_review items from the plan.
6. Production quality: logging.getLogger("agent_dhara"), guards for required columns, no placeholder TODOs for listed actions.
7. Output ONLY the artifact — no markdown fences, no prose before/after.
8. DATA COMPLETENESS MANDATE: For EVERY column in the plan, at minimum apply:
   - Whitespace trim (strings)
   - Type coercion to target_dtype
   - Null fill/flag per fill_strategy
   If a column has issues in the assessment but no explicit step was planned, ADD the
   most appropriate step from the supported actions list — DO NOT leave the column untouched.
9. NEVER emit placeholder comments like "# TODO: handle nulls" or "# Add cleaning here".
   Every step must be executable, production code.
10. BOOLEAN columns: ALWAYS standardize to 0/1 (int). Map: yes/true/1/y -> 1; no/false/0/n -> 0.
11. CATEGORY/ENUM columns: if valid_values is provided in business_rules, enforce it — 
    map unknowns to NULL or a 'Other' category, never leave invalid values in clean output.
"""

SYSTEM_PROMPTS: Dict[str, str] = {
    "python": f"""You are a senior data engineer writing production Python ETL with pandas.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

PYTHON REQUIREMENTS:
- Module docstring with plan_id summary.
- Imports: pandas, logging (and sys if needed). No os/subprocess/socket/shutil/ctypes/eval/exec.
- One transform_<dataset> function per dataset; each receives pd.DataFrame and returns pd.DataFrame.
- Start each function with df.copy(); use nullable Int64 for integer columns.
- Required columns: raise ValueError with clear message if missing.
- never_drop_rows: use fillna only, never dropna on rows.
- I/O: use connector_manifest read_snippet_python and write_snippet_python EXACTLY per dataset.
- NEVER read .xml with read_csv. Use read_xml for format=xml. NEVER write CSV to a .xml path.
- Use _resolve_data_path(location) helper when manifest shows blob paths (not bare filenames).
- if __name__ == "__main__": load_all_datasets / transform_all / run_joins / write_outputs from manifest.
- Executable, syntactically valid Python 3.10+.
""",
    "sql-tsql": f"""You are a senior data engineer writing production T-SQL ETL scripts.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

T-SQL REQUIREMENTS:
- Header comment block with plan_id.
- Idempotent T-SQL DDL (CRITICAL): NEVER use `CREATE TABLE IF NOT EXISTS` — that is MySQL/PostgreSQL syntax and is INVALID in T-SQL/SQL Server. For every infrastructure table (etl_log, etl_rejects, etl_watermark, clean tables, indexes, procedures), you MUST use the correct T-SQL idempotent guard pattern:
  - Tables: `IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TableName' AND schema_id = SCHEMA_ID('dbo')) CREATE TABLE dbo.TableName (...);`
  - Indexes: `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IndexName' AND object_id = OBJECT_ID('dbo.TableName')) CREATE NONCLUSTERED INDEX IndexName ON dbo.TableName (...);`
  - Procedures: `IF OBJECT_ID('dbo.ProcName', 'P') IS NOT NULL DROP PROCEDURE dbo.ProcName; GO` followed by `CREATE PROCEDURE dbo.ProcName AS BEGIN ... END; GO`
  Each outer script DDL statement (infrastructure tables, staging tables, procedures, views) MUST be followed by a GO batch separator on its own line. NEVER output GO inside a stored procedure body.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `dbo.etl_log` with columns `id INT IDENTITY(1,1) PRIMARY KEY`, `process_name VARCHAR(100) NOT NULL`, `start_time DATETIME NOT NULL`, `end_time DATETIME NULL`, `status VARCHAR(20) NOT NULL`, `error_message VARCHAR(MAX) NULL` using the T-SQL idempotent guard described above. 
  Inside each stored procedure's TRY block, you MUST first run the `INSERT INTO dbo.etl_log (process_name, start_time, status) VALUES ('...', GETDATE(), 'RUNNING');` statement. IMMEDIATELY AFTER this insert, define and set the batch run ID: `DECLARE @run_id INT = SCOPE_IDENTITY();`. NEVER declare `@run_id = SCOPE_IDENTITY();` before any insert statement has occurred in the procedure, as this returns NULL and breaks audit batch tracking. Wrap the block in a transaction. Commit on success and rollback on failure.
- Balanced Transactions: Every Try-Catch block MUST wrap data modifications in an explicit transaction block. Begin the transaction inside `BEGIN TRY` using `BEGIN TRANSACTION;` immediately after defining `@run_id`. Commit the transaction using `COMMIT TRANSACTION;` at the very end of the `BEGIN TRY` block (after all updates and logging are completed). At the beginning of the `BEGIN CATCH` block, you MUST verify if a transaction is still active and roll it back using: `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;`. Never omit `BEGIN TRANSACTION` if `COMMIT/ROLLBACK` are used, or vice versa, to ensure compilation succeeds.
- FORBIDDEN MySQL/PostgreSQL Syntax: NEVER output `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE`, `ON CONFLICT`, `RETURNING`, `ILIKE`, `SERIAL`, `AUTO_INCREMENT`, or any other non-T-SQL syntax. These are invalid in SQL Server and will cause the entire batch to fail with a hard rollback. T-SQL equivalents MUST be used at all times.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table `#<TableName>_Staging`.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `dbo.etl_default_values` and `dbo.etl_invalid_values` via `LEFT JOIN`s.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `dbo.Orders_Clean` for `dbo.Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at DATETIME DEFAULT GETDATE()`, `etl_updated_at DATETIME DEFAULT GETDATE()`, and `etl_batch_id INT`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM dbo.Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `dbo.Orders_Clean` and insert the fully transformed records from the staging table into `dbo.Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `dbo.etl_clean_<table_base_name>`.
- Master Orchestration: Generate a master coordinator procedure named `dbo.etl_main` that calls all the individual table cleaning stored procedures.
- Incremental Loading, Watermarking & Watermark Storage: Stored procedures and the main procedure must accept parameters `@load_type VARCHAR(20) = 'FULL'` and `@last_run DATETIME = NULL`. Generate DDL for `dbo.etl_watermark (process_name VARCHAR(100) PRIMARY KEY, last_run_time DATETIME NOT NULL)`. 
  If `@load_type = 'INCREMENTAL'` and `@last_run IS NULL`, retrieve it using `SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = '...';`. Ensure this `@last_run` filter is fully applied to the copy queries (e.g. `WHERE watermark_col > @last_run`). Update the watermark using `MERGE` upon successful completion.
- Performance Indexing: When creating the clean target table, add DDL commands to create NONCLUSTERED indexes on the primary keys, join/relationship keys, and watermark columns.
- Outlier Mitigation Safety (Catalog Checks): Implement the outlier flagging logic using a reusable stored procedure `dbo.sp_flag_outliers_iqr` that computes IQR and updates outlier flags dynamically. The procedure takes exactly two arguments: `@table_name` and `@column_name`. Validate that input table and column parameters exist in `sys.tables` and `sys.columns` (or `tempdb.sys.columns` for `#` temp tables) before dynamic SQL executions. NEVER call the procedure with extra parameters.
- Reusable Outlier Procedure Call: When executing IQR flagging on a column, invoke it exactly as `EXEC dbo.sp_flag_outliers_iqr '#TableName_Staging', 'ColumnName'`. Do not repeat the execution or call it multiple times for the same column. Only run outlier stored procedures on numeric/metric columns. NEVER execute it on string identifiers, phones, or emails.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: If applying `LTRIM`, `RTRIM`, `LOWER`, or `UPPER` on a non-string column (such as numeric/date columns), first cast the column explicitly to a string type (e.g. `CAST(col AS NVARCHAR(MAX))`) before calling the string function, then cast back to the target type. (e.g. `TRY_CAST(NULLIF(LTRIM(RTRIM(CAST(credits AS NVARCHAR(50)))), '') AS INT)`).
- Rejects & Quarantine Logging: Enforce the use of `dbo.etl_rejects` table. Generate DDL to create the rejects logging table `dbo.etl_rejects` using the T-SQL idempotent guard (NEVER `CREATE TABLE IF NOT EXISTS`):
  ```sql
  IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'etl_rejects' AND schema_id = SCHEMA_ID('dbo'))
  CREATE TABLE dbo.etl_rejects (
      id INT IDENTITY(1,1) PRIMARY KEY,
      table_name VARCHAR(100) NOT NULL,
      rejected_row_data NVARCHAR(MAX) NOT NULL,
      reject_reason VARCHAR(255) NOT NULL,
      etl_batch_id INT NOT NULL,
      rejected_at DATETIME DEFAULT GETDATE()
  );
  GO
  ```
  For any row that fails validation format constraints (e.g. invalid date formats, invalid email patterns) or referential integrity (joins), you MUST insert the violating records into `dbo.etl_rejects` and delete them from the staging table prior to any transformation/cast steps. Use `(SELECT * FROM staging_alias r2 WHERE r2.[pk] = r.[pk] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)` to serialize the violating row data. Perform validation deletes before applying transformations/casts to preserve the original invalid values.
- Default Value Sanity & No Fake/Placeholder Defaults: Seed defaults/invalid values dynamically. Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Replace literal default values with lookup queries using `TRY_CAST(default_value AS <type>)` from `dbo.etl_default_values` (dynamic casting based on column data type).
- Multi-Format Date Parsing: When parsing date columns, use a coalesced chain of `TRY_CONVERT` with different format styles (e.g. style 120, 103, 101, 111) to check if the date is valid. For example: `COALESCE(TRY_CONVERT(DATETIME, [date_col], 120), TRY_CONVERT(DATETIME, [date_col], 103), TRY_CONVERT(DATETIME, [date_col], 101), TRY_CONVERT(DATETIME, [date_col], 111))`. If all conversion attempts fail and the value is not empty/null, treat it as a format validation failure, insert it into `dbo.etl_rejects`, and delete it from the staging table.
- Business-Key Deduplication: For row-level deduplication, partition by the candidate primary key and business keys (names containing `id`, `key`, `email`, `code`) instead of all non-key columns, and order the partition descending by watermark column (`ORDER BY <watermark> DESC`) to preserve the latest record. Perform deduplication inside the initial staging `INSERT INTO ... SELECT` statement using a CTE.
- Use bracket quoting [column] and TRY_CAST / TRY_CONVERT for safe casts.
- never_drop_rows: UPDATE/SET only, no DELETE FROM for data quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `dbo.etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `dbo.etl_rejects`, and delete them from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS NVARCHAR(MAX)))) AS NVARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS NVARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
- Round-number anomaly handling: flag/tag round-number anomalies if required.
- Month-end date clumping: flag month-end date clumping if required.
- Active curated views: listing selected fields and prefixing duplicate fields.
- Index-Friendly numeric checks: check numeric column placeholders without casting (e.g. `Quantity IN (-999, 999999)`).
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
""",
    "sql-ansi": f"""You are a senior data engineer writing portable ANSI SQL ETL scripts.

{_BASE_RULES}
{_PLAN_ACTIONS}

ANSI SQL REQUIREMENTS:
- Header comment block with plan_id.
- Raw -> Staging -> Transform -> Clean Architecture: Raw tables are completely immutable. Never write updates/modifications/deletions directly on raw tables. Create the target clean table (e.g., `Orders_Clean` for `Orders_Raw`) if it does not exist with standard audit columns: `etl_created_at`, `etl_updated_at`, and `etl_batch_id`. 
  Inside each table-cleaning stored procedure, initialize a temporary staging table (e.g. `#Orders_Staging`) by doing `SELECT * INTO #Orders_Staging FROM Orders_Clean WHERE 1=0;`. Copy the raw data (utilizing candidate key CTE deduplication and watermarking filters) into the staging table (populating `@run_id` to `etl_batch_id`). Execute all transformations, updates, and validations directly on the staging table `#Orders_Staging`. Finally, truncate/delete records in the target clean table `Orders_Clean` and insert the fully transformed records from the staging table into `Orders_Clean`.
- Modular Stored Procedures: Wrap all cleaning steps for each table into dedicated stored procedures named `etl_clean_<table_name>`.
- Master Orchestration: Generate a master coordinator procedure named `etl_main` that calls all the individual table cleaning stored procedures.
- Execution Logging & Log ID Bugfix: Output DDL to create a logging table named `etl_log` and log the start, success, and failure status (with errors) within exception blocks. Always insert the log row first, and then capture the generated identity/auto-increment variable immediately to define the batch run ID safely without race conditions.
- Balanced Transactions: Every block/procedure performing data modifications MUST wrap them in an explicit transaction. Start with `BEGIN TRANSACTION;` inside the try block immediately after capturing `@run_id`. End with `COMMIT TRANSACTION;` at the end of the success path. In exception handlers, verify and rollback using `ROLLBACK TRANSACTION;`.
- Incremental Loading, Watermarking & Watermark Storage: Accept `@load_type` and `@last_run` parameters. If incremental, retrieve the watermark value from `etl_watermark` if not provided, and filter raw source rows using the watermark. Prior to inserting the incremental batch, delete matching clean rows by primary key to prevent duplicate records.
- Performance Indexing: Add statements or comments recommending indexes on primary keys, join keys, and watermark columns.
- Rule Merging & Single-Scan Consolidation: NEVER generate separate, sequential `UPDATE` statements for each plan step on the same table. Instead:
  1. Merge and consolidate all expression-based updates (like `LTRIM/RTRIM`, `LOWER/UPPER`, case normalization, formatting, phone normalization, date parsing, and range clipping) into a **single multi-column `UPDATE` statement** on the staging table.
  2. Merge all default value fillings and invalid/sentinel replacements into a **single join-based `UPDATE` statement** joining `etl_default_values` and `etl_invalid_values` via `LEFT JOIN`s.
- Zero Redundant Operations: Do not output duplicate or redundant CTE statements, updates, or procedure calls. Verify that any deduplication logic, outlier checks, or date/email validation is written once per column.
- Type-Safe String Transformations: When trimming or lowercasing non-string columns, first cast the column explicitly to a string type (e.g. `CAST(col AS VARCHAR(50))`) before applying the string function.
- Rejects & Quarantine Logging: Validate date and email constraints and quarantine violating records to an `etl_rejects` table before removing them from the staging table. Perform validation deletes before applying transformations/casts to preserve the original invalid values. Create the table DDL if not exists: `CREATE TABLE etl_rejects (id INT, table_name VARCHAR(100), rejected_row_data VARCHAR(MAX), reject_reason VARCHAR(255), etl_batch_id INT, rejected_at TIMESTAMP)`.
- Default Value Sanity & No Fake/Placeholder Defaults: Use `NULL` as the default value strategy for date, email, and phone/identifier columns to prevent downstream data pollution. NEVER hardcode placeholder default values (like `'10120631.5'` for dates, or `'99999'` for Phone/IDs) when filling nulls; un-defaulted values must remain `NULL`. Use `etl_default_values` lookup table queries (with type-safe dynamic casting based on column data type) and `etl_invalid_values` lookup table queries instead of hardcoded default/sentinel values.
- Multi-Format Date Parsing: Cascaded parsing using conditional conversion attempts (e.g., trying style 120, 103, 101, 111). If all fail, quarantine the row as invalid format into `etl_rejects`.
- Business-Key Deduplication: Partition row-level deduplication by business keys/primary keys, sorting descending by the watermark column to preserve the latest record. Perform deduplication inside the initial staging `INSERT` statement using a CTE rather than doing standalone `DELETE` statements.
- Index-Friendly Numeric Checks: Avoid casting columns for sentinel/placeholder checks where possible (especially for numeric columns).
- Active Curated Views: Generate active view layers `CREATE VIEW vw_<table_base>_Fact AS` explicitly listing selected fields and renaming duplicate joined fields to prevent duplicate column errors.
- Safe casts (CAST/TRY semantics via CASE WHERE not available).
- IQR outlier logic with subqueries or CTEs, not dialect-specific hacks unless noted in comments. Only run outlier checks on numeric/metric columns.
- never_drop_rows: no DELETE for quality fixes (except when logging to rejects table).
- Email Validation: For Email columns, check for valid email syntax using the exact SQL pattern `NOT LIKE '%_@_%._%'`. If invalid, quarantine any invalid emails into `etl_rejects` and delete them from the staging table.
- Phone Normalization and Validation: For Phone columns, strip symbols `-`, ` `, `(`, `)` using nested `REPLACE` functions. If the cleaned phone number length is less than 7 or contains non-numeric characters (tested using `LIKE '%[^0-9]%'`), treat it as an invalid phone validation failure, quarantine it to `etl_rejects`, and delete it from the staging table.
- No Redundant Casts: Prohibit redundant string castings. Do not emit nested castings like `LOWER(CAST(LTRIM(RTRIM(CAST(col AS VARCHAR(MAX)))) AS VARCHAR(MAX)))`. If a column is already cast to a string type, or is the result of string functions (`LOWER`, `LTRIM`, `REPLACE`), do not wrap it in additional `CAST` statements.
- Orders Pipeline: Do not make the Orders pipeline a simple copy. Enforce strict date parsing (via coalesced `TRY_CONVERT` chains), status normalization (trim and case normalization), and invalid/null values handling.
- Duplicates Deduplication ordering: Never use non-existent columns (like `etl_created_at`) in `ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([pk] AS VARCHAR(400))))) ORDER BY ...)` inside the staging copy CTE. Use a business column (like `CreatedDate DESC` or `OrderDate DESC`) for ordering.
- No SELECT DISTINCT * for deduplication: Avoid using expensive, non-key-aware `SELECT DISTINCT` statements. Deduplication must be key-aware using CTE `ROW_NUMBER()`.
- Idempotent and Production-Safe views/joins: Ensure join views use `CREATE VIEW` instead of `SELECT ... INTO` to prevent duplicate view compilation failures or schema write conflicts.
""",
    "pyspark": f"""You are a senior data engineer writing production PySpark ETL.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

PYSPARK REQUIREMENTS:
- Module docstring with plan_id.
- CRITICAL: The FIRST lines of code after the docstring MUST be module-level imports (before any def or class):
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    import logging
  DO NOT place these inside function bodies. They must be at the very top of the file.
- One transform_<dataset>(df: DataFrame) -> DataFrame per dataset.
- Use withColumn, dropDuplicates, percentile_approx for IQR — same semantics as pandas plan.
- never_drop_rows: coalesce/fill only, no filter that drops null-quality rows.
- I/O: use connector_manifest read_snippet_pyspark and write_snippet_pyspark per dataset.
- NEVER use spark.read.csv for .xml files. Use format("com.databricks.spark.xml") for format=xml.
- NEVER write .csv() to a path ending in .xml — use parquet or json matching manifest output_path.
- Load paths via _resolve_data_path(manifest location), not bare filenames like "data_1.json".
- COPY the full _resolve_data_path helper from the user message (uses AZURE_STORAGE_ACCOUNT, DHARA_BLOB_CONTAINER, DHARA_BLOB_BASE_PATH). NEVER return only f"abfss://{{location}}".
- Pipeline order: load -> transform each dataset -> join (if needed) -> write ALL outputs (per-dataset + joined_* if joined).
- Joins: prefix right-hand columns with _prefix_columns before join; store result in dfs["joined_<parent>_<child>"] and WRITE it to parquet.
- Do NOT assign a join to a variable that is never written (no dead df_joined).
- Pre-join: _require_columns for business_rules.required_columns; _warn_duplicate_keys on join keys.
- When plan is per-dataset normalization only (lowercase/hash, no enrichment need): SKIP joins; write each cleaned dataset only.
- never_drop_rows + joins: use how="left" only, never inner.
- if __name__ == "__main__": SparkSession + run_pipeline(spark) with logging.basicConfig(INFO).
- Valid Python 3.10+ invoking PySpark APIs only.
""",
    "adf": f"""You are a senior Azure Data Factory engineer.

{_BASE_RULES}
{_PLAN_PARAMS}
{_PLAN_ACTIONS}

ADF REQUIREMENTS:
- Output JSON with bundle.flows: [clean_only flow, clean_and_joined flow] when relationships.joins exist.
- Use ADF expression language: toLower, toUpper, trim, coalesce, iif, percentile, sha2, regexpReplace.
- derivedColumn transformations: typeProperties.columns[] with name + expression per step params.
- Join transforms: joinType left (never inner when never_drop_rows), leftStream/rightStream from upstream chain.
- Linked services: LS_AzureBlob, datasets DS_<dataset>, DS_<dataset>_cleaned.
- Valid JSON only (no markdown).
""",
}


def is_llm_generation_error(text: str) -> bool:
    return (text or "").strip().startswith(LLM_ERROR_PREFIX)


def normalize_codegen_engine(engine: str, sql_dialect: str = "tsql") -> str:
    e = (engine or "python").lower().strip()
    d = (sql_dialect or "tsql").lower().strip()
    if e in ("spark", "pyspark"):
        return "pyspark"
    if e == "adf":
        return "adf"
    if e in ("sql", "tsql", "ansi") or "sql" in e:
        if e == "ansi" or d == "ansi":
            return "sql-ansi"
        return "sql-tsql"
    return "python"


def _get_llm_client():
    cfg = load_llm_config(purpose="etl_codegen")
    if not cfg:
        return None, None
    if cfg.provider == "azure_openai" and AzureOpenAI and cfg.endpoint:
        client = AzureOpenAI(
            azure_endpoint=cfg.endpoint,
            api_key=cfg.api_key,
            api_version=cfg.api_version or "2024-02-01",
        )
        return client, cfg.model
    if cfg.provider == "openai" and OpenAI:
        return OpenAI(api_key=cfg.api_key), cfg.model
    return None, None


def _strip_markdown_fences(text: str) -> str:
    code = (text or "").strip()
    code = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", code)
    code = re.sub(r"\n?```\s*$", "", code)
    return code.strip()


def _safe_max_tokens(prompt_text: str, engine_key: str) -> int:
    cfg = load_llm_config(purpose="etl_codegen")
    context_window = get_context_window(cfg.model) if cfg else 128_000
    # Measured: sql-tsql system prompt ≈ 7500 tokens; python ≈ 4500; pyspark ≈ 5000
    overhead_map = {
        "sql-tsql": 8000,
        "sql-ansi": 8000,
        "python":   5000,
        "pyspark":  5500,
        "adf":      3500,
    }
    system_overhead = overhead_map.get(engine_key, 5000)
    input_tokens = _estimate_tokens(prompt_text)
    available = context_window - input_tokens - system_overhead - 500  # 500 buffer
    
    # Model-specific caps to prevent out-of-limits API errors
    cap_map = {
        "gpt-4o-mini": 16000,
        "gpt-4o": 4096,
        "gpt-4": 4096,
        "gpt-35-turbo": 4096,
    }
    model_name = (cfg.model.lower() if cfg else "gpt-4o")
    cap = 4096
    for k, v in cap_map.items():
        if k in model_name:
            cap = v
            break
            
    if engine_key == "adf":
        cap = min(cap, 6000)
        
    return max(2000, min(cap, available))


def _classify_column(
    col_name: str,
    col_meta: dict | None,
    sem_schema: dict | None = None,
    ds_name: str = "",
) -> str:
    from agent.etl_pipeline.semantic_classifier import profile_column
    meta = col_meta or {}
    dtype = meta.get("dtype") or meta.get("target_dtype") or meta.get("inferred_type") or "string"
    semantic_type = meta.get("semantic_type") or ""
    
    if sem_schema:
        desc = sem_schema.get(f"{ds_name}.{col_name}") or {}
        if desc.get("semantic_type"):
            semantic_type = desc.get("semantic_type")
            
    prof = profile_column(col_name, dtype, semantic_type)
    if prof.is_temporal:
        return "date"
    if prof.is_numeric:
        return "metric"
    if prof.is_categorical:
        return "categorical"
    if prof.is_identifier:
        return "id"
    return "string"


def _is_numeric_column(col_name: str, col_meta: dict) -> bool:
    from agent.etl_pipeline.semantic_classifier import profile_column
    dtype = col_meta.get("dtype") or col_meta.get("target_dtype") or col_meta.get("inferred_type") or "string"
    semantic_type = col_meta.get("semantic_type") or ""
    return profile_column(col_name, dtype, semantic_type).is_numeric



def _consolidate_and_filter_datasets(
    datasets: Dict[str, Any],
    source_metadata: Dict[str, Any],
    sem_schema: Dict[str, Any] = None,
) -> Dict[str, Any]:
    cleaned_datasets = {}
    
    # Priority for sorting steps
    priority = {
        "trim": 10,
        "lowercase": 11,
        "uppercase": 11,
        "sanitize_email": 12,
        
        "coerce_numeric": 20,
        "cast_type": 21,
        
        "zero_to_null": 30,
        
        "parse_dates": 50,
        
        "regex_replace": 60,
        "replace_values": 61,
        "standardize_boolean": 62,
        "normalize_phone": 63,
        "hash_phone": 64,
        "mask_phone": 65,
        
        "range_clip": 70,
        "clip_or_flag": 71,
        "flag_outliers": 72,
        "clip_outliers": 73,
        "cap_outliers": 74,
        
        "fill_or_drop": 78,
        "fill_nulls_simple": 78,
        
        "deduplicate": 80,
        "validate_referential_integrity_or_stage": 90
    }
    
    for ds_name, block in (datasets or {}).items():
        if not isinstance(block, dict):
            cleaned_datasets[ds_name] = block
            continue
            
        steps = block.get("steps") or []
        if not steps:
            cleaned_datasets[ds_name] = block
            continue
            
        ds_meta = source_metadata.get(ds_name) or {}
        columns_meta = ds_meta.get("columns") or {}
        
        filtered_steps = []
        seen_operations = set()
        
        for step in steps:
            if not isinstance(step, dict):
                continue
            action = str(step.get("action") or "").strip().lower()
            col = step.get("column")
            
            # 1. Type-aware filtering: trim/lower/upper/sanitize_email are string-only
            if action in ("trim", "lowercase", "uppercase", "sanitize_email"):
                if col:
                    col_meta = columns_meta.get(col) or {}
                    col_class = _classify_column(col, col_meta, sem_schema, ds_name)
                    if col_class in ("metric", "date", "metadata"):
                        continue
            
            # Type-aware filtering: outlier logic is numeric-only
            if action in ("flag_outliers", "clip_or_flag", "clip_outliers", "cap_outliers"):
                if col:
                    col_meta = columns_meta.get(col) or {}
                    if not _is_numeric_column(col, col_meta):
                        continue
                        
            # 2. Operation Deduplication / Normalization
            norm_action = action
            if action in ("clip_or_flag", "flag_outliers"):
                norm_action = "flag_outliers"
            elif action in ("fill_nulls_simple", "fill_or_drop"):
                norm_action = "fill_or_drop"
            elif action in ("clip_outliers", "cap_outliers"):
                norm_action = "modify_outliers"
                
            is_internal_trim = (norm_action == "trim") and (step.get("source_issue_type") == "internal_whitespace" or step.get("issue_type") == "internal_whitespace")
            op_key = (norm_action, str(col).lower() if col else None, is_internal_trim)
            if op_key in seen_operations:
                continue
            seen_operations.add(op_key)
            
            filtered_steps.append(step)
            
        # 3. Sort steps according to transform priority order
        def get_step_priority(st):
            act = str(st.get("action") or "").strip().lower()
            return priority.get(act, 99)
            
        sorted_steps = sorted(filtered_steps, key=get_step_priority)
        
        # Re-assign order field
        import copy
        for idx, st in enumerate(sorted_steps):
            st_copy = copy.deepcopy(st)
            st_copy["order"] = idx + 1
            sorted_steps[idx] = st_copy
            
        cleaned_block = copy.deepcopy(block)
        cleaned_block["steps"] = sorted_steps
        cleaned_datasets[ds_name] = cleaned_block
        
    return cleaned_datasets


def _build_codegen_payload(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    source_metadata: Dict[str, Any] = {}
    sem_schema = plan.get("semantic_schema") or {}
    for ds_name, meta in (assessment.get("datasets") or {}).items():
        cols = meta.get("columns") or {}
        source_metadata[ds_name] = {
            "row_count": meta.get("row_count"),
            "columns": {
                col: {
                    "dtype": cmeta.get("dtype") or cmeta.get("inferred_type"),
                    "null_percentage": cmeta.get("null_percentage"),
                    "semantic_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("semantic_type") or cmeta.get("semantic_type", "unknown"),
                    "sub_type": (sem_schema.get(f"{ds_name}.{col}") or {}).get("sub_type") or cmeta.get("sub_type", "unknown"),
                    "pii_level": (sem_schema.get(f"{ds_name}.{col}") or {}).get("pii_level") or "none",
                }
                for col, cmeta in cols.items()
                if isinstance(cmeta, dict)
            },
        }
    
    # Consolidate, deduplicate, type-filter and sort plan steps before passing to LLM
    raw_datasets = plan.get("datasets") or {}
    cleaned_datasets = _consolidate_and_filter_datasets(raw_datasets, source_metadata, plan.get("semantic_schema"))
    
    from agent.etl_pipeline.dq_gate import evaluate_dq_gate
    try:
        gate_res = evaluate_dq_gate(assessment, plan.get("business_rules"))
        dq_gate_summary = {
            "passed": gate_res.get("passed"),
            "blocking_issues": gate_res.get("blocking_issues") or [],
            "warnings": gate_res.get("warnings") or [],
        }
    except Exception:
        dq_gate_summary = {"passed": True, "blocking_issues": [], "warnings": []}

    from agent.etl_pipeline.llm_rec_mapper import enrich_with_catalog
    manual_review_enriched = enrich_with_catalog(plan.get("manual_review") or [])

    base = {
        "plan_id": plan.get("plan_id"),
        "engine": plan.get("engine"),
        "output_mode": output_mode,
        "output_path": output_path,
        "business_rules": plan.get("business_rules"),
        "datasets": cleaned_datasets,
        "global_steps": plan.get("global_steps"),
        "manual_review": manual_review_enriched,
        "blocked": plan.get("blocked"),
        "source_metadata": source_metadata,
        "source_context": plan.get("source_context") or {},
        "connector_manifest": plan.get("connector_manifest") or {},
        "engine_recommendation": plan.get("engine_recommendation") or {},
        "relationships": plan.get("relationships") or {},
        "etl_intent": plan.get("etl_intent") or {},
        "dq_gate_summary": dq_gate_summary,
        "domain_rules": plan.get("domain_rules") or {},
        "business_keys": plan.get("business_keys") or {},
    }
    base.update(llm_codegen_extra_context(plan))
    return base


_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = pd.read_csv(r"{loc}")',
    "excel": 'df = pd.read_excel(r"{loc}", sheet_name=0)',
    "json": 'df = pd.read_json(r"{loc}")',
    "parquet": 'df = pd.read_parquet(r"{loc}")',
    "sql_server": (
        'engine = create_engine("mssql+pyodbc://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "azure_sql": (
        'engine = create_engine("mssql+pyodbc://...database.windows.net/...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "postgres": (
        'engine = create_engine("postgresql://...")\n'
        'df = pd.read_sql("SELECT * FROM {loc}", engine)'
    ),
    "blob_storage": (
        "# Read from Azure Blob — configure connection string\n"
        'df = pd.read_csv("downloaded_{loc}")'
    ),
    "unknown": 'df = pd.read_csv(r"{loc}")  # TODO: adjust read for your source',
}

_PYSPARK_READ_TEMPLATES: Dict[str, str] = {
    "csv_file": 'df = spark.read.option("header","true").csv(r"{loc}")',
    "parquet": 'df = spark.read.parquet(r"{loc}")',
    "json": 'df = spark.read.json(r"{loc}")',
    "sql_server": (
        'df = spark.read.format("jdbc").option("dbtable", "{loc}").load()'
    ),
    "unknown": 'df = spark.read.option("header","true").csv(r"{loc}")',
}


def _get_blob_read_template(conn: dict) -> str:
    account = conn.get("storage_account") or conn.get("account") or "{storage_account}"
    container = conn.get("container_name") or conn.get("container") or "{container_name}"
    return (
        f'df = spark.read.option("header","true")'
        f'.csv("wasbs://{container}@{account}.blob.core.windows.net/{{loc}}")'
    )


def _read_hint_for_payload(engine_key: str, payload: Dict[str, Any]) -> str:
    ctx = payload.get("source_context") or {}
    src_type = str(ctx.get("type") or "unknown")
    loc = str(ctx.get("location") or "data_file")

    # Connector validation gate: only enforce when the payload has been explicitly flagged
    # as requiring a resolved connection (i.e., connector_validation_required=True).
    # Standard blob sessions use env-var credentials via _read_blob_pandas / _resolve_data_path
    # and do NOT need resolved_connection in source_context.
    if (
        payload.get("connector_validation_required")
        and src_type in ("blob_storage", "azure_sql", "sql_server", "postgres")
        and not ctx.get("resolved_connection")
    ):
        raise ConnectorConfigError(
            f"Source type '{src_type}' requires resolved_connection details before codegen; "
            "connector validation must complete first."
        )

    if engine_key == "pyspark":
        if src_type == "blob_storage":
            conn = ctx.get("resolved_connection") or {}
            tmpl = _get_blob_read_template(conn)
        else:
            tmpl = _PYSPARK_READ_TEMPLATES.get(src_type, _PYSPARK_READ_TEMPLATES["unknown"])
    else:
        if src_type == "blob_storage":
            conn = ctx.get("resolved_connection") or {}
            from agent.etl_pipeline.io_snippets import _get_pandas_blob_read_snippet
            tmpl = _get_pandas_blob_read_snippet(loc, conn)
            return tmpl
        else:
            tmpl = _READ_TEMPLATES.get(src_type, _READ_TEMPLATES["unknown"])
    return tmpl.format(loc=loc)


def _trim_payload_for_window(payload: dict, engine_key: str, force_level: int = 0) -> dict:
    from agent.etl_pipeline.payload_trimmer import trim_payload, _FLOOR_TRIM_CONFIG
    return trim_payload(payload, _FLOOR_TRIM_CONFIG)


def _build_dynamic_sql_prompt(payload: dict, base_prompt: str) -> str:
    has_outliers = False
    has_dates = False
    
    datasets = payload.get("datasets") or {}
    for ds_name, block in datasets.items():
        if not isinstance(block, dict):
            continue
        steps = block.get("steps") or []
        for st in steps:
            act = str(st.get("action") or "").lower().strip()
            if act in ("flag_outliers", "clip_outliers", "cap_outliers", "clip_or_flag"):
                has_outliers = True
            if act == "parse_dates":
                has_dates = True
                
    br = payload.get("business_rules") or {}
    notes = str(br.get("notes") or "").lower()
    has_incremental = "incremental" in notes or "watermark" in notes
    
    rel = payload.get("relationships") or {}
    joins = rel.get("joins") or []
    has_joins = len(joins) > 0
    
    SECTION_GUARDS = {
        "Incremental Loading": has_incremental,
        "Outlier Mitigation Safety": has_outliers,
        "Reusable Outlier Procedure": has_outliers,
        "Multi-Format Date Parsing": has_dates,
        "Active curated views": has_joins,
        "Idempotent and Production-Safe views": has_joins,
    }
    
    lines = base_prompt.splitlines()
    result, skip = [], False
    for line in lines:
        heading = next((k for k in SECTION_GUARDS if k in line), None)
        if heading is not None:
            skip = not SECTION_GUARDS[heading]
        if not skip:
            result.append(line)
        if skip and line.strip() == "":  # blank line = end of section
            skip = False
            
    return "\n".join(result)


def _build_user_message_parts(
    engine_key: str,
    payload: Dict[str, Any],
    fix_errors: Optional[List[str]] = None,
    previous_output: Optional[str] = None,
) -> List[str]:
    br = payload.get("business_rules") or {}
    user_parts = [
        f"Target engine: {engine_key}",
        f"ETL policy (must follow):\n{payload.get('policy') or ''}",
        f"Generate complete ETL for this approved plan:\n{json.dumps(payload, separators=(',', ':'), default=str)}",
    ]
    manifest = payload.get("connector_manifest") or {}
    m_ds = manifest.get("datasets") or {}
    if m_ds:
        read_lines = []
        for ds_name, ent in m_ds.items():
            if not isinstance(ent, dict):
                continue
            snip = ent.get("read_snippet_python") or ent.get("read_snippet_pyspark") or ""
            read_lines.append(f"- {ds_name}: {ent.get('source_type')} @ {ent.get('location')}")
            if snip:
                read_lines.append(f"  read: {snip}")
            if ent.get("output_path"):
                read_lines.append(f"  write: {ent.get('output_path')}")
            fmt = ent.get("format")
            if fmt == "xml":
                read_lines.append(
                    "  CRITICAL: format=xml — do NOT use read_csv or write.csv; use XML read + parquet/json write"
                )
        user_parts.append(
            "CONNECTOR MANIFEST (use these exact read/write patterns per dataset):\n"
            + "\n".join(read_lines[:40])
        )
        if engine_key == "pyspark":
            user_parts.append(
                "REQUIRED _resolve_data_path helper (copy verbatim into generated code):\n"
                f"```python\n{resolve_path_fabric_pyspark_helper(None, None)}\n```"
            )
            user_parts.append(
                "REQUIRED production helpers (copy if you emit joins or required_columns):\n"
                "```python\n"
                "def _require_columns(df, required, label): ...\n"
                "def _warn_duplicate_keys(df, key_col, label): ...\n"
                "def _prefix_columns(df, prefix, except_cols): ...\n"
                "```\n"
                "Use the template implementations from Agent Dhara io_snippets — do not stub paths."
            )
    elif payload.get("source_context"):
        ctx = payload["source_context"]
        sources = ctx.get("sources") or []
        if len(sources) > 1:
            src_lines = [
                f"- {s.get('dataset')}: {s.get('type')} @ {s.get('location')} ({s.get('row_count', 0):,} rows)"
                for s in sources[:15]
            ]
            user_parts.append(
                "MULTI-SOURCE CONTEXT (one loader per dataset):\n" + "\n".join(src_lines)
            )
        read_hint = _read_hint_for_payload(engine_key, payload)
        user_parts.append(
            f"PRIMARY READ PATTERN:\n```python\n{read_hint}\n```"
        )
    if br.get("notes"):
        user_parts.append(
            "BUSINESS NOTES (must honor in generated transforms):\n" + str(br.get("notes"))
        )
    manual = payload.get("manual_review") or []
    if manual:
        mr_lines = []
        PII_ACTIONS = {"hash_phone", "mask_phone", "hash_email", "exclude"}
        manual_sorted = sorted(
            manual,
            key=lambda x: 0 if str(x.get("action") or x.get("catalog_guidance") or "").lower().strip() in PII_ACTIONS else 1
        )
        for item in manual_sorted[:20]:
            ds = item.get("dataset") or "?"
            col = item.get("column") or "?"
            msg = item.get("message") or item.get("guidance") or ""
            cat_guidance = item.get("catalog_guidance") or ""
            if cat_guidance:
                msg = f"{msg} (Catalog guidance: {cat_guidance})"
            mr_lines.append(f"- [{ds}] {col}: {msg}")
        user_parts.append(
            "MANUAL REVIEW (implement in code when business notes require it, especially phone hash/mask):\n"
            + "\n".join(mr_lines)
        )
    gate = payload.get("dq_gate_summary") or {}
    blocking = gate.get("blocking_issues") or []
    if blocking:
        block_lines = [f"- [{b.get('dataset')}] {b.get('reason')}" for b in blocking[:10]]
        user_parts.append(
            "DQ GATE BLOCKING ISSUES (these columns/datasets must be cleaned — do NOT skip these steps):\n"
            + "\n".join(block_lines)
        )

    dr = payload.get("domain_rules") or {}
    if dr:
        dr_lines = []
        for col_name, rule in dr.items():
            dr_lines.append(f"- {col_name}: rule_name={rule.get('rule_name')}, regex={rule.get('regex')}, msg={rule.get('message')}")
        user_parts.append(
            "DOMAIN RULES (enforce these: domain rules take priority over default heuristic cleaning):\n"
            + "\n".join(dr_lines)
        )
        
    bk = payload.get("business_keys") or {}
    if bk:
        bk_lines = []
        for ds_name, keys in bk.items():
            bk_lines.append(f"- {ds_name}: keys={keys}")
        user_parts.append(
            "BUSINESS KEYS / DEDUPLICATION KEYS (use these columns in ROW_NUMBER PARTITION BY for deduplication):\n"
            + "\n".join(bk_lines)
        )

    if br.get("never_drop_rows"):
        user_parts.append(
            "NEVER_DROP_ROWS (mandatory): preserve every input row. "
            "No inner join, dropna(), or row-filtering that removes records. "
            "For normalization-only plans, transform and write each dataset separately — "
            "skip joins unless business_rules.notes explicitly require a join."
        )
    rel = payload.get("relationships") or {}
    joins = rel.get("joins") or []
    if joins:
        join_lines = []
        for j in joins[:8]:
            join_lines.append(
                f"- {j.get('left_dataset')}.{j.get('left_key')} "
                f"{j.get('join_type', 'inner')} join "
                f"{j.get('right_dataset')}.{j.get('right_key')} "
                f"({j.get('cardinality')}, overlap={j.get('overlap_count')})"
            )
        per_ds_only = all(
            str(st.get("action") or "")
            in (
                "lowercase",
                "uppercase",
                "trim",
                "sanitize_email",
                "normalize_phone",
                "hash_phone",
                "mask_phone",
            )
            for block in (payload.get("datasets") or {}).values()
            for st in (block or {}).get("steps") or []
        )
        if per_ds_only and br.get("never_drop_rows"):
            user_parts.append(
                "JOIN POLICY: Per-dataset normalization only — do NOT emit joins unless "
                "business_rules.notes explicitly require enrichment. Write each cleaned dataset."
            )
        else:
            user_parts.append(
                "DETECTED JOINS (after per-dataset transforms; write joined_* parquet):\n"
                + "\n".join(join_lines)
            )
        if rel.get("load_order"):
            user_parts.append(f"LOAD ORDER: {rel.get('load_order')}")
    if fix_errors:
        user_parts.append(
            "PREVIOUS ATTEMPT FAILED VALIDATION. Fix these specific issues:\n"
            + "\n".join(f"  - {e}" for e in fix_errors)
            + "\nDo NOT repeat these errors."
        )
        if previous_output:
            user_parts.append(f"Previous output (truncated):\n{previous_output}")
    return user_parts


def _call_llm(
    engine_key: str,
    payload: Dict[str, Any],
    *,
    fix_errors: Optional[List[str]] = None,
    previous_output: Optional[str] = None,
) -> str:
    client, model = _get_llm_client()
    if not client or not model:
        return f"{LLM_ERROR_PREFIX} No LLM credentials (configure AZURE_OPENAI_* or OPENAI_API_KEY)."

    # 1.5 dynamic system prompt injection
    system = SYSTEM_PROMPTS.get(engine_key, SYSTEM_PROMPTS["python"])
    if engine_key in ("sql-tsql", "sql-ansi"):
        system = _build_dynamic_sql_prompt(payload, system)

    user_parts = _build_user_message_parts(engine_key, payload, fix_errors, previous_output)
    user_message = "\n\n".join(user_parts)
    max_tokens = _safe_max_tokens(user_message, engine_key)

    # 1.1 Floor check: if max_tokens < 3000, force level 2 trim and rebuild message
    if max_tokens < 3000:
        logger.info(f"Available tokens too small ({max_tokens}). Forcing Level 2 payload trim...")
        payload = trim_payload(payload, _FLOOR_TRIM_CONFIG)
        user_parts = _build_user_message_parts(engine_key, payload, fix_errors, previous_output)
        user_message = "\n\n".join(user_parts)
        max_tokens = _safe_max_tokens(user_message, engine_key)

    # 1.3 OpenAI/Azure retry & timeout handling loop
    max_attempts = 3
    response = None
    last_err = None
    for attempt in range(max_attempts):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_message},
                ],
                temperature=0,
                max_tokens=max_tokens,
                timeout=120,
            )
            break
        except (RateLimitError, APITimeoutError) as e:
            last_err = e
            if attempt == max_attempts - 1:
                raise LLMInfraError(f"LLM request failed after retries: {e}") from e
            sleep_time = 3.0 if attempt > 0 else 1.0
            logger.warning(f"LLM rate limit or timeout on attempt {attempt + 1}, retrying in {sleep_time}s... Error: {e}")
            time.sleep(sleep_time)
        except Exception as e:
            raise LLMInfraError(f"LLM infrastructure error: {e}") from e

    if not response:
        raise LLMInfraError(f"LLM request failed entirely. Last error: {last_err}")

    choice = response.choices[0]
    text = _strip_markdown_fences(choice.message.content or "")
    if choice.finish_reason == "length":
        raise LLMInfraError("LLM response truncated (finish_reason=length) — plan/payload too large for available tokens.")
        
    return text


def parse_adf_json_from_llm(text: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Parse ADF mapping JSON from LLM text; returns (object, errors)."""
    errs: List[str] = []
    raw = _strip_markdown_fences(text)
    if not raw:
        return None, ["empty ADF response"]
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj, []
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return obj, []
        except json.JSONDecodeError as e:
            errs.append(f"JSON parse: {e}")
    else:
        errs.append("no JSON object found in LLM response")
    return None, errs or ["invalid ADF JSON"]


def _build_retry_context(code: str, errors: List[str]) -> str:
    lines = code.splitlines()
    error_lines = set()
    for e in errors:
        m = re.search(r"line (\d+)", e, re.IGNORECASE)
        if m:
            error_lines.add(int(m.group(1)))
    
    if error_lines:
        excerpt_lines = []
        for ln in sorted(error_lines):
            start = max(0, ln - 10)
            end = min(len(lines), ln + 10)
            excerpt_lines.append(f"... (lines {start + 1}-{end}) ...")
            excerpt_lines.extend(lines[start:end])
        return "\n".join(excerpt_lines[:150])
    else:
        error_msg = "; ".join(errors) if isinstance(errors, list) else str(errors)
        return f"[FULL REWRITE REQUESTED]\nPrevious attempt failed with: {error_msg}\nGenerate a completely new implementation."


_RETRY_BUDGET: Dict[str, int] = {
    "python": 2,
    "sql-tsql": 3,
    "sql-ansi": 2,
    "pyspark": 2,
    "adf": 2,
}




def _get_cache_key(plan: Dict[str, Any], assessment: Dict[str, Any], engine_key: str) -> str:
    import hashlib
    try:
        blob = json.dumps(assessment, sort_keys=True, default=str)
    except Exception:
        blob = str(assessment)
    assess_sig = hashlib.sha256(blob.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{plan.get('plan_id')}_{assess_sig}_{engine_key}"


async def _reset_circuit_breaker(plan_id: str, delay: int = 300):
    await asyncio.sleep(delay)
    _PLAN_FAILURE_COUNTS[plan_id] = 0
    logger.info(f"Circuit breaker reset for plan_id: {plan_id}")


async def generate_etl_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    engine: str = "python",
    *,
    sql_dialect: str = "tsql",
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    engine_key = normalize_codegen_engine(engine, sql_dialect)
    cache_key = _get_cache_key(plan, assessment, engine_key)

    # Check failure cache first:
    if cached_err := _is_failure_cached(cache_key):
        return None, f"[Cached failure] {cached_err}"

    if not validation_errors:
        now = time.time()
        if cache_key in _CODE_CACHE:
            _entry = _CODE_CACHE[cache_key]
            if isinstance(_entry, dict):
                cached_code = _entry["code"]
                cached_time = _entry["time"]
                cached_ok = _entry.get("ok", True)
            else:
                cached_code, cached_time = _entry
                cached_ok = True
            if cached_ok and isinstance(cached_code, str) and cached_code.strip().startswith("# VALIDATION WARNING:"):
                cached_ok = False
            if now - cached_time < 3600:
                if cached_ok:
                    logger.info("Returning cached generated code for key: %s", cache_key)
                    return cached_code, None
                else:
                    logger.info("Skipping failed cached code for key: %s — will regenerate", cache_key)
                    _CODE_CACHE.pop(cache_key, None)

    plan_id = plan.get("plan_id", "unknown")
    lock = _PLAN_LOCKS[plan_id]
    await asyncio.to_thread(lock.acquire)
    try:
        if _PLAN_FAILURE_COUNTS[plan_id] >= _MAX_PLAN_FAILURES:
            return None, "Circuit open: too many failures for this plan"

        # Execute blocking LLM call in a thread pool
        code, err = await asyncio.to_thread(
            _generate_etl_with_llm_impl,
            plan,
            assessment,
            engine,
            sql_dialect=sql_dialect,
            output_mode=output_mode,
            output_path=output_path,
            validation_errors=validation_errors,
            validate_fn=validate_fn,
            cache_key=cache_key,
        )

        if code is None:
            _PLAN_FAILURE_COUNTS[plan_id] += 1
            if _PLAN_FAILURE_COUNTS[plan_id] >= _MAX_PLAN_FAILURES:
                def reset_circuit():
                    _PLAN_FAILURE_COUNTS[plan_id] = 0
                    logger.info(f"Circuit breaker reset for plan_id: {plan_id}")
                threading.Timer(300.0, reset_circuit).start()
        else:
            _PLAN_FAILURE_COUNTS[plan_id] = 0  # reset on success
            
        return code, err
    finally:
        lock.release()


def _generate_etl_with_llm_impl(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    engine: str = "python",
    *,
    sql_dialect: str = "tsql",
    output_mode: str = "dataframe_only",
    output_path: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
    cache_key: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    engine_key = normalize_codegen_engine(engine, sql_dialect)
    if engine_key == "adf":
        return None, "Use generate_adf_with_llm for ADF engine."

    if not cache_key:
        cache_key = _get_cache_key(plan, assessment, engine_key)

    payload = _build_codegen_payload(
        plan, assessment, output_mode=output_mode, output_path=output_path
    )
    # Trim ONCE before retry loop (Fix 4)
    trimmed_payload = trim_payload(payload, _CODEGEN_TRIM_CONFIG)

    prev: Optional[str] = None
    fix_errors = list(validation_errors or [])
    
    from agent.etl_pipeline.codegen_validator import get_validator
    validator = get_validator(engine_key)
    
    max_retries = _RETRY_BUDGET.get(engine_key, 2)
    
    code = ""
    for attempt in range(max_retries + 1):
        if fix_errors and attempt > 0:
            prev = _build_retry_context(code, fix_errors)
        elif validation_errors:
            prev = "(retry — see validation errors in user message)"
        else:
            prev = None

        try:
            code = _call_llm(
                engine_key,
                trimmed_payload,
                fix_errors=fix_errors or None,
                previous_output=prev,
            )
        except (LLMInfraError, ConnectorConfigError) as exc:
            last_error = str(exc)
            _write_failure_cache(cache_key, last_error)
            if isinstance(exc, ConnectorConfigError):
                return None, f"Connector configuration error: {exc}. Check your source connection settings."
            if "truncated" in last_error.lower() and attempt < max_retries:
                logger.info(f"LLM output truncated. Retrying with harder payload trim...")
                trimmed_payload = trim_payload(trimmed_payload, _FLOOR_TRIM_CONFIG)
                fix_errors = []
                continue
            if attempt < max_retries:
                continue
            return None, f"LLM infrastructure error: {last_error}"

        if is_llm_generation_error(code):
            _write_failure_cache(cache_key, code)
            return None, code

        # Auto-inject missing PySpark module-level imports and Fabric IDs before validation
        if engine_key == "pyspark":
            code = _inject_pyspark_imports(code)
            code = _inject_pyspark_fabric_ids(code)
        
        # Run local validation loop
        rules = plan.get("business_rules") or {}
        never_drop_rows = bool(rules.get("never_drop_rows"))

        active_validator = validate_fn if validate_fn is not None else validator
        if active_validator:
            import inspect
            fn_key = f"fn_{id(active_validator)}" if validate_fn is not None else engine_key
            accepts_ndr = _VALIDATOR_ACCEPTS_NDR.get(fn_key)
            if accepts_ndr is None:
                sig = inspect.signature(active_validator)
                accepts_ndr = "never_drop_rows" in sig.parameters
                _VALIDATOR_ACCEPTS_NDR[fn_key] = accepts_ndr

            if accepts_ndr:
                res = active_validator(code, never_drop_rows=never_drop_rows)
            else:
                res = active_validator(code)
            
            if len(res) == 3:
                ok, errs, warnings = res
                if warnings:
                    logger.warning(f"[Validation Advisory] {warnings}")
            else:
                ok, errs = res

            if ok:
                _CODE_CACHE[cache_key] = {"code": code, "time": time.time(), "ok": True}
                return code, None
            if attempt < max_retries:
                logger.info(f"[Retry Debug] Attempt {attempt + 1} code failed validation with errors: {errs}. Code snippet:\n{code[:1200]}")
                fix_errors = errs
                continue
            # After max retries, return with validation warnings prepended
            warning = "\n".join(f"# VALIDATION WARNING: {e}" for e in errs)
            res_code = f"{warning}\n\n{code}"
            _CODE_CACHE[cache_key] = {"code": res_code, "time": time.time(), "ok": False}
            _write_failure_cache(cache_key, f"Validation errors: {errs}")
            return res_code, f"Validation errors: {errs}"
        else:
            logger.warning(f"No validator or validate_fn found for engine '{engine_key}' — skipping validation")
            _CODE_CACHE[cache_key] = {"code": code, "time": time.time(), "ok": True}
            return code, None

    return code, None


async def generate_adf_with_llm(
    plan: Dict[str, Any],
    assessment: Dict[str, Any],
    *,
    validation_errors: Optional[List[str]] = None,
    validate_fn: Optional[Callable[[Dict[str, Any]], Tuple[bool, List[str]]]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (adf_object, error_message).
    error_message empty on success; on LLM failure error_message is set and first element may be None.
    """
    payload = _build_codegen_payload(plan, assessment)
    trimmed = trim_payload(payload, _CODEGEN_TRIM_CONFIG)
    last_error = None
    fix_errors = list(validation_errors or [])
    raw = ""
    
    MAX_ADF_RETRIES = 3
    for attempt in range(MAX_ADF_RETRIES):
        prev_out = raw if attempt > 0 else None
        
        try:
            raw = await asyncio.to_thread(
                _call_llm,
                "adf",
                trimmed,
                fix_errors=fix_errors or None,
                previous_output=prev_out,
            )
        except Exception as e:
            last_error = str(e)
            import copy
            trimmed = copy.deepcopy(trimmed)
            trimmed["_adf_fix_context"] = {
                "attempt": attempt + 1,
                "parse_error": last_error,
                "prev_output_snippet": ""
            }
            continue

        if is_llm_generation_error(raw):
            last_error = raw
            import copy
            trimmed = copy.deepcopy(trimmed)
            trimmed["_adf_fix_context"] = {
                "attempt": attempt + 1,
                "parse_error": raw,
                "prev_output_snippet": ""
            }
            continue
            
        parsed, errs = parse_adf_json_from_llm(raw)
        if parsed:
            if validate_fn:
                ok, val_errs = validate_fn(parsed)
                if ok:
                    return parsed, None
                else:
                    last_error = "; ".join(val_errs)
                    fix_errors = val_errs
            else:
                return parsed, None
        else:
            last_error = "; ".join(errs)
            fix_errors = errs

        # On failure, add error context for next attempt
        import copy
        trimmed = copy.deepcopy(trimmed)
        trimmed["_adf_fix_context"] = {
            "attempt": attempt + 1,
            "parse_error": last_error,
            "prev_output_snippet": (raw or "")[:2000]
        }

    return None, f"ADF JSON generation failed after {MAX_ADF_RETRIES} attempts: {last_error}"


def run_codegen_sync(coro):
    """Bridge async code into a synchronous calling environment."""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: asyncio.run(coro))
            return future.result()
    else:
        new_loop = asyncio.new_event_loop()
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()

