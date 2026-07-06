## Assessment Report of `dbo.EmployeeData`

> [!NOTE]
> **dbo.EmployeeData**: Full dataset has 20 rows. Statistics (nulls, min/max, uniqueness) profiled in-database on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `dbo.EmployeeData` | Azure SQL (SQL data) | 20 | 10 | 28 | 1 | 8 | 19 |

### Columns (per dataset)


#### `dbo.EmployeeData`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `Department` | `object` | 5.0% | 6 | `categorical` | ✗ |
| `Email` | `object` | 5.0% | 18 | `email` | ✗ |
| `EmployeeID` | `object` | 0.0% | 19 | `categorical` | ✗ |
| `EmployeeName` | `object` | 5.0% | 18 | `categorical` | ✗ |
| `HireDate` | `object` | 5.0% | 17 | `date` | ✗ |
| `JobTitle` | `object` | 5.0% | 15 | `categorical` | ✗ |
| `Location` | `object` | 5.0% | 9 | `categorical` | ✗ |
| `Phone` | `object` | 10.0% | 16 | `phone` | ✗ |
| `Salary` | `object` | 5.0% | 17 | `categorical` | ✗ |
| `Status` | `object` | 5.0% | 3 | `categorical` | ✗ |

### Top issues (per dataset)

#### `dbo.EmployeeData`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| high | `duplicate_primary_key` | `[Row-level]` | 2 | 2 duplicate in candidate PK | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. |
| low | `nulls` | `EmployeeName` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `Email` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `invalid_email` | `Email` | 2 | 2 invalid email(s) | Correct formatting errors if minor, or reject record and request re-entry; add regex validation on email input. |
| low | `nulls` | `Phone` | 2 | 2 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `invalid_phone` | `Phone` | 2 | 2 invalid phone number(s) | Standardize phone format (e.g. E.164); check for missing country codes or area codes. |
| low | `nulls` | `Department` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `JobTitle` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `HireDate` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `invalid_date_format` | `HireDate` | 3 | 3 bad date(s) (failed parsing) | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| low | `nulls` | `Salary` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `Location` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `Status` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `EmployeeID` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `EmployeeName` | 2 | 2 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `Email` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `Location` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `nulls` | `EmployeeID` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `EmployeeName` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `Email` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `Phone` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `near_duplicate_rows` | `[Row-level]` | 1 | Found 1 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |
| medium | `mixed_date_formats` | `HireDate` | 16 | Multiple date formats detected: ISO(YYYY-MM-DD)=15, US(MM/DD/YYYY)=1 | Multiple date formats in the same column (e.g. DD/MM/YYYY vs YYYY-MM-DD) cause silent parse errors. |
| low | `all_caps_values` | `Status` | 1 | 1 ALL-CAPS value(s) mixed with 18 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| low | `duplicate_insensitive_values` | `Status` | 2 | 2 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |
| medium | `custom_rule_violation` | `HireDate` | 3 | HireDate: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `EmployeeID` | 1 | EmployeeID: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `Email` | 1 | Email: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |

### Relationships

| Dataset A | Column A | Dataset B | Column B | Cardinality | Shared keys |
|---|---|---|---|---|---:|
| _none_ |  | _none_ |  |  |  |

### Global issues


#### Cross-table row issues (orphan keys)

- (none)

#### Relationship warnings

- (none)

### LLM Cleaning Recommendations


| Priority | Dataset | Column | Severity | Suggested Fix | Risk |
|---|---|---|---|---|---|
| 99 | `dbo.EmployeeData` | `[Row-level]` | high | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | medium | Correct formatting errors if minor, or reject record and request re-entry; add regex validation on email input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | medium | Standardize phone format (e.g. E.164); check for missing country codes or area codes. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Department` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `JobTitle` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `HireDate` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `HireDate` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Salary` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Location` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Status` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Location` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `[Row-level]` | medium | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `HireDate` | medium | Multiple date formats in the same column (e.g. DD/MM/YYYY vs YYYY-MM-DD) cause silent parse errors. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Status` | low | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Status` | low | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `HireDate` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).