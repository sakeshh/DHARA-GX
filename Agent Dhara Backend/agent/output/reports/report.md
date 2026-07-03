## Assessment Report of `dbo.EmployeeData`

> [!NOTE]
> **dbo.EmployeeData**: Full dataset has 20 rows. Statistics (nulls, min/max, uniqueness) profiled in-database on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `dbo.EmployeeData` | Azure SQL (SQL data) | 20 | 10 | 33 | 14 | 13 | 6 |

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
| high | `nulls` | `EmployeeName` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Email` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Phone` | 2 | 2 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Department` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `JobTitle` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `HireDate` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Salary` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Location` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Status` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `EmployeeID` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `EmployeeName` | 2 | 2 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `Email` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `Location` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| high | `nulls` | `EmployeeID` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `EmployeeName` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Email` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| high | `nulls` | `Phone` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `invalid_lookup_value` | `Department` | 1 | Value not in allowed lookup list for Department (1 invalid value(s)) | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `EmployeeID` | 1 | EmployeeID must follow the standard format (E followed by digits). | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `Email` | 20 | Email must belong to an approved company domain. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `Phone` | 6 | Phone must contain exactly 10 numeric digits. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `EmployeeName` | 1 | EmployeeName length must be between 3 and 100 characters. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `EmployeeName` | 1 | EmployeeName must contain only valid alphabetic characters and spaces. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `Department` | 2 | Department must match an approved department list. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `Status` | 4 | Status must be one of the approved status values. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
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
| 99 | `dbo.EmployeeData` | `EmployeeName` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Department` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `JobTitle` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `HireDate` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Salary` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Location` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Status` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Location` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | high | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Department` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeID` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Email` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Phone` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `EmployeeName` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Department` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.EmployeeData` | `Status` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
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