## Assessment Report of `dbo.courses_raw`

> [!NOTE]
> **dbo.courses_raw**: Full dataset has 51 rows. Statistics (nulls, min/max, uniqueness) profiled in-database on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `dbo.courses_raw` | Azure SQL (SQL data) | 51 | 6 | 14 | 1 | 3 | 10 |

### Columns (per dataset)


#### `dbo.courses_raw`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `course_id` | `object` | 0.0% | 50 | `categorical` | ✗ |
| `course_name` | `object` | 0.0% | 49 | `categorical` | ✗ |
| `credits` | `object` | 2.0% | 5 | `numeric_id` | ✗ |
| `department` | `object` | 0.0% | 3 | `categorical` | ✗ |
| `fee` | `object` | 0.0% | 47 | `numeric_id` | ✗ |
| `instructor` | `object` | 0.0% | 49 | `categorical` | ✗ |

### Top issues (per dataset)

#### `dbo.courses_raw`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| high | `duplicate_primary_key` | `[Row-level]` | 2 | 2 duplicate in candidate PK | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. |
| low | `nulls` | `credits` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `course_name` | 2 | 2 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `instructor` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `fee` | 1 | 1 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `nulls` | `instructor` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `sentinel_numeric_value` | `credits` | 1 | 1 sentinel/magic number(s) detected (e.g. -999, 9999999) | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. |
| medium | `near_duplicate_rows` | `[Row-level]` | 5 | Found 5 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |
| low | `all_caps_values` | `course_name` | 2 | 2 ALL-CAPS value(s) mixed with 49 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| low | `string_length_outlier` | `credits` | 1 | 1 value(s) exceed 4 chars (mean=1, σ=1) | Strings significantly longer than the column average may contain concatenated data or free-text errors. |
| low | `string_length_outlier` | `fee` | 1 | 1 value(s) exceed 7 chars (mean=5, σ=0) | Strings significantly longer than the column average may contain concatenated data or free-text errors. |
| low | `duplicate_insensitive_values` | `department` | 1 | 1 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |
| low | `nulls` | `fee` | 3 | 3 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `custom_rule_violation` | `instructor` | 1 | instructor: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |

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
| 99 | `dbo.courses_raw` | `[Row-level]` | high | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `credits` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `course_name` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `instructor` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `fee` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `instructor` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `credits` | medium | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `[Row-level]` | medium | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `course_name` | low | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `credits` | low | Strings significantly longer than the column average may contain concatenated data or free-text errors. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `fee` | low | Strings significantly longer than the column average may contain concatenated data or free-text errors. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `department` | low | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `fee` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `dbo.courses_raw` | `instructor` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).