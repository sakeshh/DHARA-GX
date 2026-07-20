## Assessment Report of `data_quality_issues.csv`

> [!NOTE]
> **data_quality_issues.csv**: Analysis performed on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `data_quality_issues.csv` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/data_quality_issues_csv.csv) | 10000 | 3 | 17 | 1 | 5 | 11 |

### Columns (per dataset)


#### `data_quality_issues.csv`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `email` | `object` | 1.7% | 9248 | `email` | ✗ |
| `id` | `object` | 2.1% | 9787 | `numeric_id` | ✗ |
| `name` | `object` | 0.9% | 9908 | `categorical` | ✗ |

### Top issues (per dataset)

#### `data_quality_issues.csv`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| low | `nulls` | `id` | 213 | 213 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `suspicious_zero` | `id` | 1 | 1 suspicious zero(s) in ID column | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| low | `nulls` | `name` | 92 | 92 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `email` | 166 | 166 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `invalid_email` | `email` | 761 | 761 invalid email(s) | Correct formatting errors if minor, or reject record and request re-entry; add regex validation on email input. |
| low | `whitespace` | `name` | 186 | 186 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `email` | 173 | 173 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `internal_whitespace` | `name` | 186 | 186 value(s) with consecutive spaces | Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL. |
| low | `internal_whitespace` | `email` | 173 | 173 value(s) with consecutive spaces | Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL. |
| low | `nulls` | `name` | 1 | 1 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `sentinel_numeric_value` | `id` | 6 | 6 sentinel/magic number(s) detected (e.g. -999, 9999999) | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. |
| medium | `near_duplicate_rows` | `[Row-level]` | 55 | Found 55 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |
| low | `all_caps_values` | `name` | 315 | 315 ALL-CAPS value(s) mixed with 9593 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| low | `string_length_outlier` | `id` | 96 | 96 value(s) exceed 6 chars (mean=4, σ=0) | Strings significantly longer than the column average may contain concatenated data or free-text errors. |
| low | `duplicate_insensitive_values` | `name` | 42 | 42 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |
| medium | `invalid_numeric` | `id` | 96 | 96 non-numeric value(s) in numeric column | Cast column values to float/int; investigate why non-numeric characters are present in numeric fields. |
| high | `pii_email` | `email` | 184 | Embedded email detected in 'email': ~92.0% of sampled values | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |

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
| 99 | `data_quality_issues.csv` | `id` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `id` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `email` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `email` | medium | Correct formatting errors if minor, or reject record and request re-entry; add regex validation on email input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `email` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `email` | low | Collapse consecutive spaces with REGEXP_REPLACE or str.strip in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `id` | medium | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `[Row-level]` | medium | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `id` | low | Strings significantly longer than the column average may contain concatenated data or free-text errors. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `name` | low | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `id` | medium | Cast column values to float/int; investigate why non-numeric characters are present in numeric fields. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues.csv` | `email` | high | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).