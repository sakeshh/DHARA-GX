## Assessment Report of `data_quality_issues_v2.json`

> [!NOTE]
> **data_quality_issues_v2.json**: Analysis performed on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `data_quality_issues_v2.json` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/data_quality_issues_v2_json) | 10000 | 3 | 10 | 0 | 3 | 7 |

### Columns (per dataset)


#### `data_quality_issues_v2.json`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `country` | `object` | 14.0% | 6 | `categorical` | ✗ |
| `name` | `object` | 0.0% | 10000 | `categorical` | ✓ |
| `user_id` | `object` | 3.4% | 9658 | `numeric_id` | ✗ |

### Top issues (per dataset)

#### `data_quality_issues_v2.json`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| low | `nulls` | `user_id` | 336 | 336 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `country` | 1398 | 1398 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `name` | 192 | 192 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `country` | 1391 | 1391 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `nulls` | `country` | 1391 | 1391 null/placeholder | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| medium | `sentinel_numeric_value` | `user_id` | 2 | 2 sentinel/magic number(s) detected (e.g. -999, 9999999) | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. |
| low | `all_caps_values` | `name` | 498 | 498 ALL-CAPS value(s) mixed with 9502 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| low | `string_length_outlier` | `user_id` | 159 | 159 value(s) exceed 6 chars (mean=5, σ=0) | Strings significantly longer than the column average may contain concatenated data or free-text errors. |
| medium | `custom_rule_violation` | `country` | 1391 | country: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `invalid_numeric` | `user_id` | 166 | 166 non-numeric value(s) in numeric column | Cast column values to float/int; investigate why non-numeric characters are present in numeric fields. |

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
| 99 | `data_quality_issues_v2.json` | `user_id` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `country` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `name` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `country` | low | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `country` | low | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `user_id` | medium | Replace sentinel values (-999, 9999999, etc.) with NULL; enforce domain constraints at source. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `name` | low | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `user_id` | low | Strings significantly longer than the column average may contain concatenated data or free-text errors. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `country` | medium | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. | Fallback mode (LLM not configured). Validate before applying changes. |
| 99 | `data_quality_issues_v2.json` | `user_id` | medium | Cast column values to float/int; investigate why non-numeric characters are present in numeric fields. | Fallback mode (LLM not configured). Validate before applying changes. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).