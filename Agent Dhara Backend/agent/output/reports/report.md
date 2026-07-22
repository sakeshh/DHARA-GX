## Assessment Report

> [!NOTE]
> **sales.xml**: Analysis performed on 100% of rows.

> [!NOTE]
> **factory.json**: Analysis performed on 100% of rows.

> [!NOTE]
> **marketing.csv**: Analysis performed on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `sales.xml` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/sales_xml.xml) | 1000 | 5 | 1 | 0 | 1 | 0 |
| `factory.json` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/factory_json.json) | 1000 | 5 | 33 | 1 | 19 | 13 |
| `marketing.csv` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/marketing_csv.csv) | 1000 | 5 | 19 | 1 | 9 | 9 |

### Columns (per dataset)


#### `sales.xml`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `amount` | `object` | 41.8% | 385 | `categorical` | ✗ |
| `customer` | `object` | 18.4% | 8 | `categorical` | ✗ |
| `region` | `object` | 20.3% | 8 | `categorical` | ✗ |
| `sale_date` | `object` | 35.4% | 220 | `categorical` | ✗ |
| `sale_id` | `object` | 41.0% | 590 | `categorical` | ✗ |

#### `factory.json`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `factory_id` | `object` | 19.3% | 607 | `categorical` | ✗ |
| `inspection_date` | `object` | 20.2% | 207 | `categorical` | ✗ |
| `location` | `object` | 10.1% | 9 | `categorical` | ✗ |
| `machine_name` | `object` | 9.2% | 9 | `categorical` | ✗ |
| `production_qty` | `object` | 20.6% | 387 | `categorical` | ✗ |

#### `marketing.csv`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `budget` | `object` | 38.2% | 415 | `categorical` | ✗ |
| `campaign_id` | `object` | 39.0% | 610 | `categorical` | ✗ |
| `channel` | `object` | 23.7% | 6 | `categorical` | ✗ |
| `customer_name` | `object` | 21.5% | 8 | `categorical` | ✗ |
| `start_date` | `object` | 40.5% | 202 | `categorical` | ✗ |

### Top issues (per dataset)

#### `sales.xml`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| medium | `cross_field_non_negative` | `amount` | 3 | Negative values in amount: 3 row(s) | Clip, nullify, or reject negatives per business policy. |

#### `factory.json`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| high | `duplicate_primary_key` | `[Row-level]` | 8 | 8 duplicate in candidate PK | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. |
| low | `nulls` | `factory_id` | 193 | 193 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `machine_name` | 92 | 92 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `production_qty` | 206 | 206 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `inspection_date` | 202 | 202 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `location` | 101 | 101 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `factory_id` | 201 | 201 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `machine_name` | 204 | 204 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `production_qty` | 205 | 205 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `inspection_date` | 206 | 206 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `location` | 114 | 114 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| medium | `punctuation_only_value` | `machine_name` | 93 | 93 punctuation-only value(s) | Replace symbol-only strings with NULL; investigate upstream export bugs. |
| medium | `placeholder_detected` | `factory_id` | 201 | 201 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `machine_name` | 187 | 187 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `production_qty` | 205 | 205 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `inspection_date` | 400 | 400 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `location` | 313 | 313 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `invalid_numeric` | `production_qty` | 198 | 198 non-numeric value(s) in numeric column 'production_qty' | Cast column values to float/int; investigate why non-numeric characters are present in numeric fields. |
| medium | `string_with_only_digits_in_text_column` | `location` | 111 | 111 digit string(s) in categorical column 'location' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| medium | `near_duplicate_rows` | `[Row-level]` | 449 | Found 449 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |
| low | `all_caps_values` | `machine_name` | 194 | 194 ALL-CAPS value(s) mixed with 714 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| medium | `string_with_only_digits_in_text_column` | `production_qty` | 390 | 390 digit string(s) in categorical column 'production_qty' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| medium | `string_with_only_digits_in_text_column` | `location` | 111 | 111 digit string(s) in categorical column 'location' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| low | `duplicate_insensitive_values` | `machine_name` | 2 | 2 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |
| low | `duplicate_insensitive_values` | `location` | 1 | 1 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |
| medium | `mixed_scalar_types` | `factory_id` | 212 | Inconsistent scalar types detected in column: {'str': 595, 'int': 212} | Standardize to a single scalar type in ETL (e.g. cast all IDs to string or integer) and enforce it at ingest. |
| medium | `mixed_scalar_types` | `production_qty` | 180 | Inconsistent scalar types detected in column: {'str': 614, 'int': 180} | Standardize to a single scalar type in ETL (e.g. cast all IDs to string or integer) and enforce it at ingest. |
| medium | `custom_rule_violation` | `factory_id` | 201 | factory_id: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `machine_name` | 90 | machine_name: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `production_qty` | 205 | production_qty: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `inspection_date` | 206 | inspection_date: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `custom_rule_violation` | `location` | 114 | location: expectation failed. | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `cross_field_non_negative` | `production_qty` | 1 | Negative values in production_qty: 1 row(s) | Clip, nullify, or reject negatives per business policy. |

#### `marketing.csv`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| high | `duplicate_primary_key` | `[Row-level]` | 71 | 71 duplicate in candidate PK | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. |
| low | `nulls` | `campaign_id` | 390 | 390 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `customer_name` | 215 | 215 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `budget` | 382 | 382 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `start_date` | 405 | 405 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `nulls` | `channel` | 237 | 237 null value(s) | Check if missing values are expected (nullable column); enforce NOT NULL constraint if critical, or backfill from defaults. |
| low | `whitespace` | `customer_name` | 108 | 108 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| low | `whitespace` | `channel` | 133 | 133 leading/trailing spaces | Apply trim/strip operations in ETL to remove leading and trailing space; enforce validation on input. |
| medium | `punctuation_only_value` | `customer_name` | 110 | 110 punctuation-only value(s) | Replace symbol-only strings with NULL; investigate upstream export bugs. |
| medium | `placeholder_detected` | `customer_name` | 110 | 110 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `budget` | 204 | 204 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `placeholder_detected` | `start_date` | 188 | 188 placeholder/semantic null value(s) (e.g. '???', 'N/A') | Replace placeholder strings ('???', 'N/A', 'unknown', 'tbd') with NULL during ETL cleansing. |
| medium | `invalid_date_format` | `start_date` | 205 | 205 invalid date/sentinel date value(s) in column 'start_date' | Review with domain owners; document the expected rule; add validation at ingest or in the warehouse. |
| medium | `string_with_only_digits_in_text_column` | `channel` | 128 | 128 digit string(s) in categorical column 'channel' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| medium | `near_duplicate_rows` | `[Row-level]` | 662 | Found 662 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |
| low | `all_caps_values` | `customer_name` | 94 | 94 ALL-CAPS value(s) mixed with 691 mixed/lower-case | Inconsistent all-caps entries may indicate data entry from legacy systems; normalize case in ETL. |
| medium | `string_with_only_digits_in_text_column` | `budget` | 219 | 219 digit string(s) in categorical column 'budget' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| medium | `string_with_only_digits_in_text_column` | `channel` | 128 | 128 digit string(s) in categorical column 'channel' | Text columns containing only digits may indicate a schema mismatch or misrouted data. |
| low | `duplicate_insensitive_values` | `channel` | 1 | 1 value(s) that differ only by case/whitespace — false uniqueness | Values that differ only by case/whitespace produce false uniqueness; deduplicate after normalization. |

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
| 1 | `factory.json` | `[Row-level]` | high | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. | High risk of data integrity issues. |
| 2 | `marketing.csv` | `[Row-level]` | high | Identify source of duplicate key generation; enforce UNIQUE constraint in database; apply deduplication filter in ETL. | High risk of data integrity issues. |
| 3 | `factory.json` | `production_qty` | medium | Clip, nullify, or reject negatives per business policy. | Medium risk of incorrect data analysis. |
| 4 | `sales.xml` | `amount` | medium | Clip, nullify, or reject negatives per business policy. | Medium risk of financial reporting errors. |
| 5 | `factory.json` | `factory_id` | low | Check if missing values are expected; enforce NOT NULL constraint if critical, or backfill from defaults. | Low risk of data integrity issues. |
| 6 | `marketing.csv` | `campaign_id` | low | Check if missing values are expected; enforce NOT NULL constraint if critical, or backfill from defaults. | Low risk of incomplete analysis. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).