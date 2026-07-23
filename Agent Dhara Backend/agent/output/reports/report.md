## Assessment Report

> [!NOTE]
> **sales.xml**: Analysis performed on 100% of rows.

> [!NOTE]
> **marketing.csv**: Analysis performed on 100% of rows.

> [!NOTE]
> **factory.json**: Analysis performed on 100% of rows.

### Datasets (summary)

| Dataset | Source | Rows | Cols | Issues | High | Med | Low |
|---|---|---:|---:|---:|---:|---:|---:|
| `sales.xml` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/sales_xml.xml) | 1000 | 5 | 1 | 0 | 1 | 0 |
| `marketing.csv` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/marketing_csv.csv) | 1000 | 5 | 1 | 0 | 1 | 0 |
| `factory.json` | Filesystem (abfss://04a585ce-f0ec-4a3a-9db2-e237711fd9e7@onelake.dfs.fabric.microsoft.com/abd8c72d-b5aa-4b58-83bd-fb97587de47e/Files/raw/factory_json.json) | 1000 | 5 | 2 | 1 | 1 | 0 |

### Columns (per dataset)


#### `sales.xml`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `campaign_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `product_id` | `object` | 0.0% | 100 | `categorical` | ✗ |
| `revenue` | `object` | 0.0% | 901 | `numeric_id` | ✗ |
| `sale_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `units_sold` | `object` | 0.0% | 289 | `numeric_id` | ✗ |

#### `marketing.csv`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `ad_spend` | `int64` | 0.0% | 968 | `numeric_id` | ✗ |
| `campaign_date` | `object` | 0.0% | 365 | `date` | ✗ |
| `campaign_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `channel` | `object` | 0.0% | 5 | `categorical` | ✗ |
| `product_id` | `object` | 0.0% | 100 | `categorical` | ✗ |

#### `factory.json`

| Column | dtype | null% | unique | semantic type | candidate_pk |
|---|---|---:|---:|---|:---:|
| `plant` | `object` | 0.0% | 4 | `categorical` | ✗ |
| `product_id` | `object` | 0.0% | 100 | `categorical` | ✗ |
| `production_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `sales_id` | `object` | 0.0% | 1000 | `categorical` | ✓ |
| `units_produced` | `int64` | 0.0% | 310 | `numeric_id` | ✗ |

### Top issues (per dataset)

#### `sales.xml`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| medium | `near_duplicate_rows` | `[Row-level]` | 24 | Found 24 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |

#### `marketing.csv`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| medium | `near_duplicate_rows` | `[Row-level]` | 6777 | Found 6777 pair(s) of near-duplicate rows withsimilarity >= 0.92 | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. |

#### `factory.json`

| Severity | Type | Column | Count | Message | Recommendation |
|:--:|---|---|---:|---|---|
| medium | `numeric_outliers_iqr` | `units_produced` | 44 | 44 IQR outlier(s) outside [-66.38, 350.62] | Investigate values beyond 1.5x IQR; check if they represent extreme cases, fraud, or data entry errors. |
| high | `numeric_outliers_zscore` | `units_produced` | 2 | 2 extreme outlier(s) beyond 4σ from mean (154.41 ± 88.59) | Investigate extreme z-score outliers (>4 std devs); likely data entry errors, test records, or fraud signals. |

### Relationships

| Dataset A | Column A | Dataset B | Column B | Cardinality | Shared keys |
|---|---|---|---|---|---:|
| `sales.xml` | `campaign_id` | `marketing.csv` | `campaign_id` | `one_to_one` | 1000 |
| `sales.xml` | `product_id` | `marketing.csv` | `product_id` | `many_to_many` | 100 |
| `sales.xml` | `product_id` | `factory.json` | `product_id` | `many_to_many` | 100 |
| `marketing.csv` | `product_id` | `factory.json` | `product_id` | `many_to_many` | 100 |

### Global issues


#### Cross-table row issues (orphan keys)

- (none)

#### Relationship warnings

| Severity | Warning |
|---|---|
| medium | sales.xml.product_id <-> marketing.csv.product_id: keys repeat on both sides (max 15 rows per key in sales.xml, max 15 in marketing.csv). |
| medium | sales.xml.product_id <-> factory.json.product_id: keys repeat on both sides (max 15 rows per key in sales.xml, max 15 in factory.json). |
| medium | marketing.csv.product_id <-> factory.json.product_id: keys repeat on both sides (max 15 rows per key in marketing.csv, max 15 in factory.json). |

### LLM Cleaning Recommendations


| Priority | Dataset | Column | Severity | Suggested Fix | Risk |
|---|---|---|---|---|---|
| 1 | `factory.json` | `units_produced` | high | Investigate extreme z-score outliers (>4 std devs); likely data entry errors, test records, or fraud signals. | High risk of incorrect data influencing analysis and decision-making. |
| 2 | `factory.json` | `units_produced` | medium | Investigate values beyond 1.5x IQR; check if they represent extreme cases, fraud, or data entry errors. | Medium risk of misleading data affecting operational decisions. |
| 3 | `marketing.csv` | `[Row-level]` | medium | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. | Medium risk of inaccurate reporting and analysis. |
| 4 | `sales.xml` | `[Row-level]` | medium | Rows that are identical except for one or two fields may be erroneous duplicates; deduplicate or merge. | Medium risk of misleading sales data impacting business decisions. |

### What might still be missed

- Unmodelled business rules not captured in metadata manifest or cross-field rules.
- Drift in tails of distributions when only moments/null/distinct are snapshotted.
- Nested payload loss if raw JSON/XML is flattened without registry entries.
- GX expectations are sampled on very large tables (see GX logs).