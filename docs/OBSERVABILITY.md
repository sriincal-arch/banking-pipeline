# Observability Tracking in Banking Pipeline

This document details what observability metrics and metadata are tracked across the pipeline.

---

## Your List vs. Actual Implementation

### ✅ **Source: file location, name, timestamp**

**Status:** ✅ **Fully Tracked**

**Where:**
1. **`raw.file_ingestion_metadata` table:**
   - `file_path`: Full path (e.g., "landing/accounts.csv")
   - `file_name`: File name (e.g., "accounts.csv")
   - `ingestion_timestamp`: When file was ingested
   - `file_hash`: MD5 hash for deduplication

2. **Raw tables (row-level):**
   - `_source_file`: File path (e.g., "landing/accounts.csv")
   - `_file_hash`: File hash
   - `_ingested_at`: Timestamp when row was ingested

3. **Structured/Curated tables:**
   - `metadata_json`: JSON containing source lineage
     ```json
     {
       "source_file": "landing/accounts.csv",
       "row_number": "123",
       "file_hash": "abc123...",
       "ingested_at": "2024-01-01T10:00:00"
     }
     ```

---

### ✅ **Lineage: row-level tracking**

**Status:** ✅ **Fully Tracked**

**Where:**
1. **Raw Layer:**
   - Every row has: `_source_file`, `_file_hash`, `_row_number`, `_ingested_at`
   - Primary key: `(_file_hash, _row_number)` - unique per file/row

2. **Structured Layer:**
   - `metadata_json` preserves source lineage from raw layer
   - `_last_processed_at`: Timestamp for incremental tracking

3. **Curated Layer:**
   - `metadata_json` contains source lineage
   - For incremental updates, history is tracked in JSON

**Example Lineage Query:**
```sql
-- Trace a row from curated back to source
SELECT 
    c.account_id,
    json_extract(c.metadata_json, '$.source_file') as source_file,
    json_extract(c.metadata_json, '$.row_number') as source_row_number,
    json_extract(c.metadata_json, '$.file_hash') as source_file_hash
FROM curated.dim_account c
WHERE c.account_id = 'ACC001';
```

---

### ⚠️ **Metrics: row counts, run status, execution time**

**Status:** ⚠️ **Partially Tracked**

#### ✅ **Row Counts:**
- **Raw Layer:** Tracked in `file_ingestion_metadata.row_count`
- **Structured Layer:** Tracked in Dagster metadata (`str_accounts_rows`, `str_customers_rows`)
- **Curated Layer:** Tracked in Dagster metadata (`dim_customer_rows`, `dim_account_rows`, `fact_customer_balances_rows`)
- **Access Layer:** Tracked in Dagster metadata (`account_summary_rows`)

#### ✅ **Run Status:**
- **Raw Layer:** `file_ingestion_metadata.processing_status` ('pending', 'completed', 'failed')
- **Structured Layer:** Dagster metadata (`status`, `dbt_run`)
- **Curated Layer:** Dagster metadata (`status`, `dbt_run`)
- **Access Layer:** Dagster asset status

#### ⚠️ **Execution Time:**
- **Raw Layer:** ❌ **NOT tracked** (only timestamp, not duration)
- **Structured Layer:** ❌ **NOT tracked** in metadata
- **Curated Layer:** ✅ **Tracked** in Dagster metadata (`duration_seconds`)
- **Access Layer:** ❌ **NOT tracked**

**Gap:** Execution time is only tracked for curated layer, not for raw or structured layers.

---

### ✅ **Quality: validation results, checks**

**Status:** ✅ **Tracked (with limitations)**

**Where:**
1. **Dagster Asset Metadata:**
   - `tests_passed`: Boolean flag (true/false)
   - Available in Dagster UI for each asset

2. **dbt Logs:**
   - Full test output in `dbt/logs/dbt.log`
   - Which tests passed/failed
   - Error messages for failed tests

3. **Database Tables:**
   - `raw.file_ingestion_metadata.error_message`: Errors during ingestion
   - `curated.process_metadata.error_message`: Errors during ETL (if implemented)

**Limitations:**
- ❌ **No detailed test results stored** in database
- ❌ **No test failure counts** tracked
- ❌ **No test execution time** tracked
- ⚠️ Only boolean pass/fail flag, not detailed results

---

## Complete Observability Matrix

| Metric | Raw Layer | Structured Layer | Curated Layer | Access Layer |
|--------|-----------|------------------|--------------|--------------|
| **Source Tracking** |
| File location | ✅ | ✅ (via metadata_json) | ✅ (via metadata_json) | N/A |
| File name | ✅ | ✅ (via metadata_json) | ✅ (via metadata_json) | N/A |
| Timestamp | ✅ | ✅ | ✅ | ✅ |
| File hash | ✅ | ✅ (via metadata_json) | ✅ (via metadata_json) | N/A |
| **Lineage Tracking** |
| Row-level source | ✅ | ✅ | ✅ | N/A |
| Row number | ✅ | ✅ (via metadata_json) | ✅ (via metadata_json) | N/A |
| History tracking | N/A | ⚠️ (basic) | ✅ (incremental history) | N/A |
| **Metrics** |
| Row counts | ✅ | ✅ | ✅ | ✅ |
| Run status | ✅ | ✅ | ✅ | ✅ |
| Execution time | ❌ | ❌ | ✅ | ❌ |
| Records processed | ❌ | ⚠️ (new records only) | ✅ | N/A |
| Records inserted | ❌ | ❌ | ⚠️ (if process_metadata used) | N/A |
| Records updated | ❌ | ❌ | ⚠️ (if process_metadata used) | N/A |
| **Quality** |
| Test results | N/A | ✅ (boolean) | ✅ (boolean) | ✅ (boolean) |
| Test details | ❌ | ⚠️ (logs only) | ⚠️ (logs only) | ⚠️ (logs only) |
| Error messages | ✅ | ⚠️ (logs only) | ⚠️ (logs only) | ⚠️ (logs only) |
| Schema version | ✅ | N/A | N/A | N/A |

---

## Where to Find Observability Data

### 1. **Dagster UI** (http://localhost:3000)
- Asset metadata (row counts, status, tests_passed)
- Materialization history
- Execution logs

### 2. **Database Tables**

#### `raw.file_ingestion_metadata`
```sql
SELECT 
    file_name,
    file_path,
    ingestion_timestamp,
    row_count,
    processing_status,
    error_message
FROM raw.file_ingestion_metadata
ORDER BY ingestion_timestamp DESC;
```

#### `raw.schema_version_tracking`
```sql
SELECT 
    table_name,
    schema_version,
    column_definitions,
    detected_at,
    change_description
FROM raw.schema_version_tracking
ORDER BY table_name, schema_version DESC;
```

#### `curated.process_metadata` (if implemented)
```sql
SELECT 
    run_timestamp,
    target_table,
    records_processed,
    records_inserted,
    records_updated,
    duration_seconds,
    status,
    error_message
FROM curated.process_metadata
ORDER BY run_timestamp DESC;
```

### 3. **Row-Level Lineage**

#### In Raw Tables
```sql
SELECT 
    AccountID,
    _source_file,
    _file_hash,
    _row_number,
    _ingested_at
FROM raw.raw_accounts
WHERE AccountID = 'ACC001';
```

#### In Structured/Curated Tables
```sql
SELECT 
    account_id,
    metadata_json
FROM structured.str_accounts
WHERE account_id = 'ACC001';

-- Extract specific fields
SELECT 
    account_id,
    json_extract(metadata_json, '$.source_file') as source_file,
    json_extract(metadata_json, '$.row_number') as row_number,
    json_extract(metadata_json, '$.file_hash') as file_hash
FROM structured.str_accounts;
```

### 4. **dbt Logs**
```bash
# View test results
tail -f dbt/logs/dbt.log | grep -i "test\|fail\|pass"
```

---

## Recommendations for Improvement

### 1. **Add Execution Time Tracking**
```python
# In structured_layer.py
start_time = datetime.now()
# ... processing ...
duration = (datetime.now() - start_time).total_seconds()
return MaterializeResult(
    metadata={
        ...
        "duration_seconds": MetadataValue.float(duration),
    }
)
```

### 2. **Store Detailed Test Results**
```python
# Parse dbt test output and store in database
test_results = {
    "total_tests": 10,
    "passed": 8,
    "failed": 2,
    "failed_tests": ["test_not_null_account_id", "test_unique_customer_id"],
    "execution_time_seconds": 2.5
}
```

### 3. **Track Process Metadata for All Layers**
Currently only curated layer has `process_metadata` table structure, but it's not consistently populated. Consider:
- Populating `curated.process_metadata` for all ETL runs
- Creating similar tables for structured layer
- Tracking records processed/inserted/updated at each layer

### 4. **Enhanced Error Tracking**
```sql
CREATE TABLE IF NOT EXISTS raw.error_log (
    error_id VARCHAR PRIMARY KEY,
    layer VARCHAR,
    asset_name VARCHAR,
    error_type VARCHAR,
    error_message TEXT,
    error_timestamp TIMESTAMP,
    context JSON
);
```

### 5. **Data Quality Metrics Dashboard**
Track over time:
- Test pass rates
- Row count trends
- Execution time trends
- Error rates by layer

---

## Summary

Your observability list is **mostly accurate** with these notes:

✅ **Source tracking:** Fully implemented  
✅ **Lineage tracking:** Fully implemented (row-level)  
⚠️ **Metrics:** Row counts and status tracked, but **execution time only in curated layer**  
✅ **Quality:** Validation results tracked, but **only boolean pass/fail**, not detailed results

**Key Gaps:**
1. Execution time not tracked for raw/structured layers
2. Detailed test results not stored (only in logs)
3. Process metadata table exists but not consistently populated


