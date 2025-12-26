# Banking Pipeline

A data pipeline that processes customer accounts and calculates interest rates. Built with Dagster for orchestration, dbt for transformations, and DuckDB for analytics.

## What it does
Ingests two CSV files (accounts and customers), cleans the data, calculates interest based on balance tiers, and outputs a summary CSV with final balances.


## Prerequisites

Install Docker Desktop
```bash
docker --version  # Should be 20.10+
```

## How to run

1. **Start the container:**
   ```bash
   cd banking-pipeline
   docker-compose up -d
   ```

2. **Run the pipeline:**
**Option 2.a: Execute the pipeline from local**
   ```bash
   docker exec banking-pipeline run-pipeline
   ```

**Option 2.b: Dagster UI** 
   1. Open http://localhost:3000
   2. Navigate to Lineage → Materialize all


3. **Output:**
   ```bash
   docker exec banking-pipeline cat /opt/dagster/data/output/account_summary.csv
   ```

There should be a CSV with customer IDs, balances, interest rates, and new balances.

## I/O and Monitoring

| Type | Location |
|------|----------|
| **Input files** | `data/sample/accounts.csv`, `customers.csv` |
| **Output file** | `/opt/dagster/data/output/account_summary.csv` |
| **Dagster UI** | http://localhost:3000 |

### Sample Output

After running the pipeline, `account_summary.csv` will contain:

```csv
customer_id,account_id,original_balance,interest_rate,annual_interest,new_balance
101,A001,10000.0,0.02,200.0,10200.0
103,A003,10000.0,0.02,200.0,10200.0
104,A004,0.0,0.01,0.0,0.0
105,A006,15000.0,0.015,225.0,15225.0
106,A007,5000.0,0.015,75.0,5075.0
```

**Note:** Account A004 shows $0.00 balance (NULL value was imputed with transparency flag)

## Design Decisions

**Medallion architecture (4 layers)**
- Easier to debug, add transforms incrementally

**DuckDB**
- Embedded, no server, fast for analytics
- Downside: single threaded concurrency locks. consider Postgres/Datarbricks for production

**Testing**
- 47 tests total (26 unit + 21 data)

## Assumptions

- **Balance**: Imputed NULL values as $0.00 with transparency flag
- **HasLoan**: Imputed NULL flags as False
- **Data Export**: Full snapshot export while rest of ingestion is incremental
- **Relationship**: Assumed 1:N relationship between customer and account
- **Year-End Status**: Accounts file represents customer account status at year-end. For interest calculation, assumed each account was active throughout the full year
- **Data Cleansing**: Various cleansing rules applied following standard best practices (trim whitespace, normalize casing, type conversion)

## Next Steps

1. Modularize to support multiple backends (DuckDb/Postgres/Databricks etc)
2. Schema Evaluation and Drift Detection
3. Observability: Lineage, Monitoring, Metrics, Quality Scores/Metrics
4. Data Modeling: SCD Type 2 for Dimensions
5. Embed integration tests as part of the CI/CD Pipeline build
6. Secrets Management