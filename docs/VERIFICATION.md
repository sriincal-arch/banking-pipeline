# Repository Verification Guide

This document provides commands to verify the repository is ready for submission.

---

## Repository Cleanup Verification

### Check Repository Size
```bash
du -sh /Users/karthikkaja/StudioProjects/banking-pipeline
```
**Expected:** ~3.6MB (cleaned from 482MB)

### Verify No Build Artifacts (in repo, not in running containers)
```bash
cd /Users/karthikkaja/StudioProjects/banking-pipeline
find . -path ./venv -prune -o -name "__pycache__" -type d -print
```
**Expected:** No output (all __pycache__ removed from repo)

**Note:** Docker containers will create __pycache__ at runtime (this is normal)

### Verify dbt Artifacts Removed
```bash
ls dbt/target dbt/logs 2>/dev/null || echo "Correctly removed"
```
**Expected:** "Correctly removed"

---

## Documentation Verification

### List All Documentation Files
```bash
ls -lh *.md
ls -lh docs/
```

**Expected Root Files:**
- ASSUMPTIONS.md (16K)
- DATA_QUALITY.md (9.5K)
- QUICKSTART.md (4.4K)
- README.md (12K)
- ai_prompts.md (9.1K)

**Expected docs/ Files:**
- OBSERVABILITY.md (8.6K)

**Total:** 6 documentation files (consolidated from 10)

---

## Deliverables Verification

### Required Files Checklist
```bash
# Check all required deliverables exist
for file in README.md ASSUMPTIONS.md ai_prompts.md QUICKSTART.md DATA_QUALITY.md; do
    if [ -f "$file" ]; then
        echo "✅ $file"
    else
        echo "❌ Missing: $file"
    fi
done
```

**Expected:** All files show ✅

---

## Functional Verification

### Start Docker Environment
```bash
cd /Users/karthikkaja/StudioProjects/banking-pipeline
docker-compose up -d
```

**Expected:**
- Container banking-pipeline started
- Services running: MinIO (ports 9000, 9001), Dagster (port 3000)

### Verify Services Running
```bash
docker ps
```

**Expected:** 1 container running (banking-pipeline)

### Access Dagster UI
Open browser: http://localhost:3000

**Expected:** Dagster UI loads successfully

### Run Pipeline (Option 1: Via dbt)
```bash
# Wait 10 seconds for services to be ready
sleep 10

# Load sample data to MinIO first (if needed)
docker exec banking-pipeline python3 -c "
import boto3
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

# Create buckets
for bucket in ['landing', 'processed', 'access']:
    try:
        s3.create_bucket(Bucket=bucket)
    except:
        pass

# Upload sample files
s3.upload_file('/opt/dagster/data/sample/accounts.csv', 'landing', 'accounts.csv')
s3.upload_file('/opt/dagster/data/sample/customers.csv', 'landing', 'customers.csv')
print('Sample data uploaded to MinIO')
"

# Then run the full pipeline via Dagster UI or manually load to raw tables
# Since this is a fresh environment, you'll need to:
# 1. Access Dagster UI at http://localhost:3000
# 2. Navigate to Assets
# 3. Click "Materialize all" to run the complete pipeline
```

### Run Tests
```bash
docker exec banking-pipeline bash -c "cd /opt/dagster/app/dbt && dbt test"
```

**Expected Output:**
```
Completed successfully

Done. PASS=47 WARN=0 ERROR=0 SKIP=0 TOTAL=47
```

### Verify Output File (after pipeline run)
```bash
docker exec banking-pipeline cat /opt/dagster/data/output/account_summary.csv 2>/dev/null || echo "Run pipeline first via Dagster UI"
```

**Expected (after successful pipeline run):**
```csv
account_id,customer_id,customer_name,account_type,original_balance,hasloan,interest_rate,annual_interest,new_balance
A001,C001,Alice Johnson,savings,15000.00,true,0.020,300.00,15300.00
A002,C001,Alice Johnson,savings,8000.00,true,0.015,120.00,8120.00
A003,C001,Alice Johnson,savings,25000.00,true,0.025,625.00,25625.00
A005,C002,Bob Smith,savings,5000.00,false,0.010,50.00,5050.00
```

4 rows (savings accounts only)

---

## Git Repository Verification

### Initialize Git (if not done)
```bash
cd /Users/karthikkaja/StudioProjects/banking-pipeline
git init
git status
```

**Expected:** Clean working directory or untracked files ready to commit

### Verify .gitignore
```bash
cat .gitignore | grep -E "venv|__pycache__|target|.duckdb"
```

**Expected:**
```
venv/
__pycache__/
*.pyc
dbt/target/
dbt/logs/
*.duckdb
*.duckdb.wal
```

---

## Submission Package Verification

### Repository Structure
```bash
tree -L 2 -I 'venv|__pycache__|dbt_packages|target|logs'
```

**Expected Structure:**
```
.
├── ASSUMPTIONS.md
├── DATA_QUALITY.md
├── QUICKSTART.md
├── README.md
├── ai_prompts.md
├── banking_pipeline/
│   ├── assets/
│   ├── resources/
│   ├── sensors/
│   ├── definitions.py
│   └── ...
├── dbt/
│   ├── models/
│   ├── macros/
│   └── dbt_project.yml
├── data/
│   └── sample/
├── docs/
│   └── OBSERVABILITY.md
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

### File Count Verification
```bash
echo "Python files: $(find banking_pipeline -name '*.py' | wc -l)"
echo "SQL files: $(find dbt/models -name '*.sql' | wc -l)"
echo "Test files: $(find dbt/models -name '*__models.yml' | wc -l)"
echo "Documentation files: $(ls *.md docs/*.md 2>/dev/null | wc -l)"
```

**Expected:**
- Python files: ~10-15
- SQL files: 6 (str_accounts, str_customers, dim_account, dim_customer, fact_customer_balances, account_summary)
- Test files: 3-4 YAML files
- Documentation files: 6

---

## Cleanup Commands

### Stop Docker Environment
```bash
docker-compose down
```

### Stop and Remove Volumes (clean slate)
```bash
docker-compose down --volumes
```

### Remove All Docker Resources (if needed)
```bash
docker-compose down --volumes --rmi all
docker system prune -f
```

---

## Quick Demo Script (for Interview)

```bash
# 1. Start services (30 seconds)
cd /Users/karthikkaja/StudioProjects/banking-pipeline
docker-compose up -d
sleep 10

# 2. Show repository structure
tree -L 2 -I 'venv|__pycache__|dbt_packages'

# 3. Access Dagster UI
open http://localhost:3000
# Manually click "Materialize all" in Assets view

# 4. View results (after pipeline completes)
docker exec banking-pipeline cat /opt/dagster/data/output/account_summary.csv

# 5. Show test results
docker exec banking-pipeline bash -c "cd /opt/dagster/app/dbt && dbt test"
```

---

## Success Criteria

- ✅ Repository size < 30MB (currently ~3.6MB)
- ✅ All build artifacts removed from repo (venv, __pycache__, dbt/target)
- ✅ 6 core documentation files (README, QUICKSTART, ASSUMPTIONS, DATA_QUALITY, ai_prompts, docs/OBSERVABILITY)
- ✅ Docker environment starts successfully
- ✅ All 47 tests passing (run via `dbt test`)
- ✅ Pipeline produces account_summary.csv with 4 savings accounts
- ✅ All required deliverables present

---

## Troubleshooting

### Issue: "database is locked" error
**Solution:** Ensure only one DuckDB connection at a time. Wait for previous operations to complete.

### Issue: Raw tables don't exist
**Solution:** Run pipeline via Dagster UI first to ingest CSV files into raw layer.

### Issue: Tests failing
**Solution:** Ensure pipeline has run at least once to create tables. Use `dbt run --full-refresh && dbt test`.

### Issue: Container won't start
**Solution:**
```bash
docker-compose down --volumes
docker system prune -f
docker-compose build --no-cache
docker-compose up -d
```

---

## Next Steps After Verification

1. ✅ Initialize Git repository (if not done): `git init`
2. ✅ Create initial commit with all cleaned files
3. ✅ Push to GitHub (optional) or create ZIP archive
4. ✅ Prepare slides with architecture diagram and screenshots
5. ✅ Practice demo script for interview
