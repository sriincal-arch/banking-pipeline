#!/usr/bin/env python3
"""
Local test script to run the pipeline without Docker/MinIO.
Loads sample data directly into DuckDB and runs dbt models.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DATA_DIR = PROJECT_ROOT / "data" / "sample"
DB_PATH = PROJECT_ROOT / "data" / "banking.duckdb"

# Ensure data directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def init_database(conn):
    """Initialize database schemas."""
    print("Initializing database schemas...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS structured")
    conn.execute("CREATE SCHEMA IF NOT EXISTS curated")
    conn.execute("CREATE SCHEMA IF NOT EXISTS access")
    print("✓ Schemas created")


def load_raw_data(conn):
    """Load sample CSV data into raw tables."""
    print("\nLoading raw data...")
    
    # Load accounts
    accounts_file = DATA_DIR / "accounts.csv"
    if accounts_file.exists():
        df = pd.read_csv(accounts_file)
        df["_source_file"] = str(accounts_file)
        df["_file_hash"] = "local_test_hash"
        df["_row_number"] = range(1, len(df) + 1)
        df["_ingested_at"] = datetime.now().isoformat()
        
        conn.execute("DROP TABLE IF EXISTS raw.raw_accounts")
        conn.execute("CREATE TABLE raw.raw_accounts AS SELECT * FROM df")
        print(f"✓ Loaded {len(df)} rows into raw.raw_accounts")
    else:
        print(f"✗ accounts.csv not found at {accounts_file}")
        return False
    
    # Load customers
    customers_file = DATA_DIR / "customers.csv"
    if customers_file.exists():
        df = pd.read_csv(customers_file)
        df["_source_file"] = str(customers_file)
        df["_file_hash"] = "local_test_hash"
        df["_row_number"] = range(1, len(df) + 1)
        df["_ingested_at"] = datetime.now().isoformat()
        
        conn.execute("DROP TABLE IF EXISTS raw.raw_customers")
        conn.execute("CREATE TABLE raw.raw_customers AS SELECT * FROM df")
        print(f"✓ Loaded {len(df)} rows into raw.raw_customers")
    else:
        print(f"✗ customers.csv not found at {customers_file}")
        return False
    
    return True


def run_dbt(select: str = None, full_refresh: bool = False):
    """Run dbt models."""
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
    env["DUCKDB_DATABASE"] = str(DB_PATH)
    
    cmd = ["dbt", "run"]
    if select:
        cmd.extend(["--select", select])
    if full_refresh:
        cmd.append("--full-refresh")
    
    print(f"\nRunning: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        env=env,
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        print(f"✗ dbt run failed:\n{result.stderr}\n{result.stdout}")
        return False
    
    print(result.stdout)
    return True


def run_dbt_tests(select: str = None):
    """Run dbt tests."""
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
    env["DUCKDB_DATABASE"] = str(DB_PATH)
    
    cmd = ["dbt", "test"]
    if select:
        cmd.extend(["--select", select])
    
    print(f"\nRunning: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        env=env,
        capture_output=True,
        text=True,
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Some tests failed:\n{result.stderr}")
    
    return result.returncode == 0


def export_results(conn):
    """Export account_summary to CSV."""
    print("\nExporting results...")
    
    try:
        df = conn.execute("SELECT * FROM access.account_summary ORDER BY customer_id, account_id").fetchdf()
        output_file = PROJECT_ROOT / "data" / "output" / "account_summary.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"✓ Exported {len(df)} rows to {output_file}")
        
        # Print summary
        print("\n" + "="*60)
        print("ACCOUNT SUMMARY")
        print("="*60)
        print(df.to_string(index=False))
        print("="*60)
        
        # Statistics
        print(f"\nTotal Accounts: {len(df)}")
        print(f"Total Original Balance: ${df['original_balance'].sum():,.2f}")
        print(f"Total Annual Interest: ${df['annual_interest'].sum():,.2f}")
        print(f"Total New Balance: ${df['new_balance'].sum():,.2f}")
        print(f"Average Interest Rate: {df['interest_rate'].mean()*100:.2f}%")
        
        return True
    except Exception as e:
        print(f"✗ Export failed: {e}")
        return False


def main():
    print("="*60)
    print("BANKING PIPELINE - LOCAL TEST")
    print("="*60)
    
    # Connect to DuckDB
    print(f"\nUsing database: {DB_PATH}")
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Step 1: Initialize database
        init_database(conn)
        
        # Step 2: Load raw data
        if not load_raw_data(conn):
            print("\n✗ Failed to load raw data")
            return 1
        
        # Show raw data
        print("\nRaw Accounts:")
        print(conn.execute("SELECT AccountID, CustomerID, Balance, AccountType FROM raw.raw_accounts").fetchdf().to_string(index=False))
        
        print("\nRaw Customers:")
        print(conn.execute("SELECT CustomerID, Name, HasLoan FROM raw.raw_customers").fetchdf().to_string(index=False))
        
        # Step 3: Run dbt models (first time with full refresh)
        print("\n" + "="*60)
        print("RUNNING DBT MODELS")
        print("="*60)
        
        if not run_dbt(full_refresh=True):
            print("\n✗ dbt run failed")
            return 1
        
        # Step 4: Run dbt tests
        print("\n" + "="*60)
        print("RUNNING DBT TESTS")
        print("="*60)
        run_dbt_tests()
        
        # Step 5: Export results
        export_results(conn)
        
        print("\n✓ Pipeline completed successfully!")
        return 0
        
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())


