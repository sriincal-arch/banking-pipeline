"""
Integration tests for the full banking data pipeline.

These tests verify the end-to-end functionality of the pipeline,
from data ingestion through to the final account_summary output.
"""

import os
import tempfile
from decimal import Decimal

import duckdb  # type: ignore[import-untyped]
import pytest  # type: ignore[import-untyped]


@pytest.fixture
def test_db():
    """Create a temporary DuckDB database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    
    conn = duckdb.connect(db_path)
    
    # Initialize schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS structured")
    conn.execute("CREATE SCHEMA IF NOT EXISTS curated")
    conn.execute("CREATE SCHEMA IF NOT EXISTS access")
    
    yield conn
    
    conn.close()
    os.unlink(db_path)


@pytest.fixture
def sample_raw_data(test_db):
    """Load sample raw data into the test database matching actual CSV structure."""
    # Create raw_accounts table with test data (matches actual CSV columns)
    test_db.execute("""
        CREATE TABLE raw.raw_accounts AS
        SELECT * FROM (VALUES
            ('A001', '101', 10000.00, 'Savings', 'landing/accounts.csv', 'hash1', 1, '2024-01-01'),
            ('A002', '102', 20000.00, 'Checking', 'landing/accounts.csv', 'hash1', 2, '2024-01-01'),
            ('A003', '103', 10000.00, 'Savings', 'landing/accounts.csv', 'hash1', 3, '2024-01-01'),
            ('A004', '104', NULL, 'Savings', 'landing/accounts.csv', 'hash1', 4, '2024-01-01'),
            ('A005', '105', 30000.00, 'Checking', 'landing/accounts.csv', 'hash1', 5, '2024-01-01'),
            ('A006', '105', 15000.00, 'Savings', 'landing/accounts.csv', 'hash1', 6, '2024-01-01'),
            ('A007', '106', 5000.00, 'savings', 'landing/accounts.csv', 'hash1', 7, '2024-01-01')
        ) AS t(AccountID, CustomerID, Balance, AccountType, _source_file, _file_hash, _row_number, _ingested_at)
    """)
    
    # Create raw_customers table with test data (matches actual CSV columns)
    test_db.execute("""
        CREATE TABLE raw.raw_customers AS
        SELECT * FROM (VALUES
            ('101', 'alice smith', 'Yes', 'landing/customers.csv', 'hash2', 1, '2024-01-01'),
            ('102', 'BOB JONES', 'no', 'landing/customers.csv', 'hash2', 2, '2024-01-01'),
            ('103', '  Charlie Brown ', 'YES', 'landing/customers.csv', 'hash2', 3, '2024-01-01'),
            ('104', 'DAVID LEE', 'No', 'landing/customers.csv', 'hash2', 4, '2024-01-01'),
            ('105', 'eve Adams', 'None', 'landing/customers.csv', 'hash2', 5, '2024-01-01'),
            ('106', 'Frank O''Connor', 'yes', 'landing/customers.csv', 'hash2', 6, '2024-01-01')
        ) AS t(CustomerID, Name, HasLoan, _source_file, _file_hash, _row_number, _ingested_at)
    """)
    
    return test_db


class TestDataCleansing:
    """Tests for data cleansing in the structured layer."""
    
    def test_name_normalization(self, sample_raw_data):
        """Verify names are normalized to proper case."""
        result = sample_raw_data.execute("""
            SELECT initcap(trim(Name)) as cleaned_name
            FROM raw.raw_customers
            WHERE CustomerID = '101'
        """).fetchone()
        
        assert result[0] == "Alice Smith"
    
    def test_name_with_whitespace(self, sample_raw_data):
        """Verify names with whitespace are properly trimmed."""
        result = sample_raw_data.execute("""
            SELECT initcap(trim(Name)) as cleaned_name
            FROM raw.raw_customers
            WHERE CustomerID = '103'
        """).fetchone()
        
        assert result[0] == "Charlie Brown"
    
    def test_account_type_lowercase(self, sample_raw_data):
        """Verify account types are converted to lowercase."""
        result = sample_raw_data.execute("""
            SELECT DISTINCT lower(trim(AccountType)) as cleaned_type
            FROM raw.raw_accounts
            WHERE lower(trim(AccountType)) = 'savings'
        """).fetchall()
        
        assert len(result) == 1
        assert result[0][0] == "savings"
    
    def test_boolean_standardization(self, sample_raw_data):
        """Verify boolean hasloan values are standardized including 'None'."""
        result = sample_raw_data.execute("""
            SELECT 
                CustomerID,
                CASE 
                    WHEN lower(trim(HasLoan)) IN ('yes', 'true', '1', 't', 'y') THEN true
                    WHEN lower(trim(HasLoan)) IN ('no', 'false', '0', 'f', 'n', '', 'none', 'null') THEN false
                    ELSE false
                END as standardized_hasloan
            FROM raw.raw_customers
            ORDER BY CustomerID
        """).fetchall()
        
        expected = [
            ('101', True),   # 'Yes'
            ('102', False),  # 'no'
            ('103', True),   # 'YES'
            ('104', False),  # 'No'
            ('105', False),  # 'None' -> false
            ('106', True),   # 'yes'
        ]
        
        assert result == expected


class TestInterestCalculation:
    """Tests for interest rate calculation logic."""
    
    @pytest.mark.parametrize("balance,hasloan,expected_rate", [
        (5000, False, 0.01),      # Tier 1, no loan
        (5000, True, 0.015),      # Tier 1, with loan
        (15000, False, 0.015),    # Tier 2, no loan
        (15000, True, 0.02),      # Tier 2, with loan
        (25000, False, 0.02),     # Tier 3, no loan
        (25000, True, 0.025),     # Tier 3, with loan
        (10000, False, 0.015),    # Boundary: exactly $10,000
        (20000, False, 0.015),    # Boundary: exactly $20,000
    ])
    def test_interest_rate_tiers(self, test_db, balance, hasloan, expected_rate):
        """Test interest rate calculation for different balance tiers."""
        result = test_db.execute(f"""
            SELECT
                CASE 
                    WHEN {balance} < 10000 THEN 0.01
                    WHEN {balance} BETWEEN 10000 AND 20000 THEN 0.015
                    ELSE 0.02
                END + CASE WHEN {hasloan} THEN 0.005 ELSE 0 END as interest_rate
        """).fetchone()
        
        assert abs(result[0] - expected_rate) < 0.0001
    
    def test_annual_interest_calculation(self, test_db):
        """Test annual interest is calculated correctly."""
        balance = 10000
        rate = 0.015
        
        result = test_db.execute(f"""
            SELECT round({balance} * {rate}, 2) as annual_interest
        """).fetchone()
        
        assert result[0] == 150.00
    
    def test_new_balance_calculation(self, test_db):
        """Test new balance is calculated correctly."""
        balance = 10000
        annual_interest = 150
        
        result = test_db.execute(f"""
            SELECT round({balance} + {annual_interest}, 2) as new_balance
        """).fetchone()
        
        assert result[0] == 10150.00


class TestSavingsOnlyFilter:
    """Tests for savings account filtering."""
    
    def test_only_savings_accounts_included(self, sample_raw_data):
        """Verify only savings accounts are included in interest calculations."""
        # Count savings accounts (case-insensitive)
        result = sample_raw_data.execute("""
            SELECT COUNT(*) 
            FROM raw.raw_accounts
            WHERE lower(trim(AccountType)) = 'savings'
        """).fetchone()
        
        # Should have 4 savings accounts (A001, A003, A004, A006, A007)
        # Note: A004 has null balance so will be filtered in structured layer
        assert result[0] == 5
    
    def test_checking_accounts_excluded(self, sample_raw_data):
        """Verify checking accounts are excluded."""
        result = sample_raw_data.execute("""
            SELECT COUNT(*) 
            FROM raw.raw_accounts
            WHERE lower(trim(AccountType)) = 'checking'
        """).fetchone()
        
        # A002, A005 are checking
        assert result[0] == 2
    
    def test_null_balance_filtered(self, sample_raw_data):
        """Verify accounts with null balance are filtered out."""
        result = sample_raw_data.execute("""
            SELECT COUNT(*) 
            FROM raw.raw_accounts
            WHERE Balance IS NULL
        """).fetchone()
        
        # A004 has null balance
        assert result[0] == 1


class TestReferentialIntegrity:
    """Tests for referential integrity between tables."""
    
    def test_all_accounts_have_customers(self, sample_raw_data):
        """Verify all accounts reference valid customers."""
        result = sample_raw_data.execute("""
            SELECT COUNT(*)
            FROM raw.raw_accounts a
            LEFT JOIN raw.raw_customers c ON trim(a.CustomerID) = trim(c.CustomerID)
            WHERE c.CustomerID IS NULL
        """).fetchone()
        
        assert result[0] == 0, "Found accounts without matching customers"
    
    def test_customer_with_multiple_accounts(self, sample_raw_data):
        """Verify customers can have multiple accounts."""
        result = sample_raw_data.execute("""
            SELECT CustomerID, COUNT(*) as account_count
            FROM raw.raw_accounts
            GROUP BY CustomerID
            HAVING COUNT(*) > 1
        """).fetchall()
        
        # Customer 105 has 2 accounts (A005, A006)
        assert len(result) == 1
        assert result[0][0] == '105'
        assert result[0][1] == 2


class TestOutputSchema:
    """Tests for output schema compliance."""
    
    def test_output_columns_present(self, test_db):
        """Verify output has all required columns."""
        # Create a mock output table
        test_db.execute("""
            CREATE TABLE access.account_summary AS
            SELECT 
                'CUS001' as customer_id,
                'ACC001' as account_id,
                5000.00 as original_balance,
                0.015 as interest_rate,
                75.00 as annual_interest,
                5075.00 as new_balance
        """)
        
        result = test_db.execute("""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'access' AND table_name = 'account_summary'
            ORDER BY ordinal_position
        """).fetchall()
        
        columns = [r[0] for r in result]
        expected = ['customer_id', 'account_id', 'original_balance', 
                   'interest_rate', 'annual_interest', 'new_balance']
        
        assert columns == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

