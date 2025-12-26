#!/usr/bin/env python3
"""
Generate sample banking data with intentional inconsistencies for testing.

This script generates accounts.csv and customers.csv files with:
- Various casing inconsistencies in names
- Different boolean representations for hasloan
- Some whitespace issues
- Mix of account types (savings, checking, money_market)
- Realistic balance distributions
"""

import argparse
import csv
import os
import random
from datetime import datetime

# Sample names with intentional casing variations
FIRST_NAMES = [
    "john", "JANE", "Bob", "ALICE", "Charlie",
    "   david", "eve  ", "FRANK", "grace", "HENRY",
    "Isabel", "JACK", "kate", "LIAM", "Mary",
    "nathan", "OLIVIA", "peter", "QUINN", "rachel",
]

LAST_NAMES = [
    "smith", "JOHNSON", "Williams", "BROWN", "jones",
    "garcia", "MILLER", "Davis", "RODRIGUEZ", "martinez",
    "HERNANDEZ", "lopez", "GONZALEZ", "Wilson", "ANDERSON",
    "thomas", "TAYLOR", "Moore", "JACKSON", "martin",
]

# Different boolean representations for hasloan
BOOLEAN_TRUE_VALUES = ["yes", "YES", "Yes", "true", "TRUE", "True", "1", "Y", "y", "T", "t"]
BOOLEAN_FALSE_VALUES = ["no", "NO", "No", "false", "FALSE", "False", "0", "N", "n", "F", "f"]

# Account types
ACCOUNT_TYPES = ["savings", "SAVINGS", "Savings", "checking", "CHECKING", "Checking", "money_market", "MONEY_MARKET"]

# Weight distribution for account types (60% savings)
ACCOUNT_TYPE_WEIGHTS = [0.2, 0.2, 0.2, 0.13, 0.13, 0.07, 0.03, 0.02]


def generate_customer_id(index: int) -> str:
    """Generate customer ID with some variations."""
    variations = [
        f"CUS{index:03d}",
        f"CUS{index:04d}",
        f" CUS{index:03d}",
        f"CUS{index:03d} ",
    ]
    return random.choice(variations) if random.random() < 0.2 else f"CUS{index:03d}"


def generate_account_id(index: int) -> str:
    """Generate account ID with some variations."""
    variations = [
        f"ACC{index:03d}",
        f"ACC{index:04d}",
        f" ACC{index:03d}",
        f"ACC{index:03d} ",
    ]
    return random.choice(variations) if random.random() < 0.2 else f"ACC{index:03d}"


def generate_name() -> str:
    """Generate a name with intentional casing inconsistencies."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f"{first} {last}"


def generate_hasloan() -> str:
    """Generate hasloan value with various representations."""
    has_loan = random.random() < 0.4  # 40% have loans
    if has_loan:
        return random.choice(BOOLEAN_TRUE_VALUES)
    return random.choice(BOOLEAN_FALSE_VALUES)


def generate_balance() -> str:
    """Generate balance with realistic distribution and some formatting variations."""
    # Distribution weighted towards lower balances
    tier = random.choices(
        ["low", "mid", "high", "very_high"],
        weights=[0.5, 0.3, 0.15, 0.05]
    )[0]
    
    if tier == "low":
        balance = random.uniform(100, 9999)
    elif tier == "mid":
        balance = random.uniform(10000, 20000)
    elif tier == "high":
        balance = random.uniform(20001, 50000)
    else:
        balance = random.uniform(50001, 100000)
    
    # Sometimes format with commas (dirty data)
    if random.random() < 0.3:
        return f"{balance:,.2f}"
    return f"{balance:.2f}"


def generate_account_type() -> str:
    """Generate account type with casing variations."""
    return random.choices(ACCOUNT_TYPES, weights=ACCOUNT_TYPE_WEIGHTS)[0]


def generate_customers(num_customers: int) -> list[dict]:
    """Generate customer records."""
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            "customerid": generate_customer_id(i),
            "name": generate_name(),
            "hasloan": generate_hasloan(),
        })
    return customers


def generate_accounts(num_accounts: int, num_customers: int) -> list[dict]:
    """Generate account records linked to customers."""
    accounts = []
    for i in range(1, num_accounts + 1):
        # Some customers have multiple accounts
        customer_idx = random.randint(1, num_customers)
        accounts.append({
            "accountid": generate_account_id(i),
            "account_type": generate_account_type(),
            "balance": generate_balance(),
            "customerid": generate_customer_id(customer_idx),
        })
    return accounts


def write_csv(filepath: str, data: list[dict], fieldnames: list[str]) -> None:
    """Write data to CSV file."""
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {len(data)} records in {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Generate sample banking data")
    parser.add_argument(
        "--customers", type=int, default=500,
        help="Number of customers to generate (default: 500)"
    )
    parser.add_argument(
        "--accounts", type=int, default=1000,
        help="Number of accounts to generate (default: 1000)"
    )
    parser.add_argument(
        "--output-dir", type=str, default="data/sample",
        help="Output directory for CSV files (default: data/sample)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility"
    )
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    # Generate data
    customers = generate_customers(args.customers)
    accounts = generate_accounts(args.accounts, args.customers)
    
    # Write to files
    write_csv(
        os.path.join(args.output_dir, "customers.csv"),
        customers,
        ["customerid", "name", "hasloan"]
    )
    write_csv(
        os.path.join(args.output_dir, "accounts.csv"),
        accounts,
        ["accountid", "account_type", "balance", "customerid"]
    )
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Customers: {len(customers)}")
    print(f"  Accounts: {len(accounts)}")
    
    # Count account types
    account_types = {}
    for acc in accounts:
        at = acc["account_type"].lower()
        account_types[at] = account_types.get(at, 0) + 1
    
    print(f"\nAccount type distribution:")
    for at, count in sorted(account_types.items()):
        print(f"  {at}: {count} ({count/len(accounts)*100:.1f}%)")
    
    # Count loan status
    has_loan = sum(1 for c in customers if c["hasloan"].lower() in ["yes", "true", "1", "y", "t"])
    print(f"\nCustomers with loans: {has_loan} ({has_loan/len(customers)*100:.1f}%)")


if __name__ == "__main__":
    main()


