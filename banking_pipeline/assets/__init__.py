"""Dagster assets for the banking pipeline."""

from banking_pipeline.assets.raw_layer import (
    initialize_database,
    raw_accounts,
    raw_customers,
)
from banking_pipeline.assets.structured_layer import run_dbt_structured
from banking_pipeline.assets.curated_layer import run_dbt_curated
from banking_pipeline.assets.access_layer import run_dbt_access, export_account_summary

__all__ = [
    "initialize_database",
    "raw_accounts",
    "raw_customers",
    "run_dbt_structured",
    "run_dbt_curated",
    "run_dbt_access",
    "export_account_summary",
]

