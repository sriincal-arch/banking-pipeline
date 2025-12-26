"""Dagster sensors for the banking pipeline."""

from banking_pipeline.sensors.file_sensor import new_file_sensor

__all__ = ["new_file_sensor"]

