"""
Data Pipeline Package

A flexible data processing pipeline for large dataset analysis supporting
multiple data sources (S3, CSV, JSON), processing engines (Spark, PySpark, Python),
and PostgreSQL storage backend.
"""

__version__ = "0.1.0"
__author__ = "Data Pipeline Team"

from .core.pipeline import Pipeline
from .core.config import Config
from .sources import CSVSource, JSONSource, S3Source
from .processors import PandasProcessor, SparkProcessor
from .storage import PostgreSQLStorage

__all__ = [
    "Pipeline",
    "Config", 
    "CSVSource",
    "JSONSource", 
    "S3Source",
    "PandasProcessor",
    "SparkProcessor",
    "PostgreSQLStorage",
]