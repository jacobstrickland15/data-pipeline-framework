"""Data processor implementations."""

from .pandas_processor import PandasProcessor
from .spark_processor import SparkProcessor

__all__ = ["PandasProcessor", "SparkProcessor"]