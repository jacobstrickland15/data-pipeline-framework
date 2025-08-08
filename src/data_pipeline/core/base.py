"""Base classes for data pipeline components."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def read(self, source: str, **kwargs) -> Union[pd.DataFrame, Any]:
        """Read data from the source."""
        pass
    
    @abstractmethod
    def list_sources(self) -> List[str]:
        """List available data sources."""
        pass
    
    @abstractmethod
    def get_schema(self, source: str) -> Dict[str, Any]:
        """Get schema information for a data source."""
        pass


class DataProcessor(ABC):
    """Abstract base class for data processors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def process(self, data: Any, operations: List[Dict[str, Any]]) -> Any:
        """Process data with given operations."""
        pass
    
    @abstractmethod
    def validate_data(self, data: Any, schema: Optional[Dict[str, Any]] = None) -> bool:
        """Validate data against schema."""
        pass


class DataStorage(ABC):
    """Abstract base class for data storage."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def write(self, data: Any, destination: str, **kwargs) -> bool:
        """Write data to storage."""
        pass
    
    @abstractmethod
    def read(self, source: str, **kwargs) -> Any:
        """Read data from storage."""
        pass
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        """List available tables/datasets."""
        pass
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table schema and metadata."""
        pass


class Pipeline:
    """Data processing pipeline orchestrator."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source: Optional[DataSource] = None
        self.processor: Optional[DataProcessor] = None
        self.storage: Optional[DataStorage] = None
    
    def set_source(self, source: DataSource) -> 'Pipeline':
        """Set data source."""
        self.source = source
        return self
    
    def set_processor(self, processor: DataProcessor) -> 'Pipeline':
        """Set data processor."""
        self.processor = processor
        return self
    
    def set_storage(self, storage: DataStorage) -> 'Pipeline':
        """Set data storage."""
        self.storage = storage
        return self
    
    def run(self, source_path: str, destination: str, operations: Optional[List[Dict[str, Any]]] = None) -> bool:
        """Run the complete pipeline."""
        if not all([self.source, self.processor, self.storage]):
            raise ValueError("Source, processor, and storage must be set before running pipeline")
        
        # Read data from source
        data = self.source.read(source_path)
        
        # Process data if operations are provided
        if operations:
            data = self.processor.process(data, operations)
        
        # Write to storage
        return self.storage.write(data, destination)