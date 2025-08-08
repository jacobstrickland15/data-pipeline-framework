"""CSV data source implementation."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pathlib import Path
import glob
import os
from ..core.base import DataSource


class CSVSource(DataSource):
    """Data source for CSV files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_path = Path(config.get('base_path', './data'))
        self.encoding = config.get('encoding', 'utf-8')
        self.delimiter = config.get('delimiter', ',')
        
    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """Read CSV file(s) into DataFrame."""
        # Handle both single files and glob patterns
        if '*' in source or '?' in source:
            return self._read_multiple_files(source, **kwargs)
        else:
            return self._read_single_file(source, **kwargs)
    
    def _read_single_file(self, source: str, **kwargs) -> pd.DataFrame:
        """Read a single CSV file."""
        file_path = self._resolve_path(source)
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Merge config defaults with kwargs
        read_kwargs = {
            'encoding': self.encoding,
            'delimiter': self.delimiter,
            **kwargs
        }
        
        try:
            return pd.read_csv(file_path, **read_kwargs)
        except Exception as e:
            raise ValueError(f"Error reading CSV file {file_path}: {e}")
    
    def _read_multiple_files(self, pattern: str, **kwargs) -> pd.DataFrame:
        """Read multiple CSV files matching a pattern."""
        search_pattern = self._resolve_path(pattern)
        matching_files = glob.glob(str(search_pattern))
        
        if not matching_files:
            raise FileNotFoundError(f"No CSV files found matching pattern: {pattern}")
        
        dataframes = []
        for file_path in matching_files:
            df = pd.read_csv(file_path, encoding=self.encoding, delimiter=self.delimiter, **kwargs)
            # Add source file column if multiple files
            df['_source_file'] = os.path.basename(file_path)
            dataframes.append(df)
        
        return pd.concat(dataframes, ignore_index=True)
    
    def list_sources(self) -> List[str]:
        """List all CSV files in the base directory."""
        csv_files = []
        for pattern in ['*.csv', '**/*.csv']:
            csv_files.extend(glob.glob(str(self.base_path / pattern), recursive=True))
        
        # Return relative paths from base_path
        return [os.path.relpath(f, self.base_path) for f in csv_files]
    
    def get_schema(self, source: str) -> Dict[str, Any]:
        """Get schema information for a CSV file."""
        file_path = self._resolve_path(source)
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Read first few rows to infer schema
        sample_df = pd.read_csv(
            file_path, 
            nrows=100,
            encoding=self.encoding,
            delimiter=self.delimiter
        )
        
        schema = {
            'columns': {},
            'shape': (len(sample_df), len(sample_df.columns)),
            'file_size_bytes': file_path.stat().st_size,
            'encoding': self.encoding,
            'delimiter': self.delimiter
        }
        
        for col in sample_df.columns:
            schema['columns'][col] = {
                'dtype': str(sample_df[col].dtype),
                'null_count': sample_df[col].isnull().sum(),
                'unique_count': sample_df[col].nunique(),
                'sample_values': sample_df[col].dropna().head(5).tolist()
            }
        
        return schema
    
    def _resolve_path(self, source: str) -> Path:
        """Resolve source path relative to base_path."""
        source_path = Path(source)
        if source_path.is_absolute():
            return source_path
        return self.base_path / source