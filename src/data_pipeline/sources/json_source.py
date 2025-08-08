"""JSON data source implementation."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import json
from pathlib import Path
import glob
import os
from ..core.base import DataSource


class JSONSource(DataSource):
    """Data source for JSON files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_path = Path(config.get('base_path', './data'))
        self.encoding = config.get('encoding', 'utf-8')
        self.lines_format = config.get('lines_format', False)  # For JSONL/NDJSON
        
    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """Read JSON file(s) into DataFrame."""
        if '*' in source or '?' in source:
            return self._read_multiple_files(source, **kwargs)
        else:
            return self._read_single_file(source, **kwargs)
    
    def _read_single_file(self, source: str, **kwargs) -> pd.DataFrame:
        """Read a single JSON file."""
        file_path = self._resolve_path(source)
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        
        try:
            if self.lines_format or kwargs.get('lines', False):
                # Handle JSONL/NDJSON format
                return pd.read_json(file_path, lines=True, encoding=self.encoding, **kwargs)
            else:
                # Handle regular JSON format
                return pd.read_json(file_path, encoding=self.encoding, **kwargs)
        except ValueError as e:
            # If pandas fails, try manual parsing for complex structures
            return self._manual_json_parse(file_path, **kwargs)
    
    def _manual_json_parse(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """Manually parse JSON when pandas fails."""
        with open(file_path, 'r', encoding=self.encoding) as f:
            if self.lines_format:
                # JSONL format
                data = [json.loads(line) for line in f]
            else:
                # Regular JSON
                data = json.load(f)
        
        # If data is a list of dictionaries, convert directly
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return pd.json_normalize(data)
        
        # If data is a nested structure, normalize it
        if isinstance(data, dict):
            return pd.json_normalize(data)
        
        # Fallback: create a single column DataFrame
        return pd.DataFrame({'data': [data]})
    
    def _read_multiple_files(self, pattern: str, **kwargs) -> pd.DataFrame:
        """Read multiple JSON files matching a pattern."""
        search_pattern = self._resolve_path(pattern)
        matching_files = glob.glob(str(search_pattern))
        
        if not matching_files:
            raise FileNotFoundError(f"No JSON files found matching pattern: {pattern}")
        
        dataframes = []
        for file_path in matching_files:
            if self.lines_format:
                df = pd.read_json(file_path, lines=True, encoding=self.encoding, **kwargs)
            else:
                try:
                    df = pd.read_json(file_path, encoding=self.encoding, **kwargs)
                except ValueError:
                    df = self._manual_json_parse(Path(file_path), **kwargs)
            
            # Add source file column
            df['_source_file'] = os.path.basename(file_path)
            dataframes.append(df)
        
        return pd.concat(dataframes, ignore_index=True)
    
    def list_sources(self) -> List[str]:
        """List all JSON files in the base directory."""
        json_files = []
        for pattern in ['*.json', '*.jsonl', '*.ndjson', '**/*.json', '**/*.jsonl', '**/*.ndjson']:
            json_files.extend(glob.glob(str(self.base_path / pattern), recursive=True))
        
        # Return relative paths from base_path
        return [os.path.relpath(f, self.base_path) for f in json_files]
    
    def get_schema(self, source: str) -> Dict[str, Any]:
        """Get schema information for a JSON file."""
        file_path = self._resolve_path(source)
        
        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        
        # Read a sample to infer schema
        if self.lines_format:
            sample_df = pd.read_json(file_path, lines=True, nrows=100, encoding=self.encoding)
        else:
            try:
                sample_df = pd.read_json(file_path, encoding=self.encoding)
                # If too large, sample first 100 rows
                if len(sample_df) > 100:
                    sample_df = sample_df.head(100)
            except ValueError:
                sample_df = self._manual_json_parse(file_path)
                if len(sample_df) > 100:
                    sample_df = sample_df.head(100)
        
        schema = {
            'columns': {},
            'shape': (len(sample_df), len(sample_df.columns)),
            'file_size_bytes': file_path.stat().st_size,
            'encoding': self.encoding,
            'format': 'lines' if self.lines_format else 'standard'
        }
        
        for col in sample_df.columns:
            schema['columns'][col] = {
                'dtype': str(sample_df[col].dtype),
                'null_count': sample_df[col].isnull().sum(),
                'unique_count': sample_df[col].nunique(),
                'sample_values': sample_df[col].dropna().head(3).tolist()
            }
        
        return schema
    
    def _resolve_path(self, source: str) -> Path:
        """Resolve source path relative to base_path."""
        source_path = Path(source)
        if source_path.is_absolute():
            return source_path
        return self.base_path / source