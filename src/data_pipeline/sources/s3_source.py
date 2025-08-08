"""S3 data source implementation."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import io
from pathlib import Path
from ..core.base import DataSource


class S3Source(DataSource):
    """Data source for S3 objects."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bucket = config.get('bucket')
        self.region = config.get('region', 'us-east-1')
        self.prefix = config.get('prefix', '')
        
        # Initialize S3 client
        self._init_s3_client(config)
        
    def _init_s3_client(self, config: Dict[str, Any]):
        """Initialize S3 client with credentials."""
        try:
            # Try to use provided credentials first
            if config.get('access_key_id') and config.get('secret_access_key'):
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=config['access_key_id'],
                    aws_secret_access_key=config['secret_access_key'],
                    region_name=self.region
                )
            else:
                # Use default credential chain (IAM roles, environment variables, etc.)
                self.s3_client = boto3.client('s3', region_name=self.region)
                
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket)
            
        except NoCredentialsError:
            raise ValueError("AWS credentials not found. Please provide access_key_id and secret_access_key or configure AWS credentials.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise ValueError(f"S3 bucket '{self.bucket}' not found or not accessible.")
            else:
                raise ValueError(f"Error connecting to S3: {e}")
    
    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """Read S3 object(s) into DataFrame."""
        file_format = kwargs.get('format', self._infer_format(source))
        
        if '*' in source:
            return self._read_multiple_objects(source, file_format, **kwargs)
        else:
            return self._read_single_object(source, file_format, **kwargs)
    
    def _read_single_object(self, source: str, file_format: str, **kwargs) -> pd.DataFrame:
        """Read a single S3 object."""
        s3_key = self._build_s3_key(source)
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read()
            
            if file_format.lower() == 'csv':
                return pd.read_csv(io.BytesIO(content), **kwargs)
            elif file_format.lower() in ['json', 'jsonl', 'ndjson']:
                if file_format.lower() in ['jsonl', 'ndjson']:
                    return pd.read_json(io.BytesIO(content), lines=True, **kwargs)
                else:
                    return pd.read_json(io.BytesIO(content), **kwargs)
            elif file_format.lower() == 'parquet':
                return pd.read_parquet(io.BytesIO(content), **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"S3 object not found: s3://{self.bucket}/{s3_key}")
            else:
                raise ValueError(f"Error reading S3 object: {e}")
    
    def _read_multiple_objects(self, pattern: str, file_format: str, **kwargs) -> pd.DataFrame:
        """Read multiple S3 objects matching a pattern."""
        matching_keys = self._list_objects_matching_pattern(pattern)
        
        if not matching_keys:
            raise FileNotFoundError(f"No S3 objects found matching pattern: {pattern}")
        
        dataframes = []
        for key in matching_keys:
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                content = response['Body'].read()
                
                if file_format.lower() == 'csv':
                    df = pd.read_csv(io.BytesIO(content), **kwargs)
                elif file_format.lower() in ['json', 'jsonl', 'ndjson']:
                    if file_format.lower() in ['jsonl', 'ndjson']:
                        df = pd.read_json(io.BytesIO(content), lines=True, **kwargs)
                    else:
                        df = pd.read_json(io.BytesIO(content), **kwargs)
                elif file_format.lower() == 'parquet':
                    df = pd.read_parquet(io.BytesIO(content), **kwargs)
                else:
                    continue  # Skip unsupported formats
                
                # Add source information
                df['_source_s3_key'] = key
                dataframes.append(df)
                
            except Exception as e:
                print(f"Warning: Error reading {key}: {e}")
                continue
        
        if not dataframes:
            raise ValueError("No valid data found in matching S3 objects")
        
        return pd.concat(dataframes, ignore_index=True)
    
    def list_sources(self) -> List[str]:
        """List all objects in the S3 bucket with the configured prefix."""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
            
            objects = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Remove prefix from key for relative path
                        key = obj['Key']
                        if key.startswith(self.prefix):
                            key = key[len(self.prefix):].lstrip('/')
                        objects.append(key)
            
            return objects
            
        except ClientError as e:
            raise ValueError(f"Error listing S3 objects: {e}")
    
    def get_schema(self, source: str) -> Dict[str, Any]:
        """Get schema information for an S3 object."""
        s3_key = self._build_s3_key(source)
        file_format = self._infer_format(source)
        
        try:
            # Get object metadata
            head_response = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            
            # Read a sample for schema inference
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read()
            
            # Read sample based on format
            if file_format.lower() == 'csv':
                sample_df = pd.read_csv(io.BytesIO(content), nrows=100)
            elif file_format.lower() in ['json', 'jsonl', 'ndjson']:
                if file_format.lower() in ['jsonl', 'ndjson']:
                    sample_df = pd.read_json(io.BytesIO(content), lines=True, nrows=100)
                else:
                    sample_df = pd.read_json(io.BytesIO(content))
                    if len(sample_df) > 100:
                        sample_df = sample_df.head(100)
            elif file_format.lower() == 'parquet':
                # For Parquet, read full file (usually pre-optimized)
                sample_df = pd.read_parquet(io.BytesIO(content))
                if len(sample_df) > 100:
                    sample_df = sample_df.head(100)
            else:
                raise ValueError(f"Unsupported format for schema inference: {file_format}")
            
            schema = {
                'columns': {},
                'shape': (len(sample_df), len(sample_df.columns)),
                'file_size_bytes': head_response['ContentLength'],
                'last_modified': head_response['LastModified'],
                's3_key': s3_key,
                'format': file_format
            }
            
            for col in sample_df.columns:
                schema['columns'][col] = {
                    'dtype': str(sample_df[col].dtype),
                    'null_count': sample_df[col].isnull().sum(),
                    'unique_count': sample_df[col].nunique(),
                    'sample_values': sample_df[col].dropna().head(3).tolist()
                }
            
            return schema
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"S3 object not found: s3://{self.bucket}/{s3_key}")
            else:
                raise ValueError(f"Error getting S3 object schema: {e}")
    
    def _build_s3_key(self, source: str) -> str:
        """Build full S3 key with prefix."""
        if self.prefix:
            return f"{self.prefix.rstrip('/')}/{source.lstrip('/')}"
        return source
    
    def _infer_format(self, source: str) -> str:
        """Infer file format from extension."""
        extension = Path(source).suffix.lower()
        format_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.jsonl': 'jsonl',
            '.ndjson': 'ndjson',
            '.parquet': 'parquet'
        }
        return format_map.get(extension, 'csv')  # Default to CSV
    
    def _list_objects_matching_pattern(self, pattern: str) -> List[str]:
        """List S3 objects matching a wildcard pattern."""
        import fnmatch
        
        # Get prefix part before any wildcards
        prefix_parts = pattern.split('*')[0].split('?')[0]
        search_prefix = self._build_s3_key(prefix_parts)
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=search_prefix)
            
            matching_keys = []
            full_pattern = self._build_s3_key(pattern)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if fnmatch.fnmatch(obj['Key'], full_pattern):
                            matching_keys.append(obj['Key'])
            
            return matching_keys
            
        except ClientError as e:
            raise ValueError(f"Error listing S3 objects with pattern: {e}")