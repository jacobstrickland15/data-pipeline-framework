"""Schema inference utilities for data pipeline."""

from typing import Dict, Any, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class SchemaInferrer:
    """Infer and manage data schemas."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.sample_size = self.config.get('sample_size', 10000)
        self.null_threshold = self.config.get('null_threshold', 0.9)  # Consider column null if >90% nulls
        self.cardinality_threshold = self.config.get('cardinality_threshold', 50)  # Low cardinality threshold
        
        # Date patterns for inference
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        ]
        
        # Datetime patterns
        self.datetime_patterns = [
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
        ]
    
    def infer_schema(self, data: pd.DataFrame, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Infer comprehensive schema from DataFrame."""
        # Sample data if too large
        if len(data) > self.sample_size:
            sample_data = data.sample(n=self.sample_size, random_state=42)
            logger.info(f"Sampling {self.sample_size} rows from {len(data)} total rows")
        else:
            sample_data = data
        
        schema = {
            'table_name': table_name or 'inferred_table',
            'total_rows': len(data),
            'sample_rows': len(sample_data),
            'total_columns': len(data.columns),
            'columns': {},
            'primary_key_candidates': [],
            'foreign_key_candidates': [],
            'constraints': {},
            'indexes_recommended': [],
            'data_quality': {}
        }
        
        # Analyze each column
        for column in data.columns:
            col_data = sample_data[column]
            schema['columns'][column] = self._analyze_column(col_data, column)
        
        # Find primary key candidates
        schema['primary_key_candidates'] = self._find_primary_key_candidates(sample_data, schema['columns'])
        
        # Find foreign key candidates
        schema['foreign_key_candidates'] = self._find_foreign_key_candidates(sample_data, schema['columns'])
        
        # Generate constraints
        schema['constraints'] = self._generate_constraints(sample_data, schema['columns'])
        
        # Recommend indexes
        schema['indexes_recommended'] = self._recommend_indexes(schema['columns'])
        
        # Data quality assessment
        schema['data_quality'] = self._assess_data_quality(sample_data, schema['columns'])
        
        return schema
    
    def _analyze_column(self, col_data: pd.Series, column_name: str) -> Dict[str, Any]:
        """Analyze individual column characteristics."""
        analysis = {
            'name': column_name,
            'pandas_dtype': str(col_data.dtype),
            'inferred_sql_type': None,
            'nullable': col_data.isnull().any(),
            'null_count': col_data.isnull().sum(),
            'null_percentage': (col_data.isnull().sum() / len(col_data)) * 100,
            'unique_count': col_data.nunique(),
            'cardinality': 'high' if col_data.nunique() / len(col_data) > 0.8 else 'medium' if col_data.nunique() / len(col_data) > 0.1 else 'low',
            'sample_values': [],
            'min_value': None,
            'max_value': None,
            'avg_length': None,
            'max_length': None,
            'patterns': [],
            'is_primary_key_candidate': False,
            'is_foreign_key_candidate': False,
            'data_quality_score': 0.0,
            'anomalies': []
        }
        
        # Get sample non-null values
        non_null_data = col_data.dropna()
        if len(non_null_data) > 0:
            analysis['sample_values'] = non_null_data.head(10).tolist()
            
            # Infer SQL data type and get statistics
            analysis.update(self._infer_sql_type(non_null_data))
            
            # Find patterns for string columns
            if analysis['inferred_sql_type'] in ['VARCHAR', 'TEXT']:
                analysis['patterns'] = self._find_patterns(non_null_data)
            
            # Check for anomalies
            analysis['anomalies'] = self._detect_anomalies(col_data)
        
        # Calculate data quality score
        analysis['data_quality_score'] = self._calculate_quality_score(analysis)
        
        return analysis
    
    def _infer_sql_type(self, data: pd.Series) -> Dict[str, Any]:
        """Infer SQL data type from pandas Series."""
        result = {
            'inferred_sql_type': 'TEXT',
            'min_value': None,
            'max_value': None,
            'avg_length': None,
            'max_length': None
        }
        
        # Check if numeric
        if pd.api.types.is_numeric_dtype(data):
            result['min_value'] = float(data.min()) if not pd.isna(data.min()) else None
            result['max_value'] = float(data.max()) if not pd.isna(data.max()) else None
            
            if pd.api.types.is_integer_dtype(data):
                # Determine integer size based on range
                min_val, max_val = result['min_value'] or 0, result['max_value'] or 0
                if -2147483648 <= min_val <= 2147483647 and -2147483648 <= max_val <= 2147483647:
                    result['inferred_sql_type'] = 'INTEGER'
                else:
                    result['inferred_sql_type'] = 'BIGINT'
            else:
                # Float/decimal
                result['inferred_sql_type'] = 'DOUBLE PRECISION'
        
        # Check if boolean
        elif pd.api.types.is_bool_dtype(data):
            result['inferred_sql_type'] = 'BOOLEAN'
        
        # Check if datetime
        elif pd.api.types.is_datetime64_any_dtype(data):
            result['inferred_sql_type'] = 'TIMESTAMP'
            result['min_value'] = str(data.min()) if not pd.isna(data.min()) else None
            result['max_value'] = str(data.max()) if not pd.isna(data.max()) else None
        
        # String types
        else:
            string_data = data.astype(str)
            lengths = string_data.str.len()
            result['avg_length'] = float(lengths.mean())
            result['max_length'] = int(lengths.max())
            
            # Try to parse as dates
            if self._is_date_column(string_data):
                result['inferred_sql_type'] = 'DATE'
            elif self._is_datetime_column(string_data):
                result['inferred_sql_type'] = 'TIMESTAMP'
            else:
                # Determine string type based on length
                max_len = result['max_length']
                if max_len <= 255:
                    result['inferred_sql_type'] = f'VARCHAR({max(max_len, 50)})'
                else:
                    result['inferred_sql_type'] = 'TEXT'
        
        return result
    
    def _is_date_column(self, string_data: pd.Series) -> bool:
        """Check if string column contains dates."""
        sample = string_data.dropna().head(100)
        date_count = 0
        
        for pattern in self.date_patterns:
            for value in sample:
                if re.match(pattern, str(value)):
                    date_count += 1
                    break
        
        return date_count / len(sample) > 0.7 if len(sample) > 0 else False
    
    def _is_datetime_column(self, string_data: pd.Series) -> bool:
        """Check if string column contains datetimes."""
        sample = string_data.dropna().head(100)
        datetime_count = 0
        
        for pattern in self.datetime_patterns:
            for value in sample:
                if re.match(pattern, str(value)):
                    datetime_count += 1
                    break
        
        return datetime_count / len(sample) > 0.7 if len(sample) > 0 else False
    
    def _find_patterns(self, data: pd.Series) -> List[str]:
        """Find common patterns in string data."""
        patterns = []
        sample = data.head(100).astype(str)
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if sample.str.match(email_pattern).sum() / len(sample) > 0.7:
            patterns.append('email')
        
        # Phone pattern
        phone_pattern = r'^[\+]?[1-9]?[0-9]{7,15}$'
        if sample.str.match(phone_pattern).sum() / len(sample) > 0.5:
            patterns.append('phone')
        
        # URL pattern
        url_pattern = r'^https?://'
        if sample.str.match(url_pattern).sum() / len(sample) > 0.5:
            patterns.append('url')
        
        # Numeric ID pattern
        id_pattern = r'^\d+$'
        if sample.str.match(id_pattern).sum() / len(sample) > 0.8:
            patterns.append('numeric_id')
        
        return patterns
    
    def _detect_anomalies(self, data: pd.Series) -> List[str]:
        """Detect data anomalies."""
        anomalies = []
        
        # High null percentage
        null_pct = (data.isnull().sum() / len(data)) * 100
        if null_pct > self.null_threshold * 100:
            anomalies.append(f'high_null_percentage_{null_pct:.1f}%')
        
        # For numeric columns
        if pd.api.types.is_numeric_dtype(data):
            # Outliers using IQR method
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = data[(data < lower_bound) | (data > upper_bound)]
            if len(outliers) > 0:
                anomalies.append(f'outliers_detected_{len(outliers)}_values')
        
        # For string columns
        elif data.dtype == 'object':
            # Inconsistent formats
            sample = data.dropna().head(1000).astype(str)
            unique_lengths = sample.str.len().nunique()
            if unique_lengths > len(sample) * 0.5:  # Many different lengths
                anomalies.append('inconsistent_string_lengths')
        
        return anomalies
    
    def _calculate_quality_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate data quality score for a column."""
        score = 100.0
        
        # Penalize for high null percentage
        null_pct = analysis['null_percentage']
        if null_pct > 50:
            score -= 40
        elif null_pct > 20:
            score -= 20
        elif null_pct > 5:
            score -= 10
        
        # Penalize for anomalies
        score -= len(analysis['anomalies']) * 10
        
        # Bonus for good patterns
        if analysis['patterns']:
            score += 5
        
        return max(0.0, min(100.0, score))
    
    def _find_primary_key_candidates(self, data: pd.DataFrame, columns: Dict[str, Any]) -> List[str]:
        """Find potential primary key candidates."""
        candidates = []
        
        for col_name, col_info in columns.items():
            # Check if column could be a primary key
            if (col_info['unique_count'] == len(data) and  # All unique
                col_info['null_count'] == 0 and  # No nulls
                col_info['inferred_sql_type'] in ['INTEGER', 'BIGINT', 'VARCHAR']):  # Appropriate type
                candidates.append(col_name)
                columns[col_name]['is_primary_key_candidate'] = True
        
        return candidates
    
    def _find_foreign_key_candidates(self, data: pd.DataFrame, columns: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find potential foreign key relationships."""
        candidates = []
        
        for col_name, col_info in columns.items():
            # Look for columns that might reference other tables
            if (col_name.endswith('_id') or col_name.endswith('Id') or 
                'id' in col_name.lower() and col_name not in ['id']):
                
                if col_info['inferred_sql_type'] in ['INTEGER', 'BIGINT']:
                    candidates.append({
                        'column': col_name,
                        'referenced_table': col_name.replace('_id', '').replace('Id', ''),
                        'confidence': 'medium'
                    })
                    columns[col_name]['is_foreign_key_candidate'] = True
        
        return candidates
    
    def _generate_constraints(self, data: pd.DataFrame, columns: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate recommended database constraints."""
        constraints = {
            'not_null': [],
            'unique': [],
            'check': []
        }
        
        for col_name, col_info in columns.items():
            # NOT NULL constraints
            if col_info['null_percentage'] < 5:  # Less than 5% nulls
                constraints['not_null'].append(col_name)
            
            # UNIQUE constraints
            if col_info['unique_count'] == len(data) and not col_info.get('is_primary_key_candidate', False):
                constraints['unique'].append(col_name)
            
            # CHECK constraints for numeric columns
            if col_info['inferred_sql_type'] in ['INTEGER', 'BIGINT', 'DOUBLE PRECISION']:
                if col_info['min_value'] is not None and col_info['min_value'] >= 0:
                    constraints['check'].append(f"{col_name} >= 0")
        
        return constraints
    
    def _recommend_indexes(self, columns: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Recommend database indexes."""
        recommendations = []
        
        for col_name, col_info in columns.items():
            # Index foreign key candidates
            if col_info.get('is_foreign_key_candidate', False):
                recommendations.append({
                    'type': 'btree',
                    'columns': [col_name],
                    'reason': 'foreign_key_performance'
                })
            
            # Index low cardinality columns for filtering
            elif col_info['cardinality'] == 'low' and col_info['unique_count'] > 1:
                recommendations.append({
                    'type': 'btree',
                    'columns': [col_name],
                    'reason': 'low_cardinality_filtering'
                })
            
            # Index date/timestamp columns
            elif col_info['inferred_sql_type'] in ['DATE', 'TIMESTAMP']:
                recommendations.append({
                    'type': 'btree',
                    'columns': [col_name],
                    'reason': 'time_based_queries'
                })
        
        return recommendations
    
    def _assess_data_quality(self, data: pd.DataFrame, columns: Dict[str, Any]) -> Dict[str, Any]:
        """Assess overall data quality."""
        total_cells = len(data) * len(data.columns)
        null_cells = sum(col['null_count'] for col in columns.values())
        
        quality_scores = [col['data_quality_score'] for col in columns.values()]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        return {
            'overall_score': avg_quality,
            'completeness': ((total_cells - null_cells) / total_cells) * 100,
            'total_anomalies': sum(len(col['anomalies']) for col in columns.values()),
            'columns_with_issues': len([col for col in columns.values() if col['anomalies']]),
            'primary_key_coverage': len([col for col in columns.values() if col.get('is_primary_key_candidate', False)]) > 0
        }