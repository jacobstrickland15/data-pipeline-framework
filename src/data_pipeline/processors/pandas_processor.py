"""Pandas-based data processor implementation."""

from typing import Any, Dict, List, Optional, Union, Callable
import pandas as pd
import numpy as np
from datetime import datetime
import re
from ..core.base import DataProcessor


class PandasProcessor(DataProcessor):
    """Data processor using pandas for in-memory operations."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.memory_limit_gb = config.get('memory_limit_gb', 8)
        self.chunk_size = config.get('chunk_size', 10000)
        
    def process(self, data: pd.DataFrame, operations: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process data with a list of operations."""
        result = data.copy()
        
        for operation in operations:
            op_type = operation.get('type')
            op_params = operation.get('params', {})
            
            if op_type == 'filter':
                result = self._filter(result, **op_params)
            elif op_type == 'select':
                result = self._select_columns(result, **op_params)
            elif op_type == 'transform':
                result = self._transform_columns(result, **op_params)
            elif op_type == 'aggregate':
                result = self._aggregate(result, **op_params)
            elif op_type == 'join':
                result = self._join(result, **op_params)
            elif op_type == 'sort':
                result = self._sort(result, **op_params)
            elif op_type == 'deduplicate':
                result = self._deduplicate(result, **op_params)
            elif op_type == 'clean':
                result = self._clean_data(result, **op_params)
            elif op_type == 'pivot':
                result = self._pivot(result, **op_params)
            elif op_type == 'melt':
                result = self._melt(result, **op_params)
            elif op_type == 'custom':
                result = self._apply_custom_function(result, **op_params)
            else:
                raise ValueError(f"Unknown operation type: {op_type}")
                
        return result
    
    def _filter(self, df: pd.DataFrame, condition: str = None, column: str = None, 
                operator: str = None, value: Any = None, **kwargs) -> pd.DataFrame:
        """Filter rows based on conditions."""
        if condition:
            # Use eval for complex conditions
            return df.query(condition)
        elif column and operator and value is not None:
            # Simple column-operator-value filtering
            if operator == '==':
                return df[df[column] == value]
            elif operator == '!=':
                return df[df[column] != value]
            elif operator == '>':
                return df[df[column] > value]
            elif operator == '<':
                return df[df[column] < value]
            elif operator == '>=':
                return df[df[column] >= value]
            elif operator == '<=':
                return df[df[column] <= value]
            elif operator == 'in':
                return df[df[column].isin(value)]
            elif operator == 'not_in':
                return df[~df[column].isin(value)]
            elif operator == 'contains':
                return df[df[column].str.contains(str(value), na=False)]
            elif operator == 'starts_with':
                return df[df[column].str.startswith(str(value), na=False)]
            elif operator == 'ends_with':
                return df[df[column].str.endswith(str(value), na=False)]
            else:
                raise ValueError(f"Unknown operator: {operator}")
        else:
            raise ValueError("Either 'condition' or 'column', 'operator', 'value' must be provided")
    
    def _select_columns(self, df: pd.DataFrame, columns: List[str] = None, 
                       exclude: List[str] = None, **kwargs) -> pd.DataFrame:
        """Select or exclude columns."""
        if columns:
            return df[columns]
        elif exclude:
            return df.drop(columns=exclude)
        else:
            return df
    
    def _transform_columns(self, df: pd.DataFrame, transformations: Dict[str, Dict] = None, **kwargs) -> pd.DataFrame:
        """Transform columns with various operations."""
        result = df.copy()
        
        for column, transform_config in transformations.items():
            transform_type = transform_config.get('type')
            params = transform_config.get('params', {})
            
            if transform_type == 'rename':
                new_name = params.get('new_name')
                result = result.rename(columns={column: new_name})
            elif transform_type == 'cast':
                dtype = params.get('dtype')
                result[column] = result[column].astype(dtype)
            elif transform_type == 'fill_null':
                value = params.get('value')
                method = params.get('method', 'value')
                if method == 'value':
                    result[column] = result[column].fillna(value)
                elif method == 'forward_fill':
                    result[column] = result[column].fillna(method='ffill')
                elif method == 'backward_fill':
                    result[column] = result[column].fillna(method='bfill')
                elif method == 'mean':
                    result[column] = result[column].fillna(result[column].mean())
                elif method == 'median':
                    result[column] = result[column].fillna(result[column].median())
            elif transform_type == 'calculate':
                expression = params.get('expression')
                result[column] = result.eval(expression)
            elif transform_type == 'extract_date_parts':
                result[column] = pd.to_datetime(result[column])
                if params.get('year'):
                    result[f"{column}_year"] = result[column].dt.year
                if params.get('month'):
                    result[f"{column}_month"] = result[column].dt.month
                if params.get('day'):
                    result[f"{column}_day"] = result[column].dt.day
                if params.get('weekday'):
                    result[f"{column}_weekday"] = result[column].dt.dayofweek
            elif transform_type == 'regex_extract':
                pattern = params.get('pattern')
                result[column] = result[column].str.extract(pattern)
            elif transform_type == 'string_operations':
                operation = params.get('operation')
                if operation == 'lower':
                    result[column] = result[column].str.lower()
                elif operation == 'upper':
                    result[column] = result[column].str.upper()
                elif operation == 'strip':
                    result[column] = result[column].str.strip()
                elif operation == 'replace':
                    old_value = params.get('old_value')
                    new_value = params.get('new_value')
                    result[column] = result[column].str.replace(old_value, new_value)
        
        return result
    
    def _aggregate(self, df: pd.DataFrame, group_by: List[str] = None, 
                  aggregations: Dict[str, Union[str, List[str]]] = None, **kwargs) -> pd.DataFrame:
        """Aggregate data with grouping."""
        if group_by:
            grouped = df.groupby(group_by)
            if aggregations:
                return grouped.agg(aggregations).reset_index()
            else:
                return grouped.size().reset_index(name='count')
        elif aggregations:
            # Aggregate without grouping
            result = {}
            for column, agg_func in aggregations.items():
                if isinstance(agg_func, str):
                    result[f"{column}_{agg_func}"] = getattr(df[column], agg_func)()
                elif isinstance(agg_func, list):
                    for func in agg_func:
                        result[f"{column}_{func}"] = getattr(df[column], func)()
            return pd.DataFrame([result])
        else:
            return df.describe()
    
    def _join(self, df: pd.DataFrame, right_data: pd.DataFrame, 
             join_type: str = 'inner', on: Union[str, List[str]] = None, 
             left_on: str = None, right_on: str = None, **kwargs) -> pd.DataFrame:
        """Join with another DataFrame."""
        if on:
            return df.merge(right_data, on=on, how=join_type)
        elif left_on and right_on:
            return df.merge(right_data, left_on=left_on, right_on=right_on, how=join_type)
        else:
            raise ValueError("Either 'on' or both 'left_on' and 'right_on' must be specified")
    
    def _sort(self, df: pd.DataFrame, columns: Union[str, List[str]], 
             ascending: Union[bool, List[bool]] = True, **kwargs) -> pd.DataFrame:
        """Sort DataFrame by columns."""
        return df.sort_values(by=columns, ascending=ascending)
    
    def _deduplicate(self, df: pd.DataFrame, subset: List[str] = None, 
                    keep: str = 'first', **kwargs) -> pd.DataFrame:
        """Remove duplicate rows."""
        return df.drop_duplicates(subset=subset, keep=keep)
    
    def _clean_data(self, df: pd.DataFrame, operations: List[str] = None, **kwargs) -> pd.DataFrame:
        """Clean data with common operations."""
        result = df.copy()
        
        if not operations:
            operations = ['remove_empty_rows', 'trim_strings']
        
        for operation in operations:
            if operation == 'remove_empty_rows':
                result = result.dropna(how='all')
            elif operation == 'remove_empty_columns':
                result = result.dropna(axis=1, how='all')
            elif operation == 'trim_strings':
                string_columns = result.select_dtypes(include=['object']).columns
                for col in string_columns:
                    result[col] = result[col].astype(str).str.strip()
            elif operation == 'standardize_nulls':
                # Replace common null representations with NaN
                null_values = ['', 'null', 'NULL', 'None', 'N/A', 'n/a', '#N/A']
                result = result.replace(null_values, np.nan)
        
        return result
    
    def _pivot(self, df: pd.DataFrame, index: Union[str, List[str]], 
              columns: str, values: str = None, **kwargs) -> pd.DataFrame:
        """Pivot DataFrame."""
        return df.pivot_table(index=index, columns=columns, values=values, **kwargs)
    
    def _melt(self, df: pd.DataFrame, id_vars: List[str] = None, 
             value_vars: List[str] = None, var_name: str = 'variable', 
             value_name: str = 'value', **kwargs) -> pd.DataFrame:
        """Melt DataFrame from wide to long format."""
        return df.melt(id_vars=id_vars, value_vars=value_vars, 
                      var_name=var_name, value_name=value_name)
    
    def _apply_custom_function(self, df: pd.DataFrame, function_name: str, 
                              function_params: Dict[str, Any] = None, **kwargs) -> pd.DataFrame:
        """Apply a custom function."""
        # This is a placeholder for custom function application
        # In practice, you would register custom functions somewhere accessible
        if function_params is None:
            function_params = {}
            
        # Example: apply a lambda function if provided
        if 'lambda' in function_params:
            func = eval(function_params['lambda'])
            return df.apply(func, axis=1)
        
        raise ValueError(f"Custom function '{function_name}' not implemented")
    
    def validate_data(self, data: pd.DataFrame, schema: Optional[Dict[str, Any]] = None) -> bool:
        """Validate data against schema or basic checks."""
        if schema is None:
            # Basic validation
            return not data.empty
        
        # Validate columns exist
        expected_columns = set(schema.get('columns', []))
        actual_columns = set(data.columns)
        
        if not expected_columns.issubset(actual_columns):
            missing_columns = expected_columns - actual_columns
            raise ValueError(f"Missing columns: {missing_columns}")
        
        # Validate data types if specified
        if 'dtypes' in schema:
            for column, expected_dtype in schema['dtypes'].items():
                if column in data.columns:
                    actual_dtype = str(data[column].dtype)
                    if actual_dtype != expected_dtype:
                        print(f"Warning: Column {column} has dtype {actual_dtype}, expected {expected_dtype}")
        
        return True