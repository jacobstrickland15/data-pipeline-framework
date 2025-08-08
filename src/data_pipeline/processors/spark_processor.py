"""Spark-based data processor implementation."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from ..core.base import DataProcessor

try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    SparkDataFrame = None
    F = None
    SPARK_AVAILABLE = False


class SparkProcessor(DataProcessor):
    """Data processor using Apache Spark for distributed processing."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is not installed. Install with: pip install pyspark")
        
        self.app_name = config.get('app_name', 'data-pipeline-spark')
        self.master = config.get('master', 'local[*]')
        self.spark_config = config.get('spark_config', {})
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        builder = SparkSession.builder.appName(self.app_name).master(self.master)
        
        # Apply additional Spark configurations
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def process(self, data: Union[pd.DataFrame, SparkDataFrame], operations: List[Dict[str, Any]]) -> SparkDataFrame:
        """Process data with a list of operations."""
        # Convert pandas DataFrame to Spark DataFrame if needed
        if isinstance(data, pd.DataFrame):
            spark_df = self.spark.createDataFrame(data)
        else:
            spark_df = data
            
        result = spark_df
        
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
            elif op_type == 'pivot':
                result = self._pivot(result, **op_params)
            elif op_type == 'window':
                result = self._window_functions(result, **op_params)
            elif op_type == 'custom_sql':
                result = self._execute_sql(result, **op_params)
            else:
                raise ValueError(f"Unknown operation type: {op_type}")
                
        return result
    
    def _filter(self, df: SparkDataFrame, condition: str = None, **kwargs) -> SparkDataFrame:
        """Filter rows based on conditions."""
        if condition:
            return df.filter(condition)
        else:
            raise ValueError("'condition' parameter is required for Spark filtering")
    
    def _select_columns(self, df: SparkDataFrame, columns: List[str] = None, 
                       exclude: List[str] = None, **kwargs) -> SparkDataFrame:
        """Select or exclude columns."""
        if columns:
            return df.select(*columns)
        elif exclude:
            remaining_cols = [col for col in df.columns if col not in exclude]
            return df.select(*remaining_cols)
        else:
            return df
    
    def _transform_columns(self, df: SparkDataFrame, transformations: Dict[str, Dict] = None, **kwargs) -> SparkDataFrame:
        """Transform columns with various operations."""
        result = df
        
        for column, transform_config in transformations.items():
            transform_type = transform_config.get('type')
            params = transform_config.get('params', {})
            
            if transform_type == 'rename':
                new_name = params.get('new_name')
                result = result.withColumnRenamed(column, new_name)
            elif transform_type == 'cast':
                dtype = params.get('dtype')
                result = result.withColumn(column, F.col(column).cast(dtype))
            elif transform_type == 'fill_null':
                value = params.get('value')
                method = params.get('method', 'value')
                if method == 'value':
                    result = result.fillna({column: value})
                elif method == 'mean':
                    mean_value = result.agg(F.mean(column)).collect()[0][0]
                    result = result.fillna({column: mean_value})
            elif transform_type == 'calculate':
                expression = params.get('expression')
                result = result.withColumn(column, F.expr(expression))
            elif transform_type == 'extract_date_parts':
                result = result.withColumn(column, F.to_date(F.col(column)))
                if params.get('year'):
                    result = result.withColumn(f"{column}_year", F.year(column))
                if params.get('month'):
                    result = result.withColumn(f"{column}_month", F.month(column))
                if params.get('day'):
                    result = result.withColumn(f"{column}_day", F.dayofmonth(column))
                if params.get('weekday'):
                    result = result.withColumn(f"{column}_weekday", F.dayofweek(column))
            elif transform_type == 'regex_extract':
                pattern = params.get('pattern')
                group_idx = params.get('group_idx', 1)
                result = result.withColumn(column, F.regexp_extract(column, pattern, group_idx))
            elif transform_type == 'string_operations':
                operation = params.get('operation')
                if operation == 'lower':
                    result = result.withColumn(column, F.lower(column))
                elif operation == 'upper':
                    result = result.withColumn(column, F.upper(column))
                elif operation == 'trim':
                    result = result.withColumn(column, F.trim(column))
                elif operation == 'replace':
                    old_value = params.get('old_value')
                    new_value = params.get('new_value')
                    result = result.withColumn(column, F.regexp_replace(column, old_value, new_value))
        
        return result
    
    def _aggregate(self, df: SparkDataFrame, group_by: List[str] = None, 
                  aggregations: Dict[str, Union[str, List[str]]] = None, **kwargs) -> SparkDataFrame:
        """Aggregate data with grouping."""
        if group_by:
            grouped = df.groupBy(*group_by)
            if aggregations:
                agg_exprs = []
                for column, agg_func in aggregations.items():
                    if isinstance(agg_func, str):
                        agg_exprs.append(getattr(F, agg_func)(column).alias(f"{column}_{agg_func}"))
                    elif isinstance(agg_func, list):
                        for func in agg_func:
                            agg_exprs.append(getattr(F, func)(column).alias(f"{column}_{func}"))
                return grouped.agg(*agg_exprs)
            else:
                return grouped.count()
        elif aggregations:
            # Aggregate without grouping
            agg_exprs = []
            for column, agg_func in aggregations.items():
                if isinstance(agg_func, str):
                    agg_exprs.append(getattr(F, agg_func)(column).alias(f"{column}_{agg_func}"))
                elif isinstance(agg_func, list):
                    for func in agg_func:
                        agg_exprs.append(getattr(F, func)(column).alias(f"{column}_{func}"))
            return df.agg(*agg_exprs)
        else:
            return df.describe()
    
    def _join(self, df: SparkDataFrame, right_data: SparkDataFrame, 
             join_type: str = 'inner', on: Union[str, List[str]] = None, 
             condition: str = None, **kwargs) -> SparkDataFrame:
        """Join with another DataFrame."""
        if on:
            return df.join(right_data, on=on, how=join_type)
        elif condition:
            return df.join(right_data, F.expr(condition), how=join_type)
        else:
            raise ValueError("Either 'on' or 'condition' must be specified")
    
    def _sort(self, df: SparkDataFrame, columns: Union[str, List[str]], 
             ascending: Union[bool, List[bool]] = True, **kwargs) -> SparkDataFrame:
        """Sort DataFrame by columns."""
        if isinstance(columns, str):
            columns = [columns]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(columns)
        
        order_cols = []
        for col, asc in zip(columns, ascending):
            if asc:
                order_cols.append(F.asc(col))
            else:
                order_cols.append(F.desc(col))
        
        return df.orderBy(*order_cols)
    
    def _deduplicate(self, df: SparkDataFrame, subset: List[str] = None, **kwargs) -> SparkDataFrame:
        """Remove duplicate rows."""
        if subset:
            return df.dropDuplicates(subset)
        else:
            return df.distinct()
    
    def _pivot(self, df: SparkDataFrame, group_cols: List[str], pivot_col: str, 
              value_col: str, agg_func: str = 'sum', **kwargs) -> SparkDataFrame:
        """Pivot DataFrame."""
        return df.groupBy(*group_cols).pivot(pivot_col).agg(getattr(F, agg_func)(value_col))
    
    def _window_functions(self, df: SparkDataFrame, window_spec: Dict[str, Any], 
                         functions: List[Dict[str, Any]], **kwargs) -> SparkDataFrame:
        """Apply window functions."""
        from pyspark.sql.window import Window
        
        # Build window specification
        partition_by = window_spec.get('partition_by', [])
        order_by = window_spec.get('order_by', [])
        
        window = Window.partitionBy(*partition_by).orderBy(*order_by)
        
        # Apply range/rows if specified
        if 'rows_between' in window_spec:
            start, end = window_spec['rows_between']
            window = window.rowsBetween(start, end)
        elif 'range_between' in window_spec:
            start, end = window_spec['range_between']
            window = window.rangeBetween(start, end)
        
        result = df
        for func_spec in functions:
            func_type = func_spec.get('type')
            column = func_spec.get('column')
            alias = func_spec.get('alias', f"{column}_{func_type}")
            
            if func_type == 'row_number':
                result = result.withColumn(alias, F.row_number().over(window))
            elif func_type == 'rank':
                result = result.withColumn(alias, F.rank().over(window))
            elif func_type == 'dense_rank':
                result = result.withColumn(alias, F.dense_rank().over(window))
            elif func_type == 'lag':
                offset = func_spec.get('offset', 1)
                result = result.withColumn(alias, F.lag(column, offset).over(window))
            elif func_type == 'lead':
                offset = func_spec.get('offset', 1)
                result = result.withColumn(alias, F.lead(column, offset).over(window))
            elif func_type in ['sum', 'avg', 'count', 'min', 'max']:
                result = result.withColumn(alias, getattr(F, func_type)(column).over(window))
        
        return result
    
    def _execute_sql(self, df: SparkDataFrame, query: str, temp_view_name: str = 'temp_table', **kwargs) -> SparkDataFrame:
        """Execute custom SQL query."""
        df.createOrReplaceTempView(temp_view_name)
        return self.spark.sql(query)
    
    def validate_data(self, data: SparkDataFrame, schema: Optional[Dict[str, Any]] = None) -> bool:
        """Validate Spark DataFrame against schema or basic checks."""
        if schema is None:
            # Basic validation
            return data.count() > 0
        
        # Validate columns exist
        expected_columns = set(schema.get('columns', []))
        actual_columns = set(data.columns)
        
        if not expected_columns.issubset(actual_columns):
            missing_columns = expected_columns - actual_columns
            raise ValueError(f"Missing columns: {missing_columns}")
        
        return True
    
    def to_pandas(self, spark_df: SparkDataFrame) -> pd.DataFrame:
        """Convert Spark DataFrame to Pandas DataFrame."""
        return spark_df.toPandas()
    
    def stop_spark(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()