"""Advanced window functions for data analysis."""

import logging
from typing import Dict, List, Optional, Any, Union, Callable
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class WindowFunctions:
    """Implementation of SQL-style window functions for pandas DataFrames."""
    
    @staticmethod
    def row_number(df: pd.DataFrame, partition_by: Optional[List[str]] = None, 
                   order_by: Optional[List[str]] = None) -> pd.Series:
        """Add row numbers within partitions.
        
        Args:
            df: Input DataFrame
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with row numbers
        """
        if order_by:
            df_sorted = df.sort_values(order_by)
        else:
            df_sorted = df
        
        if partition_by:
            return df_sorted.groupby(partition_by).cumcount() + 1
        else:
            return pd.Series(range(1, len(df_sorted) + 1), index=df_sorted.index)
    
    @staticmethod
    def rank(df: pd.DataFrame, column: str, partition_by: Optional[List[str]] = None,
             method: str = 'min') -> pd.Series:
        """Calculate rank within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to rank by
            partition_by: Columns to partition by
            method: Ranking method ('min', 'max', 'first', 'dense')
            
        Returns:
            Series with ranks
        """
        if partition_by:
            return df.groupby(partition_by)[column].rank(method=method)
        else:
            return df[column].rank(method=method)
    
    @staticmethod
    def dense_rank(df: pd.DataFrame, column: str, 
                   partition_by: Optional[List[str]] = None) -> pd.Series:
        """Calculate dense rank within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to rank by
            partition_by: Columns to partition by
            
        Returns:
            Series with dense ranks
        """
        return WindowFunctions.rank(df, column, partition_by, method='dense')
    
    @staticmethod
    def percent_rank(df: pd.DataFrame, column: str, 
                     partition_by: Optional[List[str]] = None) -> pd.Series:
        """Calculate percent rank within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to rank by
            partition_by: Columns to partition by
            
        Returns:
            Series with percent ranks (0-1)
        """
        if partition_by:
            def calc_percent_rank(group):
                ranks = group.rank()
                return (ranks - 1) / (len(group) - 1) if len(group) > 1 else 0
            
            return df.groupby(partition_by)[column].transform(calc_percent_rank)
        else:
            ranks = df[column].rank()
            return (ranks - 1) / (len(df) - 1) if len(df) > 1 else pd.Series([0] * len(df))
    
    @staticmethod
    def ntile(df: pd.DataFrame, column: str, n: int,
              partition_by: Optional[List[str]] = None) -> pd.Series:
        """Divide rows into n buckets within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to use for bucketing
            n: Number of buckets
            partition_by: Columns to partition by
            
        Returns:
            Series with bucket numbers (1 to n)
        """
        if partition_by:
            def calc_ntile(group):
                return pd.qcut(group.rank(method='first'), n, labels=False) + 1
            
            return df.groupby(partition_by)[column].transform(calc_ntile)
        else:
            return pd.qcut(df[column].rank(method='first'), n, labels=False) + 1
    
    @staticmethod
    def lag(df: pd.DataFrame, column: str, periods: int = 1,
            partition_by: Optional[List[str]] = None,
            order_by: Optional[List[str]] = None,
            default_value: Any = None) -> pd.Series:
        """Get value from previous row within partition.
        
        Args:
            df: Input DataFrame
            column: Column to lag
            periods: Number of periods to lag
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            default_value: Value to use for missing values
            
        Returns:
            Series with lagged values
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            result = df_work.groupby(partition_by)[column].shift(periods)
        else:
            result = df_work[column].shift(periods)
        
        if default_value is not None:
            result = result.fillna(default_value)
        
        return result
    
    @staticmethod
    def lead(df: pd.DataFrame, column: str, periods: int = 1,
             partition_by: Optional[List[str]] = None,
             order_by: Optional[List[str]] = None,
             default_value: Any = None) -> pd.Series:
        """Get value from next row within partition.
        
        Args:
            df: Input DataFrame
            column: Column to lead
            periods: Number of periods to lead
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            default_value: Value to use for missing values
            
        Returns:
            Series with lead values
        """
        return WindowFunctions.lag(df, column, -periods, partition_by, order_by, default_value)
    
    @staticmethod
    def first_value(df: pd.DataFrame, column: str,
                    partition_by: Optional[List[str]] = None,
                    order_by: Optional[List[str]] = None) -> pd.Series:
        """Get first value within partition.
        
        Args:
            df: Input DataFrame
            column: Column to get first value from
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with first values
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].transform('first')
        else:
            return pd.Series([df_work[column].iloc[0]] * len(df_work), index=df_work.index)
    
    @staticmethod
    def last_value(df: pd.DataFrame, column: str,
                   partition_by: Optional[List[str]] = None,
                   order_by: Optional[List[str]] = None) -> pd.Series:
        """Get last value within partition.
        
        Args:
            df: Input DataFrame
            column: Column to get last value from
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with last values
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].transform('last')
        else:
            return pd.Series([df_work[column].iloc[-1]] * len(df_work), index=df_work.index)
    
    @staticmethod
    def nth_value(df: pd.DataFrame, column: str, n: int,
                  partition_by: Optional[List[str]] = None,
                  order_by: Optional[List[str]] = None) -> pd.Series:
        """Get nth value within partition.
        
        Args:
            df: Input DataFrame
            column: Column to get nth value from
            n: Position (1-based)
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with nth values
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        def get_nth(group):
            if len(group) >= n:
                return group.iloc[n-1]
            else:
                return None
        
        if partition_by:
            return df_work.groupby(partition_by)[column].transform(get_nth)
        else:
            nth_val = df_work[column].iloc[n-1] if len(df_work) >= n else None
            return pd.Series([nth_val] * len(df_work), index=df_work.index)
    
    @staticmethod
    def running_sum(df: pd.DataFrame, column: str,
                    partition_by: Optional[List[str]] = None,
                    order_by: Optional[List[str]] = None) -> pd.Series:
        """Calculate running sum within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to sum
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with running sums
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].cumsum()
        else:
            return df_work[column].cumsum()
    
    @staticmethod
    def running_average(df: pd.DataFrame, column: str,
                        partition_by: Optional[List[str]] = None,
                        order_by: Optional[List[str]] = None) -> pd.Series:
        """Calculate running average within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to average
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            
        Returns:
            Series with running averages
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].expanding().mean().reset_index(level=0, drop=True)
        else:
            return df_work[column].expanding().mean()
    
    @staticmethod
    def moving_average(df: pd.DataFrame, column: str, window: int,
                       partition_by: Optional[List[str]] = None,
                       order_by: Optional[List[str]] = None,
                       min_periods: int = 1) -> pd.Series:
        """Calculate moving average within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to average
            window: Size of moving window
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            min_periods: Minimum number of periods required
            
        Returns:
            Series with moving averages
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].rolling(
                window=window, min_periods=min_periods
            ).mean().reset_index(level=0, drop=True)
        else:
            return df_work[column].rolling(window=window, min_periods=min_periods).mean()
    
    @staticmethod
    def moving_sum(df: pd.DataFrame, column: str, window: int,
                   partition_by: Optional[List[str]] = None,
                   order_by: Optional[List[str]] = None,
                   min_periods: int = 1) -> pd.Series:
        """Calculate moving sum within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to sum
            window: Size of moving window
            partition_by: Columns to partition by
            order_by: Columns to order by within partitions
            min_periods: Minimum number of periods required
            
        Returns:
            Series with moving sums
        """
        df_work = df.copy()
        
        if order_by:
            df_work = df_work.sort_values(order_by)
        
        if partition_by:
            return df_work.groupby(partition_by)[column].rolling(
                window=window, min_periods=min_periods
            ).sum().reset_index(level=0, drop=True)
        else:
            return df_work[column].rolling(window=window, min_periods=min_periods).sum()
    
    @staticmethod
    def cume_dist(df: pd.DataFrame, column: str,
                  partition_by: Optional[List[str]] = None) -> pd.Series:
        """Calculate cumulative distribution within partitions.
        
        Args:
            df: Input DataFrame
            column: Column to calculate distribution for
            partition_by: Columns to partition by
            
        Returns:
            Series with cumulative distribution values (0-1)
        """
        if partition_by:
            def calc_cume_dist(group):
                ranks = group.rank(method='max')
                return ranks / len(group)
            
            return df.groupby(partition_by)[column].transform(calc_cume_dist)
        else:
            ranks = df[column].rank(method='max')
            return ranks / len(df)
    
    @staticmethod
    def apply_window_function(
        df: pd.DataFrame,
        function_name: str,
        column: str,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        **kwargs
    ) -> pd.Series:
        """Apply a window function by name.
        
        Args:
            df: Input DataFrame
            function_name: Name of the window function
            column: Column to operate on
            partition_by: Columns to partition by
            order_by: Columns to order by
            **kwargs: Additional arguments for the function
            
        Returns:
            Series with function results
        """
        function_map = {
            'row_number': WindowFunctions.row_number,
            'rank': WindowFunctions.rank,
            'dense_rank': WindowFunctions.dense_rank,
            'percent_rank': WindowFunctions.percent_rank,
            'lag': WindowFunctions.lag,
            'lead': WindowFunctions.lead,
            'first_value': WindowFunctions.first_value,
            'last_value': WindowFunctions.last_value,
            'running_sum': WindowFunctions.running_sum,
            'running_average': WindowFunctions.running_average,
            'moving_average': WindowFunctions.moving_average,
            'moving_sum': WindowFunctions.moving_sum,
            'cume_dist': WindowFunctions.cume_dist
        }
        
        if function_name not in function_map:
            raise ValueError(f"Unknown window function: {function_name}")
        
        func = function_map[function_name]
        
        # Prepare arguments based on function signature
        if function_name in ['row_number']:
            return func(df, partition_by=partition_by, order_by=order_by, **kwargs)
        elif function_name in ['cume_dist']:
            return func(df, column, partition_by=partition_by, **kwargs)
        else:
            return func(df, column, partition_by=partition_by, order_by=order_by, **kwargs)