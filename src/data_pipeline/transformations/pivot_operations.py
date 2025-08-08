"""Advanced pivot and unpivot operations."""

import logging
from typing import Dict, List, Optional, Any, Union, Callable
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class PivotOperations:
    """Advanced pivot and unpivot operations for data reshaping."""
    
    @staticmethod
    def pivot_table(
        df: pd.DataFrame,
        values: Union[str, List[str]],
        index: Union[str, List[str]],
        columns: Union[str, List[str]],
        aggfunc: Union[str, Callable, Dict] = 'mean',
        fill_value: Any = None,
        margins: bool = False,
        margins_name: str = 'All'
    ) -> pd.DataFrame:
        """Create a pivot table with multiple aggregation functions.
        
        Args:
            df: Input DataFrame
            values: Column(s) to aggregate
            index: Column(s) to use as index
            columns: Column(s) to use as columns
            aggfunc: Aggregation function(s)
            fill_value: Value to replace missing values with
            margins: Whether to add subtotals
            margins_name: Name for margin totals
            
        Returns:
            Pivoted DataFrame
        """
        try:
            result = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=fill_value,
                margins=margins,
                margins_name=margins_name
            )
            
            # Flatten column names if multi-level
            if isinstance(result.columns, pd.MultiIndex):
                if isinstance(values, str):
                    # Single value column, just use the column level
                    result.columns = [str(col) for col in result.columns]
                else:
                    # Multiple value columns, combine levels
                    result.columns = ['_'.join([str(c) for c in col if c != '']).strip('_') 
                                    for col in result.columns.values]
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error creating pivot table: {e}")
            raise
    
    @staticmethod
    def unpivot(
        df: pd.DataFrame,
        id_vars: Union[str, List[str]],
        value_vars: Optional[Union[str, List[str]]] = None,
        var_name: str = 'variable',
        value_name: str = 'value',
        ignore_index: bool = True
    ) -> pd.DataFrame:
        """Unpivot (melt) a DataFrame from wide to long format.
        
        Args:
            df: Input DataFrame
            id_vars: Column(s) to use as identifier variables
            value_vars: Column(s) to unpivot (default: all others)
            var_name: Name for the variable column
            value_name: Name for the value column
            ignore_index: Whether to ignore the index
            
        Returns:
            Unpivoted DataFrame
        """
        try:
            result = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=var_name,
                value_name=value_name
            )
            
            if ignore_index:
                result = result.reset_index(drop=True)
            
            return result
            
        except Exception as e:
            logger.error(f"Error unpivoting DataFrame: {e}")
            raise
    
    @staticmethod
    def cross_tab(
        df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: Union[str, List[str]],
        values: Optional[str] = None,
        aggfunc: Union[str, Callable] = 'count',
        normalize: Union[bool, str] = False,
        margins: bool = False
    ) -> pd.DataFrame:
        """Create a cross-tabulation of two or more factors.
        
        Args:
            df: Input DataFrame
            index: Column(s) for rows
            columns: Column(s) for columns
            values: Column to aggregate (optional)
            aggfunc: Aggregation function
            normalize: Whether to normalize (True, 'all', 'index', 'columns')
            margins: Whether to add row/column totals
            
        Returns:
            Cross-tabulation DataFrame
        """
        try:
            # Prepare index and columns data
            if isinstance(index, str):
                index_data = df[index]
            else:
                index_data = [df[col] for col in index]
            
            if isinstance(columns, str):
                columns_data = df[columns]
            else:
                columns_data = [df[col] for col in columns]
            
            # Create cross-tabulation
            if values:
                result = pd.crosstab(
                    index_data,
                    columns_data,
                    values=df[values],
                    aggfunc=aggfunc,
                    normalize=normalize,
                    margins=margins
                )
            else:
                result = pd.crosstab(
                    index_data,
                    columns_data,
                    normalize=normalize,
                    margins=margins
                )
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error creating cross-tabulation: {e}")
            raise
    
    @staticmethod
    def pivot_multiple_values(
        df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: str,
        values: List[str],
        aggfunc: Union[str, Callable, Dict] = 'first',
        fill_value: Any = None
    ) -> pd.DataFrame:
        """Pivot multiple value columns simultaneously.
        
        Args:
            df: Input DataFrame
            index: Column(s) to use as index
            columns: Column to use for pivot columns
            values: List of value columns to pivot
            aggfunc: Aggregation function
            fill_value: Value to replace missing values
            
        Returns:
            Pivoted DataFrame with multiple value columns
        """
        try:
            pivoted_dfs = []
            
            for value_col in values:
                # Create pivot for each value column
                pivot = df.pivot_table(
                    values=value_col,
                    index=index,
                    columns=columns,
                    aggfunc=aggfunc,
                    fill_value=fill_value
                )
                
                # Rename columns to include the value column name
                pivot.columns = [f"{value_col}_{col}" for col in pivot.columns]
                pivoted_dfs.append(pivot)
            
            # Concatenate all pivoted DataFrames
            result = pd.concat(pivoted_dfs, axis=1)
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error pivoting multiple values: {e}")
            raise
    
    @staticmethod
    def pivot_with_subtotals(
        df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: Union[str, List[str]],
        values: str,
        aggfunc: Union[str, Callable] = 'sum',
        subtotal_label: str = 'Subtotal'
    ) -> pd.DataFrame:
        """Create a pivot table with subtotals for each level.
        
        Args:
            df: Input DataFrame
            index: Column(s) to use as index
            columns: Column(s) to use as columns
            values: Column to aggregate
            aggfunc: Aggregation function
            subtotal_label: Label for subtotal rows
            
        Returns:
            Pivoted DataFrame with subtotals
        """
        try:
            # Create base pivot table
            pivot = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=0
            )
            
            # Add grand total
            grand_total = pivot.sum().to_frame().T
            grand_total.index = ['Grand Total']
            
            # Add subtotals if multiple index levels
            if isinstance(index, list) and len(index) > 1:
                # Group by first n-1 index levels and sum
                for i in range(len(index) - 1, 0, -1):
                    subtotal_index = index[:i]
                    subtotal_pivot = pd.pivot_table(
                        df,
                        values=values,
                        index=subtotal_index,
                        columns=columns,
                        aggfunc=aggfunc,
                        fill_value=0
                    )
                    
                    # Add subtotal label
                    subtotal_pivot.index = pd.MultiIndex.from_tuples(
                        [tuple(list(idx) + [subtotal_label] * (len(index) - len(idx))) 
                         for idx in subtotal_pivot.index]
                    )
                    
                    # Combine with main pivot
                    pivot = pd.concat([pivot, subtotal_pivot]).sort_index()
            
            # Add grand total
            pivot = pd.concat([pivot, grand_total])
            
            return pivot.reset_index()
            
        except Exception as e:
            logger.error(f"Error creating pivot with subtotals: {e}")
            raise
    
    @staticmethod
    def dynamic_pivot(
        df: pd.DataFrame,
        index_cols: List[str],
        pivot_col: str,
        value_cols: List[str],
        agg_funcs: Optional[Dict[str, Union[str, Callable]]] = None,
        prefix_sep: str = '_'
    ) -> pd.DataFrame:
        """Create a dynamic pivot where column names are generated from data.
        
        Args:
            df: Input DataFrame
            index_cols: Columns to keep as index
            pivot_col: Column containing values that become new column names
            value_cols: Columns containing values to pivot
            agg_funcs: Aggregation functions for each value column
            prefix_sep: Separator for new column names
            
        Returns:
            Dynamically pivoted DataFrame
        """
        try:
            if agg_funcs is None:
                agg_funcs = {col: 'first' for col in value_cols}
            
            result_dfs = []
            
            for value_col in value_cols:
                # Get the aggregation function for this value column
                aggfunc = agg_funcs.get(value_col, 'first')
                
                # Create pivot for this value column
                pivot = df.pivot_table(
                    values=value_col,
                    index=index_cols,
                    columns=pivot_col,
                    aggfunc=aggfunc,
                    fill_value=0
                )
                
                # Rename columns to include value column prefix
                pivot.columns = [f"{value_col}{prefix_sep}{col}" for col in pivot.columns]
                result_dfs.append(pivot)
            
            # Combine all pivoted DataFrames
            if len(result_dfs) == 1:
                result = result_dfs[0]
            else:
                result = result_dfs[0].join(result_dfs[1:])
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error in dynamic pivot: {e}")
            raise
    
    @staticmethod
    def conditional_pivot(
        df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: str,
        values: str,
        condition_col: str,
        condition_values: List[Any],
        aggfunc: Union[str, Callable] = 'sum'
    ) -> pd.DataFrame:
        """Create a pivot table with conditional filtering.
        
        Args:
            df: Input DataFrame
            index: Column(s) to use as index
            columns: Column to use for pivot columns
            values: Column to aggregate
            condition_col: Column to apply condition on
            condition_values: Values to filter by in condition column
            aggfunc: Aggregation function
            
        Returns:
            Conditional pivot DataFrame
        """
        try:
            # Filter DataFrame based on condition
            filtered_df = df[df[condition_col].isin(condition_values)]
            
            if filtered_df.empty:
                logger.warning("No data matches the specified condition")
                return pd.DataFrame()
            
            # Create pivot table
            result = pd.pivot_table(
                filtered_df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=0
            )
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error in conditional pivot: {e}")
            raise
    
    @staticmethod
    def time_based_pivot(
        df: pd.DataFrame,
        datetime_col: str,
        index: Union[str, List[str]],
        values: str,
        time_freq: str = 'M',
        aggfunc: Union[str, Callable] = 'sum'
    ) -> pd.DataFrame:
        """Create a time-based pivot table.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            index: Column(s) to use as index
            values: Column to aggregate
            time_freq: Time frequency for grouping ('D', 'W', 'M', 'Q', 'Y')
            aggfunc: Aggregation function
            
        Returns:
            Time-based pivot DataFrame
        """
        try:
            # Ensure datetime column is datetime type
            df[datetime_col] = pd.to_datetime(df[datetime_col])
            
            # Create time period column
            df['time_period'] = df[datetime_col].dt.to_period(time_freq)
            
            # Create pivot table
            result = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns='time_period',
                aggfunc=aggfunc,
                fill_value=0
            )
            
            # Convert period columns back to strings
            result.columns = [str(col) for col in result.columns]
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error in time-based pivot: {e}")
            raise
    
    @staticmethod
    def pivot_summary_stats(
        df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: str,
        values: str,
        stats: List[str] = ['count', 'mean', 'std', 'min', 'max']
    ) -> pd.DataFrame:
        """Create a pivot table with multiple summary statistics.
        
        Args:
            df: Input DataFrame
            index: Column(s) to use as index
            columns: Column to use for pivot columns
            values: Column to calculate statistics for
            stats: List of statistics to calculate
            
        Returns:
            Pivot table with summary statistics
        """
        try:
            # Create pivot table with multiple aggregation functions
            result = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=stats,
                fill_value=0
            )
            
            # Flatten multi-level columns
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join([str(c) for c in col]).strip('_') 
                                for col in result.columns.values]
            
            return result.reset_index()
            
        except Exception as e:
            logger.error(f"Error creating pivot summary stats: {e}")
            raise