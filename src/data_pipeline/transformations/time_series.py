"""Time series analysis and transformation utilities."""

import logging
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy import signal
from scipy.stats import zscore

logger = logging.getLogger(__name__)


class TimeSeriesTransformations:
    """Advanced time series transformations and analysis."""
    
    @staticmethod
    def resample_time_series(
        df: pd.DataFrame,
        datetime_col: str,
        freq: str,
        agg_funcs: Dict[str, Union[str, Callable]] = None,
        fill_method: Optional[str] = None
    ) -> pd.DataFrame:
        """Resample time series data to different frequency.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            freq: Target frequency ('D', 'H', '15min', 'M', etc.)
            agg_funcs: Aggregation functions for each column
            fill_method: Method to fill missing values ('ffill', 'bfill', 'interpolate')
            
        Returns:
            Resampled DataFrame
        """
        try:
            # Set datetime column as index
            df_ts = df.copy()
            df_ts[datetime_col] = pd.to_datetime(df_ts[datetime_col])
            df_ts = df_ts.set_index(datetime_col)
            
            # Default aggregation
            if agg_funcs is None:
                numeric_cols = df_ts.select_dtypes(include=[np.number]).columns
                agg_funcs = {col: 'mean' for col in numeric_cols}
                
                # Use first() for non-numeric columns
                non_numeric_cols = df_ts.select_dtypes(exclude=[np.number]).columns
                agg_funcs.update({col: 'first' for col in non_numeric_cols})
            
            # Resample
            resampled = df_ts.resample(freq).agg(agg_funcs)
            
            # Fill missing values if specified
            if fill_method == 'ffill':
                resampled = resampled.ffill()
            elif fill_method == 'bfill':
                resampled = resampled.bfill()
            elif fill_method == 'interpolate':
                resampled = resampled.interpolate()
            
            return resampled.reset_index()
            
        except Exception as e:
            logger.error(f"Error resampling time series: {e}")
            raise
    
    @staticmethod
    def rolling_window_stats(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        window: Union[int, str],
        stats: List[str] = ['mean', 'std', 'min', 'max'],
        min_periods: int = 1
    ) -> pd.DataFrame:
        """Calculate rolling window statistics.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to calculate statistics for
            window: Window size (int for number of periods, str for time-based)
            stats: Statistics to calculate
            min_periods: Minimum number of periods required
            
        Returns:
            DataFrame with rolling statistics
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                # Calculate rolling statistics
                rolling = result_df[col].rolling(window=window, min_periods=min_periods)
                
                for stat in stats:
                    if hasattr(rolling, stat):
                        result_df[f'{col}_rolling_{stat}_{window}'] = getattr(rolling, stat)()
                    elif stat == 'median':
                        result_df[f'{col}_rolling_median_{window}'] = rolling.median()
                    elif stat == 'quantile':
                        result_df[f'{col}_rolling_q25_{window}'] = rolling.quantile(0.25)
                        result_df[f'{col}_rolling_q75_{window}'] = rolling.quantile(0.75)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating rolling statistics: {e}")
            raise
    
    @staticmethod
    def seasonal_decomposition(
        df: pd.DataFrame,
        datetime_col: str,
        value_col: str,
        model: str = 'additive',
        period: Optional[int] = None,
        extrapolate_trend: Union[int, str] = 0
    ) -> pd.DataFrame:
        """Perform seasonal decomposition of time series.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_col: Column to decompose
            model: Type of seasonal component ('additive' or 'multiplicative')
            period: Seasonal period (auto-detected if None)
            extrapolate_trend: How to handle trend boundaries
            
        Returns:
            DataFrame with original series plus trend, seasonal, and residual components
        """
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            
            # Prepare time series
            ts_df = df.copy()
            ts_df[datetime_col] = pd.to_datetime(ts_df[datetime_col])
            ts_df = ts_df.sort_values(datetime_col).set_index(datetime_col)
            
            # Perform decomposition
            decomposition = seasonal_decompose(
                ts_df[value_col],
                model=model,
                period=period,
                extrapolate_trend=extrapolate_trend
            )
            
            # Create result DataFrame
            result = ts_df.copy()
            result[f'{value_col}_trend'] = decomposition.trend
            result[f'{value_col}_seasonal'] = decomposition.seasonal
            result[f'{value_col}_residual'] = decomposition.resid
            
            return result.reset_index()
            
        except ImportError:
            logger.error("statsmodels is required for seasonal decomposition")
            return df
        except Exception as e:
            logger.error(f"Error in seasonal decomposition: {e}")
            raise
    
    @staticmethod
    def detect_outliers_time_series(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        method: str = 'iqr',
        window: Optional[int] = None,
        threshold: float = 1.5
    ) -> pd.DataFrame:
        """Detect outliers in time series data.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to check for outliers
            method: Detection method ('iqr', 'zscore', 'rolling_zscore')
            window: Window size for rolling methods
            threshold: Threshold for outlier detection
            
        Returns:
            DataFrame with outlier flags
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                outlier_col = f'{col}_outlier'
                
                if method == 'iqr':
                    Q1 = result_df[col].quantile(0.25)
                    Q3 = result_df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - threshold * IQR
                    upper_bound = Q3 + threshold * IQR
                    result_df[outlier_col] = (result_df[col] < lower_bound) | (result_df[col] > upper_bound)
                
                elif method == 'zscore':
                    z_scores = np.abs(zscore(result_df[col].dropna()))
                    result_df[outlier_col] = False
                    result_df.loc[result_df[col].notna(), outlier_col] = z_scores > threshold
                
                elif method == 'rolling_zscore' and window:
                    rolling_mean = result_df[col].rolling(window=window).mean()
                    rolling_std = result_df[col].rolling(window=window).std()
                    z_scores = np.abs((result_df[col] - rolling_mean) / rolling_std)
                    result_df[outlier_col] = z_scores > threshold
                
                else:
                    logger.warning(f"Unknown outlier detection method: {method}")
                    result_df[outlier_col] = False
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error detecting outliers: {e}")
            raise
    
    @staticmethod
    def fill_missing_values(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        method: str = 'interpolate',
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fill missing values in time series data.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to fill missing values for
            method: Fill method ('interpolate', 'ffill', 'bfill', 'mean', 'median')
            limit: Maximum number of consecutive missing values to fill
            
        Returns:
            DataFrame with filled values
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                if method == 'interpolate':
                    result_df[col] = result_df[col].interpolate(limit=limit)
                elif method == 'ffill':
                    result_df[col] = result_df[col].fillna(method='ffill', limit=limit)
                elif method == 'bfill':
                    result_df[col] = result_df[col].fillna(method='bfill', limit=limit)
                elif method == 'mean':
                    mean_value = result_df[col].mean()
                    result_df[col] = result_df[col].fillna(mean_value)
                elif method == 'median':
                    median_value = result_df[col].median()
                    result_df[col] = result_df[col].fillna(median_value)
                else:
                    logger.warning(f"Unknown fill method: {method}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error filling missing values: {e}")
            raise
    
    @staticmethod
    def calculate_time_features(
        df: pd.DataFrame,
        datetime_col: str,
        features: List[str] = ['hour', 'day_of_week', 'month', 'quarter', 'is_weekend']
    ) -> pd.DataFrame:
        """Extract time-based features from datetime column.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            features: Features to extract
            
        Returns:
            DataFrame with additional time features
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            
            dt = result_df[datetime_col].dt
            
            for feature in features:
                if feature == 'hour':
                    result_df['hour'] = dt.hour
                elif feature == 'day':
                    result_df['day'] = dt.day
                elif feature == 'day_of_week':
                    result_df['day_of_week'] = dt.dayofweek
                elif feature == 'day_name':
                    result_df['day_name'] = dt.day_name()
                elif feature == 'month':
                    result_df['month'] = dt.month
                elif feature == 'month_name':
                    result_df['month_name'] = dt.month_name()
                elif feature == 'quarter':
                    result_df['quarter'] = dt.quarter
                elif feature == 'year':
                    result_df['year'] = dt.year
                elif feature == 'is_weekend':
                    result_df['is_weekend'] = dt.dayofweek.isin([5, 6])
                elif feature == 'is_business_day':
                    result_df['is_business_day'] = dt.dayofweek < 5
                elif feature == 'week_of_year':
                    result_df['week_of_year'] = dt.isocalendar().week
                elif feature == 'day_of_year':
                    result_df['day_of_year'] = dt.dayofyear
                else:
                    logger.warning(f"Unknown time feature: {feature}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating time features: {e}")
            raise
    
    @staticmethod
    def calculate_lags(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        lags: List[int],
        groupby_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate lagged values for time series features.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to calculate lags for
            lags: List of lag periods
            groupby_cols: Columns to group by (for multiple time series)
            
        Returns:
            DataFrame with lagged features
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                for lag in lags:
                    if groupby_cols:
                        result_df[f'{col}_lag_{lag}'] = result_df.groupby(groupby_cols)[col].shift(lag)
                    else:
                        result_df[f'{col}_lag_{lag}'] = result_df[col].shift(lag)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating lags: {e}")
            raise
    
    @staticmethod
    def calculate_differences(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        periods: List[int] = [1],
        groupby_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate differences (first, second, etc.) for time series.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to calculate differences for
            periods: List of difference periods
            groupby_cols: Columns to group by
            
        Returns:
            DataFrame with difference features
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                for period in periods:
                    if groupby_cols:
                        result_df[f'{col}_diff_{period}'] = result_df.groupby(groupby_cols)[col].diff(periods=period)
                    else:
                        result_df[f'{col}_diff_{period}'] = result_df[col].diff(periods=period)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error calculating differences: {e}")
            raise
    
    @staticmethod
    def smooth_time_series(
        df: pd.DataFrame,
        datetime_col: str,
        value_cols: List[str],
        method: str = 'moving_average',
        window: int = 5,
        **kwargs
    ) -> pd.DataFrame:
        """Smooth time series data using various methods.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_cols: Columns to smooth
            method: Smoothing method ('moving_average', 'exponential', 'savgol')
            window: Window size for smoothing
            **kwargs: Additional parameters for smoothing methods
            
        Returns:
            DataFrame with smoothed values
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            for col in value_cols:
                if col not in result_df.columns:
                    continue
                
                smoothed_col = f'{col}_smoothed'
                
                if method == 'moving_average':
                    result_df[smoothed_col] = result_df[col].rolling(window=window).mean()
                
                elif method == 'exponential':
                    alpha = kwargs.get('alpha', 0.3)
                    result_df[smoothed_col] = result_df[col].ewm(alpha=alpha).mean()
                
                elif method == 'savgol':
                    polyorder = kwargs.get('polyorder', 2)
                    if len(result_df) > window:
                        result_df[smoothed_col] = signal.savgol_filter(
                            result_df[col].fillna(method='ffill'),
                            window,
                            polyorder
                        )
                    else:
                        result_df[smoothed_col] = result_df[col]
                
                else:
                    logger.warning(f"Unknown smoothing method: {method}")
                    result_df[smoothed_col] = result_df[col]
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error smoothing time series: {e}")
            raise
    
    @staticmethod
    def detect_changepoints(
        df: pd.DataFrame,
        datetime_col: str,
        value_col: str,
        method: str = 'pelt',
        penalty: float = 10.0
    ) -> pd.DataFrame:
        """Detect change points in time series data.
        
        Args:
            df: Input DataFrame
            datetime_col: Column containing datetime values
            value_col: Column to analyze for change points
            method: Detection method ('pelt', 'binary_segmentation')
            penalty: Penalty parameter for change point detection
            
        Returns:
            DataFrame with change point indicators
        """
        try:
            result_df = df.copy()
            result_df[datetime_col] = pd.to_datetime(result_df[datetime_col])
            result_df = result_df.sort_values(datetime_col)
            
            # Simple change point detection using rolling statistics
            window = min(20, len(result_df) // 4)
            if window < 3:
                window = 3
            
            # Calculate rolling mean and std
            rolling_mean = result_df[value_col].rolling(window=window).mean()
            rolling_std = result_df[value_col].rolling(window=window).std()
            
            # Detect significant changes
            mean_diff = rolling_mean.diff().abs()
            std_threshold = rolling_std.median() * 2  # Dynamic threshold
            
            result_df['change_point'] = mean_diff > std_threshold
            result_df['change_magnitude'] = mean_diff
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error detecting change points: {e}")
            raise