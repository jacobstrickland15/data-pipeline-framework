"""Stream processing engine with windowing and aggregations."""

import logging
from typing import Dict, List, Optional, Any, Callable, Iterator
from datetime import datetime, timedelta
from collections import deque, defaultdict
import threading
import time
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class StreamWindow:
    """Represents a stream processing window."""
    window_id: str
    start_time: datetime
    end_time: datetime
    data: List[Dict[str, Any]]
    is_complete: bool = False


class WindowManager:
    """Manages different types of stream windows."""
    
    def __init__(self):
        self.tumbling_windows = {}
        self.sliding_windows = {}
        self.session_windows = {}
    
    def create_tumbling_window(
        self,
        window_size: timedelta,
        key_field: Optional[str] = None
    ) -> str:
        """Create a tumbling window.
        
        Args:
            window_size: Size of each window
            key_field: Field to partition windows by
            
        Returns:
            Window manager ID
        """
        window_id = f"tumbling_{int(time.time())}"
        self.tumbling_windows[window_id] = {
            'window_size': window_size,
            'key_field': key_field,
            'windows': defaultdict(list),
            'current_window_start': {}
        }
        return window_id
    
    def create_sliding_window(
        self,
        window_size: timedelta,
        slide_interval: timedelta,
        key_field: Optional[str] = None
    ) -> str:
        """Create a sliding window.
        
        Args:
            window_size: Size of each window
            slide_interval: How often to create new windows
            key_field: Field to partition windows by
            
        Returns:
            Window manager ID
        """
        window_id = f"sliding_{int(time.time())}"
        self.sliding_windows[window_id] = {
            'window_size': window_size,
            'slide_interval': slide_interval,
            'key_field': key_field,
            'windows': deque(maxlen=1000),  # Limit memory usage
            'last_slide_time': datetime.now()
        }
        return window_id
    
    def add_to_tumbling_window(self, window_id: str, record: Dict[str, Any], timestamp: datetime) -> Optional[StreamWindow]:
        """Add a record to a tumbling window.
        
        Returns:
            Completed window if one was closed, None otherwise
        """
        if window_id not in self.tumbling_windows:
            return None
        
        window_config = self.tumbling_windows[window_id]
        key = record.get(window_config['key_field'], 'default') if window_config['key_field'] else 'default'
        
        # Determine window start time
        window_size_seconds = window_config['window_size'].total_seconds()
        window_start = datetime.fromtimestamp(
            int(timestamp.timestamp() // window_size_seconds) * window_size_seconds
        )
        
        # Check if we need to close the current window
        current_start = window_config['current_window_start'].get(key)
        completed_window = None
        
        if current_start and current_start != window_start:
            # Close the current window
            completed_window = StreamWindow(
                window_id=f"{window_id}_{key}_{int(current_start.timestamp())}",
                start_time=current_start,
                end_time=current_start + window_config['window_size'],
                data=window_config['windows'][key].copy(),
                is_complete=True
            )
            window_config['windows'][key].clear()
        
        # Add record to current window
        window_config['current_window_start'][key] = window_start
        window_config['windows'][key].append(record)
        
        return completed_window
    
    def add_to_sliding_window(self, window_id: str, record: Dict[str, Any], timestamp: datetime):
        """Add a record to sliding windows."""
        if window_id not in self.sliding_windows:
            return
        
        window_config = self.sliding_windows[window_id]
        
        # Add to all active windows
        for window in window_config['windows']:
            if window['end_time'] > timestamp >= window['start_time']:
                window['data'].append(record)
        
        # Create new window if it's time to slide
        now = datetime.now()
        if now - window_config['last_slide_time'] >= window_config['slide_interval']:
            new_window = {
                'start_time': now,
                'end_time': now + window_config['window_size'],
                'data': [record] if timestamp >= now else []
            }
            window_config['windows'].append(new_window)
            window_config['last_slide_time'] = now


class StreamAggregator:
    """Performs aggregations on stream data."""
    
    @staticmethod
    def count(data: List[Dict[str, Any]]) -> int:
        """Count records in the window."""
        return len(data)
    
    @staticmethod
    def sum_field(data: List[Dict[str, Any]], field: str) -> float:
        """Sum a numeric field."""
        values = [record.get(field, 0) for record in data if isinstance(record.get(field), (int, float))]
        return sum(values)
    
    @staticmethod
    def avg_field(data: List[Dict[str, Any]], field: str) -> Optional[float]:
        """Average a numeric field."""
        values = [record.get(field) for record in data if isinstance(record.get(field), (int, float))]
        return sum(values) / len(values) if values else None
    
    @staticmethod
    def min_field(data: List[Dict[str, Any]], field: str) -> Optional[Any]:
        """Minimum value of a field."""
        values = [record.get(field) for record in data if record.get(field) is not None]
        return min(values) if values else None
    
    @staticmethod
    def max_field(data: List[Dict[str, Any]], field: str) -> Optional[Any]:
        """Maximum value of a field."""
        values = [record.get(field) for record in data if record.get(field) is not None]
        return max(values) if values else None
    
    @staticmethod
    def unique_count(data: List[Dict[str, Any]], field: str) -> int:
        """Count unique values in a field."""
        values = set(record.get(field) for record in data if record.get(field) is not None)
        return len(values)
    
    @staticmethod
    def percentile(data: List[Dict[str, Any]], field: str, percentile: float) -> Optional[float]:
        """Calculate percentile of a numeric field."""
        values = [record.get(field) for record in data if isinstance(record.get(field), (int, float))]
        if not values:
            return None
        return np.percentile(values, percentile)


class StreamProcessor:
    """Main stream processing engine."""
    
    def __init__(self):
        """Initialize the stream processor."""
        self.window_manager = WindowManager()
        self.aggregator = StreamAggregator()
        self.transformations = []
        self.filters = []
        self.output_handlers = []
        self.is_running = False
        self._stop_event = threading.Event()
    
    def add_transformation(self, transform_func: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """Add a transformation function to the processing pipeline.
        
        Args:
            transform_func: Function that takes a record dict and returns transformed dict
        """
        self.transformations.append(transform_func)
    
    def add_filter(self, filter_func: Callable[[Dict[str, Any]], bool]):
        """Add a filter function to the processing pipeline.
        
        Args:
            filter_func: Function that takes a record dict and returns True to keep it
        """
        self.filters.append(filter_func)
    
    def add_output_handler(self, output_func: Callable[[List[Dict[str, Any]]], None]):
        """Add an output handler for processed data.
        
        Args:
            output_func: Function that handles processed records
        """
        self.output_handlers.append(output_func)
    
    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record through transformations and filters.
        
        Args:
            record: Input record
            
        Returns:
            Processed record or None if filtered out
        """
        try:
            # Apply transformations
            for transform in self.transformations:
                record = transform(record)
                if record is None:
                    return None
            
            # Apply filters
            for filter_func in self.filters:
                if not filter_func(record):
                    return None
            
            return record
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return None
    
    def process_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of records.
        
        Args:
            records: List of input records
            
        Returns:
            List of processed records
        """
        processed_records = []
        
        for record in records:
            processed_record = self.process_record(record)
            if processed_record is not None:
                processed_records.append(processed_record)
        
        return processed_records
    
    def create_tumbling_window_aggregation(
        self,
        window_size: timedelta,
        aggregation_func: str,
        field: Optional[str] = None,
        key_field: Optional[str] = None
    ) -> Callable[[List[Dict[str, Any]]], None]:
        """Create a tumbling window aggregation.
        
        Args:
            window_size: Size of the tumbling window
            aggregation_func: Name of aggregation function ('count', 'sum', 'avg', etc.)
            field: Field to aggregate (required for most functions except 'count')
            key_field: Field to partition by
            
        Returns:
            Function that processes records and outputs window results
        """
        window_id = self.window_manager.create_tumbling_window(window_size, key_field)
        
        def window_aggregation_handler(records: List[Dict[str, Any]]):
            """Handle records for window aggregation."""
            for record in records:
                timestamp = record.get('timestamp', datetime.now())
                if isinstance(timestamp, str):
                    timestamp = pd.to_datetime(timestamp)
                
                completed_window = self.window_manager.add_to_tumbling_window(
                    window_id, record, timestamp
                )
                
                if completed_window:
                    # Perform aggregation on completed window
                    if aggregation_func == 'count':
                        result = self.aggregator.count(completed_window.data)
                    elif aggregation_func == 'sum' and field:
                        result = self.aggregator.sum_field(completed_window.data, field)
                    elif aggregation_func == 'avg' and field:
                        result = self.aggregator.avg_field(completed_window.data, field)
                    elif aggregation_func == 'min' and field:
                        result = self.aggregator.min_field(completed_window.data, field)
                    elif aggregation_func == 'max' and field:
                        result = self.aggregator.max_field(completed_window.data, field)
                    elif aggregation_func == 'unique_count' and field:
                        result = self.aggregator.unique_count(completed_window.data, field)
                    else:
                        logger.warning(f"Unknown aggregation function: {aggregation_func}")
                        continue
                    
                    # Create aggregation result
                    agg_result = {
                        'window_id': completed_window.window_id,
                        'window_start': completed_window.start_time,
                        'window_end': completed_window.end_time,
                        'aggregation': aggregation_func,
                        'field': field,
                        'result': result,
                        'record_count': len(completed_window.data)
                    }
                    
                    # Send to output handlers
                    for output_handler in self.output_handlers:
                        try:
                            output_handler([agg_result])
                        except Exception as e:
                            logger.error(f"Error in output handler: {e}")
        
        return window_aggregation_handler
    
    def process_stream(self, record_iterator: Iterator[List[Dict[str, Any]]]):
        """Process a stream of record batches.
        
        Args:
            record_iterator: Iterator that yields batches of records
        """
        self.is_running = True
        self._stop_event.clear()
        
        try:
            for batch in record_iterator:
                if self._stop_event.is_set():
                    break
                
                # Process the batch
                processed_batch = self.process_batch(batch)
                
                if processed_batch:
                    # Send processed batch to output handlers
                    for output_handler in self.output_handlers:
                        try:
                            output_handler(processed_batch)
                        except Exception as e:
                            logger.error(f"Error in output handler: {e}")
                
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
        finally:
            self.is_running = False
    
    def stop(self):
        """Stop the stream processor."""
        self.is_running = False
        self._stop_event.set()
        logger.info("Stream processor stopped")


class StreamTransformations:
    """Common stream transformations."""
    
    @staticmethod
    def add_timestamp(timestamp_field: str = 'processed_at') -> Callable:
        """Add current timestamp to records."""
        def transform(record: Dict[str, Any]) -> Dict[str, Any]:
            record[timestamp_field] = datetime.now()
            return record
        return transform
    
    @staticmethod
    def rename_field(old_name: str, new_name: str) -> Callable:
        """Rename a field in records."""
        def transform(record: Dict[str, Any]) -> Dict[str, Any]:
            if old_name in record:
                record[new_name] = record.pop(old_name)
            return record
        return transform
    
    @staticmethod
    def calculate_field(new_field: str, calculation_func: Callable) -> Callable:
        """Add a calculated field to records."""
        def transform(record: Dict[str, Any]) -> Dict[str, Any]:
            try:
                record[new_field] = calculation_func(record)
            except Exception as e:
                logger.warning(f"Failed to calculate field {new_field}: {e}")
            return record
        return transform
    
    @staticmethod
    def flatten_nested(field_name: str, prefix: str = "") -> Callable:
        """Flatten nested dictionary fields."""
        def transform(record: Dict[str, Any]) -> Dict[str, Any]:
            if field_name in record and isinstance(record[field_name], dict):
                nested_dict = record.pop(field_name)
                for key, value in nested_dict.items():
                    new_key = f"{prefix}{key}" if prefix else key
                    record[new_key] = value
            return record
        return transform
    
    @staticmethod
    def parse_json_field(field_name: str) -> Callable:
        """Parse a JSON string field into a dictionary."""
        import json
        
        def transform(record: Dict[str, Any]) -> Dict[str, Any]:
            if field_name in record and isinstance(record[field_name], str):
                try:
                    record[field_name] = json.loads(record[field_name])
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON in field {field_name}: {e}")
            return record
        return transform


class StreamFilters:
    """Common stream filters."""
    
    @staticmethod
    def field_equals(field_name: str, value: Any) -> Callable:
        """Filter records where field equals value."""
        def filter_func(record: Dict[str, Any]) -> bool:
            return record.get(field_name) == value
        return filter_func
    
    @staticmethod
    def field_in_list(field_name: str, values: List[Any]) -> Callable:
        """Filter records where field value is in list."""
        def filter_func(record: Dict[str, Any]) -> bool:
            return record.get(field_name) in values
        return filter_func
    
    @staticmethod
    def field_greater_than(field_name: str, threshold: float) -> Callable:
        """Filter records where numeric field is greater than threshold."""
        def filter_func(record: Dict[str, Any]) -> bool:
            value = record.get(field_name)
            return isinstance(value, (int, float)) and value > threshold
        return filter_func
    
    @staticmethod
    def field_not_null(field_name: str) -> Callable:
        """Filter out records where field is None or missing."""
        def filter_func(record: Dict[str, Any]) -> bool:
            return record.get(field_name) is not None
        return filter_func
    
    @staticmethod
    def timestamp_range(
        timestamp_field: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Callable:
        """Filter records by timestamp range."""
        def filter_func(record: Dict[str, Any]) -> bool:
            timestamp = record.get(timestamp_field)
            if not timestamp:
                return False
            
            if isinstance(timestamp, str):
                timestamp = pd.to_datetime(timestamp)
            
            if start_time and timestamp < start_time:
                return False
            if end_time and timestamp > end_time:
                return False
            
            return True
        return filter_func