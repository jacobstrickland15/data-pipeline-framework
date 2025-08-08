"""Redis sink for streaming data output."""

import json
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import threading
import time

try:
    import redis
    from redis.exceptions import RedisError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

import pandas as pd

logger = logging.getLogger(__name__)


class RedisSink:
    """Redis sink for streaming data output."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        max_connections: int = 10,
        **kwargs
    ):
        """Initialize Redis sink.
        
        Args:
            host: Redis host
            port: Redis port
            password: Redis password
            db: Redis database number
            max_connections: Maximum number of connections in pool
            **kwargs: Additional Redis connection parameters
        """
        if not REDIS_AVAILABLE:
            raise ImportError("redis is required for RedisSink. Install with: pip install redis")
        
        self.connection_pool = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            max_connections=max_connections,
            decode_responses=True,
            **kwargs
        )
        
        self.redis_client = redis.Redis(connection_pool=self.connection_pool)
        self._test_connection()
    
    def _test_connection(self):
        """Test Redis connection."""
        try:
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def write_records(
        self,
        records: List[Dict[str, Any]],
        key_prefix: str = "pipeline",
        key_field: Optional[str] = None,
        expiry_seconds: Optional[int] = None
    ) -> int:
        """Write records to Redis as individual keys.
        
        Args:
            records: List of records to write
            key_prefix: Prefix for Redis keys
            key_field: Field to use as key suffix (uses timestamp if not provided)
            expiry_seconds: TTL for keys in seconds
            
        Returns:
            Number of successfully written records
        """
        written_count = 0
        
        try:
            pipe = self.redis_client.pipeline()
            
            for i, record in enumerate(records):
                try:
                    # Generate key
                    if key_field and key_field in record:
                        key_suffix = str(record[key_field])
                    else:
                        key_suffix = f"{int(time.time())}_{i}"
                    
                    key = f"{key_prefix}:{key_suffix}"
                    
                    # Serialize record
                    serialized_record = json.dumps(record, default=str)
                    
                    # Add to pipeline
                    pipe.set(key, serialized_record)
                    
                    if expiry_seconds:
                        pipe.expire(key, expiry_seconds)
                    
                    written_count += 1
                    
                except Exception as e:
                    logger.error(f"Error preparing record for Redis: {e}")
                    continue
            
            # Execute pipeline
            pipe.execute()
            logger.debug(f"Successfully wrote {written_count} records to Redis")
            
        except RedisError as e:
            logger.error(f"Redis error writing records: {e}")
            return 0
        
        return written_count
    
    def write_to_stream(
        self,
        stream_name: str,
        records: List[Dict[str, Any]],
        maxlen: Optional[int] = None
    ) -> int:
        """Write records to a Redis Stream.
        
        Args:
            stream_name: Name of the Redis stream
            records: List of records to write
            maxlen: Maximum length of stream (approximate)
            
        Returns:
            Number of successfully written records
        """
        written_count = 0
        
        try:
            pipe = self.redis_client.pipeline()
            
            for record in records:
                try:
                    # Flatten nested objects for Redis streams
                    flattened_record = self._flatten_record(record)
                    
                    # Add to stream
                    if maxlen:
                        pipe.xadd(stream_name, flattened_record, maxlen=maxlen, approximate=True)
                    else:
                        pipe.xadd(stream_name, flattened_record)
                    
                    written_count += 1
                    
                except Exception as e:
                    logger.error(f"Error preparing record for Redis stream: {e}")
                    continue
            
            # Execute pipeline
            pipe.execute()
            logger.debug(f"Successfully wrote {written_count} records to Redis stream '{stream_name}'")
            
        except RedisError as e:
            logger.error(f"Redis error writing to stream: {e}")
            return 0
        
        return written_count
    
    def write_to_list(
        self,
        list_name: str,
        records: List[Dict[str, Any]],
        maxlen: Optional[int] = None,
        side: str = "left"
    ) -> int:
        """Write records to a Redis list.
        
        Args:
            list_name: Name of the Redis list
            records: List of records to write
            maxlen: Maximum length of list
            side: Which side to push to ('left' or 'right')
            
        Returns:
            Number of successfully written records
        """
        written_count = 0
        
        try:
            pipe = self.redis_client.pipeline()
            
            for record in records:
                try:
                    serialized_record = json.dumps(record, default=str)
                    
                    if side == "left":
                        pipe.lpush(list_name, serialized_record)
                    else:
                        pipe.rpush(list_name, serialized_record)
                    
                    written_count += 1
                    
                except Exception as e:
                    logger.error(f"Error preparing record for Redis list: {e}")
                    continue
            
            # Trim list if maxlen specified
            if maxlen:
                pipe.ltrim(list_name, 0, maxlen - 1)
            
            # Execute pipeline
            pipe.execute()
            logger.debug(f"Successfully wrote {written_count} records to Redis list '{list_name}'")
            
        except RedisError as e:
            logger.error(f"Redis error writing to list: {e}")
            return 0
        
        return written_count
    
    def write_aggregation_result(
        self,
        key: str,
        result: Dict[str, Any],
        expiry_seconds: Optional[int] = None
    ) -> bool:
        """Write an aggregation result to Redis.
        
        Args:
            key: Redis key for the result
            result: Aggregation result dictionary
            expiry_seconds: TTL for the key
            
        Returns:
            Success status
        """
        try:
            serialized_result = json.dumps(result, default=str)
            
            self.redis_client.set(key, serialized_result)
            
            if expiry_seconds:
                self.redis_client.expire(key, expiry_seconds)
            
            logger.debug(f"Successfully wrote aggregation result to Redis key '{key}'")
            return True
            
        except Exception as e:
            logger.error(f"Error writing aggregation result: {e}")
            return False
    
    def write_time_series(
        self,
        records: List[Dict[str, Any]],
        timestamp_field: str = "timestamp",
        value_field: str = "value",
        key_prefix: str = "ts"
    ) -> int:
        """Write records as time series data to Redis.
        
        Args:
            records: List of records with timestamp and value
            timestamp_field: Field containing timestamp
            value_field: Field containing the value
            key_prefix: Prefix for time series keys
            
        Returns:
            Number of successfully written records
        """
        written_count = 0
        
        # Group records by any additional dimensions
        grouped_records = {}
        
        for record in records:
            # Create a key based on all fields except timestamp and value
            key_parts = [key_prefix]
            for field, value in record.items():
                if field not in [timestamp_field, value_field]:
                    key_parts.append(f"{field}_{value}")
            
            ts_key = ":".join(key_parts)
            
            if ts_key not in grouped_records:
                grouped_records[ts_key] = []
            
            grouped_records[ts_key].append(record)
        
        # Write each time series
        try:
            pipe = self.redis_client.pipeline()
            
            for ts_key, ts_records in grouped_records.items():
                for record in ts_records:
                    try:
                        timestamp = record.get(timestamp_field)
                        value = record.get(value_field)
                        
                        if timestamp is None or value is None:
                            continue
                        
                        # Convert timestamp to Unix timestamp if needed
                        if isinstance(timestamp, (datetime, pd.Timestamp)):
                            timestamp = int(timestamp.timestamp() * 1000)  # milliseconds
                        elif isinstance(timestamp, str):
                            timestamp = int(pd.to_datetime(timestamp).timestamp() * 1000)
                        
                        # Use sorted set with timestamp as score
                        pipe.zadd(ts_key, {str(value): timestamp})
                        written_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error preparing time series record: {e}")
                        continue
            
            pipe.execute()
            logger.debug(f"Successfully wrote {written_count} time series records to Redis")
            
        except RedisError as e:
            logger.error(f"Redis error writing time series: {e}")
            return 0
        
        return written_count
    
    def _flatten_record(self, record: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
        """Flatten nested dictionary for Redis streams."""
        flattened = {}
        
        for key, value in record.items():
            new_key = f"{prefix}{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(self._flatten_record(value, f"{new_key}."))
            elif isinstance(value, list):
                flattened[new_key] = json.dumps(value)
            elif value is None:
                flattened[new_key] = ""
            else:
                flattened[new_key] = str(value)
        
        return flattened
    
    def create_consumer_group(self, stream_name: str, group_name: str) -> bool:
        """Create a consumer group for a Redis stream.
        
        Args:
            stream_name: Name of the stream
            group_name: Name of the consumer group
            
        Returns:
            Success status
        """
        try:
            self.redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
            logger.info(f"Created consumer group '{group_name}' for stream '{stream_name}'")
            return True
        except RedisError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{group_name}' already exists")
                return True
            else:
                logger.error(f"Error creating consumer group: {e}")
                return False
    
    def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """Get information about a Redis stream.
        
        Args:
            stream_name: Name of the stream
            
        Returns:
            Stream information dictionary
        """
        try:
            info = self.redis_client.xinfo_stream(stream_name)
            return {
                'length': info.get('length', 0),
                'groups': info.get('groups', 0),
                'first_entry_id': info.get('first-entry', [None])[0],
                'last_entry_id': info.get('last-entry', [None])[0]
            }
        except RedisError as e:
            logger.error(f"Error getting stream info: {e}")
            return {}
    
    def cleanup_expired_keys(self, pattern: str = "pipeline:*") -> int:
        """Clean up expired keys matching a pattern.
        
        Args:
            pattern: Key pattern to match
            
        Returns:
            Number of keys deleted
        """
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted_count = self.redis_client.delete(*keys)
                logger.info(f"Cleaned up {deleted_count} expired keys")
                return deleted_count
            return 0
        except RedisError as e:
            logger.error(f"Error cleaning up keys: {e}")
            return 0
    
    def close(self):
        """Close Redis connection."""
        try:
            if self.connection_pool:
                self.connection_pool.disconnect()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")