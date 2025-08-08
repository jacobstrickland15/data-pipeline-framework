"""Advanced caching system for the data pipeline framework."""

import hashlib
import json
import logging
import pickle
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
import threading
from dataclasses import dataclass
from enum import Enum
import gzip
import os

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """Cache levels for hierarchical caching."""
    L1_MEMORY = "l1_memory"
    L2_REDIS = "l2_redis"
    L3_DISK = "l3_disk"


class CacheStrategy(Enum):
    """Cache eviction strategies."""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    TTL = "ttl"  # Time To Live
    FIFO = "fifo"  # First In, First Out


@dataclass
class CacheEntry:
    """Represents a cache entry with metadata."""
    key: str
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    ttl_seconds: Optional[int] = None
    size_bytes: int = 0
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
    
    @property
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.ttl_seconds is None:
            return False
        return (datetime.utcnow() - self.created_at).total_seconds() > self.ttl_seconds
    
    def touch(self):
        """Update access metadata."""
        self.last_accessed = datetime.utcnow()
        self.access_count += 1


class CacheBackend(ABC):
    """Abstract base class for cache backends."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        pass
    
    @abstractmethod
    def clear(self) -> bool:
        """Clear all cache entries."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        pass
    
    @abstractmethod
    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        pass


class MemoryCache(CacheBackend):
    """In-memory cache with LRU eviction and size limits."""
    
    def __init__(self, max_size: int = 1000, max_memory_mb: int = 100, 
                 default_ttl: Optional[int] = None, strategy: CacheStrategy = CacheStrategy.LRU):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.default_ttl = default_ttl
        self.strategy = strategy
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._current_memory = 0
        
    def get(self, key: str) -> Optional[Any]:
        """Get value from memory cache."""
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # Check expiration
            if entry.is_expired:
                self.delete(key)
                return None
            
            # Update access metadata
            entry.touch()
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in memory cache."""
        with self._lock:
            # Calculate size
            size_bytes = self._calculate_size(value)
            
            # Check if we need to evict entries
            self._evict_if_needed(size_bytes)
            
            # Create cache entry
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                last_accessed=datetime.utcnow(),
                ttl_seconds=ttl or self.default_ttl,
                size_bytes=size_bytes
            )
            
            # Remove old entry if exists
            if key in self._cache:
                old_entry = self._cache[key]
                self._current_memory -= old_entry.size_bytes
            
            # Add new entry
            self._cache[key] = entry
            self._current_memory += size_bytes
            
            return True
    
    def delete(self, key: str) -> bool:
        """Delete key from memory cache."""
        with self._lock:
            if key in self._cache:
                entry = self._cache.pop(key)
                self._current_memory -= entry.size_bytes
                return True
            return False
    
    def clear(self) -> bool:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._current_memory = 0
            return True
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        with self._lock:
            if key not in self._cache:
                return False
            entry = self._cache[key]
            if entry.is_expired:
                self.delete(key)
                return False
            return True
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        with self._lock:
            import fnmatch
            return [key for key in self._cache.keys() if fnmatch.fnmatch(key, pattern)]
    
    def _calculate_size(self, value: Any) -> int:
        """Calculate approximate size of value in bytes."""
        try:
            return len(pickle.dumps(value))
        except Exception:
            # Fallback estimation
            if isinstance(value, str):
                return len(value.encode('utf-8'))
            elif isinstance(value, (int, float)):
                return 8
            elif isinstance(value, dict):
                return sum(self._calculate_size(k) + self._calculate_size(v) for k, v in value.items())
            elif isinstance(value, (list, tuple)):
                return sum(self._calculate_size(item) for item in value)
            else:
                return 1024  # Default estimate
    
    def _evict_if_needed(self, new_size: int):
        """Evict entries if needed to make room."""
        # Check size limit
        while (len(self._cache) >= self.max_size or 
               self._current_memory + new_size > self.max_memory_bytes) and self._cache:
            self._evict_one()
    
    def _evict_one(self):
        """Evict one entry based on strategy."""
        if not self._cache:
            return
        
        if self.strategy == CacheStrategy.LRU:
            # Evict least recently used
            key_to_evict = min(self._cache.keys(), 
                             key=lambda k: self._cache[k].last_accessed)
        elif self.strategy == CacheStrategy.LFU:
            # Evict least frequently used
            key_to_evict = min(self._cache.keys(), 
                             key=lambda k: self._cache[k].access_count)
        elif self.strategy == CacheStrategy.TTL:
            # Evict expired entries first, then oldest
            expired_keys = [k for k, v in self._cache.items() if v.is_expired]
            if expired_keys:
                key_to_evict = expired_keys[0]
            else:
                key_to_evict = min(self._cache.keys(), 
                                 key=lambda k: self._cache[k].created_at)
        else:  # FIFO
            # Evict oldest
            key_to_evict = min(self._cache.keys(), 
                             key=lambda k: self._cache[k].created_at)
        
        self.delete(key_to_evict)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'memory_usage_bytes': self._current_memory,
                'memory_usage_mb': round(self._current_memory / 1024 / 1024, 2),
                'max_memory_mb': self.max_memory_bytes / 1024 / 1024,
                'memory_utilization': round((self._current_memory / self.max_memory_bytes) * 100, 2) if self.max_memory_bytes > 0 else 0,
                'strategy': self.strategy.value
            }


class RedisCache(CacheBackend):
    """Redis-based cache backend."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, 
                 password: Optional[str] = None, prefix: str = "cache:"):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.prefix = prefix
        self._client = None
        self._connect()
    
    def _connect(self):
        """Connect to Redis."""
        try:
            import redis
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=False
            )
            # Test connection
            self._client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except ImportError:
            logger.warning("Redis not available, install with: pip install redis")
            self._client = None
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._client = None
    
    def _key(self, key: str) -> str:
        """Add prefix to key."""
        return f"{self.prefix}{key}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from Redis cache."""
        if not self._client:
            return None
        
        try:
            data = self._client.get(self._key(key))
            if data is None:
                return None
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Error getting key {key} from Redis: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in Redis cache."""
        if not self._client:
            return False
        
        try:
            data = pickle.dumps(value)
            if ttl:
                return self._client.setex(self._key(key), ttl, data)
            else:
                return self._client.set(self._key(key), data)
        except Exception as e:
            logger.error(f"Error setting key {key} in Redis: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        if not self._client:
            return False
        
        try:
            return bool(self._client.delete(self._key(key)))
        except Exception as e:
            logger.error(f"Error deleting key {key} from Redis: {e}")
            return False
    
    def clear(self) -> bool:
        """Clear all cache entries with prefix."""
        if not self._client:
            return False
        
        try:
            keys = self._client.keys(f"{self.prefix}*")
            if keys:
                return bool(self._client.delete(*keys))
            return True
        except Exception as e:
            logger.error(f"Error clearing Redis cache: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        if not self._client:
            return False
        
        try:
            return bool(self._client.exists(self._key(key)))
        except Exception as e:
            logger.error(f"Error checking key {key} in Redis: {e}")
            return False
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        if not self._client:
            return []
        
        try:
            keys = self._client.keys(f"{self.prefix}{pattern}")
            return [key.decode('utf-8').replace(self.prefix, '') for key in keys]
        except Exception as e:
            logger.error(f"Error getting keys from Redis: {e}")
            return []


class DiskCache(CacheBackend):
    """Disk-based cache backend with compression."""
    
    def __init__(self, cache_dir: str = ".cache", max_size_mb: int = 1000, 
                 compress: bool = True, default_ttl: Optional[int] = None):
        self.cache_dir = cache_dir
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.compress = compress
        self.default_ttl = default_ttl
        self._lock = threading.RLock()
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Cleanup expired entries on initialization
        self._cleanup_expired()
    
    def _key_path(self, key: str) -> str:
        """Get file path for key."""
        # Hash key to avoid filesystem issues
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{key_hash}.cache")
    
    def _metadata_path(self, key: str) -> str:
        """Get metadata file path for key."""
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{key_hash}.meta")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from disk cache."""
        with self._lock:
            key_path = self._key_path(key)
            meta_path = self._metadata_path(key)
            
            if not os.path.exists(key_path) or not os.path.exists(meta_path):
                return None
            
            try:
                # Read metadata
                with open(meta_path, 'r') as f:
                    metadata = json.load(f)
                
                # Check expiration
                if metadata.get('ttl_seconds'):
                    created_at = datetime.fromisoformat(metadata['created_at'])
                    if (datetime.utcnow() - created_at).total_seconds() > metadata['ttl_seconds']:
                        self.delete(key)
                        return None
                
                # Read data
                with open(key_path, 'rb') as f:
                    data = f.read()
                
                if self.compress:
                    data = gzip.decompress(data)
                
                return pickle.loads(data)
                
            except Exception as e:
                logger.error(f"Error reading key {key} from disk cache: {e}")
                # Clean up corrupted files
                self.delete(key)
                return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in disk cache."""
        with self._lock:
            try:
                # Serialize data
                data = pickle.dumps(value)
                
                if self.compress:
                    data = gzip.compress(data)
                
                # Check size limits
                self._cleanup_if_needed(len(data))
                
                # Write data
                key_path = self._key_path(key)
                with open(key_path, 'wb') as f:
                    f.write(data)
                
                # Write metadata
                metadata = {
                    'key': key,
                    'created_at': datetime.utcnow().isoformat(),
                    'size_bytes': len(data),
                    'ttl_seconds': ttl or self.default_ttl
                }
                
                meta_path = self._metadata_path(key)
                with open(meta_path, 'w') as f:
                    json.dump(metadata, f)
                
                return True
                
            except Exception as e:
                logger.error(f"Error setting key {key} in disk cache: {e}")
                return False
    
    def delete(self, key: str) -> bool:
        """Delete key from disk cache."""
        with self._lock:
            key_path = self._key_path(key)
            meta_path = self._metadata_path(key)
            
            deleted = False
            
            if os.path.exists(key_path):
                os.remove(key_path)
                deleted = True
            
            if os.path.exists(meta_path):
                os.remove(meta_path)
                deleted = True
            
            return deleted
    
    def clear(self) -> bool:
        """Clear all cache entries."""
        with self._lock:
            try:
                for filename in os.listdir(self.cache_dir):
                    if filename.endswith(('.cache', '.meta')):
                        os.remove(os.path.join(self.cache_dir, filename))
                return True
            except Exception as e:
                logger.error(f"Error clearing disk cache: {e}")
                return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in disk cache."""
        key_path = self._key_path(key)
        meta_path = self._metadata_path(key)
        return os.path.exists(key_path) and os.path.exists(meta_path)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        keys = []
        try:
            import fnmatch
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.meta'):
                    meta_path = os.path.join(self.cache_dir, filename)
                    try:
                        with open(meta_path, 'r') as f:
                            metadata = json.load(f)
                        key = metadata.get('key', '')
                        if fnmatch.fnmatch(key, pattern):
                            keys.append(key)
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Error getting keys from disk cache: {e}")
        
        return keys
    
    def _cleanup_expired(self):
        """Remove expired cache entries."""
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.meta'):
                    meta_path = os.path.join(self.cache_dir, filename)
                    try:
                        with open(meta_path, 'r') as f:
                            metadata = json.load(f)
                        
                        if metadata.get('ttl_seconds'):
                            created_at = datetime.fromisoformat(metadata['created_at'])
                            if (datetime.utcnow() - created_at).total_seconds() > metadata['ttl_seconds']:
                                key = metadata.get('key', '')
                                if key:
                                    self.delete(key)
                    except Exception:
                        # Remove corrupted metadata file
                        os.remove(meta_path)
        except Exception as e:
            logger.error(f"Error cleaning up expired entries: {e}")
    
    def _cleanup_if_needed(self, new_size: int):
        """Cleanup old entries if size limit would be exceeded."""
        try:
            current_size = self._get_current_size()
            if current_size + new_size > self.max_size_bytes:
                # Get all files sorted by modification time
                files = []
                for filename in os.listdir(self.cache_dir):
                    if filename.endswith('.cache'):
                        filepath = os.path.join(self.cache_dir, filename)
                        files.append((filepath, os.path.getmtime(filepath)))
                
                # Sort by modification time (oldest first)
                files.sort(key=lambda x: x[1])
                
                # Remove oldest files until we have enough space
                for filepath, _ in files:
                    if current_size + new_size <= self.max_size_bytes:
                        break
                    
                    file_size = os.path.getsize(filepath)
                    key_hash = os.path.basename(filepath).replace('.cache', '')
                    
                    # Find corresponding metadata file
                    meta_file = os.path.join(self.cache_dir, f"{key_hash}.meta")
                    
                    # Remove both files
                    os.remove(filepath)
                    if os.path.exists(meta_file):
                        os.remove(meta_file)
                    
                    current_size -= file_size
                    
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")
    
    def _get_current_size(self) -> int:
        """Get current cache size in bytes."""
        total_size = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.cache'):
                    filepath = os.path.join(self.cache_dir, filename)
                    total_size += os.path.getsize(filepath)
        except Exception:
            pass
        return total_size


class HierarchicalCache:
    """Multi-level hierarchical cache system."""
    
    def __init__(self, backends: Dict[CacheLevel, CacheBackend]):
        self.backends = backends
        self._stats = {
            'hits': {level: 0 for level in backends.keys()},
            'misses': {level: 0 for level in backends.keys()},
            'writes': {level: 0 for level in backends.keys()}
        }
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache, checking levels in order."""
        # Check each level in order
        for level in [CacheLevel.L1_MEMORY, CacheLevel.L2_REDIS, CacheLevel.L3_DISK]:
            if level not in self.backends:
                continue
            
            backend = self.backends[level]
            value = backend.get(key)
            
            if value is not None:
                self._stats['hits'][level] += 1
                
                # Promote to higher levels
                self._promote_to_higher_levels(key, value, level)
                
                return value
            else:
                self._stats['misses'][level] += 1
        
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in all cache levels."""
        success = True
        
        for level, backend in self.backends.items():
            if backend.set(key, value, ttl):
                self._stats['writes'][level] += 1
            else:
                success = False
        
        return success
    
    def delete(self, key: str) -> bool:
        """Delete key from all cache levels."""
        success = True
        
        for backend in self.backends.values():
            if not backend.delete(key):
                success = False
        
        return success
    
    def clear(self) -> bool:
        """Clear all cache levels."""
        success = True
        
        for backend in self.backends.values():
            if not backend.clear():
                success = False
        
        return success
    
    def _promote_to_higher_levels(self, key: str, value: Any, found_level: CacheLevel):
        """Promote cache entry to higher levels."""
        level_order = [CacheLevel.L1_MEMORY, CacheLevel.L2_REDIS, CacheLevel.L3_DISK]
        found_index = level_order.index(found_level)
        
        # Promote to all higher levels
        for i in range(found_index):
            level = level_order[i]
            if level in self.backends:
                self.backends[level].set(key, value)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(self._stats['hits'].values())
        total_requests = total_hits + sum(self._stats['misses'].values())
        hit_rate = (total_hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hit_rate_percent': round(hit_rate, 2),
            'total_requests': total_requests,
            'total_hits': total_hits,
            'level_stats': self._stats
        }


def cache_key(*args, **kwargs) -> str:
    """Generate a cache key from arguments."""
    # Create a deterministic key from arguments
    key_data = {
        'args': args,
        'kwargs': sorted(kwargs.items()) if kwargs else {}
    }
    key_json = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.sha256(key_json.encode()).hexdigest()


def cached(ttl: Optional[int] = None, cache_instance: Optional[CacheBackend] = None):
    """Decorator for caching function results."""
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            # Get or create cache instance
            if cache_instance is None:
                cache = _default_cache
            else:
                cache = cache_instance
            
            # Generate cache key
            key = f"func:{func.__name__}:{cache_key(*args, **kwargs)}"
            
            # Try to get from cache
            result = cache.get(key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            
            return result
        
        return wrapper
    return decorator


# Create default cache instance
_default_cache = MemoryCache(max_size=1000, max_memory_mb=100)


def get_default_cache() -> CacheBackend:
    """Get the default cache instance."""
    return _default_cache


def create_hierarchical_cache(
    memory_size: int = 1000,
    memory_mb: int = 100,
    redis_host: str = 'localhost',
    redis_port: int = 6379,
    disk_cache_dir: str = '.cache',
    disk_size_mb: int = 1000
) -> HierarchicalCache:
    """Create a hierarchical cache with all levels."""
    backends = {}
    
    # L1: Memory cache
    backends[CacheLevel.L1_MEMORY] = MemoryCache(
        max_size=memory_size,
        max_memory_mb=memory_mb
    )
    
    # L2: Redis cache (optional)
    try:
        redis_cache = RedisCache(host=redis_host, port=redis_port)
        if redis_cache._client is not None:
            backends[CacheLevel.L2_REDIS] = redis_cache
    except Exception as e:
        logger.warning(f"Redis cache not available: {e}")
    
    # L3: Disk cache
    backends[CacheLevel.L3_DISK] = DiskCache(
        cache_dir=disk_cache_dir,
        max_size_mb=disk_size_mb
    )
    
    return HierarchicalCache(backends)