"""Health check and system monitoring utilities."""

import time
import threading
import psutil
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from sqlalchemy import create_engine, text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status enumeration."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    message: str
    duration_ms: float
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


class HealthCheck:
    """Base class for health checks."""
    
    def __init__(self, name: str, timeout_seconds: float = 30.0):
        """Initialize health check.
        
        Args:
            name: Name of the health check
            timeout_seconds: Timeout for the check
        """
        self.name = name
        self.timeout_seconds = timeout_seconds
    
    def check(self) -> HealthCheckResult:
        """Perform the health check.
        
        Returns:
            Health check result
        """
        start_time = time.time()
        
        try:
            status, message, details = self._perform_check()
            duration_ms = (time.time() - start_time) * 1000
            
            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                duration_ms=duration_ms,
                timestamp=datetime.now(),
                details=details
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.exception(f"Health check {self.name} failed with exception")
            
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Check failed: {str(e)}",
                duration_ms=duration_ms,
                timestamp=datetime.now()
            )
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Perform the actual health check logic.
        
        Returns:
            Tuple of (status, message, details)
        """
        raise NotImplementedError("Subclasses must implement _perform_check")


class DatabaseHealthCheck(HealthCheck):
    """Health check for database connectivity."""
    
    def __init__(self, name: str, database_url: str, query: str = "SELECT 1"):
        """Initialize database health check.
        
        Args:
            name: Name of the health check
            database_url: Database connection URL
            query: Query to execute for health check
        """
        super().__init__(name)
        self.database_url = database_url
        self.query = query
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Check database connectivity."""
        if not SQLALCHEMY_AVAILABLE:
            return HealthStatus.UNKNOWN, "SQLAlchemy not available", None
        
        try:
            engine = create_engine(self.database_url, pool_pre_ping=True)
            
            with engine.connect() as conn:
                result = conn.execute(text(self.query))
                row_count = len(result.fetchall())
                
                # Get connection pool info
                pool = engine.pool
                details = {
                    "query_result_count": row_count,
                    "pool_size": pool.size(),
                    "pool_checked_in": pool.checkedin(),
                    "pool_checked_out": pool.checkedout(),
                    "pool_overflow": getattr(pool, 'overflow', lambda: 0)(),
                }
                
                return HealthStatus.HEALTHY, "Database connection successful", details
                
        except Exception as e:
            return HealthStatus.UNHEALTHY, f"Database connection failed: {str(e)}", None


class RedisHealthCheck(HealthCheck):
    """Health check for Redis connectivity."""
    
    def __init__(self, name: str, redis_host: str = "localhost", 
                 redis_port: int = 6379, redis_password: Optional[str] = None):
        """Initialize Redis health check.
        
        Args:
            name: Name of the health check
            redis_host: Redis host
            redis_port: Redis port
            redis_password: Redis password
        """
        super().__init__(name)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Check Redis connectivity."""
        if not REDIS_AVAILABLE:
            return HealthStatus.UNKNOWN, "Redis library not available", None
        
        try:
            client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                socket_timeout=5,
                decode_responses=True
            )
            
            # Test connection
            ping_result = client.ping()
            
            # Get Redis info
            info = client.info()
            details = {
                "redis_version": info.get("redis_version"),
                "used_memory": info.get("used_memory"),
                "connected_clients": info.get("connected_clients"),
                "total_connections_received": info.get("total_connections_received"),
                "keyspace_hits": info.get("keyspace_hits"),
                "keyspace_misses": info.get("keyspace_misses")
            }
            
            if ping_result:
                return HealthStatus.HEALTHY, "Redis connection successful", details
            else:
                return HealthStatus.UNHEALTHY, "Redis ping failed", None
                
        except Exception as e:
            return HealthStatus.UNHEALTHY, f"Redis connection failed: {str(e)}", None


class DiskSpaceHealthCheck(HealthCheck):
    """Health check for disk space."""
    
    def __init__(self, name: str, path: str = "/", warning_threshold: float = 0.8,
                 critical_threshold: float = 0.9):
        """Initialize disk space health check.
        
        Args:
            name: Name of the health check
            path: Path to check disk space for
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
        """
        super().__init__(name)
        self.path = path
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Check disk space usage."""
        try:
            disk_usage = psutil.disk_usage(self.path)
            usage_percent = disk_usage.used / disk_usage.total
            
            details = {
                "path": self.path,
                "total_bytes": disk_usage.total,
                "used_bytes": disk_usage.used,
                "free_bytes": disk_usage.free,
                "usage_percent": usage_percent
            }
            
            if usage_percent >= self.critical_threshold:
                return HealthStatus.UNHEALTHY, f"Disk usage critical: {usage_percent:.1%}", details
            elif usage_percent >= self.warning_threshold:
                return HealthStatus.DEGRADED, f"Disk usage high: {usage_percent:.1%}", details
            else:
                return HealthStatus.HEALTHY, f"Disk usage normal: {usage_percent:.1%}", details
                
        except Exception as e:
            return HealthStatus.UNHEALTHY, f"Could not check disk usage: {str(e)}", None


class MemoryHealthCheck(HealthCheck):
    """Health check for memory usage."""
    
    def __init__(self, name: str, warning_threshold: float = 0.8,
                 critical_threshold: float = 0.9):
        """Initialize memory health check.
        
        Args:
            name: Name of the health check
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
        """
        super().__init__(name)
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Check memory usage."""
        try:
            memory = psutil.virtual_memory()
            usage_percent = memory.percent / 100.0
            
            details = {
                "total_bytes": memory.total,
                "available_bytes": memory.available,
                "used_bytes": memory.used,
                "usage_percent": usage_percent,
                "swap_total": psutil.swap_memory().total,
                "swap_used": psutil.swap_memory().used
            }
            
            if usage_percent >= self.critical_threshold:
                return HealthStatus.UNHEALTHY, f"Memory usage critical: {usage_percent:.1%}", details
            elif usage_percent >= self.warning_threshold:
                return HealthStatus.DEGRADED, f"Memory usage high: {usage_percent:.1%}", details
            else:
                return HealthStatus.HEALTHY, f"Memory usage normal: {usage_percent:.1%}", details
                
        except Exception as e:
            return HealthStatus.UNHEALTHY, f"Could not check memory usage: {str(e)}", None


class CPUHealthCheck(HealthCheck):
    """Health check for CPU usage."""
    
    def __init__(self, name: str, warning_threshold: float = 0.8,
                 critical_threshold: float = 0.95, interval: float = 1.0):
        """Initialize CPU health check.
        
        Args:
            name: Name of the health check
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
            interval: Interval for CPU measurement
        """
        super().__init__(name)
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.interval = interval
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Check CPU usage."""
        try:
            # Get CPU usage over interval
            cpu_percent = psutil.cpu_percent(interval=self.interval)
            usage_percent = cpu_percent / 100.0
            
            # Get additional CPU info
            cpu_count = psutil.cpu_count()
            load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)
            
            details = {
                "cpu_percent": usage_percent,
                "cpu_count": cpu_count,
                "load_avg_1min": load_avg[0],
                "load_avg_5min": load_avg[1],
                "load_avg_15min": load_avg[2]
            }
            
            if usage_percent >= self.critical_threshold:
                return HealthStatus.UNHEALTHY, f"CPU usage critical: {usage_percent:.1%}", details
            elif usage_percent >= self.warning_threshold:
                return HealthStatus.DEGRADED, f"CPU usage high: {usage_percent:.1%}", details
            else:
                return HealthStatus.HEALTHY, f"CPU usage normal: {usage_percent:.1%}", details
                
        except Exception as e:
            return HealthStatus.UNHEALTHY, f"Could not check CPU usage: {str(e)}", None


class CustomHealthCheck(HealthCheck):
    """Custom health check with user-defined function."""
    
    def __init__(self, name: str, check_function: Callable[[], tuple[HealthStatus, str, Optional[Dict[str, Any]]]]):
        """Initialize custom health check.
        
        Args:
            name: Name of the health check
            check_function: Function that performs the health check
        """
        super().__init__(name)
        self.check_function = check_function
    
    def _perform_check(self) -> tuple[HealthStatus, str, Optional[Dict[str, Any]]]:
        """Perform custom health check."""
        return self.check_function()


class HealthCheckManager:
    """Manages multiple health checks and provides aggregated status."""
    
    def __init__(self):
        """Initialize health check manager."""
        self.health_checks: Dict[str, HealthCheck] = {}
        self.last_results: Dict[str, HealthCheckResult] = {}
        self._lock = threading.Lock()
        self._background_thread = None
        self._stop_event = threading.Event()
        self.check_interval = 60  # seconds
    
    def register_health_check(self, health_check: HealthCheck):
        """Register a health check.
        
        Args:
            health_check: Health check instance
        """
        with self._lock:
            self.health_checks[health_check.name] = health_check
    
    def unregister_health_check(self, name: str):
        """Unregister a health check.
        
        Args:
            name: Name of the health check to remove
        """
        with self._lock:
            if name in self.health_checks:
                del self.health_checks[name]
            if name in self.last_results:
                del self.last_results[name]
    
    def run_health_check(self, name: str) -> Optional[HealthCheckResult]:
        """Run a specific health check.
        
        Args:
            name: Name of the health check
            
        Returns:
            Health check result or None if not found
        """
        with self._lock:
            if name not in self.health_checks:
                return None
            
            health_check = self.health_checks[name]
        
        result = health_check.check()
        
        with self._lock:
            self.last_results[name] = result
        
        return result
    
    def run_all_health_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all registered health checks.
        
        Returns:
            Dictionary of health check results
        """
        results = {}
        
        with self._lock:
            health_checks = list(self.health_checks.items())
        
        for name, health_check in health_checks:
            result = health_check.check()
            results[name] = result
            
            with self._lock:
                self.last_results[name] = result
        
        return results
    
    def get_overall_status(self) -> HealthStatus:
        """Get overall health status based on all checks.
        
        Returns:
            Overall health status
        """
        with self._lock:
            if not self.last_results:
                return HealthStatus.UNKNOWN
            
            statuses = [result.status for result in self.last_results.values()]
        
        # Overall status logic:
        # - If any check is UNHEALTHY, overall is UNHEALTHY
        # - If any check is DEGRADED, overall is DEGRADED
        # - If all checks are HEALTHY, overall is HEALTHY
        # - Otherwise, UNKNOWN
        
        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        elif all(status == HealthStatus.HEALTHY for status in statuses):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all checks.
        
        Returns:
            Health summary dictionary
        """
        with self._lock:
            results = dict(self.last_results)
        
        overall_status = self.get_overall_status()
        
        healthy_count = sum(1 for result in results.values() 
                          if result.status == HealthStatus.HEALTHY)
        degraded_count = sum(1 for result in results.values() 
                           if result.status == HealthStatus.DEGRADED)
        unhealthy_count = sum(1 for result in results.values() 
                            if result.status == HealthStatus.UNHEALTHY)
        unknown_count = sum(1 for result in results.values() 
                          if result.status == HealthStatus.UNKNOWN)
        
        return {
            "overall_status": overall_status.value,
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(results),
            "healthy_count": healthy_count,
            "degraded_count": degraded_count,
            "unhealthy_count": unhealthy_count,
            "unknown_count": unknown_count,
            "checks": {
                name: {
                    "status": result.status.value,
                    "message": result.message,
                    "duration_ms": result.duration_ms,
                    "timestamp": result.timestamp.isoformat(),
                    "details": result.details
                }
                for name, result in results.items()
            }
        }
    
    def start_background_monitoring(self, interval: int = 60):
        """Start background health check monitoring.
        
        Args:
            interval: Check interval in seconds
        """
        self.check_interval = interval
        self._stop_event.clear()
        
        if self._background_thread is None or not self._background_thread.is_alive():
            self._background_thread = threading.Thread(target=self._background_monitor)
            self._background_thread.daemon = True
            self._background_thread.start()
            logger.info(f"Started background health monitoring with {interval}s interval")
    
    def stop_background_monitoring(self):
        """Stop background health check monitoring."""
        self._stop_event.set()
        if self._background_thread and self._background_thread.is_alive():
            self._background_thread.join(timeout=5)
        logger.info("Stopped background health monitoring")
    
    def _background_monitor(self):
        """Background monitoring loop with memory leak prevention."""
        iteration = 0
        while not self._stop_event.wait(self.check_interval):
            try:
                self.run_all_health_checks()
                
                # Force garbage collection every 10 iterations to prevent memory buildup
                iteration += 1
                if iteration % 10 == 0:
                    import gc
                    gc.collect()
                    
            except Exception as e:
                logger.exception(f"Error in background health monitoring: {e}")
                # Prevent infinite error loops by backing off on errors
                self._stop_event.wait(min(self.check_interval * 2, 300))
    
    def setup_default_checks(self, database_url: Optional[str] = None,
                           redis_host: str = "localhost", redis_port: int = 6379):
        """Setup default health checks.
        
        Args:
            database_url: Database URL for database health check
            redis_host: Redis host
            redis_port: Redis port
        """
        # System resource checks
        self.register_health_check(DiskSpaceHealthCheck("disk_space"))
        self.register_health_check(MemoryHealthCheck("memory"))
        self.register_health_check(CPUHealthCheck("cpu"))
        
        # Database check
        if database_url:
            self.register_health_check(DatabaseHealthCheck("database", database_url))
        
        # Redis check
        if REDIS_AVAILABLE:
            self.register_health_check(RedisHealthCheck("redis", redis_host, redis_port))
        
        logger.info("Default health checks configured")