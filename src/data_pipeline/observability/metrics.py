"""Enterprise-grade metrics collection and monitoring."""

import time
import logging
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import threading
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class Metric:
    """Metric data structure."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "type": self.metric_type.value,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
            "labels": self.labels
        }


@dataclass
class Alert:
    """Alert data structure."""
    name: str
    message: str
    level: AlertLevel
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)
    resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "message": self.message,
            "level": self.level.value,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
            "resolved": self.resolved
        }


class MetricsCollector:
    """Thread-safe metrics collector with advanced features."""
    
    def __init__(self, retention_hours: int = 24):
        self._metrics: defaultdict[str, deque] = defaultdict(lambda: deque(maxlen=1000))  # Reduced from 10000 to 1000
        self._lock = threading.RLock()
        self._retention_hours = retention_hours
        self._cleanup_interval = 300  # 5 minutes instead of 1 hour for more frequent cleanup
        self._last_cleanup = time.time()
        
        # Aggregation windows
        self._windows = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '1h': timedelta(hours=1),
            '24h': timedelta(hours=24)
        }
        
    def record_counter(self, name: str, value: float = 1, tags: Dict[str, str] = None) -> None:
        """Record a counter metric."""
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.COUNTER,
            tags=tags or {}
        )
        self._add_metric(metric)
    
    def record_gauge(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """Record a gauge metric."""
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.GAUGE,
            tags=tags or {}
        )
        self._add_metric(metric)
    
    def record_histogram(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """Record a histogram metric."""
        metric = Metric(
            name=name,
            value=value,
            metric_type=MetricType.HISTOGRAM,
            tags=tags or {}
        )
        self._add_metric(metric)
    
    def timer(self, name: str, tags: Dict[str, str] = None):
        """Context manager for timing operations."""
        return TimerContext(self, name, tags or {})
    
    def _add_metric(self, metric: Metric) -> None:
        """Add metric to collection."""
        with self._lock:
            self._metrics[metric.name].append(metric)
            self._cleanup_old_metrics()
    
    def _cleanup_old_metrics(self) -> None:
        """Clean up old metrics with memory leak prevention."""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return
            
        cutoff = datetime.utcnow() - timedelta(hours=self._retention_hours)
        
        # Prevent unbounded growth by limiting total metrics per type
        for metric_name, metric_deque in list(self._metrics.items()):
            # Remove old metrics
            while metric_deque and metric_deque[0].timestamp < cutoff:
                metric_deque.popleft()
            
            # Remove empty metric collections to prevent memory leaks
            if not metric_deque:
                del self._metrics[metric_name]
        
        # Force garbage collection after cleanup
        import gc
        gc.collect()
        
        self._last_cleanup = current_time
    
    def get_metrics(self, name: str, window: str = '1h') -> List[Metric]:
        """Get metrics for a specific time window."""
        if window not in self._windows:
            raise ValueError(f"Invalid window: {window}")
        
        cutoff = datetime.utcnow() - self._windows[window]
        
        with self._lock:
            metrics = self._metrics.get(name, deque())
            return [m for m in metrics if m.timestamp >= cutoff]
    
    def get_aggregated_metrics(self, name: str, window: str = '1h') -> Dict[str, float]:
        """Get aggregated metrics for a time window."""
        metrics = self.get_metrics(name, window)
        if not metrics:
            return {}
        
        values = [m.value for m in metrics]
        
        return {
            'count': len(values),
            'sum': sum(values),
            'avg': sum(values) / len(values),
            'min': min(values),
            'max': max(values),
            'latest': values[-1] if values else 0
        }
    
    def export_metrics(self, format_type: str = 'json') -> str:
        """Export metrics in various formats."""
        with self._lock:
            all_metrics = []
            for name, metric_deque in self._metrics.items():
                all_metrics.extend([m.to_dict() for m in metric_deque])
        
        if format_type == 'json':
            return json.dumps(all_metrics, indent=2)
        elif format_type == 'prometheus':
            return self._export_prometheus_format(all_metrics)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _export_prometheus_format(self, metrics: List[Dict[str, Any]]) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        grouped = defaultdict(list)
        
        for metric in metrics:
            grouped[metric['name']].append(metric)
        
        for name, metric_list in grouped.items():
            # Add help and type comments
            lines.append(f"# HELP {name} Generated metric")
            lines.append(f"# TYPE {name} gauge")
            
            for metric in metric_list[-1:]:  # Latest value only
                tags_str = ','.join([f'{k}="{v}"' for k, v in metric['tags'].items()])
                if tags_str:
                    lines.append(f"{name}{{{tags_str}}} {metric['value']}")
                else:
                    lines.append(f"{name} {metric['value']}")
        
        return '\n'.join(lines)


class TimerContext:
    """Context manager for timing operations."""
    
    def __init__(self, collector: MetricsCollector, name: str, tags: Dict[str, str]):
        self.collector = collector
        self.name = name
        self.tags = tags
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.collector.record_histogram(
                f"{self.name}_duration_seconds",
                duration,
                self.tags
            )


class AlertManager:
    """Manages alerts with rules and notifications."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self._rules: List[AlertRule] = []
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: List[Alert] = []
        self._lock = threading.RLock()
        
    def add_rule(self, rule: 'AlertRule') -> None:
        """Add an alert rule."""
        with self._lock:
            self._rules.append(rule)
            logger.info(f"Added alert rule: {rule.name}")
    
    def check_alerts(self) -> List[Alert]:
        """Check all rules and generate alerts."""
        new_alerts = []
        
        with self._lock:
            for rule in self._rules:
                try:
                    alert = rule.evaluate(self.metrics_collector)
                    if alert:
                        alert_key = f"{rule.name}_{hash(frozenset(alert.tags.items()))}"
                        
                        if alert_key not in self._active_alerts:
                            self._active_alerts[alert_key] = alert
                            self._alert_history.append(alert)
                            new_alerts.append(alert)
                            logger.warning(f"New alert: {alert.name} - {alert.message}")
                        
                    else:
                        # Check for resolved alerts
                        alert_key = f"{rule.name}_{hash(frozenset(rule.tags.items()))}"
                        if alert_key in self._active_alerts:
                            resolved_alert = self._active_alerts[alert_key]
                            resolved_alert.resolved = True
                            del self._active_alerts[alert_key]
                            logger.info(f"Alert resolved: {resolved_alert.name}")
                
                except Exception as e:
                    logger.error(f"Error evaluating alert rule {rule.name}: {e}")
        
        return new_alerts
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        with self._lock:
            return list(self._active_alerts.values())
    
    def get_alert_history(self, hours: int = 24) -> List[Alert]:
        """Get alert history for specified time period."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [alert for alert in self._alert_history if alert.timestamp >= cutoff]


class AlertRule:
    """Base class for alert rules."""
    
    def __init__(self, name: str, metric_name: str, tags: Dict[str, str] = None):
        self.name = name
        self.metric_name = metric_name
        self.tags = tags or {}
    
    def evaluate(self, metrics_collector: MetricsCollector) -> Optional[Alert]:
        """Evaluate the rule and return an alert if triggered."""
        raise NotImplementedError


class ThresholdRule(AlertRule):
    """Alert rule based on threshold comparison."""
    
    def __init__(self, name: str, metric_name: str, threshold: float, 
                 operator: str = 'gt', level: AlertLevel = AlertLevel.WARNING,
                 window: str = '5m', tags: Dict[str, str] = None):
        super().__init__(name, metric_name, tags)
        self.threshold = threshold
        self.operator = operator
        self.level = level
        self.window = window
        
        self.operators = {
            'gt': lambda x, y: x > y,
            'gte': lambda x, y: x >= y,
            'lt': lambda x, y: x < y,
            'lte': lambda x, y: x <= y,
            'eq': lambda x, y: x == y,
            'ne': lambda x, y: x != y
        }
    
    def evaluate(self, metrics_collector: MetricsCollector) -> Optional[Alert]:
        """Evaluate threshold rule."""
        aggregated = metrics_collector.get_aggregated_metrics(self.metric_name, self.window)
        
        if not aggregated:
            return None
        
        current_value = aggregated.get('latest', 0)
        
        if self.operators[self.operator](current_value, self.threshold):
            return Alert(
                name=self.name,
                message=f"{self.metric_name} is {current_value} (threshold: {self.threshold})",
                level=self.level,
                tags=self.tags
            )
        
        return None


class HealthChecker:
    """System health monitoring with multiple checks."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self._checks: List[Callable[[], bool]] = []
        self._check_names: List[str] = []
    
    def add_check(self, name: str, check_func: Callable[[], bool]) -> None:
        """Add a health check."""
        self._checks.append(check_func)
        self._check_names.append(name)
    
    def run_checks(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        overall_healthy = True
        
        for name, check_func in zip(self._check_names, self._checks):
            try:
                start_time = time.time()
                is_healthy = check_func()
                check_duration = time.time() - start_time
                
                results[name] = {
                    'healthy': is_healthy,
                    'duration_ms': round(check_duration * 1000, 2)
                }
                
                # Record metrics
                self.metrics_collector.record_gauge(
                    f"health_check_{name}",
                    1.0 if is_healthy else 0.0,
                    {'check_name': name}
                )
                
                self.metrics_collector.record_histogram(
                    f"health_check_duration_seconds",
                    check_duration,
                    {'check_name': name}
                )
                
                if not is_healthy:
                    overall_healthy = False
                    
            except Exception as e:
                results[name] = {
                    'healthy': False,
                    'error': str(e),
                    'duration_ms': 0
                }
                overall_healthy = False
                logger.error(f"Health check {name} failed: {e}")
        
        return {
            'overall_healthy': overall_healthy,
            'checks': results,
            'timestamp': datetime.utcnow().isoformat()
        }


class PerformanceMonitor:
    """Monitor system and application performance."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self._monitoring = False
        self._monitor_thread = None
        self._monitor_interval = 60  # seconds
    
    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        if self._monitoring:
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Performance monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join()
        logger.info("Performance monitoring stopped")
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop with memory leak prevention."""
        loop_count = 0
        while self._monitoring:
            try:
                self._collect_system_metrics()
                
                # Force garbage collection every 10 iterations to prevent memory leaks
                loop_count += 1
                if loop_count % 10 == 0:
                    import gc
                    gc.collect()
                    
                time.sleep(self._monitor_interval)
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")
                # Prevent infinite error loops
                time.sleep(self._monitor_interval)
    
    def _collect_system_metrics(self) -> None:
        """Collect system performance metrics."""
        try:
            import psutil
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.metrics_collector.record_gauge("system_cpu_usage_percent", cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.metrics_collector.record_gauge("system_memory_usage_percent", memory.percent)
            self.metrics_collector.record_gauge("system_memory_available_bytes", memory.available)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            self.metrics_collector.record_gauge("system_disk_usage_percent", (disk.used / disk.total) * 100)
            self.metrics_collector.record_gauge("system_disk_free_bytes", disk.free)
            
            # Network I/O
            network = psutil.net_io_counters()
            self.metrics_collector.record_counter("system_network_bytes_sent", network.bytes_sent)
            self.metrics_collector.record_counter("system_network_bytes_recv", network.bytes_recv)
            
        except ImportError:
            logger.warning("psutil not available, system metrics disabled")
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")


# Global instances
_metrics_collector = MetricsCollector()
_alert_manager = AlertManager(_metrics_collector)
_health_checker = HealthChecker(_metrics_collector)
_performance_monitor = PerformanceMonitor(_metrics_collector)

# Setup default alert rules
_alert_manager.add_rule(ThresholdRule(
    name="high_cpu_usage",
    metric_name="system_cpu_usage_percent",
    threshold=80.0,
    operator="gt",
    level=AlertLevel.WARNING
))

_alert_manager.add_rule(ThresholdRule(
    name="high_memory_usage",
    metric_name="system_memory_usage_percent",
    threshold=85.0,
    operator="gt",
    level=AlertLevel.CRITICAL
))


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    return _metrics_collector


def get_alert_manager() -> AlertManager:
    """Get the global alert manager."""
    return _alert_manager


def get_health_checker() -> HealthChecker:
    """Get the global health checker."""
    return _health_checker


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor."""
    return _performance_monitor