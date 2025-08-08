"""Metrics collection and reporting utilities."""

import time
import threading
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import statistics
import json

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


@dataclass
class MetricValue:
    """Represents a metric value with timestamp."""
    value: Union[int, float]
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass 
class PipelineMetrics:
    """Pipeline execution metrics."""
    pipeline_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    records_processed: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    error_message: Optional[str] = None
    step_durations: Dict[str, float] = field(default_factory=dict)
    data_quality_scores: Dict[str, float] = field(default_factory=dict)


class MetricsCollector:
    """Collects and manages various pipeline metrics."""
    
    def __init__(self, enable_prometheus: bool = False, prometheus_port: int = 8000):
        """Initialize metrics collector.
        
        Args:
            enable_prometheus: Whether to enable Prometheus metrics export
            prometheus_port: Port for Prometheus metrics server
        """
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = defaultdict(float)
        self.timers: Dict[str, List[float]] = defaultdict(list)
        
        self._lock = threading.Lock()
        self._pipeline_metrics: Dict[str, PipelineMetrics] = {}
        
        # Prometheus integration
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE
        if self.enable_prometheus:
            self._setup_prometheus_metrics()
            if prometheus_port:
                start_http_server(prometheus_port)
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics."""
        if not PROMETHEUS_AVAILABLE:
            return
        
        self.prom_pipeline_duration = Histogram(
            'pipeline_duration_seconds',
            'Time spent executing pipelines',
            ['pipeline_name', 'status']
        )
        
        self.prom_records_processed = Counter(
            'records_processed_total',
            'Total number of records processed',
            ['pipeline_name', 'step']
        )
        
        self.prom_records_failed = Counter(
            'records_failed_total',
            'Total number of records that failed processing',
            ['pipeline_name', 'step']
        )
        
        self.prom_active_pipelines = Gauge(
            'active_pipelines',
            'Number of currently active pipelines'
        )
        
        self.prom_data_quality_score = Gauge(
            'data_quality_score',
            'Data quality score for tables',
            ['table_name', 'metric_type']
        )
        
        self.prom_memory_usage = Gauge(
            'memory_usage_bytes',
            'Memory usage in bytes',
            ['pipeline_name', 'component']
        )
    
    def increment_counter(self, name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric.
        
        Args:
            name: Counter name
            value: Value to increment by
            labels: Optional labels
        """
        with self._lock:
            self.counters[name] += value
            
            metric_value = MetricValue(
                value=self.counters[name],
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric_value)
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value.
        
        Args:
            name: Gauge name
            value: Gauge value
            labels: Optional labels
        """
        with self._lock:
            self.gauges[name] = value
            
            metric_value = MetricValue(
                value=value,
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric_value)
    
    def record_timer(self, name: str, duration_seconds: float, 
                    labels: Optional[Dict[str, str]] = None):
        """Record a timer metric.
        
        Args:
            name: Timer name
            duration_seconds: Duration in seconds
            labels: Optional labels
        """
        with self._lock:
            self.timers[name].append(duration_seconds)
            
            metric_value = MetricValue(
                value=duration_seconds,
                timestamp=datetime.now(),
                labels=labels or {}
            )
            self.metrics[name].append(metric_value)
    
    def start_pipeline_metrics(self, pipeline_name: str, run_id: str) -> str:
        """Start collecting metrics for a pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline
            run_id: Unique run identifier
            
        Returns:
            Metrics key for this pipeline run
        """
        metrics_key = f"{pipeline_name}_{run_id}"
        
        with self._lock:
            self._pipeline_metrics[metrics_key] = PipelineMetrics(
                pipeline_name=pipeline_name,
                run_id=run_id,
                start_time=datetime.now()
            )
        
        # Update active pipelines gauge
        if self.enable_prometheus:
            self.prom_active_pipelines.inc()
        
        return metrics_key
    
    def end_pipeline_metrics(self, metrics_key: str, status: str,
                           records_processed: int = 0, records_failed: int = 0,
                           error_message: Optional[str] = None):
        """End collecting metrics for a pipeline run.
        
        Args:
            metrics_key: Metrics key from start_pipeline_metrics
            status: Final status ('success', 'failed', 'cancelled')
            records_processed: Number of records processed
            records_failed: Number of records that failed
            error_message: Error message if status is 'failed'
        """
        with self._lock:
            if metrics_key in self._pipeline_metrics:
                pipeline_metrics = self._pipeline_metrics[metrics_key]
                pipeline_metrics.end_time = datetime.now()
                pipeline_metrics.status = status
                pipeline_metrics.records_processed = records_processed
                pipeline_metrics.records_failed = records_failed
                pipeline_metrics.error_message = error_message
                pipeline_metrics.duration_seconds = (
                    pipeline_metrics.end_time - pipeline_metrics.start_time
                ).total_seconds()
        
        # Update Prometheus metrics
        if self.enable_prometheus:
            pipeline_metrics = self._pipeline_metrics.get(metrics_key)
            if pipeline_metrics:
                self.prom_pipeline_duration.labels(
                    pipeline_name=pipeline_metrics.pipeline_name,
                    status=status
                ).observe(pipeline_metrics.duration_seconds)
                
                self.prom_records_processed.labels(
                    pipeline_name=pipeline_metrics.pipeline_name,
                    step='total'
                ).inc(records_processed)
                
                if records_failed > 0:
                    self.prom_records_failed.labels(
                        pipeline_name=pipeline_metrics.pipeline_name,
                        step='total'
                    ).inc(records_failed)
            
            self.prom_active_pipelines.dec()
    
    def record_step_metrics(self, metrics_key: str, step_name: str,
                           duration_seconds: float, records_processed: int = 0):
        """Record metrics for a pipeline step.
        
        Args:
            metrics_key: Pipeline metrics key
            step_name: Name of the step
            duration_seconds: Step duration
            records_processed: Records processed in this step
        """
        with self._lock:
            if metrics_key in self._pipeline_metrics:
                pipeline_metrics = self._pipeline_metrics[metrics_key]
                pipeline_metrics.step_durations[step_name] = duration_seconds
        
        # Record step timer
        self.record_timer(f"step_{step_name}_duration", duration_seconds)
        
        # Update Prometheus metrics
        if self.enable_prometheus:
            pipeline_metrics = self._pipeline_metrics.get(metrics_key)
            if pipeline_metrics and records_processed > 0:
                self.prom_records_processed.labels(
                    pipeline_name=pipeline_metrics.pipeline_name,
                    step=step_name
                ).inc(records_processed)
    
    def record_data_quality_score(self, table_name: str, metric_type: str, score: float):
        """Record data quality score.
        
        Args:
            table_name: Name of the table
            metric_type: Type of quality metric
            score: Quality score (0.0 to 1.0)
        """
        metric_name = f"data_quality_{metric_type}"
        self.set_gauge(metric_name, score, {'table': table_name})
        
        # Update Prometheus metrics
        if self.enable_prometheus:
            self.prom_data_quality_score.labels(
                table_name=table_name,
                metric_type=metric_type
            ).set(score)
    
    def record_memory_usage(self, component: str, usage_bytes: float,
                          pipeline_name: Optional[str] = None):
        """Record memory usage.
        
        Args:
            component: Component name
            usage_bytes: Memory usage in bytes
            pipeline_name: Optional pipeline name
        """
        metric_name = f"memory_usage_{component}"
        labels = {'component': component}
        if pipeline_name:
            labels['pipeline'] = pipeline_name
        
        self.set_gauge(metric_name, usage_bytes, labels)
        
        # Update Prometheus metrics
        if self.enable_prometheus:
            self.prom_memory_usage.labels(
                pipeline_name=pipeline_name or 'unknown',
                component=component
            ).set(usage_bytes)
    
    def get_metric_stats(self, name: str) -> Dict[str, float]:
        """Get statistics for a metric.
        
        Args:
            name: Metric name
            
        Returns:
            Dictionary with metric statistics
        """
        with self._lock:
            if name not in self.metrics:
                return {}
            
            values = [m.value for m in self.metrics[name]]
            
            if not values:
                return {}
            
            return {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'std_dev': statistics.stdev(values) if len(values) > 1 else 0.0,
                'p95': statistics.quantiles(values, n=20)[18] if len(values) >= 20 else max(values),
                'p99': statistics.quantiles(values, n=100)[98] if len(values) >= 100 else max(values)
            }
    
    def get_pipeline_metrics(self, pipeline_name: Optional[str] = None) -> List[PipelineMetrics]:
        """Get pipeline metrics.
        
        Args:
            pipeline_name: Optional pipeline name filter
            
        Returns:
            List of pipeline metrics
        """
        with self._lock:
            metrics = list(self._pipeline_metrics.values())
            
            if pipeline_name:
                metrics = [m for m in metrics if m.pipeline_name == pipeline_name]
            
            return metrics
    
    def get_active_pipelines(self) -> List[PipelineMetrics]:
        """Get currently active pipeline metrics.
        
        Returns:
            List of active pipeline metrics
        """
        with self._lock:
            return [m for m in self._pipeline_metrics.values() if m.status == 'running']
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get overall metrics summary.
        
        Returns:
            Metrics summary
        """
        with self._lock:
            active_pipelines = len([m for m in self._pipeline_metrics.values() if m.status == 'running'])
            total_pipelines = len(self._pipeline_metrics)
            
            # Calculate success rate
            completed_pipelines = [m for m in self._pipeline_metrics.values() if m.status != 'running']
            successful_pipelines = [m for m in completed_pipelines if m.status == 'success']
            success_rate = len(successful_pipelines) / len(completed_pipelines) if completed_pipelines else 0
            
            # Calculate total records processed
            total_records = sum(m.records_processed for m in self._pipeline_metrics.values())
            
            # Get average duration
            durations = [m.duration_seconds for m in completed_pipelines if m.duration_seconds > 0]
            avg_duration = statistics.mean(durations) if durations else 0
            
            return {
                'active_pipelines': active_pipelines,
                'total_pipeline_runs': total_pipelines,
                'success_rate': success_rate,
                'total_records_processed': total_records,
                'average_duration_seconds': avg_duration,
                'metrics_collected': len(self.metrics),
                'last_updated': datetime.now().isoformat()
            }
    
    def export_metrics_to_json(self, file_path: str):
        """Export metrics to JSON file.
        
        Args:
            file_path: Path to export file
        """
        with self._lock:
            export_data = {
                'summary': self.get_metrics_summary(),
                'pipeline_metrics': [
                    {
                        'pipeline_name': m.pipeline_name,
                        'run_id': m.run_id,
                        'start_time': m.start_time.isoformat(),
                        'end_time': m.end_time.isoformat() if m.end_time else None,
                        'status': m.status,
                        'duration_seconds': m.duration_seconds,
                        'records_processed': m.records_processed,
                        'records_failed': m.records_failed,
                        'step_durations': m.step_durations,
                        'data_quality_scores': m.data_quality_scores
                    }
                    for m in self._pipeline_metrics.values()
                ],
                'metric_stats': {
                    name: self.get_metric_stats(name)
                    for name in self.metrics.keys()
                }
            }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)
    
    def clear_old_metrics(self, hours: int = 24):
        """Clear metrics older than specified hours.
        
        Args:
            hours: Hours threshold for clearing metrics
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self._lock:
            # Clear old pipeline metrics
            keys_to_remove = []
            for key, metrics in self._pipeline_metrics.items():
                if metrics.start_time < cutoff_time and metrics.status != 'running':
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._pipeline_metrics[key]
            
            # Clear old time-series metrics
            for name, metric_deque in self.metrics.items():
                # Remove old entries
                while metric_deque and metric_deque[0].timestamp < cutoff_time:
                    metric_deque.popleft()


class TimerContextManager:
    """Context manager for timing operations."""
    
    def __init__(self, metrics_collector: MetricsCollector, timer_name: str,
                 labels: Optional[Dict[str, str]] = None):
        """Initialize timer context manager.
        
        Args:
            metrics_collector: Metrics collector instance
            timer_name: Name of the timer
            labels: Optional labels
        """
        self.metrics_collector = metrics_collector
        self.timer_name = timer_name
        self.labels = labels
        self.start_time = None
    
    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timing and record metric."""
        if self.start_time:
            duration = time.time() - self.start_time
            self.metrics_collector.record_timer(self.timer_name, duration, self.labels)


def timer(metrics_collector: MetricsCollector, name: str, 
          labels: Optional[Dict[str, str]] = None):
    """Decorator for timing functions.
    
    Args:
        metrics_collector: Metrics collector instance
        name: Timer name
        labels: Optional labels
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            with TimerContextManager(metrics_collector, name, labels):
                return func(*args, **kwargs)
        return wrapper
    return decorator