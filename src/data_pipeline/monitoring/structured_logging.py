"""Structured logging configuration and utilities."""

import json
import logging
import logging.config
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pathlib import Path
import uuid

import structlog


class StructuredLogger:
    """Structured logging wrapper with JSON output and context management."""
    
    def __init__(self, name: str, config: Optional['LoggingConfig'] = None):
        """Initialize structured logger.
        
        Args:
            name: Logger name
            config: Logging configuration
        """
        self.name = name
        self.config = config or LoggingConfig()
        self._setup_logging()
        self.logger = structlog.get_logger(name)
        self._context = {}
    
    def _setup_logging(self):
        """Setup structlog configuration."""
        # Configure standard logging first
        logging.basicConfig(
            level=self.config.level,
            format='%(message)s',
            stream=sys.stdout
        )
        
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="ISO"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                self._add_context_processor,
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    
    def _add_context_processor(self, logger, method_name, event_dict):
        """Add context information to log events."""
        event_dict.update(self._context)
        return event_dict
    
    def set_context(self, **kwargs):
        """Set persistent context for this logger."""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear persistent context."""
        self._context.clear()
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self.logger.info(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.logger.debug(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self.logger.critical(message, **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        kwargs['traceback'] = traceback.format_exc()
        self.logger.error(message, **kwargs)
    
    def log_pipeline_start(self, pipeline_name: str, config: Dict[str, Any]):
        """Log pipeline start event."""
        self.info(
            "Pipeline started",
            event_type="pipeline_start",
            pipeline_name=pipeline_name,
            pipeline_config=config,
            run_id=str(uuid.uuid4())
        )
    
    def log_pipeline_end(self, pipeline_name: str, status: str, 
                        duration_seconds: float, records_processed: int = 0,
                        error_message: Optional[str] = None):
        """Log pipeline end event."""
        log_data = {
            "event_type": "pipeline_end",
            "pipeline_name": pipeline_name,
            "status": status,
            "duration_seconds": duration_seconds,
            "records_processed": records_processed
        }
        
        if error_message:
            log_data["error_message"] = error_message
        
        if status == "success":
            self.info("Pipeline completed successfully", **log_data)
        else:
            self.error("Pipeline failed", **log_data)
    
    def log_data_quality_check(self, table_name: str, metric_name: str,
                              value: float, threshold: float, status: str):
        """Log data quality check event."""
        self.info(
            "Data quality check completed",
            event_type="data_quality_check",
            table_name=table_name,
            metric_name=metric_name,
            metric_value=value,
            threshold=threshold,
            status=status
        )
    
    def log_performance_metric(self, operation: str, duration_seconds: float,
                              records_count: Optional[int] = None,
                              memory_usage_mb: Optional[float] = None):
        """Log performance metric."""
        log_data = {
            "event_type": "performance_metric",
            "operation": operation,
            "duration_seconds": duration_seconds
        }
        
        if records_count is not None:
            log_data["records_count"] = records_count
            log_data["records_per_second"] = records_count / duration_seconds if duration_seconds > 0 else 0
        
        if memory_usage_mb is not None:
            log_data["memory_usage_mb"] = memory_usage_mb
        
        self.info("Performance metric", **log_data)
    
    def log_transformation(self, transformation_type: str, input_records: int,
                          output_records: int, duration_seconds: float):
        """Log data transformation event."""
        self.info(
            "Data transformation completed",
            event_type="data_transformation",
            transformation_type=transformation_type,
            input_records=input_records,
            output_records=output_records,
            duration_seconds=duration_seconds,
            records_per_second=input_records / duration_seconds if duration_seconds > 0 else 0
        )
    
    def log_data_lineage(self, source_table: str, target_table: str,
                        transformation_type: str, pipeline_name: str):
        """Log data lineage event."""
        self.info(
            "Data lineage tracked",
            event_type="data_lineage",
            source_table=source_table,
            target_table=target_table,
            transformation_type=transformation_type,
            pipeline_name=pipeline_name
        )


class LoggingConfig:
    """Configuration for structured logging."""
    
    def __init__(
        self,
        level: Union[str, int] = logging.INFO,
        log_file: Optional[str] = None,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        include_source: bool = True,
        include_thread: bool = False
    ):
        """Initialize logging configuration.
        
        Args:
            level: Logging level
            log_file: Log file path (optional)
            max_file_size: Maximum log file size in bytes
            backup_count: Number of backup files to keep
            include_source: Whether to include source file info
            include_thread: Whether to include thread info
        """
        self.level = level
        self.log_file = log_file
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.include_source = include_source
        self.include_thread = include_thread


class ContextualLogger:
    """Context manager for adding temporary context to logs."""
    
    def __init__(self, logger: StructuredLogger, **context):
        """Initialize contextual logger.
        
        Args:
            logger: Structured logger instance
            **context: Context to add temporarily
        """
        self.logger = logger
        self.context = context
        self.original_context = None
    
    def __enter__(self):
        """Enter context and add temporary context."""
        self.original_context = self.logger._context.copy()
        self.logger.set_context(**self.context)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore original context."""
        self.logger._context = self.original_context
        
        if exc_type is not None:
            self.logger.exception(
                "Exception occurred in contextual logger",
                exception_type=exc_type.__name__,
                exception_message=str(exc_val)
            )


class PipelineLogger:
    """Logger specifically designed for pipeline operations."""
    
    def __init__(self, pipeline_name: str, run_id: Optional[str] = None):
        """Initialize pipeline logger.
        
        Args:
            pipeline_name: Name of the pipeline
            run_id: Unique run identifier
        """
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.logger = StructuredLogger(f"pipeline.{pipeline_name}")
        self.logger.set_context(
            pipeline_name=pipeline_name,
            run_id=self.run_id
        )
        self.start_time = None
        self.step_timers = {}
    
    def start_pipeline(self, config: Dict[str, Any]):
        """Start pipeline logging."""
        self.start_time = datetime.now()
        self.logger.log_pipeline_start(self.pipeline_name, config)
    
    def end_pipeline(self, status: str, records_processed: int = 0,
                    error_message: Optional[str] = None):
        """End pipeline logging."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            self.logger.log_pipeline_end(
                self.pipeline_name, status, duration, 
                records_processed, error_message
            )
    
    def start_step(self, step_name: str):
        """Start timing a pipeline step."""
        self.step_timers[step_name] = datetime.now()
        self.logger.info(f"Starting step: {step_name}", step=step_name)
    
    def end_step(self, step_name: str, records_processed: Optional[int] = None):
        """End timing a pipeline step."""
        if step_name in self.step_timers:
            duration = (datetime.now() - self.step_timers[step_name]).total_seconds()
            log_data = {
                "step": step_name,
                "duration_seconds": duration
            }
            
            if records_processed is not None:
                log_data["records_processed"] = records_processed
                log_data["records_per_second"] = records_processed / duration if duration > 0 else 0
            
            self.logger.info(f"Completed step: {step_name}", **log_data)
            del self.step_timers[step_name]
    
    def log_step_error(self, step_name: str, error: Exception):
        """Log an error in a pipeline step."""
        self.logger.error(
            f"Error in step: {step_name}",
            step=step_name,
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=traceback.format_exc()
        )
    
    def log_data_load(self, source: str, records_loaded: int, 
                     duration_seconds: float):
        """Log data loading operation."""
        self.logger.info(
            "Data loaded",
            event_type="data_load",
            source=source,
            records_loaded=records_loaded,
            duration_seconds=duration_seconds,
            records_per_second=records_loaded / duration_seconds if duration_seconds > 0 else 0
        )
    
    def log_data_write(self, destination: str, records_written: int,
                      duration_seconds: float):
        """Log data writing operation."""
        self.logger.info(
            "Data written",
            event_type="data_write",
            destination=destination,
            records_written=records_written,
            duration_seconds=duration_seconds,
            records_per_second=records_written / duration_seconds if duration_seconds > 0 else 0
        )


def setup_logging_from_config(config_path: str):
    """Setup logging from configuration file.
    
    Args:
        config_path: Path to logging configuration file
    """
    config_file = Path(config_path)
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            if config_file.suffix == '.json':
                config_dict = json.load(f)
            else:
                # Assume YAML
                try:
                    import yaml
                    config_dict = yaml.safe_load(f)
                except ImportError:
                    raise ImportError("PyYAML is required for YAML config files")
        
        logging.config.dictConfig(config_dict)
    else:
        # Default configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )


class LogAnalyzer:
    """Analyzer for structured logs."""
    
    def __init__(self, log_file: str):
        """Initialize log analyzer.
        
        Args:
            log_file: Path to log file
        """
        self.log_file = log_file
    
    def parse_logs(self) -> list:
        """Parse structured logs from file.
        
        Returns:
            List of parsed log entries
        """
        logs = []
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            log_entry = json.loads(line)
                            logs.append(log_entry)
                        except json.JSONDecodeError:
                            continue
        except FileNotFoundError:
            return []
        
        return logs
    
    def get_pipeline_metrics(self, pipeline_name: str) -> Dict[str, Any]:
        """Get metrics for a specific pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Pipeline metrics
        """
        logs = self.parse_logs()
        pipeline_logs = [log for log in logs 
                        if log.get('pipeline_name') == pipeline_name]
        
        if not pipeline_logs:
            return {}
        
        # Calculate metrics
        total_runs = len([log for log in pipeline_logs 
                         if log.get('event_type') == 'pipeline_start'])
        
        successful_runs = len([log for log in pipeline_logs 
                              if log.get('event_type') == 'pipeline_end' 
                              and log.get('status') == 'success'])
        
        failed_runs = len([log for log in pipeline_logs 
                          if log.get('event_type') == 'pipeline_end' 
                          and log.get('status') != 'success'])
        
        # Average duration
        durations = [log.get('duration_seconds', 0) for log in pipeline_logs 
                    if log.get('event_type') == 'pipeline_end']
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        # Total records processed
        total_records = sum([log.get('records_processed', 0) for log in pipeline_logs 
                           if log.get('event_type') == 'pipeline_end'])
        
        return {
            'pipeline_name': pipeline_name,
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'success_rate': successful_runs / total_runs if total_runs > 0 else 0,
            'average_duration_seconds': avg_duration,
            'total_records_processed': total_records
        }
    
    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of errors from logs.
        
        Returns:
            Error summary by type
        """
        logs = self.parse_logs()
        error_logs = [log for log in logs if log.get('level') == 'error']
        
        error_summary = {}
        for log in error_logs:
            error_type = log.get('error_type', 'Unknown')
            error_summary[error_type] = error_summary.get(error_type, 0) + 1
        
        return error_summary