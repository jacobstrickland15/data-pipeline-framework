"""Data quality monitoring and alerting system."""

import json
import logging
import smtplib
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


@dataclass
class QualityMetric:
    """Represents a data quality metric."""
    metric_name: str
    table_name: str
    column_name: Optional[str]
    metric_value: float
    threshold_value: float
    status: str  # PASS, WARN, FAIL
    details: Optional[Dict[str, Any]] = None
    measured_at: Optional[datetime] = None


@dataclass
class QualityAlert:
    """Represents a data quality alert."""
    alert_id: str
    metric: QualityMetric
    severity: str  # INFO, WARNING, CRITICAL
    message: str
    created_at: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None


class QualityMetrics:
    """Collection of data quality metric calculators."""
    
    @staticmethod
    def completeness(df: pd.DataFrame, column: str = None) -> float:
        """Calculate completeness (non-null ratio)."""
        if column:
            return df[column].notna().sum() / len(df)
        return df.notna().sum().sum() / (len(df) * len(df.columns))
    
    @staticmethod
    def uniqueness(df: pd.DataFrame, column: str) -> float:
        """Calculate uniqueness ratio."""
        return df[column].nunique() / len(df)
    
    @staticmethod
    def validity(df: pd.DataFrame, column: str, validator: Callable) -> float:
        """Calculate validity using a validator function."""
        try:
            valid_count = df[column].apply(validator).sum()
            return valid_count / len(df)
        except Exception as e:
            logger.error(f"Validity check failed: {e}")
            return 0.0
    
    @staticmethod
    def consistency(df: pd.DataFrame, columns: List[str], rule: str) -> float:
        """Calculate consistency based on business rules."""
        if rule == "sum_equals":
            # Check if sum of first n-1 columns equals the last column
            if len(columns) < 2:
                return 1.0
            calculated_sum = df[columns[:-1]].sum(axis=1)
            actual_sum = df[columns[-1]]
            matches = np.isclose(calculated_sum, actual_sum, rtol=1e-5)
            return matches.sum() / len(df)
        
        elif rule == "range_check":
            # Check if values fall within expected ranges
            valid_rows = df[columns].apply(
                lambda row: all(pd.notna(val) and 0 <= val <= 100 for val in row),
                axis=1
            )
            return valid_rows.sum() / len(df)
        
        return 1.0  # Default to passing
    
    @staticmethod
    def timeliness(df: pd.DataFrame, timestamp_column: str, max_age_hours: int = 24) -> float:
        """Calculate timeliness (how recent the data is)."""
        if timestamp_column not in df.columns:
            return 0.0
        
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        now = datetime.now()
        max_age = pd.Timedelta(hours=max_age_hours)
        
        recent_records = df[df[timestamp_column] >= now - max_age]
        return len(recent_records) / len(df)
    
    @staticmethod
    def outlier_detection(df: pd.DataFrame, column: str, method: str = "iqr") -> float:
        """Detect outliers and return the proportion of non-outliers."""
        if column not in df.columns or not pd.api.types.is_numeric_dtype(df[column]):
            return 1.0
        
        values = df[column].dropna()
        if len(values) == 0:
            return 1.0
        
        if method == "iqr":
            Q1 = values.quantile(0.25)
            Q3 = values.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = (values < lower_bound) | (values > upper_bound)
            return 1.0 - (outliers.sum() / len(values))
        
        elif method == "zscore":
            z_scores = np.abs((values - values.mean()) / values.std())
            outliers = z_scores > 3
            return 1.0 - (outliers.sum() / len(values))
        
        return 1.0


class DataQualityMonitor:
    """Main data quality monitoring system."""
    
    def __init__(self, db_url: str, config: Optional[Dict[str, Any]] = None):
        """Initialize the data quality monitor.
        
        Args:
            db_url: Database connection URL
            config: Configuration dictionary
        """
        self.engine = create_engine(db_url)
        self.config = config or {}
        self.metrics_calculator = QualityMetrics()
        self._ensure_schema_exists()
    
    def _ensure_schema_exists(self):
        """Ensure the data_quality schema exists."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_quality"))
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Failed to create data_quality schema: {e}")
            raise
    
    def run_quality_checks(
        self,
        table_name: str,
        schema_name: str = "public",
        checks: Optional[List[Dict[str, Any]]] = None
    ) -> List[QualityMetric]:
        """Run data quality checks on a table.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name
            checks: List of quality checks to run
            
        Returns:
            List of QualityMetric objects
        """
        metrics = []
        
        try:
            # Load data from the table
            query = f"SELECT * FROM {schema_name}.{table_name}"
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logger.warning(f"Table {schema_name}.{table_name} is empty")
                return metrics
            
            # Default checks if none provided
            if not checks:
                checks = self._get_default_checks(df)
            
            # Run each quality check
            for check in checks:
                metric = self._run_single_check(df, table_name, check)
                if metric:
                    metrics.append(metric)
                    # Store metric in database
                    self._store_metric(metric)
            
        except Exception as e:
            logger.error(f"Failed to run quality checks on {table_name}: {e}")
        
        return metrics
    
    def _get_default_checks(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate default quality checks based on data types."""
        checks = []
        
        for column in df.columns:
            # Completeness check for all columns
            checks.append({
                'metric': 'completeness',
                'column': column,
                'threshold': 0.95
            })
            
            # Uniqueness check for potential ID columns
            if any(keyword in column.lower() for keyword in ['id', 'key', 'code']):
                checks.append({
                    'metric': 'uniqueness',
                    'column': column,
                    'threshold': 1.0
                })
            
            # Outlier detection for numeric columns
            if pd.api.types.is_numeric_dtype(df[column]):
                checks.append({
                    'metric': 'outlier_detection',
                    'column': column,
                    'threshold': 0.95,
                    'params': {'method': 'iqr'}
                })
        
        # Timeliness check for timestamp columns
        timestamp_columns = df.select_dtypes(include=['datetime64']).columns
        for col in timestamp_columns:
            checks.append({
                'metric': 'timeliness',
                'column': col,
                'threshold': 0.8,
                'params': {'max_age_hours': 24}
            })
        
        return checks
    
    def _run_single_check(
        self,
        df: pd.DataFrame,
        table_name: str,
        check: Dict[str, Any]
    ) -> Optional[QualityMetric]:
        """Run a single quality check."""
        try:
            metric_name = check['metric']
            column = check.get('column')
            threshold = check.get('threshold', 0.95)
            params = check.get('params', {})
            
            # Calculate metric value
            if metric_name == 'completeness':
                value = self.metrics_calculator.completeness(df, column)
            elif metric_name == 'uniqueness':
                value = self.metrics_calculator.uniqueness(df, column)
            elif metric_name == 'timeliness':
                value = self.metrics_calculator.timeliness(df, column, **params)
            elif metric_name == 'outlier_detection':
                value = self.metrics_calculator.outlier_detection(df, column, **params)
            else:
                logger.warning(f"Unknown metric: {metric_name}")
                return None
            
            # Determine status
            if value >= threshold:
                status = 'PASS'
            elif value >= threshold * 0.8:  # Warning threshold
                status = 'WARN'
            else:
                status = 'FAIL'
            
            return QualityMetric(
                metric_name=metric_name,
                table_name=table_name,
                column_name=column,
                metric_value=round(value, 4),
                threshold_value=threshold,
                status=status,
                details={'params': params},
                measured_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Failed to run check {check}: {e}")
            return None
    
    def _store_metric(self, metric: QualityMetric):
        """Store a quality metric in the database."""
        try:
            with self.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO data_quality.quality_metrics 
                    (table_name, metric_name, metric_value, threshold_value, 
                     status, created_at, details)
                    VALUES (:table_name, :metric_name, :metric_value, :threshold_value,
                            :status, :created_at, :details)
                """)
                
                conn.execute(insert_query, {
                    'table_name': metric.table_name,
                    'metric_name': f"{metric.metric_name}_{metric.column_name}" if metric.column_name else metric.metric_name,
                    'metric_value': metric.metric_value,
                    'threshold_value': metric.threshold_value,
                    'status': metric.status,
                    'created_at': metric.measured_at,
                    'details': json.dumps(metric.details) if metric.details else None
                })
                conn.commit()
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to store metric: {e}")
    
    def check_alerts(self, metrics: List[QualityMetric]) -> List[QualityAlert]:
        """Check for data quality alerts based on metrics."""
        alerts = []
        
        for metric in metrics:
            if metric.status in ['WARN', 'FAIL']:
                severity = 'CRITICAL' if metric.status == 'FAIL' else 'WARNING'
                
                message = self._generate_alert_message(metric)
                
                alert = QualityAlert(
                    alert_id=f"DQ_{metric.table_name}_{metric.metric_name}_{int(metric.measured_at.timestamp())}",
                    metric=metric,
                    severity=severity,
                    message=message,
                    created_at=datetime.now()
                )
                
                alerts.append(alert)
        
        return alerts
    
    def _generate_alert_message(self, metric: QualityMetric) -> str:
        """Generate a human-readable alert message."""
        column_info = f" for column '{metric.column_name}'" if metric.column_name else ""
        
        return (
            f"Data quality issue detected in table '{metric.table_name}'{column_info}. "
            f"{metric.metric_name.title()} is {metric.metric_value:.2%} "
            f"(threshold: {metric.threshold_value:.2%}). "
            f"Status: {metric.status}"
        )
    
    def send_alerts(self, alerts: List[QualityAlert]):
        """Send alerts via configured channels."""
        for alert in alerts:
            try:
                # Send email alerts
                if self.config.get('email_enabled', False):
                    self._send_email_alert(alert)
                
                # Send Slack alerts
                if self.config.get('slack_enabled', False):
                    self._send_slack_alert(alert)
                    
                logger.info(f"Sent alert: {alert.alert_id}")
                
            except Exception as e:
                logger.error(f"Failed to send alert {alert.alert_id}: {e}")
    
    def _send_email_alert(self, alert: QualityAlert):
        """Send email alert."""
        smtp_config = self.config.get('smtp', {})
        recipients = self.config.get('alert_recipients', [])
        
        if not recipients or not smtp_config:
            return
        
        msg = MIMEMultipart()
        msg['From'] = smtp_config.get('sender')
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f"Data Quality Alert: {alert.severity}"
        
        body = f"""
        Data Quality Alert
        
        Alert ID: {alert.alert_id}
        Severity: {alert.severity}
        Table: {alert.metric.table_name}
        Metric: {alert.metric.metric_name}
        
        {alert.message}
        
        Metric Value: {alert.metric.metric_value:.4f}
        Threshold: {alert.metric.threshold_value:.4f}
        Measured At: {alert.metric.measured_at}
        
        Please investigate and resolve this data quality issue.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(smtp_config.get('host'), smtp_config.get('port', 587)) as server:
            if smtp_config.get('use_tls', True):
                server.starttls()
            if smtp_config.get('username'):
                server.login(smtp_config['username'], smtp_config['password'])
            server.send_message(msg)
    
    def _send_slack_alert(self, alert: QualityAlert):
        """Send Slack alert."""
        webhook_url = self.config.get('slack_webhook_url')
        
        if not webhook_url:
            return
        
        color = '#ff0000' if alert.severity == 'CRITICAL' else '#ffaa00'
        
        payload = {
            'attachments': [{
                'color': color,
                'title': f'Data Quality Alert: {alert.severity}',
                'text': alert.message,
                'fields': [
                    {'title': 'Table', 'value': alert.metric.table_name, 'short': True},
                    {'title': 'Metric', 'value': alert.metric.metric_name, 'short': True},
                    {'title': 'Value', 'value': f"{alert.metric.metric_value:.2%}", 'short': True},
                    {'title': 'Threshold', 'value': f"{alert.metric.threshold_value:.2%}", 'short': True}
                ],
                'ts': int(alert.created_at.timestamp())
            }]
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    
    def get_quality_dashboard_data(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get data quality dashboard data."""
        try:
            with self.engine.connect() as conn:
                where_clause = "WHERE table_name = :table_name" if table_name else ""
                params = {'table_name': table_name} if table_name else {}
                
                # Get recent metrics
                query = text(f"""
                    SELECT table_name, metric_name, metric_value, threshold_value,
                           status, created_at
                    FROM data_quality.quality_metrics
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT 100
                """)
                
                metrics_df = pd.read_sql(query, conn, params=params)
                
                # Calculate summary statistics
                summary = {
                    'total_checks': len(metrics_df),
                    'passed': len(metrics_df[metrics_df['status'] == 'PASS']),
                    'warnings': len(metrics_df[metrics_df['status'] == 'WARN']),
                    'failures': len(metrics_df[metrics_df['status'] == 'FAIL'])
                }
                
                # Group by table
                table_summary = metrics_df.groupby('table_name').agg({
                    'status': lambda x: (x == 'PASS').sum() / len(x),
                    'created_at': 'max'
                }).round(4).to_dict()
                
                return {
                    'summary': summary,
                    'table_scores': table_summary.get('status', {}),
                    'last_check': table_summary.get('created_at', {}),
                    'recent_metrics': metrics_df.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return {'summary': {}, 'table_scores': {}, 'recent_metrics': []}