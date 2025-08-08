"""Tests for data quality monitoring."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from datetime import datetime

from src.data_pipeline.utils.quality_monitor import (
    QualityMetrics, 
    DataQualityMonitor, 
    QualityMetric,
    QualityAlert
)


class TestQualityMetrics:
    """Test the QualityMetrics class."""
    
    def test_completeness_full_column(self):
        """Test completeness calculation for a complete column."""
        df = pd.DataFrame({'col1': [1, 2, 3, 4, 5]})
        result = QualityMetrics.completeness(df, 'col1')
        assert result == 1.0
    
    def test_completeness_partial_column(self):
        """Test completeness calculation for a column with missing values."""
        df = pd.DataFrame({'col1': [1, 2, None, 4, None]})
        result = QualityMetrics.completeness(df, 'col1')
        assert result == 0.6  # 3 out of 5 are non-null
    
    def test_completeness_entire_dataframe(self):
        """Test completeness calculation for entire dataframe."""
        df = pd.DataFrame({
            'col1': [1, 2, None, 4, 5],
            'col2': ['a', 'b', 'c', None, 'e']
        })
        result = QualityMetrics.completeness(df)
        assert result == 0.8  # 8 out of 10 values are non-null
    
    def test_uniqueness_all_unique(self):
        """Test uniqueness calculation for all unique values."""
        df = pd.DataFrame({'col1': [1, 2, 3, 4, 5]})
        result = QualityMetrics.uniqueness(df, 'col1')
        assert result == 1.0
    
    def test_uniqueness_with_duplicates(self):
        """Test uniqueness calculation with duplicate values."""
        df = pd.DataFrame({'col1': [1, 2, 2, 3, 3]})
        result = QualityMetrics.uniqueness(df, 'col1')
        assert result == 0.6  # 3 unique out of 5 total
    
    def test_validity_email_format(self):
        """Test validity calculation for email format."""
        df = pd.DataFrame({
            'email': ['valid@example.com', 'invalid-email', 'another@test.org', 'bad_email']
        })
        
        def is_valid_email(email):
            return '@' in str(email) and '.' in str(email)
        
        result = QualityMetrics.validity(df, 'email', is_valid_email)
        assert result == 0.5  # 2 out of 4 are valid
    
    def test_timeliness_recent_data(self):
        """Test timeliness calculation for recent data."""
        now = datetime.now()
        df = pd.DataFrame({
            'timestamp': [
                now,
                now - pd.Timedelta(hours=1),
                now - pd.Timedelta(hours=12),
                now - pd.Timedelta(hours=25),  # Old
                now - pd.Timedelta(hours=30)   # Old
            ]
        })
        
        result = QualityMetrics.timeliness(df, 'timestamp', max_age_hours=24)
        assert result == 0.6  # 3 out of 5 are within 24 hours
    
    def test_outlier_detection_iqr(self):
        """Test outlier detection using IQR method."""
        # Create data with clear outliers
        df = pd.DataFrame({
            'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]  # 100 is an outlier
        })
        
        result = QualityMetrics.outlier_detection(df, 'value', method='iqr')
        assert result == 0.9  # 9 out of 10 are not outliers
    
    def test_outlier_detection_zscore(self):
        """Test outlier detection using Z-score method."""
        # Create data with extreme outlier
        df = pd.DataFrame({
            'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 1000]  # 1000 is an extreme outlier
        })
        
        result = QualityMetrics.outlier_detection(df, 'value', method='zscore')
        assert result == 0.9  # 9 out of 10 are not outliers
    
    def test_consistency_sum_equals(self):
        """Test consistency check for sum equals rule."""
        df = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': [2, 3, 4, 5],
            'total': [3, 5, 7, 9]  # col1 + col2
        })
        
        result = QualityMetrics.consistency(df, ['col1', 'col2', 'total'], 'sum_equals')
        assert result == 1.0
    
    def test_consistency_sum_equals_with_errors(self):
        """Test consistency check with some inconsistent rows."""
        df = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': [2, 3, 4, 5],
            'total': [3, 5, 8, 9]  # Third row is inconsistent (should be 7)
        })
        
        result = QualityMetrics.consistency(df, ['col1', 'col2', 'total'], 'sum_equals')
        assert result == 0.75  # 3 out of 4 rows are consistent


class TestDataQualityMonitor:
    """Test the DataQualityMonitor class."""
    
    def test_init_with_mock_db(self):
        """Test initialization with mock database."""
        with patch('src.data_pipeline.utils.quality_monitor.create_engine') as mock_engine:
            monitor = DataQualityMonitor('mock://db_url')
            assert monitor.engine == mock_engine.return_value
    
    def test_run_single_check_completeness(self):
        """Test running a single completeness check."""
        df = pd.DataFrame({'col1': [1, 2, None, 4, 5]})
        
        with patch('src.data_pipeline.utils.quality_monitor.create_engine'):
            monitor = DataQualityMonitor('mock://db_url')
            
            check = {
                'metric': 'completeness',
                'column': 'col1',
                'threshold': 0.8
            }
            
            metric = monitor._run_single_check(df, 'test_table', check)
            
            assert metric is not None
            assert metric.metric_name == 'completeness'
            assert metric.column_name == 'col1'
            assert metric.metric_value == 0.8  # 4 out of 5 are non-null
            assert metric.threshold_value == 0.8
            assert metric.status == 'PASS'
    
    def test_run_single_check_uniqueness_fail(self):
        """Test running a uniqueness check that fails."""
        df = pd.DataFrame({'id': [1, 2, 2, 3, 4]})  # Has duplicates
        
        with patch('src.data_pipeline.utils.quality_monitor.create_engine'):
            monitor = DataQualityMonitor('mock://db_url')
            
            check = {
                'metric': 'uniqueness',
                'column': 'id',
                'threshold': 1.0  # Expecting 100% unique
            }
            
            metric = monitor._run_single_check(df, 'test_table', check)
            
            assert metric is not None
            assert metric.metric_name == 'uniqueness'
            assert metric.metric_value == 0.8  # 4 unique out of 5 total
            assert metric.status == 'FAIL'  # Below threshold
    
    def test_check_alerts_generation(self):
        """Test alert generation from metrics."""
        with patch('src.data_pipeline.utils.quality_monitor.create_engine'):
            monitor = DataQualityMonitor('mock://db_url')
            
            # Create failing metric
            failing_metric = QualityMetric(
                metric_name='completeness',
                table_name='test_table',
                column_name='col1',
                metric_value=0.5,
                threshold_value=0.8,
                status='FAIL',
                measured_at=datetime.now()
            )
            
            # Create warning metric
            warning_metric = QualityMetric(
                metric_name='uniqueness',
                table_name='test_table',
                column_name='col2',
                metric_value=0.75,
                threshold_value=0.9,
                status='WARN',
                measured_at=datetime.now()
            )
            
            # Create passing metric
            passing_metric = QualityMetric(
                metric_name='validity',
                table_name='test_table',
                column_name='col3',
                metric_value=0.95,
                threshold_value=0.8,
                status='PASS',
                measured_at=datetime.now()
            )
            
            metrics = [failing_metric, warning_metric, passing_metric]
            alerts = monitor.check_alerts(metrics)
            
            assert len(alerts) == 2  # Only failing and warning metrics should generate alerts
            
            # Check critical alert
            critical_alert = next(a for a in alerts if a.severity == 'CRITICAL')
            assert 'completeness' in critical_alert.message
            assert 'test_table' in critical_alert.message
            
            # Check warning alert
            warning_alert = next(a for a in alerts if a.severity == 'WARNING')
            assert 'uniqueness' in warning_alert.message
    
    def test_generate_alert_message(self):
        """Test alert message generation."""
        with patch('src.data_pipeline.utils.quality_monitor.create_engine'):
            monitor = DataQualityMonitor('mock://db_url')
            
            metric = QualityMetric(
                metric_name='completeness',
                table_name='users',
                column_name='email',
                metric_value=0.75,
                threshold_value=0.95,
                status='FAIL',
                measured_at=datetime.now()
            )
            
            message = monitor._generate_alert_message(metric)
            
            assert 'users' in message
            assert 'email' in message
            assert 'completeness' in message.lower()
            assert '75%' in message
            assert '95%' in message
            assert 'FAIL' in message
    
    @patch('src.data_pipeline.utils.quality_monitor.requests.post')
    def test_send_slack_alert(self, mock_post):
        """Test sending Slack alerts."""
        config = {
            'slack_enabled': True,
            'slack_webhook_url': 'https://hooks.slack.com/test'
        }
        
        with patch('src.data_pipeline.utils.quality_monitor.create_engine'):
            monitor = DataQualityMonitor('mock://db_url', config)
            
            metric = QualityMetric(
                metric_name='completeness',
                table_name='test_table',
                column_name='col1',
                metric_value=0.5,
                threshold_value=0.8,
                status='FAIL',
                measured_at=datetime.now()
            )
            
            alert = QualityAlert(
                alert_id='test_alert_1',
                metric=metric,
                severity='CRITICAL',
                message='Test alert message',
                created_at=datetime.now()
            )
            
            monitor._send_slack_alert(alert)
            
            # Verify that the Slack webhook was called
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            
            assert args[0] == 'https://hooks.slack.com/test'
            assert 'attachments' in kwargs['json']
            assert kwargs['json']['attachments'][0]['color'] == '#ff0000'  # Critical = red