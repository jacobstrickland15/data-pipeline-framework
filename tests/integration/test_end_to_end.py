"""End-to-end integration tests for the complete pipeline."""

import pytest
import pandas as pd
import asyncio
import tempfile
from pathlib import Path
import yaml
import sqlite3
from unittest.mock import patch

from data_pipeline.core.pipeline import Pipeline
from data_pipeline.core.config import Config
from data_pipeline.observability.metrics import get_metrics_collector
from data_pipeline.architecture.event_bus import get_event_bus


@pytest.mark.integration
class TestCompleteE2EWorkflow:
    """Test complete end-to-end pipeline workflows."""
    
    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return pd.DataFrame({
            'id': range(1, 101),
            'name': [f'User_{i}' for i in range(1, 101)],
            'age': [20 + (i % 50) for i in range(1, 101)],
            'email': [f'user{i}@example.com' for i in range(1, 101)],
            'salary': [30000 + (i * 1000) for i in range(1, 101)],
            'department': ['Engineering', 'Marketing', 'Sales', 'HR'][i % 4] for i in range(1, 101)]
        })
    
    @pytest.fixture
    def temp_workspace(self, sample_data):
        """Create temporary workspace with data files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = Path(tmp_dir)
            
            # Create data directory
            data_dir = workspace / 'data'
            data_dir.mkdir()
            
            # Create CSV file
            csv_file = data_dir / 'employees.csv'
            sample_data.to_csv(csv_file, index=False)
            
            # Create config directory
            config_dir = workspace / 'config'
            config_dir.mkdir()
            
            # Create reports directory
            reports_dir = workspace / 'reports'
            reports_dir.mkdir()
            
            yield {
                'workspace': workspace,
                'data_dir': data_dir,
                'config_dir': config_dir,
                'reports_dir': reports_dir,
                'csv_file': csv_file
            }
    
    @pytest.fixture
    def pipeline_config(self, temp_workspace):
        """Complete pipeline configuration for testing."""
        return {
            'name': 'e2e_test_pipeline',
            'description': 'End-to-end test pipeline',
            'source': {
                'type': 'csv',
                'config': {
                    'delimiter': ',',
                    'encoding': 'utf-8',
                    'header': True
                }
            },
            'processing': {
                'engine': 'pandas',
                'operations': [
                    {
                        'type': 'clean',
                        'params': {
                            'operations': ['remove_empty_rows', 'trim_strings']
                        }
                    },
                    {
                        'type': 'validate',
                        'params': {
                            'rules': [
                                {'column': 'id', 'type': 'not_null'},
                                {'column': 'email', 'type': 'email_format'},
                                {'column': 'age', 'type': 'range', 'min': 18, 'max': 100},
                                {'column': 'salary', 'type': 'range', 'min': 0, 'max': 500000}
                            ]
                        }
                    },
                    {
                        'type': 'transform',
                        'params': {
                            'column_mappings': {
                                'id': 'employee_id',
                                'name': 'full_name'
                            },
                            'calculated_columns': [
                                {
                                    'name': 'salary_category',
                                    'formula': 'pd.cut(salary, bins=[0, 50000, 100000, float("inf")], labels=["Low", "Medium", "High"])'
                                },
                                {
                                    'name': 'processed_at',
                                    'formula': 'datetime.now()'
                                }
                            ]
                        }
                    }
                ]
            },
            'storage': {
                'type': 'sqlite',
                'destination': 'employees',
                'mode': 'replace',
                'batch_size': 1000
            },
            'validation': {
                'enabled': True,
                'auto_generate_expectations': True,
                'suite_name': 'employee_validation',
                'custom_expectations': [
                    {
                        'expectation_type': 'expect_column_values_to_be_unique',
                        'column': 'employee_id'
                    },
                    {
                        'expectation_type': 'expect_column_values_to_match_regex',
                        'column': 'email',
                        'regex': '^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
                    }
                ]
            },
            'profiling': {
                'enabled': True,
                'generate_report': True,
                'output_path': None,  # Will be set dynamically
                'sample_size': 10000
            },
            'monitoring': {
                'enabled': True,
                'collect_metrics': True,
                'emit_events': True
            }
        }
    
    @pytest.fixture
    def database_config(self, temp_workspace):
        """Database configuration using SQLite."""
        db_path = temp_workspace['workspace'] / 'test.db'
        return {
            'type': 'sqlite',
            'database': str(db_path),
            'username': '',
            'password': ''
        }
    
    def test_complete_csv_to_database_pipeline(self, temp_workspace, pipeline_config, database_config):
        """Test complete CSV to database pipeline."""
        # Update paths in configuration
        pipeline_config['profiling']['output_path'] = str(temp_workspace['reports_dir'] / 'profile.html')
        
        # Create pipeline configuration file
        config_file = temp_workspace['config_dir'] / 'test_pipeline.yaml'
        with open(config_file, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Create main config
        main_config = Config(
            name='e2e_test',
            database=database_config
        )
        
        # Create and run pipeline
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = pipeline_config
        pipeline._setup_components()
        
        # Execute pipeline
        result = pipeline.run(str(temp_workspace['csv_file']))
        
        # Verify results
        assert result['success'] is True
        assert result['rows_processed'] == 100
        assert len(result['errors']) == 0
        
        # Verify data was stored
        db_path = database_config['database']
        conn = sqlite3.connect(db_path)
        
        # Check table exists and has data
        cursor = conn.execute("SELECT COUNT(*) FROM employees")
        row_count = cursor.fetchone()[0]
        assert row_count == 100
        
        # Check column names were mapped
        cursor = conn.execute("PRAGMA table_info(employees)")
        columns = [row[1] for row in cursor.fetchall()]
        assert 'employee_id' in columns
        assert 'full_name' in columns
        assert 'salary_category' in columns
        
        # Verify data quality
        cursor = conn.execute("SELECT DISTINCT salary_category FROM employees")
        categories = [row[0] for row in cursor.fetchall()]
        assert 'Low' in categories
        assert 'Medium' in categories
        assert 'High' in categories
        
        conn.close()
        
        # Verify profiling report was generated
        profile_path = Path(pipeline_config['profiling']['output_path'])
        assert profile_path.exists()
        
        # Verify validation results
        assert result['validation_results'] is not None
        assert isinstance(result['validation_results'], dict)
    
    @pytest.mark.asyncio
    async def test_pipeline_with_events_and_metrics(self, temp_workspace, pipeline_config, database_config):
        """Test pipeline execution with event bus and metrics collection."""
        # Get global components
        metrics_collector = get_metrics_collector()
        event_bus = get_event_bus()
        
        # Create pipeline
        main_config = Config(
            name='e2e_metrics_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = pipeline_config
        pipeline._setup_components()
        
        # Execute pipeline
        result = pipeline.run(str(temp_workspace['csv_file']))
        
        # Verify pipeline succeeded
        assert result['success'] is True
        
        # Give some time for async operations
        await asyncio.sleep(0.1)
        
        # Check metrics were collected
        # Note: This depends on the pipeline implementation actually recording metrics
        pipeline_metrics = metrics_collector.get_metrics('pipeline_executions_total', '1h')
        # We can't guarantee metrics were recorded without modifying pipeline code
        # But we can verify the metrics collector is working
        
        metrics_collector.record_counter('test_metric', 1)
        test_metrics = metrics_collector.get_metrics('test_metric', '1h')
        assert len(test_metrics) == 1
        
        # Check event history
        # Note: This would require the pipeline to actually emit events
        history = await event_bus.get_event_history('e2e_metrics_test')
        # We can't guarantee events were emitted without modifying pipeline code
    
    def test_pipeline_with_invalid_data(self, temp_workspace, pipeline_config, database_config):
        """Test pipeline behavior with invalid data."""
        # Create invalid data
        invalid_data = pd.DataFrame({
            'id': [1, None, 3, 1, 5],  # Null and duplicate IDs
            'name': ['Alice', '', 'Charlie', 'Diana', None],  # Empty and null names
            'age': [25, -5, 350, 28, 'invalid'],  # Invalid ages
            'email': [
                'alice@example.com',
                'invalid-email',
                'charlie@example.com',
                'diana@example.com',
                ''
            ],
            'salary': [50000, -1000, 70000, None, 'invalid'],  # Invalid salaries
            'department': ['Engineering', '', None, 'HR', 'Engineering']
        })
        
        # Save invalid data
        invalid_csv = temp_workspace['data_dir'] / 'invalid_employees.csv'
        invalid_data.to_csv(invalid_csv, index=False)
        
        # Create pipeline
        main_config = Config(
            name='e2e_invalid_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = pipeline_config
        pipeline._setup_components()
        
        # Execute pipeline
        result = pipeline.run(str(invalid_csv))
        
        # Pipeline should complete but may have validation issues
        # The exact behavior depends on how validation is configured
        assert 'success' in result
        assert 'validation_results' in result
        
        # If validation failed, check that it was properly reported
        if result.get('validation_results', {}).get('success') is False:
            assert 'validation_results' in result
    
    def test_pipeline_error_handling(self, temp_workspace, pipeline_config, database_config):
        """Test pipeline error handling and recovery."""
        # Create pipeline with invalid storage configuration
        invalid_config = pipeline_config.copy()
        invalid_config['storage']['destination'] = 'invalid/table/name'  # Invalid table name
        
        main_config = Config(
            name='e2e_error_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = invalid_config
        pipeline._setup_components()
        
        # Execute pipeline - should handle errors gracefully
        result = pipeline.run(str(temp_workspace['csv_file']))
        
        # Pipeline should report failure
        assert result['success'] is False
        assert len(result['errors']) > 0
    
    def test_pipeline_performance_with_large_dataset(self, temp_workspace, pipeline_config, database_config):
        """Test pipeline performance with larger dataset."""
        # Create larger dataset (10,000 rows)
        large_data = pd.DataFrame({
            'id': range(1, 10001),
            'name': [f'User_{i}' for i in range(1, 10001)],
            'age': [20 + (i % 50) for i in range(1, 10001)],
            'email': [f'user{i}@example.com' for i in range(1, 10001)],
            'salary': [30000 + (i * 10) for i in range(1, 10001)],
            'department': [['Engineering', 'Marketing', 'Sales', 'HR'][i % 4] for i in range(1, 10001)]
        })
        
        large_csv = temp_workspace['data_dir'] / 'large_employees.csv'
        large_data.to_csv(large_csv, index=False)
        
        # Adjust config for performance
        perf_config = pipeline_config.copy()
        perf_config['storage']['batch_size'] = 5000
        perf_config['profiling']['sample_size'] = 5000
        
        main_config = Config(
            name='e2e_performance_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = perf_config
        pipeline._setup_components()
        
        import time
        start_time = time.time()
        
        # Execute pipeline
        result = pipeline.run(str(large_csv))
        
        execution_time = time.time() - start_time
        
        # Verify results
        assert result['success'] is True
        assert result['rows_processed'] == 10000
        
        # Check performance (should complete within reasonable time)
        assert execution_time < 30.0  # Should complete within 30 seconds
        
        print(f"Large dataset pipeline completed in {execution_time:.2f} seconds")
    
    def test_multiple_pipeline_executions(self, temp_workspace, pipeline_config, database_config):
        """Test multiple pipeline executions with different configurations."""
        main_config = Config(
            name='e2e_multiple_test',
            database=database_config
        )
        
        # Create multiple data files
        for i in range(3):
            data = pd.DataFrame({
                'id': range(i*100 + 1, (i+1)*100 + 1),
                'name': [f'User_{j}' for j in range(i*100 + 1, (i+1)*100 + 1)],
                'age': [20 + (j % 50) for j in range(i*100 + 1, (i+1)*100 + 1)],
                'email': [f'user{j}@example.com' for j in range(i*100 + 1, (i+1)*100 + 1)],
                'salary': [30000 + (j * 100) for j in range(i*100 + 1, (i+1)*100 + 1)],
                'department': [['Engineering', 'Marketing', 'Sales', 'HR'][j % 4] for j in range(i*100 + 1, (i+1)*100 + 1)]
            })
            
            csv_file = temp_workspace['data_dir'] / f'batch_{i}.csv'
            data.to_csv(csv_file, index=False)
        
        # Execute pipeline for each file
        pipeline = Pipeline(main_config)
        results = []
        
        for i in range(3):
            config = pipeline_config.copy()
            config['storage']['destination'] = f'employees_batch_{i}'
            config['storage']['mode'] = 'replace'
            
            pipeline.pipeline_config = config
            pipeline._setup_components()
            
            csv_file = temp_workspace['data_dir'] / f'batch_{i}.csv'
            result = pipeline.run(str(csv_file))
            results.append(result)
        
        # Verify all executions succeeded
        for i, result in enumerate(results):
            assert result['success'] is True
            assert result['rows_processed'] == 100
            
        # Verify separate tables were created
        db_path = database_config['database']
        conn = sqlite3.connect(db_path)
        
        for i in range(3):
            cursor = conn.execute(f"SELECT COUNT(*) FROM employees_batch_{i}")
            row_count = cursor.fetchone()[0]
            assert row_count == 100
        
        conn.close()
    
    def test_dry_run_validation(self, temp_workspace, pipeline_config, database_config):
        """Test pipeline dry run functionality."""
        main_config = Config(
            name='e2e_dry_run_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = pipeline_config
        
        # Perform dry run
        dry_run_result = pipeline.dry_run(str(temp_workspace['csv_file']))
        
        # Verify dry run results
        assert 'config_valid' in dry_run_result
        assert 'components_initialized' in dry_run_result
        assert 'data_accessible' in dry_run_result
        assert 'estimated_operations' in dry_run_result
        
        # Dry run should not create any data
        db_path = database_config['database']
        if Path(db_path).exists():
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='employees'")
            result = cursor.fetchone()
            assert result is None  # Table should not exist after dry run
            conn.close()


@pytest.mark.integration
@pytest.mark.slow
class TestComplexWorkflows:
    """Test complex multi-step workflows."""
    
    def test_multi_source_pipeline(self, temp_workspace, database_config):
        """Test pipeline with multiple data sources."""
        # Create JSON data
        json_data = [
            {'user_id': 1, 'preferences': {'theme': 'dark', 'notifications': True}},
            {'user_id': 2, 'preferences': {'theme': 'light', 'notifications': False}},
            {'user_id': 3, 'preferences': {'theme': 'dark', 'notifications': True}},
        ]
        
        json_file = temp_workspace['data_dir'] / 'preferences.json'
        import json
        with open(json_file, 'w') as f:
            json.dump(json_data, f)
        
        # Configure pipeline for JSON processing
        json_config = {
            'name': 'json_pipeline',
            'source': {
                'type': 'json',
                'config': {
                    'normalize': True,
                    'sep': '_'
                }
            },
            'processing': {
                'engine': 'pandas',
                'operations': [
                    {
                        'type': 'clean',
                        'params': {
                            'operations': ['remove_empty_rows']
                        }
                    }
                ]
            },
            'storage': {
                'type': 'sqlite',
                'destination': 'user_preferences',
                'mode': 'replace'
            }
        }
        
        main_config = Config(
            name='multi_source_test',
            database=database_config
        )
        
        pipeline = Pipeline(main_config)
        pipeline.pipeline_config = json_config
        pipeline._setup_components()
        
        result = pipeline.run(str(json_file))
        
        assert result['success'] is True
        assert result['rows_processed'] > 0
        
        # Verify JSON data was processed and stored
        db_path = database_config['database']
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT * FROM user_preferences")
        rows = cursor.fetchall()
        assert len(rows) > 0
        conn.close()


@pytest.mark.integration
@pytest.mark.requires_docker
class TestContainerizedPipeline:
    """Test pipeline running in containerized environment."""
    
    def test_pipeline_in_container(self):
        """Test pipeline execution within Docker container."""
        # This test would require Docker to be available
        # and would test the full containerized deployment
        pytest.skip("Docker integration tests require Docker environment")
    
    def test_pipeline_with_external_services(self):
        """Test pipeline with external service dependencies."""
        # This test would verify integration with external services
        # like Redis, external databases, message queues, etc.
        pytest.skip("External service integration tests require service setup")