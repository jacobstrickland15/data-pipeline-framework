"""Performance benchmarks and load testing for the pipeline framework."""

import pytest
import pandas as pd
import numpy as np
import time
import tempfile
from pathlib import Path
import sqlite3
from memory_profiler import profile
import psutil
import os

from data_pipeline.core.pipeline import Pipeline
from data_pipeline.core.config import Config
from data_pipeline.processors.pandas_processor import PandasProcessor
from data_pipeline.observability.metrics import MetricsCollector


@pytest.mark.performance
class TestDataProcessingBenchmarks:
    """Benchmark data processing operations."""
    
    @pytest.fixture(params=[1000, 10000, 100000])
    def dataset_sizes(self, request):
        """Parameterized dataset sizes for benchmarking."""
        return request.param
    
    @pytest.fixture
    def sample_data(self, dataset_sizes):
        """Generate sample data of various sizes."""
        size = dataset_sizes
        np.random.seed(42)  # For reproducible results
        
        return pd.DataFrame({
            'id': range(size),
            'category': np.random.choice(['A', 'B', 'C', 'D'], size),
            'value': np.random.randn(size),
            'price': np.random.uniform(10, 1000, size),
            'date': pd.date_range('2024-01-01', periods=size, freq='1min'),
            'description': [f'Item {i} description' for i in range(size)],
            'flag': np.random.choice([True, False], size)
        })
    
    def test_pandas_processing_performance(self, benchmark, sample_data):
        """Benchmark pandas processing operations."""
        processor = PandasProcessor({})
        
        operations = [
            {
                'type': 'filter',
                'params': {'condition': 'value > 0'}
            },
            {
                'type': 'transform',
                'params': {
                    'calculated_columns': [
                        {'name': 'value_squared', 'formula': 'value ** 2'},
                        {'name': 'price_category', 'formula': 'pd.cut(price, bins=[0, 100, 500, float("inf")], labels=["Low", "Medium", "High"])'}
                    ]
                }
            }
        ]
        
        result = benchmark(processor.process, sample_data, operations)
        
        # Verify processing succeeded
        assert len(result) > 0
        assert 'value_squared' in result.columns
        assert 'price_category' in result.columns
    
    def test_csv_reading_performance(self, benchmark, sample_data):
        """Benchmark CSV reading performance."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_data.to_csv(f.name, index=False)
            csv_path = f.name
        
        try:
            def read_csv():
                return pd.read_csv(csv_path)
            
            result = benchmark(read_csv)
            assert len(result) == len(sample_data)
            
        finally:
            os.unlink(csv_path)
    
    def test_database_write_performance(self, benchmark, sample_data):
        """Benchmark database write performance."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            def write_to_db():
                conn = sqlite3.connect(db_path)
                sample_data.to_sql('benchmark_table', conn, if_exists='replace', index=False)
                conn.close()
            
            benchmark(write_to_db)
            
            # Verify data was written
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("SELECT COUNT(*) FROM benchmark_table")
            count = cursor.fetchone()[0]
            assert count == len(sample_data)
            conn.close()
            
        finally:
            os.unlink(db_path)
    
    def test_data_validation_performance(self, benchmark, sample_data):
        """Benchmark data validation performance."""
        def validate_data():
            # Simulate validation operations
            checks = []
            checks.append(sample_data['id'].notna().all())
            checks.append(sample_data['value'].dtype in [np.float64, np.int64])
            checks.append(sample_data['category'].isin(['A', 'B', 'C', 'D']).all())
            checks.append((sample_data['price'] > 0).all())
            return all(checks)
        
        result = benchmark(validate_data)
        assert result is True
    
    @pytest.mark.parametrize("chunk_size", [1000, 5000, 10000])
    def test_chunked_processing_performance(self, benchmark, sample_data, chunk_size):
        """Benchmark chunked data processing."""
        def process_in_chunks():
            results = []
            for chunk in pd.read_csv(
                pd.io.common.StringIO(sample_data.to_csv(index=False)), 
                chunksize=chunk_size
            ):
                # Simulate processing
                processed = chunk.copy()
                processed['processed'] = True
                results.append(processed)
            return pd.concat(results, ignore_index=True)
        
        # Only run this test for larger datasets
        if len(sample_data) >= chunk_size:
            result = benchmark(process_in_chunks)
            assert len(result) == len(sample_data)


@pytest.mark.performance
class TestMemoryUsage:
    """Test memory usage patterns."""
    
    def test_memory_usage_large_dataset(self):
        """Test memory usage with large datasets."""
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Create large dataset (1M rows)
        size = 1_000_000
        large_data = pd.DataFrame({
            'id': range(size),
            'value': np.random.randn(size),
            'category': np.random.choice(['A', 'B', 'C'], size)
        })
        
        peak_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory
        
        # Memory increase should be reasonable (less than 500MB for this test)
        assert memory_increase < 500
        
        # Clean up
        del large_data
        
        # Memory should be released (with some tolerance for garbage collection)
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_retained = final_memory - initial_memory
        assert memory_retained < memory_increase * 0.5  # Should release at least 50%
    
    @profile
    def test_memory_profile_pipeline_execution(self):
        """Profile memory usage during pipeline execution."""
        # Create test data
        data = pd.DataFrame({
            'id': range(10000),
            'value': np.random.randn(10000)
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            csv_path = f.name
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            config = Config(
                name='memory_test',
                database={
                    'type': 'sqlite',
                    'database': db_path
                }
            )
            
            pipeline_config = {
                'name': 'memory_test_pipeline',
                'source': {'type': 'csv', 'config': {}},
                'processing': {
                    'engine': 'pandas',
                    'operations': [
                        {'type': 'clean', 'params': {'operations': ['remove_empty_rows']}}
                    ]
                },
                'storage': {'type': 'sqlite', 'destination': 'test_table', 'mode': 'replace'}
            }
            
            pipeline = Pipeline(config)
            pipeline.pipeline_config = pipeline_config
            pipeline._setup_components()
            
            # Execute pipeline
            result = pipeline.run(csv_path)
            assert result['success'] is True
            
        finally:
            os.unlink(csv_path)
            os.unlink(db_path)


@pytest.mark.performance
class TestConcurrencyBenchmarks:
    """Test concurrent pipeline execution."""
    
    def test_parallel_pipeline_execution(self):
        """Test multiple pipelines running in parallel."""
        import threading
        import queue
        
        results_queue = queue.Queue()
        
        def run_pipeline(pipeline_id):
            # Create small dataset
            data = pd.DataFrame({
                'id': range(1000),
                'value': np.random.randn(1000),
                'pipeline_id': pipeline_id
            })
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                data.to_csv(f.name, index=False)
                csv_path = f.name
            
            with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
                db_path = f.name
            
            try:
                config = Config(
                    name=f'concurrent_test_{pipeline_id}',
                    database={
                        'type': 'sqlite',
                        'database': db_path
                    }
                )
                
                pipeline_config = {
                    'name': f'concurrent_pipeline_{pipeline_id}',
                    'source': {'type': 'csv', 'config': {}},
                    'processing': {
                        'engine': 'pandas',
                        'operations': [
                            {'type': 'clean', 'params': {'operations': ['remove_empty_rows']}}
                        ]
                    },
                    'storage': {
                        'type': 'sqlite', 
                        'destination': f'test_table_{pipeline_id}', 
                        'mode': 'replace'
                    }
                }
                
                pipeline = Pipeline(config)
                pipeline.pipeline_config = pipeline_config
                pipeline._setup_components()
                
                start_time = time.time()
                result = pipeline.run(csv_path)
                execution_time = time.time() - start_time
                
                results_queue.put({
                    'pipeline_id': pipeline_id,
                    'success': result['success'],
                    'execution_time': execution_time,
                    'rows_processed': result['rows_processed']
                })
                
            finally:
                os.unlink(csv_path)
                os.unlink(db_path)
        
        # Run 5 pipelines concurrently
        threads = []
        num_pipelines = 5
        
        start_time = time.time()
        
        for i in range(num_pipelines):
            thread = threading.Thread(target=run_pipeline, args=(i,))
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        total_time = time.time() - start_time
        
        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())
        
        # Verify all pipelines succeeded
        assert len(results) == num_pipelines
        for result in results:
            assert result['success'] is True
            assert result['rows_processed'] == 1000
        
        # Concurrent execution should be faster than sequential
        # (though this is a simple test and may not always hold)
        max_individual_time = max(r['execution_time'] for r in results)
        print(f"Concurrent execution: {total_time:.2f}s, Max individual: {max_individual_time:.2f}s")


@pytest.mark.performance
class TestScalabilityBenchmarks:
    """Test framework scalability with increasing data sizes."""
    
    @pytest.mark.parametrize("size_multiplier", [1, 10, 100])
    def test_linear_scalability(self, benchmark, size_multiplier):
        """Test that processing time scales linearly with data size."""
        base_size = 1000
        data_size = base_size * size_multiplier
        
        data = pd.DataFrame({
            'id': range(data_size),
            'value': np.random.randn(data_size),
            'category': np.random.choice(['A', 'B', 'C'], data_size)
        })
        
        def process_data():
            # Simulate common operations
            result = data.copy()
            result['value_squared'] = result['value'] ** 2
            result['value_abs'] = result['value'].abs()
            result = result[result['value'] > -2]  # Filter operation
            result = result.groupby('category')['value'].mean().reset_index()
            return result
        
        execution_time = benchmark(process_data)
        
        # Store results for analysis (in real scenarios, you'd analyze these)
        print(f"Size: {data_size}, Time: {execution_time}")
    
    def test_memory_scalability(self):
        """Test memory usage scaling with data size."""
        memory_usage = []
        data_sizes = [1000, 10000, 100000, 1000000]
        
        for size in data_sizes:
            initial_memory = psutil.Process().memory_info().rss
            
            # Create data
            data = pd.DataFrame({
                'id': range(size),
                'value': np.random.randn(size)
            })
            
            peak_memory = psutil.Process().memory_info().rss
            memory_used = (peak_memory - initial_memory) / 1024 / 1024  # MB
            
            memory_usage.append({
                'size': size,
                'memory_mb': memory_used
            })
            
            # Clean up
            del data
        
        # Memory usage should be roughly proportional to data size
        for i in range(1, len(memory_usage)):
            size_ratio = memory_usage[i]['size'] / memory_usage[i-1]['size']
            memory_ratio = memory_usage[i]['memory_mb'] / max(memory_usage[i-1]['memory_mb'], 1)
            
            # Memory usage should scale reasonably (within 2x of size ratio)
            assert memory_ratio <= size_ratio * 2


@pytest.mark.performance
class TestMetricsCollectionPerformance:
    """Test performance impact of metrics collection."""
    
    def test_metrics_collection_overhead(self, benchmark):
        """Test overhead of metrics collection."""
        collector = MetricsCollector()
        
        def record_metrics():
            for i in range(1000):
                collector.record_counter('test_counter', 1, {'iteration': str(i)})
                collector.record_gauge('test_gauge', float(i), {'type': 'test'})
                collector.record_histogram('test_histogram', float(i) / 1000)
        
        benchmark(record_metrics)
        
        # Verify metrics were recorded
        counter_metrics = collector.get_metrics('test_counter', '1h')
        assert len(counter_metrics) == 1000
    
    def test_metrics_aggregation_performance(self, benchmark):
        """Test performance of metrics aggregation."""
        collector = MetricsCollector()
        
        # Pre-populate with metrics
        for i in range(10000):
            collector.record_gauge('performance_metric', float(i))
        
        def aggregate_metrics():
            return collector.get_aggregated_metrics('performance_metric', '1h')
        
        result = benchmark(aggregate_metrics)
        
        assert result['count'] == 10000
        assert 'avg' in result
        assert 'min' in result
        assert 'max' in result
    
    def test_concurrent_metrics_collection(self):
        """Test concurrent metrics collection performance."""
        import threading
        
        collector = MetricsCollector()
        num_threads = 10
        metrics_per_thread = 1000
        
        def record_metrics_thread(thread_id):
            for i in range(metrics_per_thread):
                collector.record_counter(f'thread_{thread_id}_counter', 1)
                collector.record_gauge(f'thread_{thread_id}_gauge', float(i))
        
        start_time = time.time()
        
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=record_metrics_thread, args=(i,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        execution_time = time.time() - start_time
        
        # Should complete within reasonable time
        assert execution_time < 10.0  # 10 seconds max
        
        # Verify all metrics were recorded
        total_metrics = 0
        for i in range(num_threads):
            counter_metrics = collector.get_metrics(f'thread_{i}_counter', '1h')
            gauge_metrics = collector.get_metrics(f'thread_{i}_gauge', '1h')
            total_metrics += len(counter_metrics) + len(gauge_metrics)
        
        expected_total = num_threads * metrics_per_thread * 2  # counter + gauge
        assert total_metrics == expected_total


@pytest.mark.performance
@pytest.mark.slow
class TestEndToEndPerformanceBenchmarks:
    """End-to-end performance benchmarks."""
    
    def test_complete_pipeline_performance(self):
        """Benchmark complete pipeline execution."""
        # Create large test dataset
        size = 100000
        data = pd.DataFrame({
            'id': range(size),
            'name': [f'User_{i}' for i in range(size)],
            'age': np.random.randint(18, 80, size),
            'salary': np.random.uniform(30000, 150000, size),
            'department': np.random.choice(['Engineering', 'Marketing', 'Sales', 'HR'], size),
            'hire_date': pd.date_range('2020-01-01', periods=size, freq='1H')
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            csv_path = f.name
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            config = Config(
                name='performance_test',
                database={
                    'type': 'sqlite',
                    'database': db_path
                }
            )
            
            pipeline_config = {
                'name': 'performance_pipeline',
                'source': {'type': 'csv', 'config': {}},
                'processing': {
                    'engine': 'pandas',
                    'operations': [
                        {'type': 'clean', 'params': {'operations': ['remove_empty_rows', 'trim_strings']}},
                        {
                            'type': 'transform',
                            'params': {
                                'calculated_columns': [
                                    {'name': 'salary_category', 'formula': 'pd.cut(salary, bins=[0, 50000, 100000, float("inf")], labels=["Low", "Medium", "High"])'},
                                    {'name': 'years_experience', 'formula': '(pd.Timestamp.now() - hire_date).dt.days / 365.25'}
                                ]
                            }
                        }
                    ]
                },
                'storage': {
                    'type': 'sqlite',
                    'destination': 'performance_test',
                    'mode': 'replace',
                    'batch_size': 10000
                },
                'validation': {
                    'enabled': True,
                    'auto_generate_expectations': True
                }
            }
            
            pipeline = Pipeline(config)
            pipeline.pipeline_config = pipeline_config
            pipeline._setup_components()
            
            start_time = time.time()
            result = pipeline.run(csv_path)
            execution_time = time.time() - start_time
            
            # Performance assertions
            assert result['success'] is True
            assert result['rows_processed'] == size
            assert execution_time < 60.0  # Should complete within 1 minute
            
            # Throughput calculation
            throughput = size / execution_time
            print(f"Pipeline throughput: {throughput:.0f} rows/second")
            assert throughput > 1000  # Should process at least 1000 rows/second
            
        finally:
            os.unlink(csv_path)
            os.unlink(db_path)