"""Unit tests for core pipeline functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

from data_pipeline.core.pipeline import Pipeline, PipelineFactory
from data_pipeline.core.config import Config


@pytest.mark.unit
class TestPipelineFactory:
    """Test the PipelineFactory class."""
    
    def test_create_csv_source(self):
        """Test creating CSV source."""
        config = {
            'delimiter': ',',
            'encoding': 'utf-8'
        }
        
        source = PipelineFactory.create_source('csv', config)
        assert source is not None
        assert hasattr(source, 'read')
    
    def test_create_json_source(self):
        """Test creating JSON source."""
        config = {
            'lines': True
        }
        
        source = PipelineFactory.create_source('json', config)
        assert source is not None
        assert hasattr(source, 'read')
    
    def test_create_invalid_source_raises_error(self):
        """Test that invalid source type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported source type"):
            PipelineFactory.create_source('invalid', {})
    
    def test_create_pandas_processor(self):
        """Test creating Pandas processor."""
        processor = PipelineFactory.create_processor('pandas', {})
        assert processor is not None
        assert hasattr(processor, 'process')
    
    def test_create_spark_processor(self):
        """Test creating Spark processor."""
        processor = PipelineFactory.create_processor('spark', {})
        assert processor is not None
        assert hasattr(processor, 'process')
    
    def test_create_invalid_processor_raises_error(self):
        """Test that invalid processor type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported processor type"):
            PipelineFactory.create_processor('invalid', {})
    
    def test_create_postgresql_storage(self):
        """Test creating PostgreSQL storage."""
        config = {
            'host': 'localhost',
            'database': 'test',
            'username': 'user',
            'password': 'pass'
        }
        
        storage = PipelineFactory.create_storage('postgresql', config)
        assert storage is not None
        assert hasattr(storage, 'write')
    
    def test_create_invalid_storage_raises_error(self):
        """Test that invalid storage type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported storage type"):
            PipelineFactory.create_storage('invalid', {})


@pytest.mark.unit
class TestPipeline:
    """Test the Pipeline class."""
    
    @pytest.fixture
    def config(self):
        """Sample configuration for testing."""
        return Config(
            name="test_pipeline",
            database={
                'type': 'postgresql',
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'username': 'test_user',
                'password': 'test_pass'
            }
        )
    
    @pytest.fixture
    def pipeline_config_dict(self):
        """Sample pipeline configuration dictionary."""
        return {
            'name': 'test_pipeline',
            'description': 'Test pipeline',
            'source': {
                'type': 'csv',
                'config': {
                    'delimiter': ',',
                    'encoding': 'utf-8'
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
                'type': 'postgresql',
                'destination': 'test_table',
                'mode': 'append'
            },
            'validation': {
                'enabled': True,
                'auto_generate_expectations': True,
                'suite_name': 'test_suite'
            }
        }
    
    def test_pipeline_initialization_with_config_object(self, config):
        """Test pipeline initialization with Config object."""
        pipeline = Pipeline(config)
        assert pipeline.config == config
        assert pipeline.source is None
        assert pipeline.processor is None
        assert pipeline.storage is None
    
    def test_pipeline_initialization_with_dict(self, config):
        """Test pipeline initialization with dictionary."""
        config_dict = {
            'name': 'test',
            'database': {
                'type': 'postgresql',
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'username': 'test_user',
                'password': 'test_pass'
            }
        }
        
        pipeline = Pipeline(config_dict)
        assert pipeline.config.name == 'test'
        assert pipeline.config.database['type'] == 'postgresql'
    
    def test_pipeline_initialization_with_yaml_file(self, tmp_path, pipeline_config_dict):
        """Test pipeline initialization with YAML file."""
        import yaml
        
        config_file = tmp_path / "test_pipeline.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(pipeline_config_dict, f)
        
        pipeline = Pipeline(str(config_file))
        assert pipeline.pipeline_config['name'] == 'test_pipeline'
    
    def test_pipeline_yaml_file_not_found(self):
        """Test pipeline initialization with non-existent YAML file."""
        with pytest.raises(FileNotFoundError):
            Pipeline("/non/existent/file.yaml")
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_setup_components(self, mock_factory, config, pipeline_config_dict):
        """Test component setup from configuration."""
        # Mock factory methods
        mock_source = Mock()
        mock_processor = Mock() 
        mock_storage = Mock()
        mock_profiler = Mock()
        mock_validator = Mock()
        
        mock_factory.create_source.return_value = mock_source
        mock_factory.create_processor.return_value = mock_processor
        mock_factory.create_storage.return_value = mock_storage
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline._setup_components()
        
        # Verify factory methods were called
        mock_factory.create_source.assert_called_once_with('csv', {'delimiter': ',', 'encoding': 'utf-8'})
        mock_factory.create_processor.assert_called_once_with('pandas', {})
        mock_factory.create_storage.assert_called_once()
        
        # Verify components were set
        assert pipeline.source == mock_source
        assert pipeline.processor == mock_processor
        assert pipeline.storage == mock_storage
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_dry_run_success(self, mock_factory, config, pipeline_config_dict):
        """Test successful dry run."""
        # Mock components
        mock_source = Mock()
        mock_source.list_sources.return_value = ['file1.csv', 'file2.csv']
        
        mock_processor = Mock()
        mock_storage = Mock()
        
        mock_factory.create_source.return_value = mock_source
        mock_factory.create_processor.return_value = mock_processor
        mock_factory.create_storage.return_value = mock_storage
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        
        result = pipeline.dry_run()
        
        assert result['config_valid'] is True
        assert result['components_initialized'] is True
        assert result['data_accessible'] is True
        assert result['estimated_operations'] == 1
        assert len(result['errors']) == 0
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_dry_run_missing_sections(self, mock_factory, config):
        """Test dry run with missing configuration sections."""
        incomplete_config = {
            'name': 'test_pipeline',
            'source': {
                'type': 'csv',
                'config': {}
            }
            # Missing processing and storage sections
        }
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = incomplete_config
        
        result = pipeline.dry_run()
        
        assert result['config_valid'] is False
        assert 'Missing configuration sections' in result['warnings'][0]
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_dry_run_component_initialization_failure(self, mock_factory, config, pipeline_config_dict):
        """Test dry run when component initialization fails."""
        mock_factory.create_source.side_effect = Exception("Source creation failed")
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        
        result = pipeline.dry_run()
        
        assert len(result['errors']) > 0
        assert "Dry run failed" in result['errors'][0]
    
    def test_get_pipeline_info(self, config, pipeline_config_dict):
        """Test getting pipeline information."""
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        
        info = pipeline.get_pipeline_info()
        
        assert info['name'] == 'test_pipeline'
        assert info['description'] == 'Test pipeline'
        assert info['source_type'] == 'csv'
        assert info['processing_engine'] == 'pandas'
        assert info['storage_type'] == 'postgresql'
        assert info['validation_enabled'] is True
        assert info['total_operations'] == 1
        assert info['components_initialized'] is False
    
    @pytest.mark.asyncio
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    async def test_run_success(self, mock_factory, config, pipeline_config_dict, sample_csv_data):
        """Test successful pipeline execution."""
        # Mock components
        mock_source = Mock()
        mock_source.read.return_value = sample_csv_data
        
        mock_processor = Mock()
        mock_processor.process.return_value = sample_csv_data
        
        mock_storage = Mock()
        mock_storage.write.return_value = True
        
        mock_profiler = Mock()
        mock_profiler.profile_dataset.return_value = {'summary': 'test'}
        
        mock_validator = Mock()
        mock_validator.validate_data.return_value = {'success': True}
        
        mock_factory.create_source.return_value = mock_source
        mock_factory.create_processor.return_value = mock_processor
        mock_factory.create_storage.return_value = mock_storage
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline.pipeline_config['input'] = {'path': 'test.csv'}
        pipeline._setup_components()
        
        # Mock the profiler and validator
        pipeline.profiler = mock_profiler
        pipeline.validator = mock_validator
        
        result = pipeline.run()
        
        assert result['success'] is True
        assert result['rows_processed'] == len(sample_csv_data)
        assert len(result['errors']) == 0
        
        # Verify method calls
        mock_source.read.assert_called_once_with('test.csv')
        mock_processor.process.assert_called_once()
        mock_storage.write.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    async def test_run_with_validation_failure(self, mock_factory, config, pipeline_config_dict, sample_csv_data):
        """Test pipeline execution with validation failure."""
        # Mock components
        mock_source = Mock()
        mock_source.read.return_value = sample_csv_data
        
        mock_processor = Mock()
        mock_processor.process.return_value = sample_csv_data
        
        mock_storage = Mock()
        mock_storage.write.return_value = True
        
        mock_validator = Mock()
        mock_validator.validate_data.return_value = {'success': False, 'errors': ['Validation failed']}
        
        mock_factory.create_source.return_value = mock_source
        mock_factory.create_processor.return_value = mock_processor
        mock_factory.create_storage.return_value = mock_storage
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline.pipeline_config['input'] = {'path': 'test.csv'}
        pipeline._setup_components()
        
        # Mock the validator
        pipeline.validator = mock_validator
        
        result = pipeline.run()
        
        assert result['success'] is True  # Pipeline still succeeds even with validation warnings
        assert result['validation_results']['success'] is False
    
    @pytest.mark.asyncio
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    async def test_run_with_storage_failure(self, mock_factory, config, pipeline_config_dict, sample_csv_data):
        """Test pipeline execution with storage failure."""
        # Mock components
        mock_source = Mock()
        mock_source.read.return_value = sample_csv_data
        
        mock_processor = Mock()
        mock_processor.process.return_value = sample_csv_data
        
        mock_storage = Mock()
        mock_storage.write.return_value = False  # Storage failure
        
        mock_factory.create_source.return_value = mock_source
        mock_factory.create_processor.return_value = mock_processor
        mock_factory.create_storage.return_value = mock_storage
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline.pipeline_config['input'] = {'path': 'test.csv'}
        pipeline._setup_components()
        
        result = pipeline.run()
        
        assert result['success'] is False
        assert len(result['errors']) > 0
        assert 'Failed to store data' in result['errors'][0]
    
    @pytest.mark.asyncio
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    async def test_run_with_exception(self, mock_factory, config, pipeline_config_dict):
        """Test pipeline execution with exception."""
        # Mock source that raises exception
        mock_source = Mock()
        mock_source.read.side_effect = Exception("Read failed")
        
        mock_factory.create_source.return_value = mock_source
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline.pipeline_config['input'] = {'path': 'test.csv'}
        pipeline._setup_components()
        
        result = pipeline.run()
        
        assert result['success'] is False
        assert len(result['errors']) > 0
        assert 'Pipeline execution failed' in result['errors'][0]
    
    def test_load_data_no_source_configured(self, config, pipeline_config_dict):
        """Test loading data when no source is configured."""
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        
        with pytest.raises(ValueError, match="No data source configured"):
            pipeline._load_data()
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_load_data_no_input_specified(self, mock_factory, config, pipeline_config_dict):
        """Test loading data when no input is specified."""
        mock_source = Mock()
        mock_factory.create_source.return_value = mock_source
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline._setup_components()
        
        with pytest.raises(ValueError, match="No input source specified"):
            pipeline._load_data()
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_load_data_with_input_config(self, mock_factory, config, pipeline_config_dict, sample_csv_data):
        """Test loading data with input configuration."""
        mock_source = Mock()
        mock_source.read.return_value = sample_csv_data
        mock_factory.create_source.return_value = mock_source
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline.pipeline_config['input'] = {
            'path': 'test.csv',
            'nrows': 1000,
            'skiprows': 1
        }
        pipeline._setup_components()
        
        data = pipeline._load_data()
        
        assert data.equals(sample_csv_data)
        mock_source.read.assert_called_once_with('test.csv', nrows=1000, skiprows=1)
    
    @patch('data_pipeline.core.pipeline.PipelineFactory')
    def test_load_data_with_explicit_input_source(self, mock_factory, config, pipeline_config_dict, sample_csv_data):
        """Test loading data with explicit input source."""
        mock_source = Mock()
        mock_source.read.return_value = sample_csv_data
        mock_factory.create_source.return_value = mock_source
        
        pipeline = Pipeline(config)
        pipeline.pipeline_config = pipeline_config_dict
        pipeline._setup_components()
        
        data = pipeline._load_data('custom_input.csv')
        
        assert data.equals(sample_csv_data)
        mock_source.read.assert_called_once_with('custom_input.csv')


@pytest.mark.unit
@pytest.mark.parametrize("source_type,config", [
    ('csv', {'delimiter': ',', 'encoding': 'utf-8'}),
    ('json', {'lines': True}),
    ('s3', {'region': 'us-east-1'})
])
def test_create_different_sources(source_type, config):
    """Test creating different types of data sources."""
    source = PipelineFactory.create_source(source_type, config)
    assert source is not None
    assert hasattr(source, 'read')


@pytest.mark.unit
@pytest.mark.parametrize("processor_type,config", [
    ('pandas', {}),
    ('spark', {'app_name': 'test'})
])
def test_create_different_processors(processor_type, config):
    """Test creating different types of processors."""
    processor = PipelineFactory.create_processor(processor_type, config)
    assert processor is not None
    assert hasattr(processor, 'process')


@pytest.mark.unit  
def test_pipeline_metadata_tracking():
    """Test that pipeline tracks metadata correctly."""
    config = Config(name="test", database={'type': 'sqlite', 'database': ':memory:'})
    pipeline = Pipeline(config)
    
    assert pipeline.metadata == {}
    
    # Test metadata can be updated
    pipeline.metadata['run_id'] = '12345'
    pipeline.metadata['start_time'] = '2024-01-15T10:00:00Z'
    
    assert pipeline.metadata['run_id'] == '12345'
    assert pipeline.metadata['start_time'] == '2024-01-15T10:00:00Z'


@pytest.mark.unit
def test_pipeline_error_handling():
    """Test pipeline error handling and reporting."""
    config = Config(name="test", database={'type': 'sqlite', 'database': ':memory:'})
    pipeline = Pipeline(config)
    
    # Test with invalid YAML file
    with pytest.raises(FileNotFoundError):
        pipeline._load_pipeline_config('/invalid/path.yaml')