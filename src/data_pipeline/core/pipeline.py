"""Main pipeline orchestration and execution."""

import logging
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import yaml
import pandas as pd

from .config import Config
from .base import DataSource, DataProcessor, DataStorage
from ..sources import CSVSource, JSONSource, S3Source
from ..processors import PandasProcessor, SparkProcessor
from ..storage import PostgreSQLStorage
from ..utils import DataProfiler, SchemaInferrer, DataValidator

logger = logging.getLogger(__name__)


class PipelineFactory:
    """Factory for creating pipeline components."""
    
    @staticmethod
    def create_source(source_type: str, config: Dict[str, Any]) -> DataSource:
        """Create data source based on type."""
        source_map = {
            'csv': CSVSource,
            'json': JSONSource,
            's3': S3Source
        }
        
        if source_type not in source_map:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        return source_map[source_type](config)
    
    @staticmethod
    def create_processor(processor_type: str, config: Dict[str, Any]) -> DataProcessor:
        """Create data processor based on type."""
        processor_map = {
            'pandas': PandasProcessor,
            'spark': SparkProcessor
        }
        
        if processor_type not in processor_map:
            raise ValueError(f"Unsupported processor type: {processor_type}")
        
        return processor_map[processor_type](config)
    
    @staticmethod
    def create_storage(storage_type: str, config: Dict[str, Any]) -> DataStorage:
        """Create data storage based on type."""
        storage_map = {
            'postgresql': PostgreSQLStorage
        }
        
        if storage_type not in storage_map:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        return storage_map[storage_type](config)


class Pipeline:
    """Main data processing pipeline."""
    
    def __init__(self, config: Union[Config, Dict[str, Any], str]):
        """Initialize pipeline with configuration."""
        if isinstance(config, str):
            # Load from file
            self.config = self._load_pipeline_config(config)
        elif isinstance(config, Config):
            self.config = config
        else:
            # Dictionary config
            self.config = Config(**config)
        
        # Initialize components
        self.source: Optional[DataSource] = None
        self.processor: Optional[DataProcessor] = None
        self.storage: Optional[DataStorage] = None
        
        # Utilities
        self.profiler: Optional[DataProfiler] = None
        self.schema_inferrer: Optional[SchemaInferrer] = None
        self.validator: Optional[DataValidator] = None
        
        # Pipeline state
        self.pipeline_config: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        
    def _load_pipeline_config(self, config_path: str) -> Config:
        """Load pipeline configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Pipeline config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            pipeline_config = yaml.safe_load(f)
        
        self.pipeline_config = pipeline_config
        
        # Extract base config or create default
        base_config = pipeline_config.get('base_config', {})
        if not base_config:
            # Create minimal config
            base_config = {
                'name': pipeline_config.get('name', 'pipeline'),
                'database': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'data_warehouse',
                    'username': 'postgres',
                    'password': ''
                }
            }
        
        return Config(**base_config)
    
    def from_yaml(self, pipeline_config_path: str) -> 'Pipeline':
        """Create pipeline from YAML configuration file."""
        config_file = Path(pipeline_config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Pipeline config file not found: {pipeline_config_path}")
        
        with open(config_file, 'r') as f:
            pipeline_config = yaml.safe_load(f)
        
        self.pipeline_config = pipeline_config
        
        # Initialize components from config
        self._setup_components()
        
        return self
    
    def _setup_components(self):
        """Set up pipeline components from configuration."""
        config = self.pipeline_config
        
        # Setup data source
        if 'source' in config:
            source_config = config['source']
            self.source = PipelineFactory.create_source(
                source_config['type'], 
                source_config.get('config', {})
            )
        
        # Setup processor
        if 'processing' in config:
            processing_config = config['processing']
            engine = processing_config.get('engine', 'pandas')
            
            processor_config = {}
            if engine == 'spark' and 'performance' in config:
                spark_config = config['performance'].get('spark_config', {})
                processor_config.update(spark_config)
            
            self.processor = PipelineFactory.create_processor(engine, processor_config)
        
        # Setup storage
        if 'storage' in config:
            storage_config = config['storage']
            storage_type = storage_config.get('type', 'postgresql')
            self.storage = PipelineFactory.create_storage(
                storage_type,
                self.config.database.dict()
            )
        
        # Setup utilities
        if config.get('profiling', {}).get('enabled', False):
            profiler_config = config['profiling']
            self.profiler = DataProfiler({
                'sample_size': profiler_config.get('sample_size', 100000),
                'output_format': 'html'
            })
        
        if config.get('schema', {}).get('auto_infer', False):
            self.schema_inferrer = SchemaInferrer({
                'sample_size': 10000,
                'cardinality_threshold': 50
            })
        
        if config.get('validation', {}).get('enabled', False):
            self.validator = DataValidator({
                'store_results': True
            })
    
    def run(self, input_source: Optional[str] = None, output_destination: Optional[str] = None) -> Dict[str, Any]:
        """Execute the complete pipeline."""
        logger.info(f"Starting pipeline execution: {self.pipeline_config.get('name', 'Unnamed Pipeline')}")
        
        results = {
            'success': False,
            'metadata': {},
            'profiling_results': None,
            'validation_results': None,
            'schema_info': None,
            'rows_processed': 0,
            'errors': []
        }
        
        try:
            # Step 1: Load data
            data = self._load_data(input_source)
            results['rows_processed'] = len(data) if hasattr(data, '__len__') else 0
            logger.info(f"Loaded {results['rows_processed']} rows")
            
            # Step 2: Profile data (if enabled)
            if self.profiler:
                logger.info("Running data profiling...")
                profile = self.profiler.profile_dataset(data, self.pipeline_config.get('name', 'Dataset'))
                results['profiling_results'] = profile
                
                # Generate HTML report if configured
                profiling_config = self.pipeline_config.get('profiling', {})
                if profiling_config.get('generate_report', False):
                    output_path = profiling_config.get('output_path', './data_profile_report.html')
                    self.profiler.generate_html_report(profile, output_path)
                    logger.info(f"Profiling report saved to {output_path}")
            
            # Step 3: Infer schema (if enabled)
            if self.schema_inferrer:
                logger.info("Inferring data schema...")
                schema = self.schema_inferrer.infer_schema(data)
                results['schema_info'] = schema
            
            # Step 4: Process data
            if self.processor and 'processing' in self.pipeline_config:
                logger.info("Processing data...")
                operations = self.pipeline_config['processing'].get('operations', [])
                if operations:
                    data = self.processor.process(data, operations)
                    logger.info(f"Data processing completed")
            
            # Step 5: Validate data (if enabled)
            if self.validator and 'validation' in self.pipeline_config:
                logger.info("Validating data...")
                validation_config = self.pipeline_config['validation']
                
                # Create or use existing expectation suite
                suite_name = validation_config.get('suite_name', 'default_suite')
                
                if validation_config.get('auto_generate_expectations', False):
                    self.validator.create_expectation_suite(suite_name, data, auto_generate=True)
                
                # Add custom expectations if provided
                if 'custom_expectations' in validation_config:
                    custom_suite = self.validator.create_custom_expectation_suite(
                        f"{suite_name}_custom", 
                        validation_config['custom_expectations']
                    )
                    suite_name = f"{suite_name}_custom"
                
                # Run validation
                validation_results = self.validator.validate_data(data, suite_name)
                results['validation_results'] = validation_results
                logger.info(f"Validation completed. Success: {validation_results['success']}")
            
            # Step 6: Store data
            if self.storage:
                logger.info("Storing data...")
                destination = output_destination or self.pipeline_config.get('storage', {}).get('destination', 'processed_data')
                storage_config = self.pipeline_config.get('storage', {})
                
                success = self.storage.write(
                    data, 
                    destination,
                    if_exists=storage_config.get('mode', 'append'),
                    chunk_size=storage_config.get('batch_size', 10000)
                )
                
                if success:
                    logger.info(f"Data successfully stored to {destination}")
                else:
                    raise Exception("Failed to store data")
            
            results['success'] = True
            logger.info("Pipeline execution completed successfully")
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            results['success'] = False
        
        return results
    
    def _load_data(self, input_source: Optional[str] = None) -> Union[pd.DataFrame, Any]:
        """Load data from configured source."""
        if not self.source:
            raise ValueError("No data source configured")
        
        # Determine input source
        source_path = input_source
        if not source_path and 'input' in self.pipeline_config:
            input_config = self.pipeline_config['input']
            source_path = input_config.get('file_pattern') or input_config.get('path')
        
        if not source_path:
            raise ValueError("No input source specified")
        
        # Load data with any additional parameters
        input_config = self.pipeline_config.get('input', {})
        load_params = {k: v for k, v in input_config.items() if k not in ['file_pattern', 'path']}
        
        return self.source.read(source_path, **load_params)
    
    def dry_run(self, input_source: Optional[str] = None) -> Dict[str, Any]:
        """Perform a dry run to validate pipeline configuration."""
        logger.info("Performing pipeline dry run...")
        
        dry_run_results = {
            'config_valid': False,
            'components_initialized': False,
            'data_accessible': False,
            'estimated_operations': 0,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Validate configuration
            required_sections = ['source', 'processing', 'storage']
            missing_sections = [section for section in required_sections if section not in self.pipeline_config]
            
            if missing_sections:
                dry_run_results['warnings'].append(f"Missing configuration sections: {missing_sections}")
            else:
                dry_run_results['config_valid'] = True
            
            # Check if components can be initialized
            self._setup_components()
            
            if all([self.source, self.processor, self.storage]):
                dry_run_results['components_initialized'] = True
            else:
                missing_components = []
                if not self.source:
                    missing_components.append('source')
                if not self.processor:
                    missing_components.append('processor')
                if not self.storage:
                    missing_components.append('storage')
                dry_run_results['warnings'].append(f"Failed to initialize components: {missing_components}")
            
            # Test data access (read first few rows)
            try:
                if self.source:
                    # Try to access data source
                    available_sources = self.source.list_sources()
                    if available_sources:
                        dry_run_results['data_accessible'] = True
                        logger.info(f"Found {len(available_sources)} available data sources")
                    else:
                        dry_run_results['warnings'].append("No data sources found")
            except Exception as e:
                dry_run_results['errors'].append(f"Data access test failed: {str(e)}")
            
            # Count operations
            if 'processing' in self.pipeline_config:
                operations = self.pipeline_config['processing'].get('operations', [])
                dry_run_results['estimated_operations'] = len(operations)
            
            logger.info("Dry run completed")
            
        except Exception as e:
            error_msg = f"Dry run failed: {str(e)}"
            logger.error(error_msg)
            dry_run_results['errors'].append(error_msg)
        
        return dry_run_results
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get information about the pipeline configuration."""
        return {
            'name': self.pipeline_config.get('name', 'Unnamed Pipeline'),
            'description': self.pipeline_config.get('description', 'No description'),
            'source_type': self.pipeline_config.get('source', {}).get('type'),
            'processing_engine': self.pipeline_config.get('processing', {}).get('engine'),
            'storage_type': self.pipeline_config.get('storage', {}).get('type'),
            'validation_enabled': self.pipeline_config.get('validation', {}).get('enabled', False),
            'profiling_enabled': self.pipeline_config.get('profiling', {}).get('enabled', False),
            'total_operations': len(self.pipeline_config.get('processing', {}).get('operations', [])),
            'components_initialized': all([self.source, self.processor, self.storage])
        }