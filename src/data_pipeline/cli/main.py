"""Main CLI entry point for the data pipeline framework.

This module provides the main command-line interface for the data pipeline framework,
including subcommands for pipeline operations, queue management, data quality checks,
lineage tracking, and various utilities.

The CLI is built using Click and provides rich terminal output for better user experience.
Commands are organized into logical groups for different aspects of data pipeline management.
"""

import click
import json
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from data_pipeline.core.pipeline import Pipeline
from data_pipeline.core.config import Config
from data_pipeline.core.queue_manager import FileQueueManager, QueueItemStatus
from data_pipeline.utils import DataProfiler, SchemaInferrer


@click.group()
@click.version_option(version='0.1.0')
def main():
    """Data Pipeline CLI - A flexible data processing pipeline."""
    pass


@main.command()
@click.argument('pipeline_config', type=click.Path(exists=True))
@click.option('--input-source', '-i', help='Input source path (overrides config)')
@click.option('--output-destination', '-o', help='Output destination (overrides config)')
@click.option('--dry-run', is_flag=True, help='Perform dry run without executing')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def run(pipeline_config: str, input_source: Optional[str], output_destination: Optional[str], 
        dry_run: bool, verbose: bool):
    """Run a data pipeline from configuration file."""
    import logging
    
    # Setup logging
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Initialize pipeline
        pipeline = Pipeline(pipeline_config).from_yaml(pipeline_config)
        
        if dry_run:
            click.echo("Performing dry run...")
            results = pipeline.dry_run(input_source)
            
            click.echo("\n=== Dry Run Results ===")
            click.echo(f"Config Valid: {results['config_valid']}")
            click.echo(f"Components Initialized: {results['components_initialized']}")
            click.echo(f"Data Accessible: {results['data_accessible']}")
            click.echo(f"Estimated Operations: {results['estimated_operations']}")
            
            if results['warnings']:
                click.echo("\nWarnings:")
                for warning in results['warnings']:
                    click.echo(f"  - {warning}")
            
            if results['errors']:
                click.echo("\nErrors:")
                for error in results['errors']:
                    click.echo(f"  - {error}")
        else:
            # Run pipeline
            click.echo(f"Running pipeline: {Path(pipeline_config).name}")
            results = pipeline.run(input_source, output_destination)
            
            click.echo("\n=== Pipeline Results ===")
            click.echo(f"Success: {results['success']}")
            click.echo(f"Rows Processed: {results['rows_processed']}")
            
            if results['validation_results']:
                val_results = results['validation_results']
                click.echo(f"Validation Success: {val_results['success']}")
                click.echo(f"Success Rate: {val_results['statistics']['success_percent']:.1f}%")
            
            if results['errors']:
                click.echo("\nErrors:")
                for error in results['errors']:
                    click.echo(f"  - {error}")
                    
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@main.command()
@click.argument('data_path', type=click.Path(exists=True))
@click.option('--source-type', '-t', type=click.Choice(['csv', 'json', 's3']), default='csv',
              help='Data source type')
@click.option('--output', '-o', type=click.Path(), help='Output file for profile report')
@click.option('--format', 'output_format', type=click.Choice(['html', 'json']), default='html',
              help='Output format')
@click.option('--sample-size', type=int, default=100000, help='Sample size for large datasets')
def profile(data_path: str, source_type: str, output: Optional[str], 
           output_format: str, sample_size: int):
    """Profile a dataset and generate analysis report."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        from data_pipeline.sources import CSVSource, JSONSource
        
        # Initialize appropriate source
        if source_type == 'csv':
            source = CSVSource({'base_path': str(Path(data_path).parent)})
            data = source.read(Path(data_path).name)
        elif source_type == 'json':
            source = JSONSource({'base_path': str(Path(data_path).parent)})
            data = source.read(Path(data_path).name)
        else:
            click.echo(f"Source type {source_type} not supported for local profiling", err=True)
            return
        
        # Profile data
        profiler = DataProfiler({'sample_size': sample_size})
        profile = profiler.profile_dataset(data, Path(data_path).name)
        
        # Output results
        if output_format == 'html':
            output_path = output or f"{Path(data_path).stem}_profile.html"
            profiler.generate_html_report(profile, output_path)
            click.echo(f"HTML profile report saved to: {output_path}")
        else:
            output_path = output or f"{Path(data_path).stem}_profile.json"
            with open(output_path, 'w') as f:
                json.dump(profile, f, indent=2, default=str)
            click.echo(f"JSON profile report saved to: {output_path}")
        
        # Display summary
        click.echo(f"\nDataset: {profile['dataset_info']['name']}")
        click.echo(f"Rows: {profile['dataset_info']['total_rows']:,}")
        click.echo(f"Columns: {profile['dataset_info']['total_columns']}")
        click.echo(f"Quality Score: {profile['data_quality']['overall_score']:.1f}%")
        
    except Exception as e:
        click.echo(f"Error profiling data: {str(e)}", err=True)
        sys.exit(1)


@main.command()
@click.argument('data_path', type=click.Path(exists=True))
@click.option('--source-type', '-t', type=click.Choice(['csv', 'json']), default='csv',
              help='Data source type')
@click.option('--table-name', help='Target table name')
@click.option('--output', '-o', type=click.Path(), help='Output file for schema')
def infer_schema(data_path: str, source_type: str, table_name: Optional[str], output: Optional[str]):
    """Infer schema from a dataset."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        from data_pipeline.sources import CSVSource, JSONSource
        
        # Load data
        if source_type == 'csv':
            source = CSVSource({'base_path': str(Path(data_path).parent)})
            data = source.read(Path(data_path).name)
        elif source_type == 'json':
            source = JSONSource({'base_path': str(Path(data_path).parent)})
            data = source.read(Path(data_path).name)
        
        # Infer schema
        inferrer = SchemaInferrer()
        schema = inferrer.infer_schema(data, table_name or Path(data_path).stem)
        
        # Output schema
        if output:
            with open(output, 'w') as f:
                json.dump(schema, f, indent=2, default=str)
            click.echo(f"Schema saved to: {output}")
        
        # Display summary
        click.echo(f"\nTable: {schema['table_name']}")
        click.echo(f"Columns: {schema['total_columns']}")
        
        click.echo("\nColumn Details:")
        for col_name, col_info in schema['columns'].items():
            click.echo(f"  {col_name}: {col_info['inferred_sql_type']} "
                      f"(nullable: {col_info['nullable']}, quality: {col_info['data_quality_score']:.1f}%)")
        
        if schema['primary_key_candidates']:
            click.echo(f"\nPrimary Key Candidates: {', '.join(schema['primary_key_candidates'])}")
        
        if schema['indexes_recommended']:
            click.echo(f"\nRecommended Indexes: {len(schema['indexes_recommended'])}")
            for idx in schema['indexes_recommended'][:3]:  # Show first 3
                click.echo(f"  - {idx['columns']} ({idx['reason']})")
        
    except Exception as e:
        click.echo(f"Error inferring schema: {str(e)}", err=True)
        sys.exit(1)


@main.command()
@click.option('--name', prompt='Pipeline name', help='Name of the pipeline')
@click.option('--source-type', type=click.Choice(['csv', 'json', 's3']), 
              prompt='Source type', help='Data source type')
@click.option('--processing-engine', type=click.Choice(['pandas', 'spark']), 
              default='pandas', help='Processing engine')
@click.option('--output', '-o', type=click.Path(), help='Output configuration file')
def init(name: str, source_type: str, processing_engine: str, output: Optional[str]):
    """Initialize a new pipeline configuration."""
    
    # Create basic pipeline configuration
    config = {
        'name': name,
        'description': f'Pipeline for processing {source_type} data',
        'source': {
            'type': source_type,
            'config': {}
        },
        'processing': {
            'engine': processing_engine,
            'operations': [
                {
                    'type': 'clean',
                    'params': {
                        'operations': ['remove_empty_rows', 'trim_strings']
                    }
                }
            ]
        },
        'storage': {
            'type': 'postgresql',
            'destination': f'{name.lower()}_data',
            'mode': 'append'
        },
        'validation': {
            'enabled': True,
            'suite_name': f'{name.lower()}_validation',
            'auto_generate_expectations': True
        },
        'profiling': {
            'enabled': True,
            'generate_report': True,
            'output_path': f'./reports/{name.lower()}_profile.html'
        }
    }
    
    # Add source-specific configuration
    if source_type == 'csv':
        config['source']['config'] = {
            'base_path': './data/raw',
            'encoding': 'utf-8',
            'delimiter': ','
        }
        config['input'] = {
            'file_pattern': '*.csv'
        }
    elif source_type == 'json':
        config['source']['config'] = {
            'base_path': './data/raw',
            'encoding': 'utf-8',
            'lines_format': False
        }
        config['input'] = {
            'file_pattern': '*.json'
        }
    elif source_type == 's3':
        config['source']['config'] = {
            'bucket': 'your-s3-bucket',
            'region': 'us-east-1',
            'prefix': 'raw-data'
        }
        config['input'] = {
            'file_pattern': '*.json'
        }
    
    # Save configuration
    output_path = output or f'{name.lower()}_pipeline.yaml'
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)
    
    click.echo(f"Pipeline configuration created: {output_path}")
    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {output_path} to customize your pipeline")
    click.echo(f"2. Run: data-pipeline run {output_path}")


@main.command()
@click.argument('pipeline_config', type=click.Path(exists=True))
def info(pipeline_config: str):
    """Display information about a pipeline configuration."""
    try:
        pipeline = Pipeline(pipeline_config).from_yaml(pipeline_config)
        info_data = pipeline.get_pipeline_info()
        
        click.echo(f"=== Pipeline Information ===")
        click.echo(f"Name: {info_data['name']}")
        click.echo(f"Description: {info_data['description']}")
        click.echo(f"Source Type: {info_data['source_type']}")
        click.echo(f"Processing Engine: {info_data['processing_engine']}")
        click.echo(f"Storage Type: {info_data['storage_type']}")
        click.echo(f"Total Operations: {info_data['total_operations']}")
        click.echo(f"Validation Enabled: {info_data['validation_enabled']}")
        click.echo(f"Profiling Enabled: {info_data['profiling_enabled']}")
        click.echo(f"Components Initialized: {info_data['components_initialized']}")
        
    except Exception as e:
        click.echo(f"Error reading pipeline info: {str(e)}", err=True)
        sys.exit(1)


@main.group()
def queue():
    """Manage the file ingestion queue."""
    pass


@queue.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('table_name')
@click.option('--source-type', '-t', type=click.Choice(['csv', 'json', 's3']), default='csv',
              help='Data source type')
@click.option('--priority', type=int, default=0, help='Priority level (higher = processed first)')
@click.option('--delete-after', is_flag=True, help='Delete source file after successful ingestion')
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
@click.option('--config', type=click.Path(exists=True), help='Pipeline configuration file')
def add(file_path: str, table_name: str, source_type: str, priority: int, 
        delete_after: bool, queue_db: str, config: Optional[str]):
    """Add a file to the ingestion queue."""
    try:
        queue_manager = FileQueueManager(queue_db)
        
        # Load pipeline config if provided
        pipeline_config = None
        if config:
            with open(config, 'r') as f:
                pipeline_config = yaml.safe_load(f)
        
        # Add metadata
        metadata = {'delete_after_ingestion': delete_after}
        
        item_id = queue_manager.add_to_queue(
            file_path=file_path,
            source_type=source_type,
            destination_table=table_name,
            pipeline_config=pipeline_config,
            priority=priority,
            metadata=metadata
        )
        
        click.echo(f"Added file to queue: {file_path}")
        click.echo(f"Queue item ID: {item_id}")
        click.echo(f"Destination table: {table_name}")
        
    except Exception as e:
        click.echo(f"Error adding file to queue: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.option('--status', type=click.Choice(['pending', 'processing', 'completed', 'failed']),
              help='Filter by status')
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
def list(status: Optional[str], queue_db: str):
    """List items in the queue."""
    try:
        queue_manager = FileQueueManager(queue_db)
        
        status_filter = QueueItemStatus(status) if status else None
        items = queue_manager.list_queue_items(status_filter)
        
        if not items:
            click.echo("Queue is empty")
            return
        
        # Display items
        click.echo(f"{'ID':<8} {'Status':<10} {'Priority':<8} {'File':<40} {'Table':<20} {'Created'}")
        click.echo("-" * 110)
        
        for item in items:
            created = item.created_at.strftime("%m-%d %H:%M")
            file_display = item.file_path[-35:] if len(item.file_path) > 35 else item.file_path
            table_display = item.destination_table[:17] if len(item.destination_table) > 17 else item.destination_table
            
            click.echo(f"{item.id[:8]:<8} {item.status.value:<10} {item.priority:<8} "
                      f"{file_display:<40} {table_display:<20} {created}")
            
            if item.error_message:
                click.echo(f"         Error: {item.error_message}")
        
    except Exception as e:
        click.echo(f"Error listing queue: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
def stats(queue_db: str):
    """Show queue statistics."""
    try:
        queue_manager = FileQueueManager(queue_db)
        stats = queue_manager.get_queue_stats()
        
        click.echo("=== Queue Statistics ===")
        click.echo(f"Total items: {stats['total']}")
        click.echo(f"Pending: {stats['pending']}")
        click.echo(f"Processing: {stats['processing']}")
        click.echo(f"Completed: {stats['completed']}")
        click.echo(f"Failed: {stats['failed']}")
        
    except Exception as e:
        click.echo(f"Error getting queue stats: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.argument('item_id')
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
def remove(item_id: str, queue_db: str):
    """Remove an item from the queue."""
    try:
        queue_manager = FileQueueManager(queue_db)
        queue_manager.remove_item(item_id)
        click.echo(f"Removed item: {item_id}")
        
    except Exception as e:
        click.echo(f"Error removing item: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.option('--keep-failed', is_flag=True, default=True, help='Keep failed items')
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
def clear_completed(keep_failed: bool, queue_db: str):
    """Clear completed items from the queue."""
    try:
        queue_manager = FileQueueManager(queue_db)
        queue_manager.clear_completed(keep_failed)
        click.echo("Cleared completed items from queue")
        
    except Exception as e:
        click.echo(f"Error clearing queue: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
def retry_failed(queue_db: str):
    """Reset failed items to pending for retry."""
    try:
        queue_manager = FileQueueManager(queue_db)
        queue_manager.reset_failed_items()
        click.echo("Reset failed items to pending status")
        
    except Exception as e:
        click.echo(f"Error retrying failed items: {str(e)}", err=True)
        sys.exit(1)


@queue.command()
@click.option('--queue-db', default='data/queue.db', help='Path to queue database')
@click.option('--config', type=click.Path(exists=True), help='Pipeline configuration file')
@click.option('--max-items', type=int, help='Maximum items to process')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def process(queue_db: str, config: Optional[str], max_items: Optional[int], verbose: bool):
    """Process items from the queue (batch mode)."""
    import subprocess
    import sys
    
    # Build command for queue processor script
    cmd = [sys.executable, 'scripts/queue_processor.py', '--mode', 'batch']
    cmd.extend(['--queue-db', queue_db])
    
    if config:
        cmd.extend(['--config', config])
    if max_items:
        cmd.extend(['--max-items', str(max_items)])
    if verbose:
        cmd.append('--verbose')
    
    try:
        # Run the queue processor
        result = subprocess.run(cmd, check=True)
        click.echo("Queue processing completed")
        
    except subprocess.CalledProcessError as e:
        click.echo(f"Error processing queue: {e}", err=True)
        sys.exit(1)
    except FileNotFoundError:
        click.echo("Queue processor script not found. Make sure scripts/queue_processor.py exists.", err=True)
        sys.exit(1)


@main.group()
def generate():
    """Generate analysis code templates for database exploration."""
    pass


@generate.command()
@click.argument('table_name')
@click.option('--language', '-l', type=click.Choice(['python', 'scala']), default='python',
              help='Programming language for the generated code')
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.option('--config', '-c', type=click.Path(exists=True), 
              default='config/environments/development.yaml',
              help='Configuration file to read database settings from')
@click.option('--all-tables', is_flag=True, help='Generate analysis code for all tables')
@click.option('--output-dir', type=click.Path(), help='Output directory (for --all-tables)')
def analysis(table_name: str, language: str, output: Optional[str], 
            config: str, all_tables: bool, output_dir: Optional[str]):
    """Generate analysis code template for database tables."""
    try:
        from data_pipeline.storage.postgresql_storage import PostgreSQLStorage
        from data_pipeline.core.config import Config
        
        # Load configuration
        pipeline_config = Config(config)
        db_config = pipeline_config.get('database', {})
        
        # Initialize storage to get table info
        storage = PostgreSQLStorage(db_config)
        
        if all_tables:
            if not output_dir:
                output_dir = './analysis'
            
            # Create output directory
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Get all tables
            tables = storage.list_tables()
            
            if not tables:
                click.echo("No tables found in database")
                return
            
            click.echo(f"Generating analysis code for {len(tables)} tables...")
            
            for table in tables:
                table_info = storage.get_table_info(table)
                output_file = Path(output_dir) / f"analysis_{table}.{_get_file_extension(language)}"
                
                code = _generate_analysis_code(table, table_info, language, db_config)
                
                with open(output_file, 'w') as f:
                    f.write(code)
                
                click.echo(f"Generated: {output_file}")
        
        else:
            # Single table
            table_info = storage.get_table_info(table_name)
            
            # Generate output filename if not provided
            if not output:
                output = f"analysis_{table_name}.{_get_file_extension(language)}"
            
            # Generate code
            code = _generate_analysis_code(table_name, table_info, language, db_config)
            
            # Write to file
            with open(output, 'w') as f:
                f.write(code)
            
            click.echo(f"Generated analysis code: {output}")
            click.echo(f"Language: {language.title()}")
            click.echo(f"Table: {table_name}")
            click.echo(f"Columns: {len(table_info.get('columns', []))}")
        
    except Exception as e:
        click.echo(f"Error generating analysis code: {str(e)}", err=True)
        sys.exit(1)


def _get_file_extension(language: str) -> str:
    """Get file extension for language."""
    return {'python': 'py', 'scala': 'scala'}[language]


def _generate_analysis_code(table_name: str, table_info: Dict[str, Any], 
                          language: str, db_config: Dict[str, Any]) -> str:
    """Generate analysis code for a table."""
    if language == 'python':
        return _generate_python_code(table_name, table_info, db_config)
    elif language == 'scala':
        return _generate_scala_code(table_name, table_info, db_config)
    else:
        raise ValueError(f"Unsupported language: {language}")


def _generate_python_code(table_name: str, table_info: Dict[str, Any], 
                         db_config: Dict[str, Any]) -> str:
    """Generate Python analysis code template."""
    # Extract database connection details
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    database = db_config.get('database', 'data_warehouse')
    username = db_config.get('username', 'postgres')
    
    # Get column information
    columns = table_info.get('columns', [])
    column_names = [col['name'] for col in columns] if columns else []
    column_info_str = '\n'.join([
        f"#   {col['name']}: {col['data_type']}" + 
        (f" (nullable: {col['is_nullable']})" if 'is_nullable' in col else "")
        for col in columns
    ]) if columns else "#   No column information available"
    
    estimated_rows = table_info.get('size_info', {}).get('estimated_rows', 'unknown')
    
    template = f"""# Auto-generated analysis starter for table: {table_name}
# Generated by Data Pipeline Framework
#
# Table Information:
# - Estimated rows: {estimated_rows}
# - Columns ({len(column_names)}):
{column_info_str}

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Database connection (from your pipeline config)
# Make sure DB_PASSWORD environment variable is set
engine = create_engine(
    f"postgresql://{username}:" + "{os.environ.get('DB_PASSWORD', '')}" +
    f"@{host}:{port}/{database}"
)

# Ready-to-use data access functions
def load_{table_name}(limit=None, where=None, columns=None):
    \"\"\"Load data from {table_name} table with optional filtering.
    
    Args:
        limit (int): Maximum number of rows to return
        where (str): SQL WHERE clause (without 'WHERE' keyword)
        columns (list): List of column names to select
    
    Returns:
        pd.DataFrame: Query results
    \"\"\"
    if columns:
        cols_str = ", ".join([f'"{{col}}"' for col in columns])
        base_query = f"SELECT {{cols_str}} FROM {table_name}"
    else:
        base_query = f"SELECT * FROM {table_name}"
    
    if where:
        base_query += f" WHERE {{where}}"
    if limit:
        base_query += f" LIMIT {{limit}}"
    
    return pd.read_sql(base_query, engine)

def get_{table_name}_summary():
    \"\"\"Get basic summary statistics for {table_name} table.\"\"\"
    query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
    result = pd.read_sql(query, engine)
    return result.iloc[0]['total_rows']

# Quick data preview
try:
    print(f"=== {table_name.upper()} TABLE ANALYSIS ===")
    
    # Get total row count
    total_rows = get_{table_name}_summary()
    print(f"Total rows: {{total_rows:,}}")
    
    # Load sample data
    df_sample = load_{table_name}(limit=100)
    print(f"Sample data shape: {{df_sample.shape}}")
    print("\\nColumns:", df_sample.columns.tolist())
    print("\\nData types:")
    print(df_sample.dtypes)
    print("\\nFirst 5 rows:")
    print(df_sample.head())
    
    # Basic statistics for numeric columns
    numeric_cols = df_sample.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print("\\nNumeric column statistics:")
        print(df_sample[numeric_cols].describe())
    
    print("\\n=== READY FOR ANALYSIS ===")
    print("# Example usage:")
    print("# df_full = load_{table_name}()")
    print("# df_recent = load_{table_name}(where=\\"created_at > '2024-01-01'\\")")
    print("# df_subset = load_{table_name}(columns=['id', 'name'], limit=1000)")
    
except Exception as e:
    print(f"Error connecting to database: {{e}}")
    print("Make sure:")
    print("1. Database is running")
    print("2. DB_PASSWORD environment variable is set")
    print("3. Connection details are correct")
"""
    
    return template


def _generate_scala_code(table_name: str, table_info: Dict[str, Any], 
                        db_config: Dict[str, Any]) -> str:
    """Generate Scala analysis code template."""
    # Extract database connection details
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    database = db_config.get('database', 'data_warehouse')
    username = db_config.get('username', 'postgres')
    
    # Get column information
    columns = table_info.get('columns', [])
    column_names = [col['name'] for col in columns] if columns else []
    column_info_str = '\n'.join([
        f"//   {col['name']}: {col['data_type']}" + 
        (f" (nullable: {col['is_nullable']})" if 'is_nullable' in col else "")
        for col in columns
    ]) if columns else "//   No column information available"
    
    estimated_rows = table_info.get('size_info', {}).get('estimated_rows', 'unknown')
    
    template = f"""// Auto-generated analysis starter for table: {table_name}  
// Generated by Data Pipeline Framework
//
// Table Information:
// - Estimated rows: {estimated_rows}
// - Columns ({len(column_names)}):
{column_info_str}

import org.apache.spark.sql.{{SparkSession, DataFrame}}
import scala.util.{{Try, Success, Failure}}

object {table_name.title()}Analysis {{
  
  // Initialize Spark Session
  val spark = SparkSession.builder()
    .appName("{table_name.title()}Analysis")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()

  import spark.implicits._

  // Database connection (from your pipeline config)
  val jdbcUrl = "jdbc:postgresql://{host}:{port}/{database}"
  val connectionProps = new java.util.Properties()
  connectionProps.put("user", "{username}")
  connectionProps.put("password", sys.env.getOrElse("DB_PASSWORD", ""))
  connectionProps.put("driver", "org.postgresql.Driver")

  // Ready-to-use data access functions
  def load{table_name.title()}(limit: Option[Int] = None, where: Option[String] = None): Try[DataFrame] = {{
    Try {{
      var df = spark.read.jdbc(jdbcUrl, "{table_name}", connectionProps)
      
      where.foreach(condition => df = df.where(condition))
      limit.foreach(n => df = df.limit(n))
      
      df
    }}
  }}

  def get{table_name.title()}Summary(): Try[Long] = {{
    Try {{
      val df = spark.read.jdbc(jdbcUrl, "{table_name}", connectionProps)
      df.count()
    }}
  }}

  // Analysis execution
  def runAnalysis(): Unit = {{
    println(s"=== {table_name.upper()} TABLE ANALYSIS ===")
    
    // Get total row count
    get{table_name.title()}Summary() match {{
      case Success(totalRows) => 
        println(s"Total rows: ${{totalRows.toString.reverse.grouped(3).mkString(",").reverse}}")
      case Failure(e) => 
        println(s"Error getting row count: ${{e.getMessage}}")
        return
    }}
    
    // Load sample data
    load{table_name.title()}(Some(100)) match {{
      case Success(dfSample) =>
        println(s"Sample data shape: (${{dfSample.count()}} rows, ${{dfSample.columns.length}} columns)")
        println("\\nColumns: " + dfSample.columns.mkString(", "))
        println("\\nSchema:")
        dfSample.printSchema()
        println("\\nFirst 5 rows:")
        dfSample.show(5)
        
        // Basic statistics for numeric columns
        val numericCols = dfSample.dtypes.filter(_._2.matches(".*int.*|.*double.*|.*float.*|.*decimal.*")).map(_._1)
        if (numericCols.nonEmpty) {{
          println("\\nNumeric column statistics:")
          dfSample.select(numericCols.head, numericCols.tail: _*).describe().show()
        }}
        
        println("\\n=== READY FOR ANALYSIS ===")
        println("// Example usage:")
        println("// val dfFull = load{table_name.title()}().get")
        println("// val dfRecent = load{table_name.title()}(where = Some(\\"created_at > '2024-01-01'\\")).get")
        println("// val dfLimited = load{table_name.title()}(limit = Some(1000)).get")
        
      case Failure(e) =>
        println(s"Error connecting to database: ${{e.getMessage}}")
        println("Make sure:")
        println("1. Database is running")
        println("2. DB_PASSWORD environment variable is set") 
        println("3. Connection details are correct")
        println("4. PostgreSQL JDBC driver is in classpath")
    }}
  }}

  def main(args: Array[String]): Unit = {{
    runAnalysis()
    spark.stop()
  }}
}}
"""
    
    return template


if __name__ == '__main__':
    main()