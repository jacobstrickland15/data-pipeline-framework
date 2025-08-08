"""Command-line interface for the data pipeline."""

import click
import json
import yaml
from pathlib import Path
from typing import Optional
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


if __name__ == '__main__':
    main()