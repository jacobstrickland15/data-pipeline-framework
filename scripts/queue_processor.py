#!/usr/bin/env python3
"""Queue processor script for automated file ingestion."""

import sys
import time
import signal
import logging
from pathlib import Path
from typing import Optional
import argparse

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_pipeline.core.queue_manager import FileQueueManager, QueueItemStatus
from data_pipeline.core.pipeline import Pipeline
from data_pipeline.core.config import Config


class QueueProcessor:
    """Processes files from the ingestion queue."""
    
    def __init__(self, queue_db_path: str = "data/queue.db", 
                 config_path: Optional[str] = None):
        """Initialize the queue processor.
        
        Args:
            queue_db_path: Path to the queue database
            config_path: Optional path to pipeline configuration
        """
        self.queue_manager = FileQueueManager(queue_db_path)
        self.config_path = config_path
        self.running = False
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('logs/queue_processor.log', mode='a')
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Ensure logs directory exists
        Path('logs').mkdir(exist_ok=True)
    
    def process_single_item(self) -> bool:
        """Process a single item from the queue.
        
        Returns:
            True if an item was processed, False if queue is empty
        """
        item = self.queue_manager.get_next_item()
        if not item:
            return False
        
        self.logger.info(f"Processing queue item: {item.id} ({item.file_path})")
        
        try:
            # Create pipeline configuration
            if item.pipeline_config:
                # Use provided pipeline config
                pipeline_config = item.pipeline_config
            else:
                # Create default configuration
                pipeline_config = self._create_default_config(item)
            
            # Create temporary config file
            temp_config_path = f"temp_config_{item.id}.yaml"
            config = Config(pipeline_config)
            config.save_to_file(temp_config_path)
            
            try:
                # Initialize and run pipeline
                pipeline = Pipeline.from_yaml(temp_config_path)
                
                # Override source path
                results = pipeline.run(input_source=item.file_path)
                
                if results['success']:
                    self.queue_manager.update_status(item.id, QueueItemStatus.COMPLETED)
                    self.logger.info(f"Successfully processed {item.file_path} - "
                                   f"{results['rows_processed']} rows ingested")
                    
                    # Delete source file if configured
                    if item.metadata.get('delete_after_ingestion', False):
                        Path(item.file_path).unlink()
                        self.logger.info(f"Deleted source file: {item.file_path}")
                else:
                    error_msg = "; ".join(results.get('errors', ['Unknown error']))
                    self.queue_manager.update_status(item.id, QueueItemStatus.FAILED, error_msg)
                    self.logger.error(f"Failed to process {item.file_path}: {error_msg}")
                    
            finally:
                # Clean up temporary config file
                Path(temp_config_path).unlink(missing_ok=True)
                
        except Exception as e:
            error_msg = str(e)
            self.queue_manager.update_status(item.id, QueueItemStatus.FAILED, error_msg)
            self.logger.error(f"Error processing {item.file_path}: {error_msg}", exc_info=True)
        
        return True
    
    def _create_default_config(self, item) -> dict:
        """Create default pipeline configuration for queue item."""
        return {
            'name': f'queue_processing_{item.id}',
            'description': f'Auto-generated config for {item.file_path}',
            'source': {
                'type': item.source_type,
                'config': self._get_source_config(item.source_type, item.file_path)
            },
            'processing': {
                'engine': 'pandas',
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
                'destination': item.destination_table,
                'mode': 'append'
            },
            'validation': {
                'enabled': True,
                'auto_generate_expectations': True
            },
            'profiling': {
                'enabled': False  # Disable for automated processing
            }
        }
    
    def _get_source_config(self, source_type: str, file_path: str) -> dict:
        """Get source configuration based on type."""
        if source_type == 'csv':
            return {
                'base_path': str(Path(file_path).parent),
                'encoding': 'utf-8',
                'delimiter': ','
            }
        elif source_type == 'json':
            return {
                'base_path': str(Path(file_path).parent),
                'encoding': 'utf-8',
                'lines_format': file_path.endswith('.jsonl')
            }
        elif source_type == 's3':
            return {
                'bucket': file_path.split('/')[0],
                'region': 'us-east-1'
            }
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def run_continuous(self, poll_interval: int = 30, max_iterations: Optional[int] = None):
        """Run the processor continuously, polling for new items.
        
        Args:
            poll_interval: Seconds to wait between polls
            max_iterations: Maximum number of iterations (None for infinite)
        """
        self.running = True
        iteration = 0
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self.logger.info("Starting continuous queue processing...")
        
        while self.running:
            if max_iterations and iteration >= max_iterations:
                break
                
            try:
                # Process all available items
                items_processed = 0
                while self.process_single_item():
                    items_processed += 1
                    if not self.running:
                        break
                
                if items_processed > 0:
                    self.logger.info(f"Processed {items_processed} items in this cycle")
                
                # Display queue stats periodically
                if iteration % 10 == 0:
                    stats = self.queue_manager.get_queue_stats()
                    self.logger.info(f"Queue stats: {stats}")
                
                # Wait before next poll
                if self.running:
                    self.logger.debug(f"Waiting {poll_interval} seconds before next poll...")
                    time.sleep(poll_interval)
                    
            except Exception as e:
                self.logger.error(f"Error in processing loop: {e}", exc_info=True)
                if self.running:
                    time.sleep(poll_interval)
            
            iteration += 1
        
        self.logger.info("Queue processor stopped")
    
    def run_batch(self, max_items: Optional[int] = None):
        """Process items in batch mode (process available items and exit).
        
        Args:
            max_items: Maximum number of items to process
        """
        self.logger.info("Starting batch processing...")
        
        items_processed = 0
        while True:
            if max_items and items_processed >= max_items:
                break
                
            if not self.process_single_item():
                break  # No more items
                
            items_processed += 1
        
        stats = self.queue_manager.get_queue_stats()
        self.logger.info(f"Batch processing complete. Processed {items_processed} items. "
                        f"Queue stats: {stats}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Process files from the ingestion queue')
    parser.add_argument('--queue-db', default='data/queue.db', 
                       help='Path to queue database')
    parser.add_argument('--config', help='Path to pipeline configuration file')
    parser.add_argument('--mode', choices=['batch', 'continuous'], default='batch',
                       help='Processing mode')
    parser.add_argument('--poll-interval', type=int, default=30,
                       help='Poll interval for continuous mode (seconds)')
    parser.add_argument('--max-items', type=int,
                       help='Maximum items to process (batch mode)')
    parser.add_argument('--max-iterations', type=int,
                       help='Maximum iterations (continuous mode)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        processor = QueueProcessor(args.queue_db, args.config)
        
        if args.mode == 'continuous':
            processor.run_continuous(args.poll_interval, args.max_iterations)
        else:
            processor.run_batch(args.max_items)
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()