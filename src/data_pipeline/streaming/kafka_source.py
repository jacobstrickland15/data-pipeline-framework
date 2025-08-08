"""Kafka source for streaming data ingestion."""

import json
import logging
from typing import Dict, List, Optional, Any, Iterator, Callable
from datetime import datetime
import threading
import queue
import time

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

import pandas as pd

logger = logging.getLogger(__name__)


class KafkaSource:
    """Kafka source for streaming data ingestion."""
    
    def __init__(
        self,
        bootstrap_servers: List[str],
        topics: List[str],
        group_id: str = "data-pipeline-group",
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        max_poll_records: int = 100,
        **kwargs
    ):
        """Initialize Kafka source.
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            auto_offset_reset: Auto offset reset policy
            enable_auto_commit: Whether to auto commit offsets
            max_poll_records: Maximum number of records per poll
            **kwargs: Additional KafkaConsumer parameters
        """
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is required for KafkaSource. Install with: pip install kafka-python")
        
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.consumer_config = {
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': enable_auto_commit,
            'max_poll_records': max_poll_records,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            'key_deserializer': lambda x: x.decode('utf-8') if x else None,
            **kwargs
        }
        
        self.consumer = None
        self.is_running = False
        self._stop_event = threading.Event()
        self._message_queue = queue.Queue()
        self._consumer_thread = None
    
    def start(self):
        """Start the Kafka consumer."""
        try:
            self.consumer = KafkaConsumer(**self.consumer_config)
            self.consumer.subscribe(self.topics)
            self.is_running = True
            self._stop_event.clear()
            
            # Start consumer thread
            self._consumer_thread = threading.Thread(target=self._consume_messages)
            self._consumer_thread.daemon = True
            self._consumer_thread.start()
            
            logger.info(f"Started Kafka consumer for topics: {self.topics}")
            
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            raise
    
    def stop(self):
        """Stop the Kafka consumer."""
        self.is_running = False
        self._stop_event.set()
        
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)
        
        if self.consumer:
            self.consumer.close()
        
        logger.info("Stopped Kafka consumer")
    
    def _consume_messages(self):
        """Consume messages from Kafka in a separate thread."""
        while self.is_running and not self._stop_event.is_set():
            try:
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if self._stop_event.is_set():
                            break
                        
                        try:
                            processed_message = self._process_message(message)
                            self._message_queue.put(processed_message)
                            
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
                
            except KafkaError as e:
                logger.error(f"Kafka consumer error: {e}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}")
                time.sleep(1)
    
    def _process_message(self, message) -> Dict[str, Any]:
        """Process a single Kafka message."""
        return {
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else datetime.now(),
            'key': message.key,
            'value': message.value,
            'headers': dict(message.headers) if message.headers else {}
        }
    
    def read_batch(self, batch_size: int = 100, timeout: int = 10) -> List[Dict[str, Any]]:
        """Read a batch of messages.
        
        Args:
            batch_size: Maximum number of messages to return
            timeout: Timeout in seconds
            
        Returns:
            List of messages
        """
        messages = []
        start_time = time.time()
        
        while len(messages) < batch_size and (time.time() - start_time) < timeout:
            try:
                message = self._message_queue.get(timeout=1)
                messages.append(message)
            except queue.Empty:
                continue
        
        return messages
    
    def read_stream(self, batch_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
        """Read messages as a continuous stream.
        
        Args:
            batch_size: Size of each batch
            
        Yields:
            Batches of messages
        """
        while self.is_running:
            batch = self.read_batch(batch_size, timeout=5)
            if batch:
                yield batch
            else:
                time.sleep(0.1)  # Brief pause if no messages
    
    def to_dataframe(self, messages: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert messages to pandas DataFrame.
        
        Args:
            messages: List of processed messages
            
        Returns:
            DataFrame containing message data
        """
        if not messages:
            return pd.DataFrame()
        
        # Extract message values and metadata
        records = []
        for msg in messages:
            record = {
                'kafka_topic': msg['topic'],
                'kafka_partition': msg['partition'],
                'kafka_offset': msg['offset'],
                'kafka_timestamp': msg['timestamp'],
                'kafka_key': msg['key']
            }
            
            # Flatten the message value
            if isinstance(msg['value'], dict):
                record.update(msg['value'])
            else:
                record['message_value'] = msg['value']
            
            # Add headers as separate columns
            for header_key, header_value in msg['headers'].items():
                record[f'header_{header_key}'] = header_value
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics and statistics."""
        if not self.consumer:
            return {}
        
        try:
            metrics = self.consumer.metrics()
            
            # Extract relevant metrics
            consumer_metrics = {
                'total_records_consumed': 0,
                'total_bytes_consumed': 0,
                'average_consumption_rate': 0,
                'partition_assignments': []
            }
            
            # Process metrics (simplified)
            for metric_name, metric_value in metrics.items():
                if 'records-consumed-total' in str(metric_name):
                    consumer_metrics['total_records_consumed'] += metric_value.value
                elif 'bytes-consumed-total' in str(metric_name):
                    consumer_metrics['total_bytes_consumed'] += metric_value.value
                elif 'records-consumed-rate' in str(metric_name):
                    consumer_metrics['average_consumption_rate'] += metric_value.value
            
            # Get partition assignments
            if hasattr(self.consumer, 'assignment'):
                consumer_metrics['partition_assignments'] = [
                    {'topic': tp.topic, 'partition': tp.partition}
                    for tp in self.consumer.assignment()
                ]
            
            return consumer_metrics
            
        except Exception as e:
            logger.error(f"Failed to get consumer metrics: {e}")
            return {}


class KafkaProducerWrapper:
    """Wrapper for Kafka producer with data pipeline integration."""
    
    def __init__(
        self,
        bootstrap_servers: List[str],
        topic: str,
        **kwargs
    ):
        """Initialize Kafka producer.
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            topic: Topic to produce messages to
            **kwargs: Additional KafkaProducer parameters
        """
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is required. Install with: pip install kafka-python")
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
            'key_serializer': lambda x: str(x).encode('utf-8') if x else None,
            **kwargs
        }
        
        self.producer = KafkaProducer(**producer_config)
    
    def send_message(self, value: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send a single message to Kafka.
        
        Args:
            value: Message value (will be JSON serialized)
            key: Message key (optional)
            
        Returns:
            Boolean indicating success
        """
        try:
            future = self.producer.send(self.topic, value=value, key=key)
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Message sent to {record_metadata.topic}[{record_metadata.partition}]")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    def send_dataframe(self, df: pd.DataFrame, key_column: Optional[str] = None) -> int:
        """Send a DataFrame to Kafka as individual messages.
        
        Args:
            df: DataFrame to send
            key_column: Column to use as message key
            
        Returns:
            Number of successfully sent messages
        """
        sent_count = 0
        
        for _, row in df.iterrows():
            try:
                # Convert row to dictionary
                message = row.to_dict()
                
                # Handle NaN values
                message = {k: (v if pd.notna(v) else None) for k, v in message.items()}
                
                # Extract key if specified
                key = message.pop(key_column) if key_column and key_column in message else None
                
                if self.send_message(message, str(key) if key is not None else None):
                    sent_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to send row: {e}")
                continue
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"Sent {sent_count}/{len(df)} messages to Kafka topic: {self.topic}")
        return sent_count
    
    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")