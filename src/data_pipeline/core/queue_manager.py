"""File queue management for data pipeline ingestion."""

import json
import uuid
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class QueueItemStatus(Enum):
    """Status of queue items."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class QueueItem:
    """Represents an item in the ingestion queue."""
    id: str
    file_path: str
    source_type: str
    destination_table: str
    pipeline_config: Optional[Dict[str, Any]] = None
    priority: int = 0
    status: QueueItemStatus = QueueItemStatus.PENDING
    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class FileQueueManager:
    """Manages a queue of files to be ingested into the database."""
    
    def __init__(self, queue_db_path: str = "data/queue.db"):
        """Initialize the queue manager.
        
        Args:
            queue_db_path: Path to SQLite database for queue storage
        """
        self.queue_db_path = Path(queue_db_path)
        self.queue_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database for queue management."""
        with sqlite3.connect(self.queue_db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue_items (
                    id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    destination_table TEXT NOT NULL,
                    pipeline_config TEXT,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    metadata TEXT
                )
            """)
            
            # Create index for efficient querying
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_status_priority 
                ON queue_items(status, priority DESC, created_at ASC)
            """)
            
            conn.commit()
    
    def add_to_queue(self, 
                    file_path: str,
                    source_type: str,
                    destination_table: str,
                    pipeline_config: Optional[Dict[str, Any]] = None,
                    priority: int = 0,
                    metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add a file to the ingestion queue.
        
        Args:
            file_path: Path to the file to be ingested
            source_type: Type of source (csv, json, s3)
            destination_table: Target table name
            pipeline_config: Optional pipeline configuration
            priority: Priority level (higher = processed first)
            metadata: Additional metadata
            
        Returns:
            Queue item ID
        """
        # Validate file exists
        if not source_type == 's3' and not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        item = QueueItem(
            id=str(uuid.uuid4()),
            file_path=file_path,
            source_type=source_type,
            destination_table=destination_table,
            pipeline_config=pipeline_config,
            priority=priority,
            metadata=metadata or {}
        )
        
        with sqlite3.connect(self.queue_db_path) as conn:
            conn.execute("""
                INSERT INTO queue_items 
                (id, file_path, source_type, destination_table, pipeline_config, 
                 priority, status, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item.id,
                item.file_path,
                item.source_type,
                item.destination_table,
                json.dumps(item.pipeline_config) if item.pipeline_config else None,
                item.priority,
                item.status.value,
                item.created_at.isoformat(),
                json.dumps(item.metadata)
            ))
            conn.commit()
        
        logger.info(f"Added file to queue: {file_path} -> {destination_table} (ID: {item.id})")
        return item.id
    
    def get_next_item(self) -> Optional[QueueItem]:
        """Get the next item to process from the queue.
        
        Returns:
            Next queue item or None if queue is empty
        """
        with sqlite3.connect(self.queue_db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM queue_items 
                WHERE status = 'pending'
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Convert row to QueueItem
            item = self._row_to_queue_item(row)
            
            # Mark as processing
            self.update_status(item.id, QueueItemStatus.PROCESSING)
            item.status = QueueItemStatus.PROCESSING
            
            return item
    
    def update_status(self, item_id: str, status: QueueItemStatus, 
                     error_message: Optional[str] = None):
        """Update the status of a queue item.
        
        Args:
            item_id: Queue item ID
            status: New status
            error_message: Error message if status is FAILED
        """
        timestamp_field = None
        timestamp_value = None
        
        if status == QueueItemStatus.PROCESSING:
            timestamp_field = "started_at"
            timestamp_value = datetime.now().isoformat()
        elif status in [QueueItemStatus.COMPLETED, QueueItemStatus.FAILED]:
            timestamp_field = "completed_at"
            timestamp_value = datetime.now().isoformat()
        
        with sqlite3.connect(self.queue_db_path) as conn:
            if timestamp_field:
                conn.execute(f"""
                    UPDATE queue_items 
                    SET status = ?, {timestamp_field} = ?, error_message = ?
                    WHERE id = ?
                """, (status.value, timestamp_value, error_message, item_id))
            else:
                conn.execute("""
                    UPDATE queue_items 
                    SET status = ?, error_message = ?
                    WHERE id = ?
                """, (status.value, error_message, item_id))
            
            conn.commit()
        
        logger.info(f"Updated queue item {item_id} status to {status.value}")
    
    def remove_item(self, item_id: str):
        """Remove an item from the queue.
        
        Args:
            item_id: Queue item ID
        """
        with sqlite3.connect(self.queue_db_path) as conn:
            conn.execute("DELETE FROM queue_items WHERE id = ?", (item_id,))
            conn.commit()
        
        logger.info(f"Removed queue item: {item_id}")
    
    def list_queue_items(self, status: Optional[QueueItemStatus] = None) -> List[QueueItem]:
        """List queue items with optional status filter.
        
        Args:
            status: Optional status filter
            
        Returns:
            List of queue items
        """
        with sqlite3.connect(self.queue_db_path) as conn:
            if status:
                cursor = conn.execute("""
                    SELECT * FROM queue_items 
                    WHERE status = ?
                    ORDER BY priority DESC, created_at ASC
                """, (status.value,))
            else:
                cursor = conn.execute("""
                    SELECT * FROM queue_items 
                    ORDER BY priority DESC, created_at ASC
                """)
            
            return [self._row_to_queue_item(row) for row in cursor.fetchall()]
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get statistics about the queue.
        
        Returns:
            Dictionary with queue statistics
        """
        with sqlite3.connect(self.queue_db_path) as conn:
            stats = {}
            for status in QueueItemStatus:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM queue_items WHERE status = ?",
                    (status.value,)
                )
                stats[status.value] = cursor.fetchone()[0]
            
            # Total count
            cursor = conn.execute("SELECT COUNT(*) FROM queue_items")
            stats['total'] = cursor.fetchone()[0]
            
            return stats
    
    def clear_completed(self, keep_failed: bool = True):
        """Clear completed items from the queue.
        
        Args:
            keep_failed: Whether to keep failed items for review
        """
        with sqlite3.connect(self.queue_db_path) as conn:
            if keep_failed:
                conn.execute("DELETE FROM queue_items WHERE status = 'completed'")
            else:
                conn.execute("""
                    DELETE FROM queue_items 
                    WHERE status IN ('completed', 'failed')
                """)
            conn.commit()
        
        logger.info("Cleared completed items from queue")
    
    def reset_failed_items(self):
        """Reset failed items to pending status for retry."""
        with sqlite3.connect(self.queue_db_path) as conn:
            conn.execute("""
                UPDATE queue_items 
                SET status = 'pending', error_message = NULL, started_at = NULL, completed_at = NULL
                WHERE status = 'failed'
            """)
            conn.commit()
        
        logger.info("Reset failed items to pending status")
    
    def _row_to_queue_item(self, row) -> QueueItem:
        """Convert database row to QueueItem object."""
        return QueueItem(
            id=row[0],
            file_path=row[1],
            source_type=row[2],
            destination_table=row[3],
            pipeline_config=json.loads(row[4]) if row[4] else None,
            priority=row[5],
            status=QueueItemStatus(row[6]),
            created_at=datetime.fromisoformat(row[7]),
            started_at=datetime.fromisoformat(row[8]) if row[8] else None,
            completed_at=datetime.fromisoformat(row[9]) if row[9] else None,
            error_message=row[10],
            metadata=json.loads(row[11]) if row[11] else {}
        )
    
    def close(self):
        """Close any open connections."""
        # SQLite connections are closed automatically in context managers
        pass