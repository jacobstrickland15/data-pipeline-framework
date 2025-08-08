"""Event-driven architecture with CQRS pattern support."""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type, Union
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event types for the pipeline system."""
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_FAILED = "pipeline.failed"
    DATA_VALIDATED = "data.validated"
    DATA_PROCESSED = "data.processed"
    QUALITY_CHECK = "quality.check"
    SCHEMA_INFERRED = "schema.inferred"
    STORAGE_WRITTEN = "storage.written"


@dataclass
class DomainEvent:
    """Base domain event class."""
    event_id: str
    event_type: EventType
    aggregate_id: str
    timestamp: datetime
    version: int = 1
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if not self.event_id:
            self.event_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DomainEvent':
        """Create event from dictionary."""
        data['event_type'] = EventType(data['event_type'])
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


@dataclass
class PipelineStartedEvent(DomainEvent):
    """Event emitted when pipeline starts."""
    pipeline_name: str
    config: Dict[str, Any]
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.PIPELINE_STARTED


@dataclass
class PipelineCompletedEvent(DomainEvent):
    """Event emitted when pipeline completes successfully."""
    pipeline_name: str
    rows_processed: int
    duration_seconds: float
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.PIPELINE_COMPLETED


@dataclass
class DataValidatedEvent(DomainEvent):
    """Event emitted when data validation occurs."""
    validation_success: bool
    validation_results: Dict[str, Any]
    table_name: str
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.DATA_VALIDATED


class EventHandler(ABC):
    """Abstract base class for event handlers."""
    
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle the domain event."""
        pass
    
    @property
    @abstractmethod
    def handled_events(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        pass


class EventStore:
    """Simple in-memory event store (production would use persistent storage)."""
    
    def __init__(self):
        self._events: List[DomainEvent] = []
        self._lock = asyncio.Lock()
    
    async def append(self, event: DomainEvent) -> None:
        """Append event to store."""
        async with self._lock:
            self._events.append(event)
            logger.debug(f"Stored event: {event.event_type.value} for aggregate {event.aggregate_id}")
    
    async def get_events(self, aggregate_id: str) -> List[DomainEvent]:
        """Get all events for an aggregate."""
        async with self._lock:
            return [e for e in self._events if e.aggregate_id == aggregate_id]
    
    async def get_events_by_type(self, event_type: EventType) -> List[DomainEvent]:
        """Get all events of a specific type."""
        async with self._lock:
            return [e for e in self._events if e.event_type == event_type]


class EventBus:
    """Event bus for publishing and subscribing to domain events."""
    
    def __init__(self, event_store: Optional[EventStore] = None):
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._event_store = event_store or EventStore()
        self._middleware: List[Callable[[DomainEvent], None]] = []
    
    def subscribe(self, handler: EventHandler) -> None:
        """Subscribe an event handler to relevant events."""
        for event_type in handler.handled_events:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)
        
        logger.info(f"Subscribed handler {handler.__class__.__name__} to events: {[e.value for e in handler.handled_events]}")
    
    def add_middleware(self, middleware: Callable[[DomainEvent], None]) -> None:
        """Add middleware to process events before handlers."""
        self._middleware.append(middleware)
    
    async def publish(self, event: DomainEvent) -> None:
        """Publish an event to all subscribed handlers."""
        # Store event first
        await self._event_store.append(event)
        
        # Apply middleware
        for middleware in self._middleware:
            middleware(event)
        
        # Handle event
        handlers = self._handlers.get(event.event_type, [])
        if handlers:
            tasks = [handler.handle(event) for handler in handlers]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.debug(f"Published event {event.event_type.value} to {len(handlers)} handlers")
        else:
            logger.debug(f"No handlers for event type: {event.event_type.value}")
    
    async def get_event_history(self, aggregate_id: str) -> List[DomainEvent]:
        """Get event history for an aggregate."""
        return await self._event_store.get_events(aggregate_id)


# Example event handlers
class LoggingEventHandler(EventHandler):
    """Handler that logs all events."""
    
    @property
    def handled_events(self) -> List[EventType]:
        return list(EventType)
    
    async def handle(self, event: DomainEvent) -> None:
        logger.info(f"Event: {event.event_type.value} | Aggregate: {event.aggregate_id} | Time: {event.timestamp}")


class MetricsEventHandler(EventHandler):
    """Handler that collects metrics from events."""
    
    def __init__(self):
        self.metrics = {
            'pipelines_started': 0,
            'pipelines_completed': 0,
            'pipelines_failed': 0,
            'total_rows_processed': 0
        }
    
    @property
    def handled_events(self) -> List[EventType]:
        return [
            EventType.PIPELINE_STARTED,
            EventType.PIPELINE_COMPLETED,
            EventType.PIPELINE_FAILED
        ]
    
    async def handle(self, event: DomainEvent) -> None:
        if event.event_type == EventType.PIPELINE_STARTED:
            self.metrics['pipelines_started'] += 1
        elif event.event_type == EventType.PIPELINE_COMPLETED:
            self.metrics['pipelines_completed'] += 1
            if isinstance(event, PipelineCompletedEvent):
                self.metrics['total_rows_processed'] += event.rows_processed
        elif event.event_type == EventType.PIPELINE_FAILED:
            self.metrics['pipelines_failed'] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()


class NotificationEventHandler(EventHandler):
    """Handler that sends notifications for critical events."""
    
    @property
    def handled_events(self) -> List[EventType]:
        return [EventType.PIPELINE_FAILED, EventType.DATA_VALIDATED]
    
    async def handle(self, event: DomainEvent) -> None:
        if event.event_type == EventType.PIPELINE_FAILED:
            await self._send_alert(f"Pipeline failed: {event.aggregate_id}")
        elif event.event_type == EventType.DATA_VALIDATED:
            if isinstance(event, DataValidatedEvent) and not event.validation_success:
                await self._send_alert(f"Data validation failed for table: {event.table_name}")
    
    async def _send_alert(self, message: str) -> None:
        # In production, this would send to Slack, email, etc.
        logger.warning(f"ALERT: {message}")


# Global event bus instance
_event_bus = None

def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
        # Subscribe default handlers
        _event_bus.subscribe(LoggingEventHandler())
        _event_bus.subscribe(MetricsEventHandler())
        _event_bus.subscribe(NotificationEventHandler())
    return _event_bus