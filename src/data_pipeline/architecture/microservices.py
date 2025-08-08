"""Microservices architecture components for distributed data processing."""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Union
from dataclasses import dataclass
from enum import Enum
import aiohttp
import uuid

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceInfo:
    """Service information and metadata."""
    name: str
    version: str
    host: str
    port: int
    endpoint: str = "/health"
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
    
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"
    
    @property
    def health_url(self) -> str:
        return f"{self.url}{self.endpoint}"


@dataclass
class HealthCheck:
    """Health check result."""
    service: str
    status: ServiceStatus
    timestamp: datetime
    response_time_ms: float
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


class ServiceDiscovery:
    """Simple service discovery mechanism."""
    
    def __init__(self):
        self._services: Dict[str, ServiceInfo] = {}
        self._health_cache: Dict[str, HealthCheck] = {}
        self._cache_ttl = 30  # seconds
    
    def register_service(self, service: ServiceInfo) -> None:
        """Register a service."""
        self._services[service.name] = service
        logger.info(f"Registered service: {service.name} at {service.url}")
    
    def deregister_service(self, service_name: str) -> None:
        """Deregister a service."""
        if service_name in self._services:
            del self._services[service_name]
            if service_name in self._health_cache:
                del self._health_cache[service_name]
            logger.info(f"Deregistered service: {service_name}")
    
    def get_service(self, service_name: str) -> Optional[ServiceInfo]:
        """Get service information."""
        return self._services.get(service_name)
    
    def get_services_by_tag(self, tag: str) -> List[ServiceInfo]:
        """Get services by tag."""
        return [service for service in self._services.values() if tag in service.tags]
    
    def list_services(self) -> List[ServiceInfo]:
        """List all registered services."""
        return list(self._services.values())
    
    async def check_health(self, service_name: str) -> HealthCheck:
        """Check health of a specific service."""
        service = self.get_service(service_name)
        if not service:
            return HealthCheck(
                service=service_name,
                status=ServiceStatus.UNKNOWN,
                timestamp=datetime.utcnow(),
                response_time_ms=0,
                details={"error": "Service not found"}
            )
        
        start_time = datetime.utcnow()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(service.health_url, timeout=5) as response:
                    end_time = datetime.utcnow()
                    response_time = (end_time - start_time).total_seconds() * 1000
                    
                    if response.status == 200:
                        data = await response.json()
                        status = ServiceStatus.HEALTHY
                        details = data
                    else:
                        status = ServiceStatus.UNHEALTHY
                        details = {"http_status": response.status}
                        
        except Exception as e:
            end_time = datetime.utcnow()
            response_time = (end_time - start_time).total_seconds() * 1000
            status = ServiceStatus.UNHEALTHY
            details = {"error": str(e)}
        
        health_check = HealthCheck(
            service=service_name,
            status=status,
            timestamp=end_time,
            response_time_ms=response_time,
            details=details
        )
        
        self._health_cache[service_name] = health_check
        return health_check
    
    async def health_check_all(self) -> Dict[str, HealthCheck]:
        """Check health of all services."""
        tasks = [self.check_health(name) for name in self._services.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        health_status = {}
        for i, result in enumerate(results):
            service_name = list(self._services.keys())[i]
            if isinstance(result, Exception):
                health_status[service_name] = HealthCheck(
                    service=service_name,
                    status=ServiceStatus.UNHEALTHY,
                    timestamp=datetime.utcnow(),
                    response_time_ms=0,
                    details={"error": str(result)}
                )
            else:
                health_status[service_name] = result
        
        return health_status


class MessageBroker:
    """Simple message broker for inter-service communication."""
    
    def __init__(self):
        self._subscribers: Dict[str, List[callable]] = {}
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
    
    def subscribe(self, topic: str, callback: callable) -> None:
        """Subscribe to a topic."""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(callback)
        logger.info(f"Subscribed to topic: {topic}")
    
    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        """Publish message to a topic."""
        await self._message_queue.put({
            'topic': topic,
            'message': message,
            'timestamp': datetime.utcnow().isoformat(),
            'message_id': str(uuid.uuid4())
        })
    
    async def start_broker(self) -> None:
        """Start the message broker."""
        self._running = True
        while self._running:
            try:
                item = await asyncio.wait_for(self._message_queue.get(), timeout=1.0)
                topic = item['topic']
                
                if topic in self._subscribers:
                    tasks = [callback(item) for callback in self._subscribers[topic]]
                    await asyncio.gather(*tasks, return_exceptions=True)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    def stop_broker(self) -> None:
        """Stop the message broker."""
        self._running = False


class DataProcessingService(ABC):
    """Abstract base class for data processing microservices."""
    
    def __init__(self, name: str, version: str = "1.0.0"):
        self.name = name
        self.version = version
        self.service_id = f"{name}-{uuid.uuid4().hex[:8]}"
        self.start_time = datetime.utcnow()
        self.processed_count = 0
        self.error_count = 0
    
    @abstractmethod
    async def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process data - implemented by concrete services."""
        pass
    
    async def health_check(self) -> Dict[str, Any]:
        """Return service health information."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        
        status = ServiceStatus.HEALTHY
        if self.error_count > self.processed_count * 0.1:  # More than 10% errors
            status = ServiceStatus.DEGRADED
        
        return {
            "service": self.name,
            "version": self.version,
            "service_id": self.service_id,
            "status": status.value,
            "uptime_seconds": uptime,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def handle_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming request with error tracking."""
        try:
            result = await self.process_data(request_data)
            self.processed_count += 1
            return {
                "success": True,
                "result": result,
                "service_id": self.service_id
            }
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing request in {self.name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "service_id": self.service_id
            }


class ValidationService(DataProcessingService):
    """Microservice for data validation."""
    
    def __init__(self):
        super().__init__("validation-service", "1.0.0")
    
    async def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data using configured rules."""
        # Simulate validation logic
        await asyncio.sleep(0.1)  # Simulate processing time
        
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "row_count": data.get("row_count", 0),
            "column_count": data.get("column_count", 0)
        }
        
        # Simple validation rules
        if data.get("row_count", 0) == 0:
            validation_results["valid"] = False
            validation_results["errors"].append("Dataset is empty")
        
        if data.get("column_count", 0) == 0:
            validation_results["valid"] = False
            validation_results["errors"].append("No columns found")
        
        return validation_results


class TransformationService(DataProcessingService):
    """Microservice for data transformation."""
    
    def __init__(self):
        super().__init__("transformation-service", "1.0.0")
    
    async def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data based on configuration."""
        # Simulate transformation logic
        await asyncio.sleep(0.2)  # Simulate processing time
        
        transformations_applied = []
        
        # Example transformations
        if "operations" in data:
            for operation in data["operations"]:
                transformations_applied.append(f"Applied {operation['type']}")
        
        return {
            "transformed": True,
            "operations_applied": transformations_applied,
            "row_count": data.get("row_count", 0),
            "processing_time_ms": 200
        }


class StorageService(DataProcessingService):
    """Microservice for data storage operations."""
    
    def __init__(self):
        super().__init__("storage-service", "1.0.0")
    
    async def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Store data to configured destination."""
        # Simulate storage logic
        await asyncio.sleep(0.15)  # Simulate processing time
        
        return {
            "stored": True,
            "destination": data.get("destination", "default_table"),
            "rows_written": data.get("row_count", 0),
            "storage_time_ms": 150
        }


class ServiceOrchestrator:
    """Orchestrates multiple microservices for pipeline execution."""
    
    def __init__(self, service_discovery: ServiceDiscovery, message_broker: MessageBroker):
        self.service_discovery = service_discovery
        self.message_broker = message_broker
        self.services: Dict[str, DataProcessingService] = {}
    
    def register_service(self, service: DataProcessingService) -> None:
        """Register a processing service."""
        self.services[service.name] = service
        logger.info(f"Registered processing service: {service.name}")
    
    async def execute_pipeline(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline using microservices."""
        pipeline_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        try:
            # Publish pipeline start event
            await self.message_broker.publish("pipeline.started", {
                "pipeline_id": pipeline_id,
                "config": pipeline_config
            })
            
            results = {}
            
            # Execute services in order
            service_order = ["validation-service", "transformation-service", "storage-service"]
            
            for service_name in service_order:
                if service_name in self.services:
                    service = self.services[service_name]
                    result = await service.handle_request(pipeline_config)
                    results[service_name] = result
                    
                    if not result.get("success", False):
                        raise Exception(f"Service {service_name} failed: {result.get('error')}")
            
            # Publish pipeline completion event
            duration = (datetime.utcnow() - start_time).total_seconds()
            await self.message_broker.publish("pipeline.completed", {
                "pipeline_id": pipeline_id,
                "duration_seconds": duration,
                "results": results
            })
            
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "duration_seconds": duration,
                "service_results": results
            }
            
        except Exception as e:
            # Publish pipeline failure event
            await self.message_broker.publish("pipeline.failed", {
                "pipeline_id": pipeline_id,
                "error": str(e)
            })
            
            return {
                "success": False,
                "pipeline_id": pipeline_id,
                "error": str(e)
            }
    
    async def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health."""
        service_health = {}
        
        for name, service in self.services.items():
            health = await service.health_check()
            service_health[name] = health
        
        # Overall system status
        unhealthy_count = sum(1 for h in service_health.values() if h.get("status") != "healthy")
        total_services = len(service_health)
        
        if unhealthy_count == 0:
            overall_status = "healthy"
        elif unhealthy_count < total_services:
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"
        
        return {
            "overall_status": overall_status,
            "services": service_health,
            "total_services": total_services,
            "healthy_services": total_services - unhealthy_count,
            "timestamp": datetime.utcnow().isoformat()
        }


# Global instances
_service_discovery = ServiceDiscovery()
_message_broker = MessageBroker()
_orchestrator = ServiceOrchestrator(_service_discovery, _message_broker)

# Register default services
_orchestrator.register_service(ValidationService())
_orchestrator.register_service(TransformationService())
_orchestrator.register_service(StorageService())


def get_service_orchestrator() -> ServiceOrchestrator:
    """Get the global service orchestrator."""
    return _orchestrator


def get_message_broker() -> MessageBroker:
    """Get the global message broker."""
    return _message_broker


def get_service_discovery() -> ServiceDiscovery:
    """Get the global service discovery."""
    return _service_discovery