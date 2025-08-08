# API Documentation

## 🚀 Interactive API Explorer

Explore our APIs with live examples and try them directly in your browser:

**[📖 Open Interactive API Documentation →](https://your-pipeline-docs.com/api)**

## 📚 Quick Reference

### Core Pipeline API

```python
from data_pipeline.core import Pipeline
from data_pipeline.architecture.event_bus import get_event_bus

# Create and configure pipeline
pipeline = Pipeline.from_yaml("config/pipelines/my_pipeline.yaml")

# Execute with monitoring
results = await pipeline.run()

# Access results
print(f"Processed {results['rows_processed']} rows")
print(f"Success: {results['success']}")
```

### Microservices Architecture API

```python
from data_pipeline.architecture.microservices import get_service_orchestrator

# Get orchestrator
orchestrator = get_service_orchestrator()

# Execute distributed pipeline
result = await orchestrator.execute_pipeline({
    "source": {"type": "csv", "path": "data.csv"},
    "destination": "processed_table",
    "row_count": 1000
})
```

### Observability API

```python
from data_pipeline.observability.metrics import get_metrics_collector
from data_pipeline.observability.dashboard import start_dashboard

# Collect custom metrics
metrics = get_metrics_collector()
metrics.record_counter("custom_events", 1, {"type": "user_action"})

# Start monitoring dashboard
start_dashboard(host='0.0.0.0', port=8050)
```

## 🔧 Configuration API

### Pipeline Configuration Schema

```yaml
# Complete pipeline configuration reference
name: string                    # Pipeline identifier
description: string            # Human-readable description

# Data input configuration
input:
  path: string                 # File path or pattern
  encoding: string             # File encoding (default: utf-8)

# Source configuration
source:
  type: enum                   # csv | json | s3 | kafka
  config:
    # CSV-specific options
    delimiter: string          # Column delimiter
    header: boolean           # Has header row
    skip_bad_lines: boolean   # Skip malformed lines
    
    # S3-specific options
    bucket: string            # S3 bucket name
    prefix: string            # Object prefix
    
    # Kafka-specific options
    topic: string             # Kafka topic
    group_id: string          # Consumer group

# Processing configuration
processing:
  engine: enum                 # pandas | spark
  operations:
    - type: string             # Operation type
      params: object           # Operation parameters

# Validation configuration
validation:
  enabled: boolean
  auto_generate_expectations: boolean
  suite_name: string
  custom_expectations:
    - expectation_type: string
      column: string
      # Additional expectation parameters

# Storage configuration
storage:
  type: enum                   # postgresql | mysql | sqlite
  destination: string          # Table name
  mode: enum                   # append | replace | update
  batch_size: integer         # Bulk insert batch size
```

### Environment Configuration

```yaml
# config/environments/production.yaml
database:
  host: string
  port: integer
  database: string
  username: string
  password: string
  pool_size: integer

storage:
  local_path: string
  s3_bucket: string
  
processing:
  max_memory_gb: integer
  spark_config:
    executor_memory: string
    driver_memory: string
```

## 🛠️ Core Classes

### Pipeline Class

```python
class Pipeline:
    def __init__(self, config: Union[Config, Dict[str, Any], str])
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'Pipeline'
    
    async def run(self, input_source: str = None) -> Dict[str, Any]
    
    async def dry_run(self, input_source: str = None) -> Dict[str, Any]
    
    def get_pipeline_info(self) -> Dict[str, Any]
```

**Methods:**

- `run()`: Execute the complete pipeline
- `dry_run()`: Validate configuration without processing data  
- `from_yaml()`: Create pipeline from configuration file
- `get_pipeline_info()`: Get pipeline metadata and status

**Returns:**
```python
{
    'success': bool,
    'rows_processed': int,
    'metadata': dict,
    'profiling_results': dict,
    'validation_results': dict,
    'errors': list
}
```

### MetricsCollector Class

```python
class MetricsCollector:
    def record_counter(self, name: str, value: float = 1, tags: Dict[str, str] = None)
    
    def record_gauge(self, name: str, value: float, tags: Dict[str, str] = None)
    
    def record_histogram(self, name: str, value: float, tags: Dict[str, str] = None)
    
    def timer(self, name: str, tags: Dict[str, str] = None)
    
    def get_metrics(self, name: str, window: str = '1h') -> List[Metric]
    
    def get_aggregated_metrics(self, name: str, window: str = '1h') -> Dict[str, float]
```

### EventBus Class

```python
class EventBus:
    def subscribe(self, handler: EventHandler) -> None
    
    async def publish(self, event: DomainEvent) -> None
    
    async def get_event_history(self, aggregate_id: str) -> List[DomainEvent]
```

## 📊 Data Source APIs

### CSV Source

```python
from data_pipeline.sources import CSVSource

source = CSVSource({
    'delimiter': ',',
    'encoding': 'utf-8',
    'header': True
})

# Read single file
data = source.read('data.csv')

# Read with custom parameters
data = source.read('data.csv', 
    nrows=1000,           # Limit rows
    usecols=['col1', 'col2'],  # Select columns
    dtype={'col1': 'str'}      # Specify types
)
```

### S3 Source

```python
from data_pipeline.sources import S3Source

source = S3Source({
    'region': 'us-east-1',
    'access_key_id': 'your_key',
    'secret_access_key': 'your_secret'
})

# Read from S3
data = source.read('s3://bucket/path/file.csv')

# List available objects
objects = source.list_sources('s3://bucket/prefix/')
```

### JSON Source

```python
from data_pipeline.sources import JSONSource

source = JSONSource({
    'lines': True,  # JSONL format
    'normalize': True  # Flatten nested objects
})

data = source.read('data.jsonl')
```

## ⚙️ Processor APIs

### Pandas Processor

```python
from data_pipeline.processors import PandasProcessor

processor = PandasProcessor({})

# Apply operations
operations = [
    {
        'type': 'filter',
        'params': {'condition': 'age > 18'}
    },
    {
        'type': 'transform', 
        'params': {
            'column_mappings': {'old_name': 'new_name'},
            'calculated_columns': [
                {'name': 'full_name', 'formula': 'first_name + " " + last_name'}
            ]
        }
    }
]

result = processor.process(data, operations)
```

### Spark Processor  

```python
from data_pipeline.processors import SparkProcessor

processor = SparkProcessor({
    'app_name': 'DataProcessing',
    'executor_memory': '4g',
    'driver_memory': '2g'
})

# Process large datasets
result = processor.process(data, operations)
```

## 💾 Storage APIs

### PostgreSQL Storage

```python
from data_pipeline.storage import PostgreSQLStorage

storage = PostgreSQLStorage({
    'host': 'localhost',
    'database': 'warehouse',
    'username': 'user',
    'password': 'pass'
})

# Write data
success = storage.write(
    data=df,
    table_name='employees', 
    if_exists='append',
    chunk_size=5000
)

# Read data back
df = storage.read('SELECT * FROM employees LIMIT 100')
```

## 🔍 Validation APIs

### Data Validator

```python
from data_pipeline.utils import DataValidator

validator = DataValidator({})

# Create expectation suite
suite_name = validator.create_expectation_suite(
    'employee_validation',
    data=df,
    auto_generate=True
)

# Add custom expectations  
validator.add_expectation(
    suite_name,
    'expect_column_values_to_be_unique',
    column='employee_id'
)

# Validate data
results = validator.validate_data(df, suite_name)
```

## 📈 Monitoring APIs

### Health Checker

```python
from data_pipeline.observability.metrics import get_health_checker

health_checker = get_health_checker()

# Add custom health check
def check_database():
    # Your database connectivity check
    return True

health_checker.add_check('database', check_database)

# Run all checks
status = health_checker.run_checks()
```

### Alert Manager

```python
from data_pipeline.observability.metrics import get_alert_manager, ThresholdRule, AlertLevel

alert_manager = get_alert_manager()

# Add custom alert rule
rule = ThresholdRule(
    name='high_error_rate',
    metric_name='pipeline_failure_rate',
    threshold=0.1,  # 10% failure rate
    operator='gt',
    level=AlertLevel.CRITICAL
)

alert_manager.add_rule(rule)

# Check for alerts
alerts = alert_manager.check_alerts()
```

## 🌐 Web Dashboard API

### Dashboard Server

```python
from data_pipeline.observability.dashboard import DashboardServer

# Create dashboard
dashboard = DashboardServer(host='0.0.0.0', port=8050)

# Custom route
@dashboard.app.route('/api/custom')
def custom_endpoint():
    return {'message': 'Custom endpoint'}

# Start server
dashboard.run()
```

### REST API Endpoints

```bash
# Get current metrics
GET /api/metrics?window=1h

# Get active alerts  
GET /api/alerts

# Get system health
GET /api/health

# Get pipeline statistics
GET /api/pipeline-stats?window=24h
```

**Response Format:**
```json
{
  "status": "success",
  "data": {
    "metric_name": {
      "count": 100,
      "sum": 1500.5,
      "avg": 15.05,
      "min": 0.1,
      "max": 100.0,
      "latest": 25.3
    }
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## 🔧 CLI API

### Command Line Interface

```bash
# Initialize pipeline
data-pipeline init --name my_pipeline --source-type csv

# Run pipeline  
data-pipeline run config/pipelines/my_pipeline.yaml

# Queue management
data-pipeline queue add data.csv employees --priority 1
data-pipeline queue process

# Monitoring
data-pipeline metrics show
data-pipeline health check
data-pipeline dashboard start --port 8050

# Data profiling
data-pipeline profile data.csv --output report.html

# Schema inference
data-pipeline infer-schema data.csv --table-name employees
```

## 🧪 Testing APIs

### Test Utilities

```python
from data_pipeline.testing import PipelineTestCase, MockDataSource

class TestMyPipeline(PipelineTestCase):
    def test_csv_processing(self):
        # Create test pipeline
        pipeline = self.create_test_pipeline({
            'source': {'type': 'csv'},
            'storage': {'type': 'mock'}
        })
        
        # Run with test data
        result = pipeline.run('test_data.csv')
        
        # Assertions
        self.assertTrue(result['success'])
        self.assertEqual(result['rows_processed'], 100)
```

## 🔐 Security APIs

### Authentication & Authorization

```python
from data_pipeline.security import APIKeyAuth, RoleBasedAuth

# API key authentication
auth = APIKeyAuth(['your-api-key'])

# Role-based access control
rbac = RoleBasedAuth({
    'admin': ['read', 'write', 'execute'],
    'viewer': ['read'],
    'operator': ['read', 'execute']
})
```

## 📝 Error Handling

### Exception Classes

```python
from data_pipeline.exceptions import (
    PipelineConfigError,
    DataSourceError, 
    ProcessingError,
    ValidationError,
    StorageError
)

try:
    pipeline.run()
except ValidationError as e:
    print(f"Data validation failed: {e}")
except ProcessingError as e:
    print(f"Data processing failed: {e}")
```

## 🚀 Performance Optimization

### Batch Processing

```python
# Process data in chunks
for chunk in source.read_chunks('large_file.csv', chunk_size=10000):
    processed = processor.process(chunk, operations)
    storage.write(processed, 'output_table', if_exists='append')
```

### Parallel Processing

```python
from data_pipeline.parallel import ParallelProcessor

parallel_processor = ParallelProcessor(n_workers=4)
results = parallel_processor.process_files([
    'file1.csv', 'file2.csv', 'file3.csv'
], operations)
```

## 📞 Support & Resources

- **📖 Full Documentation**: [docs.your-pipeline.com](https://docs.your-pipeline.com)
- **💬 Community Support**: [GitHub Discussions](https://github.com/your-repo/discussions)
- **🐛 Bug Reports**: [GitHub Issues](https://github.com/your-repo/issues)
- **📧 Enterprise Support**: enterprise@your-pipeline.com

---

**💡 Pro Tip**: Use the interactive API explorer for hands-on learning and testing. It includes live examples, parameter validation, and response inspection.