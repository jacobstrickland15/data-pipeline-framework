# Data Pipeline Framework

A comprehensive data pipeline framework for large-scale dataset analysis and processing with file queue management. Supports multiple data sources (S3, CSV, JSON), processing engines (Pandas, Spark/PySpark), and PostgreSQL as the target database.

## Features

- **Multiple Data Sources**: CSV, JSON, JSONL, S3 objects
- **Processing Engines**: Pandas for smaller datasets, Spark for big data
- **Database Integration**: PostgreSQL with bulk operations and schema management
- **File Queue System**: Priority-based file ingestion with status tracking
- **Data Quality**: Automated validation using Great Expectations
- **Data Profiling**: Statistical analysis and HTML report generation
- **Configuration**: YAML-based pipeline configuration
- **CLI Interface**: Command-line tools for pipeline and queue management

## Installation


```bash
# Clone the repository
git clone https://github.com/[yourusername]/data-pipeline-framework.git
cd data-pipeline-framework

# Create virtual environment
python3.12 -m venv venv # opt for python version 3.12 for full CLI functionality
source venv/bin/activate  # Windows equivalent: venv\Scripts\activate

# Install with all optional dependencies
pip install -e ".[all]"

# Or install specific feature sets
pip install -e ".[dev, spark]"
```

## Quick Start

### 1. Initialize a Pipeline

```bash
data-pipeline init --name my_pipeline --source-type csv --processing-engine pandas
```

### 2. Add Files to Queue

```bash
# Add a single file
data-pipeline queue add data/employees.csv employees_table --source-type csv --priority 1

# Add with custom configuration
data-pipeline queue add data/sales.json sales_data --source-type json --config config/pipelines/sales_pipeline.yaml
```

### 3. Process the Queue

```bash
# Process all pending files
data-pipeline queue process

# Run continuous processing (daemon mode)
python scripts/queue_processor.py --mode continuous --poll-interval 30
```

### 4. Monitor Progress

```bash
# Check queue status
data-pipeline queue list

# View queue statistics
data-pipeline queue stats
```

### 5. Run Direct Pipeline

```bash
# Run pipeline from configuration
data-pipeline run config/pipelines/my_pipeline.yaml

# Profile your data
data-pipeline profile data/sample.csv --format html
```

## 🔬 Generate Analysis Code

Generate ready-to-use Python or Scala code for analyzing your database tables:

### Generate Analysis Templates

```bash
# Generate Python analysis code for a specific table
data-pipeline generate analysis users --language python --output analysis_users.py

# Generate Scala analysis code  
data-pipeline generate analysis orders --language scala --output analysis_orders.scala

# Generate analysis code for all tables
data-pipeline generate analysis --all-tables --language python --output-dir ./analysis/
```

### What You Get

**Python Template Features:**
- Database connection using your pipeline configuration
- Ready-to-use data loading functions with filtering options
- Sample data preview and basic statistics
- Error handling and connection troubleshooting
- Best practices for pandas-based analysis

**Scala Template Features:**
- Spark session initialization with optimal settings
- JDBC connection to your PostgreSQL database
- Type-safe data loading with error handling
- Schema introspection and data preview
- Production-ready Spark configuration

### Developer Workflow

1. **Explore** your data using IntelliJ DataGrip integration
2. **Generate** analysis starter code with one CLI command
3. **Analyze** immediately - no setup friction, no boilerplate
4. **Iterate** with confidence using proper error handling

## Queue Management

The file queue system allows you to batch process multiple files with priority handling:

### Queue Commands

```bash
# Add files to queue
data-pipeline queue add <file_path> <table_name> [options]

# List queue items
data-pipeline queue list [--status pending|processing|completed|failed]

# View queue statistics
data-pipeline queue stats

# Process queue items
data-pipeline queue process [--max-items N]

# Remove specific item
data-pipeline queue remove <item_id>

# Retry failed items
data-pipeline queue retry-failed

# Clear completed items
data-pipeline queue clear-completed
```

### Queue Processing Options

```bash
# Batch processing (process available items and exit)
python scripts/queue_processor.py --mode batch --max-items 10

# Continuous processing (daemon mode)
python scripts/queue_processor.py --mode continuous --poll-interval 30
```

## Configuration

### Pipeline Configuration

```yaml
name: "employee_data_pipeline"
description: "Process employee CSV data and store in PostgreSQL"

source:
  type: "csv"
  config:
    base_path: "./data/raw"
    encoding: "utf-8"
    delimiter: ","

processing:
  engine: "pandas"
  operations:
    - type: "clean"
      params:
        operations: ["remove_empty_rows", "trim_strings"]
    - type: "transform"
      params:
        column_mappings:
          "emp_id": "employee_id"
          "full_name": "name"

storage:
  type: "postgresql"
  destination: "employees"
  mode: "append"

validation:
  enabled: true
  auto_generate_expectations: true
  suite_name: "employee_validation"

profiling:
  enabled: true
  generate_report: true
  output_path: "./reports/employee_profile.html"
```

### Database Configuration

Create a `.env` file:

```env
# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=data_warehouse
DB_USER=postgres
DB_PASSWORD=your_password

# AWS Configuration (for S3 sources)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
```

## Architecture

### Core Components

```
src/data_pipeline/
├── core/
│   ├── pipeline.py          # Main pipeline orchestration
│   ├── config.py            # Configuration management
│   ├── queue_manager.py     # File queue management
│   └── base.py              # Abstract base classes
├── sources/                 # Data source implementations
│   ├── csv_source.py
│   ├── json_source.py
│   └── s3_source.py
├── processors/              # Data processing engines
│   ├── pandas_processor.py
│   └── spark_processor.py
├── storage/                 # Storage backends
│   └── postgresql_storage.py
├── utils/                   # Utility modules
│   ├── schema_inference.py
│   ├── data_profiler.py
│   └── data_validator.py
└── cli/                     # Command-line interface
    └── main.py
```

### Data Flow

1. **Queue**: Files added to priority queue
2. **Load**: Read data from configured source
3. **Transform**: Apply processing operations
4. **Validate**: Check data quality with Great Expectations
5. **Profile**: Generate analysis reports (optional)
6. **Store**: Write to PostgreSQL database
7. **Cleanup**: Remove processed files from queue

## Testing

```bash
# Run all tests
make test

# Run with coverage
pytest --cov=src/data_pipeline --cov-report=html

# Run specific test
pytest tests/test_queue_manager.py -v
```

## Development

### Setup Development Environment

```bash
# Install development dependencies
make install-dev

# Setup pre-commit hooks
pre-commit install

# Format code
make format

# Run linting
make lint

# Type checking
make typecheck
```

### Available Make Commands

```bash
make install      # Install dependencies
make install-dev  # Install with dev dependencies
make install-spark # Install with Spark support
make setup        # Complete environment setup
make test         # Run tests
make format       # Format code with black
make lint         # Run flake8 linting
make clean        # Clean temporary files
```

### Adding New Features

1. **New Data Source**: Inherit from `DataSource` base class
2. **New Processor**: Inherit from `DataProcessor` base class  
3. **New Storage Backend**: Inherit from `DataStorage` base class

## Monitoring & Logging

- Queue processing logs: `logs/queue_processor.log`
- Pipeline logs: Configurable via logging configuration
- Queue database: SQLite database storing queue status
- Great Expectations: Data quality validation reports

---

**Made with ❤️ for data engineers and analysts**
