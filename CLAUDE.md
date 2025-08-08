# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive data pipeline framework for large-scale dataset analysis and processing. It supports multiple data sources (S3, CSV, JSON), processing engines (Pandas, Spark/PySpark), and PostgreSQL as the target database. The architecture is designed to be flexible and adaptable for various data processing workflows.

## Project Architecture

### Core Components

- **Sources** (`src/data_pipeline/sources/`): Data source adapters for CSV, JSON, and S3
- **Processors** (`src/data_pipeline/processors/`): Data processing engines (Pandas, Spark)
- **Storage** (`src/data_pipeline/storage/`): PostgreSQL database interface
- **Utils** (`src/data_pipeline/utils/`): Schema inference, data profiling, validation
- **Core** (`src/data_pipeline/core/`): Pipeline orchestration and configuration
- **CLI** (`src/data_pipeline/cli/`): Command-line interface

### Project Structure

```
├── src/data_pipeline/              # Main package
│   ├── core/                       # Pipeline orchestration
│   │   ├── config.py              # Configuration management
│   │   ├── base.py                # Abstract base classes
│   │   └── pipeline.py            # Main pipeline class
│   ├── sources/                    # Data source implementations
│   │   ├── csv_source.py          # CSV file reader
│   │   ├── json_source.py         # JSON/JSONL reader
│   │   └── s3_source.py           # S3 object reader
│   ├── processors/                 # Data processing engines
│   │   ├── pandas_processor.py    # Pandas-based processor
│   │   └── spark_processor.py     # Spark-based processor
│   ├── storage/                    # Storage backends
│   │   └── postgresql_storage.py  # PostgreSQL interface
│   ├── utils/                      # Utility modules
│   │   ├── schema_inference.py    # Automatic schema detection
│   │   ├── data_profiler.py       # Data quality analysis
│   │   └── data_validator.py      # Great Expectations integration
│   └── cli/                        # Command-line interface
├── config/                         # Configuration files
│   ├── environments/               # Environment-specific configs
│   │   ├── base.yaml              # Base configuration
│   │   ├── development.yaml       # Development settings
│   │   └── production.yaml        # Production settings
│   └── pipelines/                  # Pipeline configurations
├── data/                          # Data directories
│   ├── raw/                       # Raw input data
│   ├── processed/                 # Processed data
│   └── external/                  # External data sources
├── tests/                         # Test suite
├── scripts/                       # Utility scripts
└── reports/                       # Generated reports
```

## Development Environment

- **Python Version**: 3.9+ (supports 3.9-3.12)
- **Package Manager**: pip with pyproject.toml
- **Virtual Environment**: `venv/` directory
- **IDE**: IntelliJ IDEA (Python module)
- **Database**: PostgreSQL (configurable)

## Common Development Commands

### Environment Setup
```bash
# Install dependencies
make install

# Development setup with all tools
make install-dev

# Install with Spark support
make install-spark

# Complete environment setup
make setup
```

### Pipeline Operations
```bash
# Initialize new pipeline
data-pipeline init --name my_pipeline --source-type csv

# Run pipeline from config
data-pipeline run config/pipelines/my_pipeline.yaml

# Dry run (validation only)
data-pipeline run pipeline.yaml --dry-run

# Profile data
data-pipeline profile data.csv --format html

# Infer schema
data-pipeline infer-schema data.csv --table-name my_table
```

### Development Tools
```bash
# Run tests
make test

# Code formatting
make format

# Linting
make lint

# Clean temporary files
make clean
```

## Configuration Management

The system uses a hierarchical configuration system:

1. **Base Configuration** (`config/environments/base.yaml`): Default settings
2. **Environment Configs** (`config/environments/`): Environment-specific overrides
3. **Pipeline Configs** (`config/pipelines/`): Individual pipeline definitions
4. **Environment Variables** (`.env`): Sensitive credentials and overrides

### Key Configuration Sections

- **database**: PostgreSQL connection settings
- **storage**: Local and S3 storage configuration
- **processing**: Pandas/Spark engine settings
- **validation**: Data quality validation rules
- **profiling**: Data analysis and reporting options

## Data Pipeline Patterns

### Basic Pipeline Flow
1. **Load**: Read data from source (CSV/JSON/S3)
2. **Transform**: Process data with configured operations
3. **Validate**: Check data quality with Great Expectations
4. **Profile**: Analyze data characteristics (optional)
5. **Store**: Write to PostgreSQL database

### Common Operations
- **Filtering**: Row-level filtering with conditions
- **Transformations**: Column operations, type casting, calculations
- **Aggregations**: Group-by operations and statistical summaries
- **Joins**: Merge datasets
- **Cleaning**: Handle nulls, duplicates, standardize formats

### Processing Engines

**Pandas Processor**: Use for datasets that fit in memory (< 8GB)
- Fast for small to medium datasets
- Rich transformation operations
- Easy debugging and development

**Spark Processor**: Use for large datasets or distributed processing
- Scales to multi-TB datasets
- Fault-tolerant distributed processing  
- SQL-like operations with window functions

## Data Sources

### CSV Files
- Supports single files and glob patterns
- Automatic encoding detection
- Configurable delimiters and parsing options
- Schema inference with type detection

### JSON Files  
- Standard JSON and JSONL (line-delimited) formats
- Nested object flattening with pandas.json_normalize
- Batch processing of multiple files
- Automatic schema inference

### S3 Objects
- AWS S3 integration with boto3
- Support for CSV, JSON, Parquet formats
- Wildcard pattern matching
- IAM role and credential chain support

## Database Integration

### PostgreSQL Features
- Bulk insert operations with conflict resolution
- Schema creation from pandas DataFrames
- Connection pooling and transaction management
- Support for schemas and table namespaces
- Metadata queries and table introspection

### Schema Management
- Automatic schema inference from data
- SQL type mapping from pandas dtypes
- Primary key and foreign key candidate detection
- Index recommendations based on data patterns
- Constraint generation for data integrity

## Data Quality & Validation

### Great Expectations Integration
- Auto-generated expectation suites
- Custom validation rules
- Data quality reporting
- Validation result tracking
- Integration with pipeline failures

### Data Profiling
- Comprehensive statistical analysis
- Data quality scoring
- Missing value analysis
- Correlation detection
- Outlier identification
- HTML report generation

## Testing & Quality Assurance

### Test Structure
- Unit tests for individual components
- Integration tests for end-to-end workflows
- Mock data generation for testing
- Database connection testing

### Code Quality Tools
- **black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting and style checking
- **mypy**: Type checking
- **pytest**: Testing framework with coverage

## Performance Considerations

### Memory Management
- Configurable sample sizes for large datasets
- Chunked processing for memory-constrained environments
- Lazy loading and streaming for large files
- Garbage collection optimization

### Spark Optimization
- Adaptive query execution enabled
- Arrow-based pandas conversion
- Configurable partitioning strategies
- Caching for intermediate results

## Security Best Practices

- Environment variable based configuration
- No credentials in code or version control
- IAM role-based S3 access when possible
- Database connection pooling with proper cleanup
- Input validation and SQL injection prevention

## Common Troubleshooting

### Memory Issues
- Reduce sample sizes in profiling
- Use Spark for large datasets
- Enable chunked processing
- Monitor memory usage with profiling tools

### Database Connectivity
- Check PostgreSQL service status
- Verify credentials in .env file
- Test connection with psql command
- Check firewall and network connectivity

### Data Loading Errors
- Validate file paths and permissions
- Check data formats and encoding
- Verify S3 bucket access and credentials
- Review error logs for specific issues

## Extension Points

### Adding New Data Sources
1. Inherit from `DataSource` base class
2. Implement required methods: `read()`, `list_sources()`, `get_schema()`
3. Add to `PipelineFactory.create_source()` mapping
4. Update configuration schema

### Adding New Processors
1. Inherit from `DataProcessor` base class
2. Implement `process()` and `validate_data()` methods
3. Add operation type handlers
4. Register in `PipelineFactory.create_processor()`

### Adding New Storage Backends
1. Inherit from `DataStorage` base class
2. Implement storage interface methods
3. Add connection management
4. Register in factory and configuration