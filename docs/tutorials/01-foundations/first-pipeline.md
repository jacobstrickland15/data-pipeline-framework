# Your First Data Pipeline

Welcome to your first hands-on data pipeline experience! In this tutorial, you'll build a complete pipeline that processes employee data and stores it in a database.

## 🎯 What You'll Learn

By the end of this tutorial, you'll understand:
- How to configure a data pipeline
- Basic data transformation concepts
- Data validation fundamentals
- Database integration basics
- Pipeline monitoring essentials

## 📊 The Scenario

You're a data engineer at a growing company. The HR department sends you daily CSV files containing employee information, and you need to:

1. Validate the data quality
2. Clean and standardize the data
3. Store it in the company database
4. Generate a daily report

Let's build this step by step!

## 🏗️ Step 1: Understanding the Data

First, let's examine our sample data:

```bash
# Look at the sample employee data
head -5 data/sample_data.csv
```

You should see something like:
```csv
employee_id,first_name,last_name,email,department,salary,hire_date
E001,John,Doe,john.doe@company.com,Engineering,75000,2023-01-15
E002,Jane,Smith,jane.smith@company.com,Marketing,65000,2023-02-01
```

### 🔍 Data Analysis Questions
Before we process any data, let's think like a data engineer:

1. **What could go wrong with this data?**
   - Missing values
   - Invalid email formats
   - Duplicate employee IDs
   - Invalid dates
   - Salary outliers

2. **How should we handle these issues?**
   - Validation rules
   - Data cleaning steps
   - Error reporting
   - Data quality metrics

## 🔧 Step 2: Creating Your First Pipeline Configuration

Create a pipeline configuration file:

**`config/pipelines/employee_pipeline.yaml`**
```yaml
name: "employee_data_pipeline"
description: "Daily employee data processing pipeline"

# Input configuration
input:
  path: "data/raw/employees*.csv"
  encoding: "utf-8"

# Data source configuration
source:
  type: "csv"
  config:
    delimiter: ","
    header: true
    skip_bad_lines: false

# Processing configuration
processing:
  engine: "pandas"
  operations:
    # Step 1: Data Cleaning
    - type: "clean"
      params:
        operations: ["remove_empty_rows", "trim_strings"]
        
    # Step 2: Data Validation
    - type: "validate"
      params:
        rules:
          - column: "employee_id"
            type: "not_null"
          - column: "email"
            type: "email_format"
          - column: "salary"
            type: "range"
            min: 30000
            max: 200000
            
    # Step 3: Data Transformation
    - type: "transform"
      params:
        column_mappings:
          "employee_id": "emp_id"
          "first_name": "fname" 
          "last_name": "lname"
        
        # Create calculated columns
        calculated_columns:
          - name: "full_name"
            formula: "fname + ' ' + lname"
          - name: "annual_salary"
            formula: "salary"
          - name: "processed_date"
            formula: "current_timestamp"

# Data validation with Great Expectations
validation:
  enabled: true
  auto_generate_expectations: true
  suite_name: "employee_validation"
  
  # Custom expectations
  custom_expectations:
    - expectation_type: "expect_column_values_to_be_unique"
      column: "emp_id"
    - expectation_type: "expect_column_values_to_match_regex"
      column: "email"
      regex: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"

# Data profiling
profiling:
  enabled: true
  generate_report: true
  output_path: "./reports/employee_profile.html"
  sample_size: 10000

# Storage configuration
storage:
  type: "postgresql"
  destination: "employees"
  mode: "append"  # append, replace, update
  
  # Performance settings
  batch_size: 5000
  create_indexes: true
  
# Monitoring and alerting
monitoring:
  enabled: true
  alerts:
    - type: "data_quality"
      threshold: 0.95  # 95% data quality score required
    - type: "row_count"
      min_rows: 1
      max_rows: 100000
```

### 🎓 Configuration Breakdown

Let's understand each section:

**Input**: Where to find your data files
- Supports file patterns (`employees*.csv`)
- Handles different encodings

**Source**: How to read the data
- CSV, JSON, S3, Kafka sources supported
- Configurable parsing options

**Processing**: What to do with the data
- Multiple processing engines (Pandas, Spark)
- Chain multiple operations
- Built-in and custom operations

**Validation**: Ensure data quality
- Great Expectations integration
- Auto-generated and custom rules
- Detailed validation reports

**Storage**: Where to save processed data
- Multiple database types
- Performance optimization
- Schema management

## 🚀 Step 3: Running Your Pipeline

Now let's run the pipeline:

```bash
# Method 1: Direct execution
data-pipeline run config/pipelines/employee_pipeline.yaml

# Method 2: With specific input file
data-pipeline run config/pipelines/employee_pipeline.yaml --input data/sample_data.csv

# Method 3: Dry run (validation only)
data-pipeline run config/pipelines/employee_pipeline.yaml --dry-run
```

### Expected Output:
```
🚀 Starting pipeline: employee_data_pipeline
📊 Loading data from: data/sample_data.csv
✅ Loaded 150 rows, 7 columns
🔍 Running data profiling...
✅ Data profiling complete: reports/employee_profile.html
🔧 Processing data with 3 operations...
  ✅ Clean: Removed 2 empty rows, trimmed strings
  ✅ Validate: 148 rows passed validation
  ✅ Transform: Applied column mappings and calculations
📋 Running data validation...
  ✅ Validation suite: employee_validation
  ✅ All expectations passed (8/8)
💾 Storing data to PostgreSQL...
  ✅ Inserted 148 rows to table: employees
⏱️  Pipeline completed in 3.2 seconds
📊 Summary: 148 rows processed, 0 errors
```

## 🔍 Step 4: Understanding What Happened

Let's examine the results:

### 1. Data Profiling Report
Open `reports/employee_profile.html` in your browser to see:
- Data distribution statistics
- Missing value analysis
- Correlation matrices
- Data quality scores

### 2. Database Results
Check what was stored in the database:

```bash
# Connect to database and verify data
data-pipeline db query "SELECT COUNT(*) FROM employees;"
data-pipeline db query "SELECT * FROM employees LIMIT 5;"

# View table schema
data-pipeline db schema employees
```

### 3. Validation Results
Check the validation details:

```bash
# View validation results
data-pipeline validation results employee_validation

# List all validation suites
data-pipeline validation list
```

## 🎯 Step 5: Adding Error Handling

Real data is messy! Let's create a version that handles errors gracefully:

**`data/bad_employee_data.csv`**
```csv
employee_id,first_name,last_name,email,department,salary,hire_date
E001,John,Doe,john.doe@company.com,Engineering,75000,2023-01-15
,Jane,Smith,invalid-email,Marketing,999999,not-a-date
E001,Bob,Wilson,bob@company.com,Sales,50000,2023-03-01
E003,,Johnson,sarah@company.com,HR,-5000,2023-04-01
```

Issues in this data:
- Missing employee_id (row 2)
- Invalid email format (row 2)
- Salary too high (row 2)
- Invalid date format (row 2)
- Duplicate employee_id (row 3)
- Missing first name (row 4)
- Negative salary (row 4)

Run the pipeline with this data:

```bash
data-pipeline run config/pipelines/employee_pipeline.yaml --input data/bad_employee_data.csv
```

### Expected Output:
```
🚀 Starting pipeline: employee_data_pipeline
📊 Loading data from: data/bad_employee_data.csv
✅ Loaded 4 rows, 7 columns
🔧 Processing data with 3 operations...
  ⚠️  Clean: Removed 1 empty rows, trimmed strings
  ❌ Validate: 2 validation errors found
     - Row 2: Invalid email format: 'invalid-email'
     - Row 4: Salary out of range: -5000
📋 Running data validation...
  ❌ Validation failed: 2 expectations failed
💾 Storing valid data to PostgreSQL...
  ✅ Inserted 2 rows to table: employees
📊 Summary: 2 rows processed, 2 errors
📝 Error report saved to: logs/employee_pipeline_errors.log
```

## 📊 Step 6: Monitoring and Alerting

Add monitoring to track your pipeline performance:

```bash
# Check pipeline metrics
data-pipeline metrics show

# View error logs
data-pipeline logs --pipeline employee_data_pipeline --level error

# Set up alerts
data-pipeline alerts configure --email your-email@company.com
```

## 🏆 Step 7: Challenge Exercises

Now that you understand the basics, try these challenges:

### Challenge 1: Custom Transformation
Add a transformation that:
- Creates a `years_of_service` column based on `hire_date`
- Categorizes employees by salary range (Low, Medium, High)
- Adds a `department_code` mapping

### Challenge 2: Advanced Validation
Create custom validation rules that:
- Ensure hire dates are not in the future
- Validate department names against a lookup table
- Check for reasonable salary ranges by department

### Challenge 3: Error Recovery
Modify the pipeline to:
- Store invalid records in a separate "quarantine" table
- Send email notifications for critical errors
- Implement retry logic for temporary failures

### Challenge 4: Performance Optimization
Optimize the pipeline for larger datasets:
- Add data chunking for memory efficiency
- Implement parallel processing
- Add database connection pooling

## 📚 Next Steps

Congratulations! You've built your first production-ready data pipeline. Here's what to explore next:

1. **[Data Sources Deep Dive](../02-core-concepts/data-sources.md)**: Learn about different data sources and formats
2. **[Advanced Processing](../02-core-concepts/pandas-processing.md)**: Master complex transformations
3. **[Data Quality Mastery](../02-core-concepts/data-quality.md)**: Become an expert in data validation
4. **[Performance Optimization](../03-intermediate/performance.md)**: Handle big data efficiently

## 🤔 Common Questions

**Q: What if my CSV has different column names?**
A: Use the `column_mappings` in the transform section to standardize names.

**Q: How do I handle multiple file formats?**
A: Create separate pipeline configurations for each format, or use our auto-detection features.

**Q: Can I process files from cloud storage?**
A: Yes! Use S3, Azure Blob, or Google Cloud Storage sources.

**Q: How do I schedule this pipeline?**
A: Use our built-in scheduler or integrate with tools like Apache Airflow.

## 💡 Pro Tips

1. **Start Simple**: Begin with basic transformations and add complexity gradually
2. **Test with Bad Data**: Always test your pipeline with invalid/missing data
3. **Monitor Everything**: Set up comprehensive monitoring from day one
4. **Document Decisions**: Use configuration comments to explain your choices
5. **Version Control**: Keep your pipeline configurations in git

---

**🎉 Congratulations!** You've completed your first data pipeline tutorial. You now understand the fundamentals of data pipeline configuration, processing, validation, and monitoring.

**Ready for more?** Continue to [Data Sources and Formats](../02-core-concepts/data-sources.md) to learn about working with different types of data.