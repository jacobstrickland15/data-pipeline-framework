"""Data validation utilities using Great Expectations framework."""

from typing import Dict, Any, List, Optional, Union
import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.checkpoint import SimpleCheckpoint
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class DataValidator:
    """Data validation using Great Expectations."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.context_root_dir = Path(self.config.get('context_root_dir', './great_expectations'))
        self.store_results = self.config.get('store_results', True)
        
        # Initialize Great Expectations context
        self.context = self._initialize_context()
        
    def _initialize_context(self) -> BaseDataContext:
        """Initialize Great Expectations data context."""
        try:
            # Try to get existing context
            if self.context_root_dir.exists():
                context = gx.get_context(context_root_dir=str(self.context_root_dir))
            else:
                # Create new context
                context = gx.get_context(mode="file", context_root_dir=str(self.context_root_dir))
            
            logger.info("Great Expectations context initialized")
            return context
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations context: {e}")
            # Fallback to basic context
            return gx.get_context()
    
    def create_expectation_suite(self, suite_name: str, data: pd.DataFrame, 
                               auto_generate: bool = True) -> ExpectationSuite:
        """Create an expectation suite for the dataset."""
        try:
            # Create or get existing suite
            suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
            
            if auto_generate:
                # Auto-generate basic expectations
                expectations = self._generate_basic_expectations(data)
                
                # Add expectations to suite
                for expectation in expectations:
                    suite.add_expectation(expectation)
            
            logger.info(f"Created expectation suite '{suite_name}' with {len(suite.expectations)} expectations")
            return suite
            
        except Exception as e:
            logger.error(f"Error creating expectation suite: {e}")
            raise
    
    def _generate_basic_expectations(self, data: pd.DataFrame) -> List[ExpectationConfiguration]:
        """Auto-generate basic expectations from data analysis."""
        expectations = []
        
        for column in data.columns:
            col_data = data[column]
            
            # Expect column to exist
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": column}
                )
            )
            
            # Non-null expectations (if column has < 10% nulls)
            null_pct = (col_data.isnull().sum() / len(col_data)) * 100
            if null_pct < 10:
                expectations.append(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={"column": column}
                    )
                )
            
            # Type-specific expectations
            if pd.api.types.is_numeric_dtype(col_data):
                expectations.extend(self._generate_numeric_expectations(col_data, column))
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                expectations.extend(self._generate_datetime_expectations(col_data, column))
            else:
                expectations.extend(self._generate_string_expectations(col_data, column))
        
        # Dataset-level expectations
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={"min_value": max(1, len(data) - 1000), "max_value": len(data) + 1000}
            )
        )
        
        return expectations
    
    def _generate_numeric_expectations(self, col_data: pd.Series, column: str) -> List[ExpectationConfiguration]:
        """Generate expectations for numeric columns."""
        expectations = []
        clean_data = col_data.dropna()
        
        if len(clean_data) == 0:
            return expectations
        
        # Range expectations
        min_val, max_val = clean_data.min(), clean_data.max()
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": column, "min_value": float(min_val), "max_value": float(max_val)}
            )
        )
        
        # Integer type validation
        if pd.api.types.is_integer_dtype(col_data):
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={"column": column, "type_": "int"}
                )
            )
        
        # Outlier detection
        Q1, Q3 = clean_data.quantile([0.25, 0.75])
        IQR = Q3 - Q1
        lower_bound = Q1 - 3 * IQR  # More lenient than 1.5 * IQR
        upper_bound = Q3 + 3 * IQR
        
        if not np.isinf(lower_bound) and not np.isinf(upper_bound):
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": column, 
                        "min_value": float(lower_bound), 
                        "max_value": float(upper_bound),
                        "mostly": 0.95  # Allow 5% outliers
                    }
                )
            )
        
        return expectations
    
    def _generate_datetime_expectations(self, col_data: pd.Series, column: str) -> List[ExpectationConfiguration]:
        """Generate expectations for datetime columns."""
        expectations = []
        clean_data = col_data.dropna()
        
        if len(clean_data) == 0:
            return expectations
        
        # Date range expectations
        min_date, max_date = clean_data.min(), clean_data.max()
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": column, 
                    "min_value": min_date.isoformat() if hasattr(min_date, 'isoformat') else str(min_date),
                    "max_value": max_date.isoformat() if hasattr(max_date, 'isoformat') else str(max_date)
                }
            )
        )
        
        return expectations
    
    def _generate_string_expectations(self, col_data: pd.Series, column: str) -> List[ExpectationConfiguration]:
        """Generate expectations for string columns."""
        expectations = []
        clean_data = col_data.dropna().astype(str)
        
        if len(clean_data) == 0:
            return expectations
        
        # Length expectations
        lengths = clean_data.str.len()
        min_length, max_length = lengths.min(), lengths.max()
        
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_value_lengths_to_be_between",
                kwargs={"column": column, "min_value": int(min_length), "max_value": int(max_length)}
            )
        )
        
        # Unique values if low cardinality
        unique_count = col_data.nunique()
        if unique_count <= 20 and unique_count > 1:  # Low cardinality
            unique_values = clean_data.unique().tolist()[:20]  # Limit to 20 values
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={"column": column, "value_set": unique_values}
                )
            )
        
        # Pattern expectations for specific data types
        if self._is_email_column(clean_data):
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_match_regex",
                    kwargs={"column": column, "regex": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', "mostly": 0.8}
                )
            )
        elif self._is_phone_column(clean_data):
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_match_regex",
                    kwargs={"column": column, "regex": r'^[\+]?[1-9]?[0-9]{7,15}$', "mostly": 0.8}
                )
            )
        
        return expectations
    
    def _is_email_column(self, data: pd.Series) -> bool:
        """Check if string column contains emails."""
        import re
        sample = data.head(100)
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        matches = sample.str.match(email_pattern).sum()
        return matches / len(sample) > 0.7
    
    def _is_phone_column(self, data: pd.Series) -> bool:
        """Check if string column contains phone numbers."""
        import re
        sample = data.head(100)
        phone_pattern = r'^[\+]?[1-9]?[0-9]{7,15}$'
        matches = sample.str.match(phone_pattern).sum()
        return matches / len(sample) > 0.5
    
    def validate_data(self, data: pd.DataFrame, suite_name: str, 
                     run_name: Optional[str] = None) -> Dict[str, Any]:
        """Validate data against expectation suite."""
        try:
            # Get expectation suite
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
            
            # Create batch
            batch = self.context.get_batch_list(
                batch_request=self.context.build_batch_request(
                    datasource_name="pandas_datasource",
                    data_asset_name="validation_data", 
                    dataframe=data
                )
            )[0]
            
            # Run validation
            results = self.context.run_validation_operator(
                validation_operator_name="action_list_operator",
                assets_to_validate=[batch],
                run_id=run_name or f"validation_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            # Process results
            validation_result = results.list_validation_results()[0]
            
            summary = {
                'success': validation_result['success'],
                'run_name': validation_result.get('meta', {}).get('run_id'),
                'validation_time': validation_result.get('meta', {}).get('validation_time'),
                'statistics': {
                    'evaluated_expectations': validation_result['statistics']['evaluated_expectations'],
                    'successful_expectations': validation_result['statistics']['successful_expectations'], 
                    'unsuccessful_expectations': validation_result['statistics']['unsuccessful_expectations'],
                    'success_percent': validation_result['statistics']['success_percent']
                },
                'failed_expectations': [],
                'warnings': []
            }
            
            # Extract failed expectations
            for result in validation_result['results']:
                if not result['success']:
                    summary['failed_expectations'].append({
                        'expectation_type': result['expectation_config']['expectation_type'],
                        'column': result['expectation_config']['kwargs'].get('column'),
                        'details': result.get('result', {}),
                        'exception_info': result.get('exception_info')
                    })
            
            logger.info(f"Validation completed. Success: {summary['success']}, Success rate: {summary['statistics']['success_percent']:.1f}%")
            return summary
            
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            raise
    
    def create_custom_expectation_suite(self, suite_name: str, expectations_config: List[Dict[str, Any]]) -> ExpectationSuite:
        """Create expectation suite from custom configuration."""
        try:
            suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
            
            for exp_config in expectations_config:
                expectation = ExpectationConfiguration(
                    expectation_type=exp_config['expectation_type'],
                    kwargs=exp_config.get('kwargs', {})
                )
                suite.add_expectation(expectation)
            
            logger.info(f"Created custom expectation suite '{suite_name}' with {len(expectations_config)} expectations")
            return suite
            
        except Exception as e:
            logger.error(f"Error creating custom expectation suite: {e}")
            raise
    
    def generate_data_docs(self) -> str:
        """Generate and build data documentation."""
        try:
            self.context.build_data_docs()
            
            # Get data docs sites
            data_docs_sites = self.context.get_docs_sites_urls()
            
            if data_docs_sites:
                docs_url = list(data_docs_sites.values())[0]
                logger.info(f"Data docs generated at: {docs_url}")
                return docs_url
            else:
                logger.warning("No data docs sites configured")
                return ""
                
        except Exception as e:
            logger.error(f"Error generating data docs: {e}")
            raise
    
    def save_expectation_suite(self, suite_name: str, file_path: Optional[str] = None) -> str:
        """Save expectation suite to file."""
        try:
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
            
            if file_path is None:
                file_path = f"expectation_suite_{suite_name}.json"
            
            # Convert suite to dictionary and save
            suite_dict = suite.to_json_dict()
            
            with open(file_path, 'w') as f:
                json.dump(suite_dict, f, indent=2, default=str)
            
            logger.info(f"Expectation suite saved to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error saving expectation suite: {e}")
            raise
    
    def load_expectation_suite(self, file_path: str) -> str:
        """Load expectation suite from file."""
        try:
            with open(file_path, 'r') as f:
                suite_dict = json.load(f)
            
            suite_name = suite_dict['expectation_suite_name']
            
            # Create expectations from dictionary
            expectations = []
            for exp_dict in suite_dict['expectations']:
                expectations.append(ExpectationConfiguration(**exp_dict))
            
            # Create suite
            suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
            
            # Add expectations
            for expectation in expectations:
                suite.add_expectation(expectation)
            
            logger.info(f"Loaded expectation suite '{suite_name}' with {len(expectations)} expectations")
            return suite_name
            
        except Exception as e:
            logger.error(f"Error loading expectation suite: {e}")
            raise
    
    def get_validation_summary(self, suite_name: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Get a quick validation summary without running full validation."""
        try:
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
            
            summary = {
                'suite_name': suite_name,
                'total_expectations': len(suite.expectations),
                'expectation_types': {},
                'columns_covered': set(),
                'data_shape': data.shape,
                'estimated_issues': []
            }
            
            # Analyze expectations
            for expectation in suite.expectations:
                exp_type = expectation.expectation_type
                summary['expectation_types'][exp_type] = summary['expectation_types'].get(exp_type, 0) + 1
                
                # Track columns
                if 'column' in expectation.kwargs:
                    summary['columns_covered'].add(expectation.kwargs['column'])
            
            summary['columns_covered'] = list(summary['columns_covered'])
            
            # Quick data quality checks
            missing_columns = set(summary['columns_covered']) - set(data.columns)
            if missing_columns:
                summary['estimated_issues'].append(f"Missing columns: {missing_columns}")
            
            null_pct = (data.isnull().sum().sum() / data.size) * 100
            if null_pct > 20:
                summary['estimated_issues'].append(f"High null percentage: {null_pct:.1f}%")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting validation summary: {e}")
            raise