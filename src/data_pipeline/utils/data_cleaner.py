"""Data cleaning utilities to fix quality issues automatically."""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CleaningResult:
    """Result of data cleaning operation."""
    column: str
    issue_type: str
    fixed_count: int
    total_count: int
    success_rate: float
    details: Optional[str] = None


class DataCleaner:
    """Automated data cleaning utilities."""
    
    def __init__(self):
        self.cleaning_results: List[CleaningResult] = []
        
        # Phone number patterns for standardization
        self.phone_patterns = [
            (r'^\+?1?[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})$', r'(\1) \2-\3'),  # US format
            (r'^(\d{10})$', r'(\1[:3]) \1[3:6]-\1[6:]'),  # 10 digits
            (r'^\+(\d{1,3})[-.\s]?(\d{3,4})[-.\s]?(\d{3,4})[-.\s]?(\d{3,4})$', r'+\1 \2-\3-\4'),  # International
        ]
        
        # Email validation pattern
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    def fix_missing_values(self, df: pd.DataFrame, column: str, strategy: str = 'drop') -> pd.DataFrame:
        """Fix missing values in specified column.
        
        Args:
            df: DataFrame to clean
            column: Column name to fix
            strategy: 'drop', 'forward_fill', 'backward_fill', 'mean', 'median', 'mode', 'default'
        
        Returns:
            Cleaned DataFrame
        """
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame")
            return df
        
        original_nulls = df[column].isnull().sum()
        total_rows = len(df)
        
        df_cleaned = df.copy()
        
        if strategy == 'drop':
            df_cleaned = df_cleaned.dropna(subset=[column])
            fixed_count = original_nulls
            
        elif strategy == 'forward_fill':
            df_cleaned[column] = df_cleaned[column].fillna(method='ffill')
            fixed_count = original_nulls - df_cleaned[column].isnull().sum()
            
        elif strategy == 'backward_fill':
            df_cleaned[column] = df_cleaned[column].fillna(method='bfill')
            fixed_count = original_nulls - df_cleaned[column].isnull().sum()
            
        elif strategy == 'mean' and pd.api.types.is_numeric_dtype(df[column]):
            mean_val = df[column].mean()
            df_cleaned[column] = df_cleaned[column].fillna(mean_val)
            fixed_count = original_nulls
            
        elif strategy == 'median' and pd.api.types.is_numeric_dtype(df[column]):
            median_val = df[column].median()
            df_cleaned[column] = df_cleaned[column].fillna(median_val)
            fixed_count = original_nulls
            
        elif strategy == 'mode':
            mode_val = df[column].mode().iloc[0] if not df[column].mode().empty else 'Unknown'
            df_cleaned[column] = df_cleaned[column].fillna(mode_val)
            fixed_count = original_nulls
            
        elif strategy == 'default':
            default_values = {
                'email': 'noemail@unknown.com',
                'phone': '000-000-0000',
                'name': 'Unknown',
                'address': 'No Address'
            }
            default_val = default_values.get(column, 'Unknown')
            df_cleaned[column] = df_cleaned[column].fillna(default_val)
            fixed_count = original_nulls
        
        else:
            logger.warning(f"Unknown strategy '{strategy}' or incompatible with column type")
            return df
        
        # Record the cleaning result
        result = CleaningResult(
            column=column,
            issue_type="missing_values",
            fixed_count=fixed_count,
            total_count=total_rows,
            success_rate=(fixed_count / original_nulls * 100) if original_nulls > 0 else 100,
            details=f"Strategy: {strategy}, Original nulls: {original_nulls}"
        )
        self.cleaning_results.append(result)
        
        logger.info(f"Fixed {fixed_count}/{original_nulls} missing values in '{column}' using {strategy}")
        return df_cleaned
    
    def standardize_phone_numbers(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Standardize phone number formats.
        
        Args:
            df: DataFrame to clean
            column: Column containing phone numbers
            
        Returns:
            DataFrame with standardized phone numbers
        """
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame")
            return df
        
        df_cleaned = df.copy()
        fixed_count = 0
        total_phones = len(df_cleaned[column].dropna())
        
        for idx, phone in df_cleaned[column].items():
            if pd.isna(phone):
                continue
                
            phone_str = str(phone).strip()
            original_phone = phone_str
            
            # Try each pattern
            for pattern, replacement in self.phone_patterns:
                match = re.match(pattern, phone_str)
                if match:
                    if pattern == r'^(\d{10})$':  # Special handling for 10-digit format
                        phone_str = f"({phone_str[:3]}) {phone_str[3:6]}-{phone_str[6:]}"
                    else:
                        phone_str = re.sub(pattern, replacement, phone_str)
                    break
            
            # If we changed the format, count it as fixed
            if phone_str != original_phone:
                df_cleaned.at[idx, column] = phone_str
                fixed_count += 1
        
        # Record the cleaning result
        result = CleaningResult(
            column=column,
            issue_type="invalid_format",
            fixed_count=fixed_count,
            total_count=total_phones,
            success_rate=(fixed_count / total_phones * 100) if total_phones > 0 else 100,
            details=f"Standardized to (XXX) XXX-XXXX format"
        )
        self.cleaning_results.append(result)
        
        logger.info(f"Standardized {fixed_count}/{total_phones} phone numbers in '{column}'")
        return df_cleaned
    
    def validate_emails(self, df: pd.DataFrame, column: str, fix_strategy: str = 'flag') -> pd.DataFrame:
        """Validate and optionally fix email formats.
        
        Args:
            df: DataFrame to clean
            column: Column containing emails
            fix_strategy: 'flag', 'remove', 'fix_common'
            
        Returns:
            DataFrame with validated emails
        """
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame")
            return df
        
        df_cleaned = df.copy()
        invalid_count = 0
        fixed_count = 0
        total_emails = len(df_cleaned[column].dropna())
        
        for idx, email in df_cleaned[column].items():
            if pd.isna(email):
                continue
                
            email_str = str(email).strip().lower()
            
            if not self.email_pattern.match(email_str):
                invalid_count += 1
                
                if fix_strategy == 'flag':
                    # Add a flag column for invalid emails
                    if f'{column}_valid' not in df_cleaned.columns:
                        df_cleaned[f'{column}_valid'] = True
                    df_cleaned.at[idx, f'{column}_valid'] = False
                    
                elif fix_strategy == 'remove':
                    df_cleaned.at[idx, column] = np.nan
                    fixed_count += 1
                    
                elif fix_strategy == 'fix_common':
                    # Try to fix common email issues
                    fixed_email = self._fix_common_email_issues(email_str)
                    if fixed_email != email_str and self.email_pattern.match(fixed_email):
                        df_cleaned.at[idx, column] = fixed_email
                        fixed_count += 1
        
        # Record the cleaning result
        result = CleaningResult(
            column=column,
            issue_type="invalid_format",
            fixed_count=fixed_count if fix_strategy != 'flag' else invalid_count,
            total_count=total_emails,
            success_rate=((total_emails - invalid_count) / total_emails * 100) if total_emails > 0 else 100,
            details=f"Strategy: {fix_strategy}, Invalid emails: {invalid_count}"
        )
        self.cleaning_results.append(result)
        
        logger.info(f"Validated {total_emails} emails, found {invalid_count} invalid, fixed {fixed_count}")
        return df_cleaned
    
    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None, 
                         keep: str = 'first') -> pd.DataFrame:
        """Remove duplicate records.
        
        Args:
            df: DataFrame to clean
            subset: Columns to check for duplicates
            keep: Which duplicate to keep ('first', 'last', False)
            
        Returns:
            DataFrame with duplicates removed
        """
        original_count = len(df)
        df_cleaned = df.drop_duplicates(subset=subset, keep=keep)
        removed_count = original_count - len(df_cleaned)
        
        # Record the cleaning result
        result = CleaningResult(
            column=', '.join(subset) if subset else 'all_columns',
            issue_type="duplicate_records",
            fixed_count=removed_count,
            total_count=original_count,
            success_rate=(removed_count / original_count * 100) if original_count > 0 else 100,
            details=f"Keep strategy: {keep}, Subset: {subset}"
        )
        self.cleaning_results.append(result)
        
        logger.info(f"Removed {removed_count} duplicate records from {original_count} total")
        return df_cleaned
    
    def _fix_common_email_issues(self, email: str) -> str:
        """Fix common email format issues."""
        # Remove extra spaces
        email = re.sub(r'\s+', '', email)
        
        # Fix common domain typos
        domain_fixes = {
            'gmial.com': 'gmail.com',
            'gmai.com': 'gmail.com',
            'yahooo.com': 'yahoo.com',
            'hotmial.com': 'hotmail.com',
            'outlok.com': 'outlook.com'
        }
        
        for typo, correct in domain_fixes.items():
            email = email.replace(typo, correct)
        
        # Ensure single @ symbol
        if email.count('@') != 1:
            return email
        
        # Basic format check
        parts = email.split('@')
        if len(parts) == 2 and parts[0] and parts[1]:
            # Ensure domain has at least one dot
            if '.' not in parts[1]:
                return email
            return email
        
        return email
    
    def clean_dataset(self, df: pd.DataFrame, cleaning_config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[CleaningResult]]:
        """Clean entire dataset based on configuration.
        
        Args:
            df: DataFrame to clean
            cleaning_config: Configuration dictionary with cleaning rules
            
        Returns:
            Tuple of (cleaned_df, cleaning_results)
        """
        self.cleaning_results = []  # Reset results
        df_cleaned = df.copy()
        
        # Handle missing values
        if 'missing_values' in cleaning_config:
            for column, strategy in cleaning_config['missing_values'].items():
                df_cleaned = self.fix_missing_values(df_cleaned, column, strategy)
        
        # Standardize phone numbers
        if 'phone_columns' in cleaning_config:
            for column in cleaning_config['phone_columns']:
                df_cleaned = self.standardize_phone_numbers(df_cleaned, column)
        
        # Validate emails
        if 'email_columns' in cleaning_config:
            for column in cleaning_config['email_columns']:
                strategy = cleaning_config.get('email_strategy', 'flag')
                df_cleaned = self.validate_emails(df_cleaned, column, strategy)
        
        # Remove duplicates
        if 'remove_duplicates' in cleaning_config:
            config = cleaning_config['remove_duplicates']
            df_cleaned = self.remove_duplicates(
                df_cleaned, 
                subset=config.get('subset'),
                keep=config.get('keep', 'first')
            )
        
        return df_cleaned, self.cleaning_results
    
    def get_cleaning_summary(self) -> Dict[str, Any]:
        """Get summary of all cleaning operations."""
        if not self.cleaning_results:
            return {"message": "No cleaning operations performed"}
        
        summary = {
            "total_operations": len(self.cleaning_results),
            "operations": [],
            "overall_success_rate": 0
        }
        
        total_success_rate = 0
        for result in self.cleaning_results:
            summary["operations"].append({
                "column": result.column,
                "issue_type": result.issue_type,
                "fixed_count": result.fixed_count,
                "total_count": result.total_count,
                "success_rate": result.success_rate,
                "details": result.details
            })
            total_success_rate += result.success_rate
        
        summary["overall_success_rate"] = total_success_rate / len(self.cleaning_results)
        return summary