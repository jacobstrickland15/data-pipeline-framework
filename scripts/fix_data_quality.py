#!/usr/bin/env python3
"""
Automated Data Quality Fixer
Automatically detects and fixes common data quality issues.
"""

import sys
import os
import pandas as pd
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_pipeline.utils.data_cleaner import DataCleaner
from data_pipeline.utils.quality_monitor import DataQualityMonitor, QualityMetrics


def load_cleaning_config(config_path: str = None):
    """Load data cleaning configuration."""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "data_cleaning_rules.yaml"
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load config from {config_path}: {e}")
        # Return default config
        return {
            "missing_values": {
                "email": "default",
                "phone": "default"
            },
            "phone_columns": ["phone"],
            "email_columns": ["email"],
            "email_strategy": "fix_common",
            "remove_duplicates": {
                "subset": ["customer_id"],
                "keep": "first"
            }
        }


def fix_data_quality_issues(file_path: str, output_path: str = None, config_path: str = None):
    """Fix data quality issues in a CSV file."""
    
    print(f"🔧 FIXING DATA QUALITY ISSUES")
    print(f"📁 Input file: {file_path}")
    
    # Load data
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return False
    
    # Load cleaning configuration
    config = load_cleaning_config(config_path)
    print(f"⚙️  Loaded cleaning configuration")
    
    # Initialize cleaner
    cleaner = DataCleaner()
    
    # Show original data quality issues
    print(f"\n📊 ORIGINAL DATA QUALITY ANALYSIS:")
    
    # Check missing values
    for column in df.columns:
        missing_pct = (df[column].isnull().sum() / len(df)) * 100
        if missing_pct > 0:
            print(f"   📋 Column '{column}': {missing_pct:.1f}% missing values")
    
    # Check for duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"   👥 {duplicates} duplicate records found")
    
    # Check email formats (if email column exists)
    email_cols = [col for col in df.columns if 'email' in col.lower()]
    for col in email_cols:
        invalid_emails = 0
        for email in df[col].dropna():
            if '@' not in str(email) or '.' not in str(email).split('@')[-1]:
                invalid_emails += 1
        if invalid_emails > 0:
            print(f"   📧 Column '{col}': {invalid_emails} invalid email formats")
    
    # Check phone formats (if phone column exists)  
    phone_cols = [col for col in df.columns if 'phone' in col.lower()]
    for col in phone_cols:
        non_standard = 0
        for phone in df[col].dropna():
            phone_str = str(phone)
            if not any(char.isdigit() for char in phone_str):
                non_standard += 1
        if non_standard > 0:
            print(f"   📞 Column '{col}': {non_standard} non-standard phone formats")
    
    print(f"\n🔧 APPLYING AUTOMATED FIXES:")
    
    # Apply cleaning
    df_cleaned, results = cleaner.clean_dataset(df, config)
    
    # Show results
    for result in results:
        print(f"   ✅ {result.issue_type} in '{result.column}': Fixed {result.fixed_count}/{result.total_count} ({result.success_rate:.1f}%)")
    
    # Calculate final quality metrics
    print(f"\n📈 FINAL QUALITY SCORES:")
    
    # Completeness
    completeness = QualityMetrics.completeness(df_cleaned) * 100
    print(f"   📊 Completeness: {completeness:.1f}%")
    
    # Show improvement
    original_completeness = QualityMetrics.completeness(df) * 100
    improvement = completeness - original_completeness
    if improvement > 0:
        print(f"   📈 Improvement: +{improvement:.1f}%")
    
    # Save cleaned data
    if output_path is None:
        output_path = file_path.replace('.csv', '_cleaned.csv')
    
    df_cleaned.to_csv(output_path, index=False)
    print(f"💾 Saved cleaned data to: {output_path}")
    
    # Generate cleaning report
    report_path = output_path.replace('.csv', '_cleaning_report.json')
    summary = cleaner.get_cleaning_summary()
    
    import json
    with open(report_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"📋 Saved cleaning report to: {report_path}")
    
    print(f"\n✅ DATA QUALITY FIXING COMPLETED!")
    print(f"   Original records: {len(df)}")
    print(f"   Cleaned records: {len(df_cleaned)}")
    print(f"   Quality improvement: {improvement:.1f}%")
    
    return True


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python fix_data_quality.py <input_file.csv> [output_file.csv] [config.yaml]")
        print("\nExample:")
        print("  python fix_data_quality.py data/customers.csv")
        print("  python fix_data_quality.py data/customers.csv data/customers_clean.csv")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    config_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return
    
    success = fix_data_quality_issues(input_file, output_file, config_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()