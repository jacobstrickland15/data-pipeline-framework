"""Data profiling utilities for comprehensive dataset analysis."""

from typing import Dict, Any, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class DataProfiler:
    """Comprehensive data profiling and analysis."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.sample_size = self.config.get('sample_size', 100000)
        self.correlation_threshold = self.config.get('correlation_threshold', 0.7)
        self.output_format = self.config.get('output_format', 'html')  # html, json, both
        
    def profile_dataset(self, data: pd.DataFrame, dataset_name: str = "Dataset") -> Dict[str, Any]:
        """Generate comprehensive profile of the dataset."""
        logger.info(f"Starting data profiling for {dataset_name}")
        
        # Sample data if too large
        if len(data) > self.sample_size:
            sample_data = data.sample(n=self.sample_size, random_state=42)
            logger.info(f"Sampling {self.sample_size} rows from {len(data)} total rows")
        else:
            sample_data = data
            
        profile = {
            'dataset_info': self._get_dataset_info(data, sample_data, dataset_name),
            'column_profiles': {},
            'correlations': {},
            'data_quality': {},
            'statistical_summary': {},
            'visualizations': {},
            'recommendations': []
        }
        
        # Profile each column
        for column in data.columns:
            profile['column_profiles'][column] = self._profile_column(sample_data[column], column)
        
        # Calculate correlations
        profile['correlations'] = self._calculate_correlations(sample_data)
        
        # Data quality assessment
        profile['data_quality'] = self._assess_data_quality(sample_data)
        
        # Statistical summary
        profile['statistical_summary'] = self._generate_statistical_summary(sample_data)
        
        # Generate visualizations
        profile['visualizations'] = self._generate_visualizations(sample_data)
        
        # Generate recommendations
        profile['recommendations'] = self._generate_recommendations(profile)
        
        logger.info(f"Data profiling completed for {dataset_name}")
        return profile
    
    def _get_dataset_info(self, original_data: pd.DataFrame, sample_data: pd.DataFrame, name: str) -> Dict[str, Any]:
        """Get basic dataset information."""
        return {
            'name': name,
            'total_rows': len(original_data),
            'sample_rows': len(sample_data),
            'total_columns': len(original_data.columns),
            'memory_usage_mb': original_data.memory_usage(deep=True).sum() / 1024 / 1024,
            'dtypes_summary': original_data.dtypes.value_counts().to_dict(),
            'created_at': datetime.now().isoformat(),
            'shape': original_data.shape
        }
    
    def _profile_column(self, column_data: pd.Series, column_name: str) -> Dict[str, Any]:
        """Profile individual column."""
        profile = {
            'name': column_name,
            'dtype': str(column_data.dtype),
            'non_null_count': column_data.count(),
            'null_count': column_data.isnull().sum(),
            'null_percentage': (column_data.isnull().sum() / len(column_data)) * 100,
            'unique_count': column_data.nunique(),
            'unique_percentage': (column_data.nunique() / len(column_data)) * 100,
            'memory_usage': column_data.memory_usage(deep=True),
            'most_frequent_values': {},
            'least_frequent_values': {},
            'statistics': {}
        }
        
        # Get value counts for categorical analysis
        if column_data.count() > 0:
            value_counts = column_data.value_counts()
            profile['most_frequent_values'] = value_counts.head(10).to_dict()
            profile['least_frequent_values'] = value_counts.tail(5).to_dict()
            
            # Type-specific statistics
            if pd.api.types.is_numeric_dtype(column_data):
                profile['statistics'] = self._numeric_statistics(column_data)
            elif pd.api.types.is_datetime64_any_dtype(column_data):
                profile['statistics'] = self._datetime_statistics(column_data)
            else:
                profile['statistics'] = self._categorical_statistics(column_data)
        
        return profile
    
    def _numeric_statistics(self, data: pd.Series) -> Dict[str, Any]:
        """Calculate statistics for numeric columns."""
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {}
            
        stats = {
            'min': float(clean_data.min()),
            'max': float(clean_data.max()),
            'mean': float(clean_data.mean()),
            'median': float(clean_data.median()),
            'std': float(clean_data.std()),
            'variance': float(clean_data.var()),
            'skewness': float(clean_data.skew()),
            'kurtosis': float(clean_data.kurtosis()),
            'q25': float(clean_data.quantile(0.25)),
            'q75': float(clean_data.quantile(0.75)),
            'iqr': float(clean_data.quantile(0.75) - clean_data.quantile(0.25)),
            'zeros_count': int((clean_data == 0).sum()),
            'positive_count': int((clean_data > 0).sum()),
            'negative_count': int((clean_data < 0).sum())
        }
        
        # Outlier detection using IQR method
        Q1, Q3 = stats['q25'], stats['q75']
        IQR = stats['iqr']
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = clean_data[(clean_data < lower_bound) | (clean_data > upper_bound)]
        stats['outliers_count'] = len(outliers)
        stats['outliers_percentage'] = (len(outliers) / len(clean_data)) * 100
        
        # Distribution analysis
        stats['distribution_type'] = self._detect_distribution_type(clean_data)
        
        return stats
    
    def _datetime_statistics(self, data: pd.Series) -> Dict[str, Any]:
        """Calculate statistics for datetime columns."""
        clean_data = data.dropna()
        
        if len(clean_data) == 0:
            return {}
            
        return {
            'min': clean_data.min().isoformat() if hasattr(clean_data.min(), 'isoformat') else str(clean_data.min()),
            'max': clean_data.max().isoformat() if hasattr(clean_data.max(), 'isoformat') else str(clean_data.max()),
            'range_days': (clean_data.max() - clean_data.min()).days if hasattr(clean_data.max() - clean_data.min(), 'days') else None,
            'most_common_year': clean_data.dt.year.mode().iloc[0] if hasattr(clean_data.dt, 'year') and not clean_data.dt.year.mode().empty else None,
            'most_common_month': clean_data.dt.month.mode().iloc[0] if hasattr(clean_data.dt, 'month') and not clean_data.dt.month.mode().empty else None,
            'most_common_day': clean_data.dt.day.mode().iloc[0] if hasattr(clean_data.dt, 'day') and not clean_data.dt.day.mode().empty else None
        }
    
    def _categorical_statistics(self, data: pd.Series) -> Dict[str, Any]:
        """Calculate statistics for categorical columns."""
        clean_data = data.dropna().astype(str)
        
        if len(clean_data) == 0:
            return {}
            
        lengths = clean_data.str.len()
        
        return {
            'min_length': int(lengths.min()) if not lengths.empty else 0,
            'max_length': int(lengths.max()) if not lengths.empty else 0,
            'avg_length': float(lengths.mean()) if not lengths.empty else 0,
            'empty_strings_count': int((clean_data == '').sum()),
            'whitespace_only_count': int(clean_data.str.strip().eq('').sum()),
            'most_common_length': int(lengths.mode().iloc[0]) if not lengths.mode().empty else 0,
            'contains_numbers': int(clean_data.str.contains(r'\d').sum()),
            'contains_special_chars': int(clean_data.str.contains(r'[^a-zA-Z0-9\s]').sum())
        }
    
    def _detect_distribution_type(self, data: pd.Series) -> str:
        """Detect the likely distribution type of numeric data."""
        if len(data) < 10:
            return "insufficient_data"
        
        skewness = data.skew()
        kurtosis = data.kurtosis()
        
        # Simple heuristics for distribution detection
        if abs(skewness) < 0.5 and abs(kurtosis) < 3:
            return "normal"
        elif skewness > 1:
            return "right_skewed"
        elif skewness < -1:
            return "left_skewed"
        elif kurtosis > 3:
            return "heavy_tailed"
        elif kurtosis < 0:
            return "light_tailed"
        else:
            return "unknown"
    
    def _calculate_correlations(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate correlations between numeric columns."""
        numeric_cols = data.select_dtypes(include=[np.number])
        
        if numeric_cols.empty or len(numeric_cols.columns) < 2:
            return {'pearson': {}, 'spearman': {}, 'highly_correlated_pairs': []}
        
        # Pearson correlation
        pearson_corr = numeric_cols.corr(method='pearson')
        spearman_corr = numeric_cols.corr(method='spearman')
        
        # Find highly correlated pairs
        highly_correlated = []
        for i in range(len(pearson_corr.columns)):
            for j in range(i+1, len(pearson_corr.columns)):
                corr_value = pearson_corr.iloc[i, j]
                if abs(corr_value) >= self.correlation_threshold:
                    highly_correlated.append({
                        'column1': pearson_corr.columns[i],
                        'column2': pearson_corr.columns[j],
                        'pearson_correlation': float(corr_value),
                        'spearman_correlation': float(spearman_corr.iloc[i, j])
                    })
        
        return {
            'pearson': pearson_corr.to_dict(),
            'spearman': spearman_corr.to_dict(),
            'highly_correlated_pairs': highly_correlated
        }
    
    def _assess_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess overall data quality."""
        total_cells = data.size
        null_cells = data.isnull().sum().sum()
        duplicate_rows = data.duplicated().sum()
        
        # Column-level quality issues
        quality_issues = {
            'high_null_columns': [],
            'low_variance_columns': [],
            'high_cardinality_columns': [],
            'potential_id_columns': []
        }
        
        for column in data.columns:
            col_data = data[column]
            null_pct = (col_data.isnull().sum() / len(col_data)) * 100
            unique_pct = (col_data.nunique() / len(col_data)) * 100
            
            if null_pct > 50:
                quality_issues['high_null_columns'].append(column)
            
            if pd.api.types.is_numeric_dtype(col_data) and col_data.std() == 0:
                quality_issues['low_variance_columns'].append(column)
                
            if unique_pct > 95:
                quality_issues['high_cardinality_columns'].append(column)
                
            if unique_pct == 100 and len(col_data.dropna()) > 0:
                quality_issues['potential_id_columns'].append(column)
        
        return {
            'completeness_score': ((total_cells - null_cells) / total_cells) * 100,
            'duplicate_rows': int(duplicate_rows),
            'duplicate_percentage': (duplicate_rows / len(data)) * 100,
            'quality_issues': quality_issues,
            'overall_score': self._calculate_overall_quality_score(data, quality_issues)
        }
    
    def _calculate_overall_quality_score(self, data: pd.DataFrame, quality_issues: Dict[str, List]) -> float:
        """Calculate overall data quality score."""
        score = 100.0
        
        # Deduct for completeness
        null_pct = (data.isnull().sum().sum() / data.size) * 100
        score -= min(null_pct, 50)  # Max 50 point deduction
        
        # Deduct for duplicates
        dup_pct = (data.duplicated().sum() / len(data)) * 100
        score -= min(dup_pct * 2, 20)  # Max 20 point deduction
        
        # Deduct for quality issues
        total_issues = sum(len(issues) for issues in quality_issues.values())
        score -= min(total_issues * 5, 30)  # Max 30 point deduction
        
        return max(0.0, score)
    
    def _generate_statistical_summary(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive statistical summary."""
        return {
            'basic_stats': data.describe(include='all').to_dict(),
            'numeric_summary': data.describe().to_dict() if not data.select_dtypes(include=[np.number]).empty else {},
            'categorical_summary': data.describe(include=[object]).to_dict() if not data.select_dtypes(include=[object]).empty else {}
        }
    
    def _generate_visualizations(self, data: pd.DataFrame) -> Dict[str, str]:
        """Generate visualization recommendations."""
        viz_recommendations = {
            'distribution_plots': [],
            'correlation_heatmap': False,
            'missing_data_plot': False,
            'outlier_plots': []
        }
        
        # Recommend distribution plots for numeric columns
        numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        viz_recommendations['distribution_plots'] = numeric_cols[:10]  # Limit to first 10
        
        # Recommend correlation heatmap if enough numeric columns
        if len(numeric_cols) >= 2:
            viz_recommendations['correlation_heatmap'] = True
        
        # Recommend missing data visualization if there are missing values
        if data.isnull().sum().sum() > 0:
            viz_recommendations['missing_data_plot'] = True
        
        # Recommend outlier plots for numeric columns with outliers
        for col in numeric_cols[:5]:  # Limit to first 5
            col_data = data[col].dropna()
            if len(col_data) > 0:
                Q1, Q3 = col_data.quantile([0.25, 0.75])
                IQR = Q3 - Q1
                outliers = col_data[(col_data < Q1 - 1.5 * IQR) | (col_data > Q3 + 1.5 * IQR)]
                if len(outliers) > 0:
                    viz_recommendations['outlier_plots'].append(col)
        
        return viz_recommendations
    
    def _generate_recommendations(self, profile: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate actionable recommendations based on profiling results."""
        recommendations = []
        
        # Data quality recommendations
        quality_score = profile['data_quality']['overall_score']
        if quality_score < 70:
            recommendations.append({
                'type': 'data_quality',
                'priority': 'high',
                'title': 'Improve Data Quality',
                'description': f'Overall data quality score is {quality_score:.1f}%. Consider addressing missing values, duplicates, and data inconsistencies.'
            })
        
        # Missing data recommendations
        high_null_cols = profile['data_quality']['quality_issues']['high_null_columns']
        if high_null_cols:
            recommendations.append({
                'type': 'missing_data',
                'priority': 'medium',
                'title': 'Address High Missing Data',
                'description': f'Columns with >50% missing data: {", ".join(high_null_cols)}. Consider imputation or removal.'
            })
        
        # Correlation recommendations
        high_corr_pairs = profile['correlations']['highly_correlated_pairs']
        if high_corr_pairs:
            recommendations.append({
                'type': 'correlation',
                'priority': 'medium',
                'title': 'Review Highly Correlated Features',
                'description': f'Found {len(high_corr_pairs)} highly correlated column pairs. Consider feature selection or dimensionality reduction.'
            })
        
        # Duplicate data recommendations
        dup_pct = profile['data_quality']['duplicate_percentage']
        if dup_pct > 5:
            recommendations.append({
                'type': 'duplicates',
                'priority': 'medium',
                'title': 'Remove Duplicate Records',
                'description': f'{dup_pct:.1f}% of records are duplicates. Consider deduplication.'
            })
        
        # Memory optimization recommendations
        memory_mb = profile['dataset_info']['memory_usage_mb']
        if memory_mb > 1000:  # > 1GB
            recommendations.append({
                'type': 'optimization',
                'priority': 'low',
                'title': 'Optimize Memory Usage',
                'description': f'Dataset uses {memory_mb:.1f}MB of memory. Consider data type optimization or chunked processing.'
            })
        
        return recommendations
    
    def generate_html_report(self, profile: Dict[str, Any], output_path: str = "data_profile_report.html") -> str:
        """Generate HTML report from profile data."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Profile Report - {profile['dataset_info']['name']}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; }}
                .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #e9e9e9; border-radius: 3px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .recommendation {{ padding: 10px; margin: 10px 0; border-left: 4px solid #007bff; background-color: #f8f9fa; }}
                .high-priority {{ border-left-color: #dc3545; }}
                .medium-priority {{ border-left-color: #ffc107; }}
                .low-priority {{ border-left-color: #28a745; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Profile Report: {profile['dataset_info']['name']}</h1>
                <p>Generated on: {profile['dataset_info']['created_at']}</p>
            </div>
            
            <div class="section">
                <h2>Dataset Overview</h2>
                <div class="metric">Rows: {profile['dataset_info']['total_rows']:,}</div>
                <div class="metric">Columns: {profile['dataset_info']['total_columns']}</div>
                <div class="metric">Memory: {profile['dataset_info']['memory_usage_mb']:.2f} MB</div>
                <div class="metric">Quality Score: {profile['data_quality']['overall_score']:.1f}%</div>
            </div>
            
            <div class="section">
                <h2>Data Quality Summary</h2>
                <div class="metric">Completeness: {profile['data_quality']['completeness_score']:.1f}%</div>
                <div class="metric">Duplicates: {profile['data_quality']['duplicate_rows']} ({profile['data_quality']['duplicate_percentage']:.1f}%)</div>
            </div>
            
            <div class="section">
                <h2>Recommendations</h2>
        """
        
        for rec in profile['recommendations']:
            priority_class = f"{rec['priority']}-priority"
            html_content += f"""
                <div class="recommendation {priority_class}">
                    <strong>{rec['title']}</strong> ({rec['priority']} priority)<br>
                    {rec['description']}
                </div>
            """
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report saved to {output_path}")
        return output_path