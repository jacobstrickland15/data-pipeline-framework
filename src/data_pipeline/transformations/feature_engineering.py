"""Advanced feature engineering utilities."""

import logging
from typing import Dict, List, Optional, Any, Union, Callable
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)


class FeatureEngineering:
    """Advanced feature engineering transformations."""
    
    @staticmethod
    def create_polynomial_features(
        df: pd.DataFrame,
        numeric_cols: List[str],
        degree: int = 2,
        include_bias: bool = False,
        interaction_only: bool = False
    ) -> pd.DataFrame:
        """Create polynomial features from numeric columns.
        
        Args:
            df: Input DataFrame
            numeric_cols: Columns to create polynomial features from
            degree: Polynomial degree
            include_bias: Whether to include bias column
            interaction_only: Whether to include only interaction terms
            
        Returns:
            DataFrame with polynomial features
        """
        try:
            from sklearn.preprocessing import PolynomialFeatures
            
            result_df = df.copy()
            
            # Extract numeric data
            X = result_df[numeric_cols].fillna(0)
            
            # Create polynomial features
            poly = PolynomialFeatures(
                degree=degree,
                include_bias=include_bias,
                interaction_only=interaction_only
            )
            X_poly = poly.fit_transform(X)
            
            # Get feature names
            feature_names = poly.get_feature_names_out(numeric_cols)
            
            # Create new DataFrame with polynomial features
            poly_df = pd.DataFrame(X_poly, columns=feature_names, index=result_df.index)
            
            # Remove original columns if they exist in polynomial features
            for col in numeric_cols:
                if col in poly_df.columns:
                    poly_df = poly_df.drop(columns=[col])
            
            # Combine with original DataFrame
            result_df = pd.concat([result_df, poly_df], axis=1)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating polynomial features: {e}")
            raise
    
    @staticmethod
    def create_binning_features(
        df: pd.DataFrame,
        numeric_cols: List[str],
        strategy: str = 'quantile',
        n_bins: int = 5,
        labels: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Create binning features from numeric columns.
        
        Args:
            df: Input DataFrame
            numeric_cols: Columns to bin
            strategy: Binning strategy ('uniform', 'quantile', 'kmeans')
            n_bins: Number of bins
            labels: Custom labels for bins
            
        Returns:
            DataFrame with binned features
        """
        try:
            result_df = df.copy()
            
            for col in numeric_cols:
                if col not in result_df.columns:
                    continue
                
                new_col = f'{col}_binned'
                
                if strategy == 'uniform':
                    result_df[new_col] = pd.cut(
                        result_df[col],
                        bins=n_bins,
                        labels=labels
                    )
                elif strategy == 'quantile':
                    result_df[new_col] = pd.qcut(
                        result_df[col],
                        q=n_bins,
                        labels=labels,
                        duplicates='drop'
                    )
                elif strategy == 'kmeans':
                    from sklearn.cluster import KMeans
                    
                    # Handle missing values
                    valid_data = result_df[col].dropna().values.reshape(-1, 1)
                    
                    if len(valid_data) > 0:
                        kmeans = KMeans(n_clusters=n_bins, random_state=42)
                        cluster_labels = kmeans.fit_predict(valid_data)
                        
                        # Create mapping back to original data
                        result_df[new_col] = np.nan
                        result_df.loc[result_df[col].notna(), new_col] = cluster_labels
                    else:
                        result_df[new_col] = np.nan
                else:
                    logger.warning(f"Unknown binning strategy: {strategy}")
                    continue
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating binning features: {e}")
            raise
    
    @staticmethod
    def create_text_features(
        df: pd.DataFrame,
        text_cols: List[str],
        method: str = 'tfidf',
        max_features: int = 100,
        ngram_range: tuple = (1, 1),
        min_df: int = 1,
        max_df: float = 1.0
    ) -> pd.DataFrame:
        """Create features from text columns.
        
        Args:
            df: Input DataFrame
            text_cols: Columns containing text data
            method: Text vectorization method ('tfidf', 'count', 'basic_stats')
            max_features: Maximum number of features to create
            ngram_range: Range of n-grams to consider
            min_df: Minimum document frequency
            max_df: Maximum document frequency
            
        Returns:
            DataFrame with text features
        """
        try:
            result_df = df.copy()
            
            for col in text_cols:
                if col not in result_df.columns:
                    continue
                
                # Fill missing values
                text_data = result_df[col].fillna('').astype(str)
                
                if method == 'tfidf':
                    vectorizer = TfidfVectorizer(
                        max_features=max_features,
                        ngram_range=ngram_range,
                        min_df=min_df,
                        max_df=max_df,
                        stop_words='english'
                    )
                    X_text = vectorizer.fit_transform(text_data)
                    feature_names = [f'{col}_tfidf_{name}' for name in vectorizer.get_feature_names_out()]
                    
                elif method == 'count':
                    vectorizer = CountVectorizer(
                        max_features=max_features,
                        ngram_range=ngram_range,
                        min_df=min_df,
                        max_df=max_df,
                        stop_words='english'
                    )
                    X_text = vectorizer.fit_transform(text_data)
                    feature_names = [f'{col}_count_{name}' for name in vectorizer.get_feature_names_out()]
                
                elif method == 'basic_stats':
                    # Basic text statistics
                    result_df[f'{col}_length'] = text_data.str.len()
                    result_df[f'{col}_word_count'] = text_data.str.split().str.len()
                    result_df[f'{col}_char_count'] = text_data.str.replace(' ', '').str.len()
                    result_df[f'{col}_sentence_count'] = text_data.str.split('.').str.len()
                    result_df[f'{col}_avg_word_length'] = (
                        result_df[f'{col}_char_count'] / result_df[f'{col}_word_count']
                    ).fillna(0)
                    continue
                
                else:
                    logger.warning(f"Unknown text feature method: {method}")
                    continue
                
                # Convert sparse matrix to DataFrame
                if method in ['tfidf', 'count']:
                    text_features_df = pd.DataFrame(
                        X_text.toarray(),
                        columns=feature_names,
                        index=result_df.index
                    )
                    result_df = pd.concat([result_df, text_features_df], axis=1)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating text features: {e}")
            raise
    
    @staticmethod
    def create_categorical_features(
        df: pd.DataFrame,
        categorical_cols: List[str],
        encoding_method: str = 'onehot',
        max_categories: Optional[int] = None,
        handle_unknown: str = 'ignore'
    ) -> pd.DataFrame:
        """Create features from categorical columns.
        
        Args:
            df: Input DataFrame
            categorical_cols: Columns with categorical data
            encoding_method: Encoding method ('onehot', 'label', 'target', 'frequency')
            max_categories: Maximum categories to keep (others become 'other')
            handle_unknown: How to handle unknown categories
            
        Returns:
            DataFrame with encoded categorical features
        """
        try:
            result_df = df.copy()
            
            for col in categorical_cols:
                if col not in result_df.columns:
                    continue
                
                # Handle missing values
                result_df[col] = result_df[col].fillna('missing')
                
                # Limit categories if specified
                if max_categories and result_df[col].nunique() > max_categories:
                    top_categories = result_df[col].value_counts().head(max_categories - 1).index
                    result_df[col] = result_df[col].where(
                        result_df[col].isin(top_categories),
                        'other'
                    )
                
                if encoding_method == 'onehot':
                    # One-hot encoding
                    dummies = pd.get_dummies(
                        result_df[col],
                        prefix=col,
                        dummy_na=False
                    )
                    result_df = pd.concat([result_df, dummies], axis=1)
                    
                elif encoding_method == 'label':
                    # Label encoding
                    le = LabelEncoder()
                    result_df[f'{col}_encoded'] = le.fit_transform(result_df[col])
                    
                elif encoding_method == 'frequency':
                    # Frequency encoding
                    freq_map = result_df[col].value_counts().to_dict()
                    result_df[f'{col}_frequency'] = result_df[col].map(freq_map)
                    
                elif encoding_method == 'target':
                    logger.warning("Target encoding requires a target variable")
                    continue
                    
                else:
                    logger.warning(f"Unknown encoding method: {encoding_method}")
                    continue
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating categorical features: {e}")
            raise
    
    @staticmethod
    def create_aggregation_features(
        df: pd.DataFrame,
        groupby_cols: List[str],
        agg_cols: List[str],
        agg_funcs: List[Union[str, Callable]] = ['mean', 'std', 'min', 'max', 'count']
    ) -> pd.DataFrame:
        """Create aggregation features by grouping.
        
        Args:
            df: Input DataFrame
            groupby_cols: Columns to group by
            agg_cols: Columns to aggregate
            agg_funcs: Aggregation functions to apply
            
        Returns:
            DataFrame with aggregation features
        """
        try:
            result_df = df.copy()
            
            # Create aggregation features
            for agg_func in agg_funcs:
                agg_result = df.groupby(groupby_cols)[agg_cols].agg(agg_func)
                
                # Rename columns
                if len(agg_cols) == 1:
                    agg_result.columns = [f'{agg_cols[0]}_{agg_func}_by_{"_".join(groupby_cols)}']
                else:
                    agg_result.columns = [f'{col}_{agg_func}_by_{"_".join(groupby_cols)}' 
                                        for col in agg_result.columns]
                
                # Merge back to original DataFrame
                result_df = result_df.merge(
                    agg_result,
                    left_on=groupby_cols,
                    right_index=True,
                    how='left'
                )
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating aggregation features: {e}")
            raise
    
    @staticmethod
    def create_ratio_features(
        df: pd.DataFrame,
        numerator_cols: List[str],
        denominator_cols: List[str],
        handle_division_by_zero: str = 'nan'
    ) -> pd.DataFrame:
        """Create ratio features between columns.
        
        Args:
            df: Input DataFrame
            numerator_cols: Columns to use as numerators
            denominator_cols: Columns to use as denominators
            handle_division_by_zero: How to handle division by zero ('nan', 'inf', 'zero')
            
        Returns:
            DataFrame with ratio features
        """
        try:
            result_df = df.copy()
            
            for num_col in numerator_cols:
                if num_col not in result_df.columns:
                    continue
                    
                for den_col in denominator_cols:
                    if den_col not in result_df.columns or den_col == num_col:
                        continue
                    
                    ratio_col = f'{num_col}_to_{den_col}_ratio'
                    
                    # Calculate ratio
                    with np.errstate(divide='ignore', invalid='ignore'):
                        ratio = result_df[num_col] / result_df[den_col]
                    
                    # Handle division by zero
                    if handle_division_by_zero == 'nan':
                        ratio = ratio.replace([np.inf, -np.inf], np.nan)
                    elif handle_division_by_zero == 'zero':
                        ratio = ratio.replace([np.inf, -np.inf], 0)
                    # 'inf' keeps infinite values as is
                    
                    result_df[ratio_col] = ratio
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating ratio features: {e}")
            raise
    
    @staticmethod
    def create_interaction_features(
        df: pd.DataFrame,
        feature_cols: List[str],
        interaction_type: str = 'multiply',
        max_interactions: Optional[int] = None
    ) -> pd.DataFrame:
        """Create interaction features between columns.
        
        Args:
            df: Input DataFrame
            feature_cols: Columns to create interactions from
            interaction_type: Type of interaction ('multiply', 'add', 'subtract')
            max_interactions: Maximum number of interactions to create
            
        Returns:
            DataFrame with interaction features
        """
        try:
            result_df = df.copy()
            interaction_count = 0
            
            for i, col1 in enumerate(feature_cols):
                if col1 not in result_df.columns:
                    continue
                    
                for col2 in feature_cols[i+1:]:
                    if col2 not in result_df.columns:
                        continue
                    
                    if max_interactions and interaction_count >= max_interactions:
                        break
                    
                    interaction_col = f'{col1}_{interaction_type}_{col2}'
                    
                    if interaction_type == 'multiply':
                        result_df[interaction_col] = result_df[col1] * result_df[col2]
                    elif interaction_type == 'add':
                        result_df[interaction_col] = result_df[col1] + result_df[col2]
                    elif interaction_type == 'subtract':
                        result_df[interaction_col] = result_df[col1] - result_df[col2]
                    else:
                        logger.warning(f"Unknown interaction type: {interaction_type}")
                        continue
                    
                    interaction_count += 1
                
                if max_interactions and interaction_count >= max_interactions:
                    break
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating interaction features: {e}")
            raise
    
    @staticmethod
    def create_clustering_features(
        df: pd.DataFrame,
        feature_cols: List[str],
        n_clusters: int = 5,
        algorithm: str = 'kmeans',
        include_distances: bool = True
    ) -> pd.DataFrame:
        """Create clustering-based features.
        
        Args:
            df: Input DataFrame
            feature_cols: Columns to use for clustering
            n_clusters: Number of clusters
            algorithm: Clustering algorithm ('kmeans', 'dbscan')
            include_distances: Whether to include distances to cluster centers
            
        Returns:
            DataFrame with clustering features
        """
        try:
            result_df = df.copy()
            
            # Prepare feature data
            feature_data = result_df[feature_cols].fillna(0)
            
            # Scale features
            scaler = StandardScaler()
            feature_data_scaled = scaler.fit_transform(feature_data)
            
            if algorithm == 'kmeans':
                clusterer = KMeans(n_clusters=n_clusters, random_state=42)
                cluster_labels = clusterer.fit_predict(feature_data_scaled)
                
                result_df['cluster_label'] = cluster_labels
                
                if include_distances:
                    # Calculate distances to cluster centers
                    distances = clusterer.transform(feature_data_scaled)
                    for i in range(n_clusters):
                        result_df[f'distance_to_cluster_{i}'] = distances[:, i]
                    
                    # Distance to closest cluster center
                    result_df['min_cluster_distance'] = distances.min(axis=1)
            
            elif algorithm == 'dbscan':
                from sklearn.cluster import DBSCAN
                
                clusterer = DBSCAN(eps=0.5, min_samples=5)
                cluster_labels = clusterer.fit_predict(feature_data_scaled)
                
                result_df['cluster_label'] = cluster_labels
                # Note: DBSCAN doesn't have cluster centers, so no distance features
            
            else:
                logger.warning(f"Unknown clustering algorithm: {algorithm}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating clustering features: {e}")
            raise
    
    @staticmethod
    def create_pca_features(
        df: pd.DataFrame,
        feature_cols: List[str],
        n_components: Optional[int] = None,
        variance_threshold: float = 0.95
    ) -> pd.DataFrame:
        """Create PCA features for dimensionality reduction.
        
        Args:
            df: Input DataFrame
            feature_cols: Columns to apply PCA to
            n_components: Number of PCA components (auto-determined if None)
            variance_threshold: Cumulative variance threshold for auto n_components
            
        Returns:
            DataFrame with PCA features
        """
        try:
            result_df = df.copy()
            
            # Prepare feature data
            feature_data = result_df[feature_cols].fillna(0)
            
            # Scale features
            scaler = StandardScaler()
            feature_data_scaled = scaler.fit_transform(feature_data)
            
            # Determine number of components
            if n_components is None:
                # Fit PCA with all components to determine optimal number
                pca_temp = PCA()
                pca_temp.fit(feature_data_scaled)
                
                cumsum_variance = np.cumsum(pca_temp.explained_variance_ratio_)
                n_components = np.argmax(cumsum_variance >= variance_threshold) + 1
                n_components = min(n_components, len(feature_cols))
            
            # Fit final PCA
            pca = PCA(n_components=n_components)
            pca_features = pca.fit_transform(feature_data_scaled)
            
            # Add PCA features to DataFrame
            for i in range(n_components):
                result_df[f'pca_component_{i+1}'] = pca_features[:, i]
            
            # Add explained variance information
            logger.info(f"PCA explained variance ratio: {pca.explained_variance_ratio_}")
            logger.info(f"Total variance explained: {pca.explained_variance_ratio_.sum():.3f}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error creating PCA features: {e}")
            raise