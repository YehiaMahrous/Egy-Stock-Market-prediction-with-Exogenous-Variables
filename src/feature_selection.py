"""
EGX Macro Significance Study - Feature Selection Module
========================================================
Implements:
1. Correlation-based feature filtering (remove highly correlated pairs)
2. Feature importance using Mutual Information
3. Keeps Endogenous and Exogenous pipelines separate
"""

import pandas as pd
import numpy as np
from sklearn.feature_selection import mutual_info_classif
from typing import List, Tuple, Set


def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Compute correlation matrix for all numeric features."""
    return df.select_dtypes(include=[np.number]).corr()


def find_highly_correlated_pairs(corr_matrix: pd.DataFrame, 
                                  threshold: float = 0.85) -> List[Tuple[str, str, float]]:
    """
    Find pairs of features with correlation above threshold.
    
    Returns:
        List of tuples (feature1, feature2, correlation)
    """
    pairs = []
    cols = corr_matrix.columns
    
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            corr = abs(corr_matrix.iloc[i, j])
            if corr > threshold:
                pairs.append((cols[i], cols[j], corr))
    
    return sorted(pairs, key=lambda x: -x[2])  # Sort by correlation (descending)


def select_feature_to_drop(feat1: str, feat2: str, 
                           importance_scores: dict) -> str:
    """
    Given two correlated features, decide which one to drop.
    Strategy: Drop the one with LOWER importance (mutual information).
    """
    score1 = importance_scores.get(feat1, 0)
    score2 = importance_scores.get(feat2, 0)
    
    # Drop the less important one
    return feat1 if score1 <= score2 else feat2


def remove_correlated_features(X: pd.DataFrame, 
                                y: pd.Series,
                                threshold: float = 0.85,
                                verbose: bool = True) -> Tuple[pd.DataFrame, List[str]]:
    """
    Remove highly correlated features using importance-guided selection.
    
    Args:
        X: Feature dataframe
        y: Target series
        threshold: Correlation threshold (default 0.85)
        verbose: Print progress
        
    Returns:
        (filtered_X, dropped_features)
    """
    if verbose:
        print(f"  Initial features: {X.shape[1]}")
    
    # 1. Compute correlation matrix
    corr_matrix = compute_correlation_matrix(X)
    
    # 2. Find highly correlated pairs
    corr_pairs = find_highly_correlated_pairs(corr_matrix, threshold)
    
    if verbose and len(corr_pairs) > 0:
        print(f"  Found {len(corr_pairs)} highly correlated pairs (>{threshold})")
    
    if len(corr_pairs) == 0:
        return X, []
    
    # 3. Compute feature importance (Mutual Information)
    mi_scores = mutual_info_classif(X.fillna(0), y, random_state=42)
    importance_scores = dict(zip(X.columns, mi_scores))
    
    # 4. Iteratively remove correlated features
    features_to_drop: Set[str] = set()
    
    for feat1, feat2, corr in corr_pairs:
        # Skip if either already marked for removal
        if feat1 in features_to_drop or feat2 in features_to_drop:
            continue
            
        # Choose which to drop based on importance
        drop_feat = select_feature_to_drop(feat1, feat2, importance_scores)
        features_to_drop.add(drop_feat)
    
    # 5. Filter
    remaining_cols = [c for c in X.columns if c not in features_to_drop]
    X_filtered = X[remaining_cols]
    
    if verbose:
        print(f"  Dropped {len(features_to_drop)} correlated features")
        print(f"  Final features: {X_filtered.shape[1]}")
    
    return X_filtered, list(features_to_drop)


def get_top_features_by_importance(X: pd.DataFrame, 
                                    y: pd.Series,
                                    top_k: int = 50) -> List[str]:
    """
    Get top K features by Mutual Information score.
    """
    mi_scores = mutual_info_classif(X.fillna(0), y, random_state=42)
    importance_df = pd.DataFrame({
        'feature': X.columns,
        'importance': mi_scores
    }).sort_values('importance', ascending=False)
    
    return importance_df['feature'].head(top_k).tolist()


def analyze_feature_importance(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    """
    Compute and return feature importance ranking.
    """
    mi_scores = mutual_info_classif(X.fillna(0), y, random_state=42)
    
    importance_df = pd.DataFrame({
        'feature': X.columns,
        'importance': mi_scores
    }).sort_values('importance', ascending=False)
    
    # Add feature category
    def get_category(feat):
        if feat.startswith('price_'):
            return 'Price'
        elif feat.startswith('tech_'):
            return 'Technical'
        elif feat.startswith('macro_'):
            return 'Macro'
        else:
            return 'Other'
    
    importance_df['category'] = importance_df['feature'].apply(get_category)
    
    return importance_df


# =============================================================================
# Test
# =============================================================================

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/Users/seifhegazy/Documents/Grad Project')
    from src.data_loader import load_raw_data, create_exogenous_samples, prepare_datasets
    
    print("Testing Feature Selection Module...")
    stocks, macro = load_raw_data()
    
    ticker = 'COMI.CA'
    df_ticker = stocks[stocks['Ticker'] == ticker].copy()
    
    samples = create_exogenous_samples(df_ticker, macro)
    data = prepare_datasets(samples)
    
    print("\n--- CORRELATION FILTERING ---")
    X_train = data['X_train']
    y_train = data['y_train']
    
    X_filtered, dropped = remove_correlated_features(X_train, y_train, threshold=0.85)
    
    print("\n--- TOP 10 FEATURES BY IMPORTANCE ---")
    importance = analyze_feature_importance(X_filtered, y_train)
    print(importance.head(10).to_string(index=False))
    
    print("\n--- CATEGORY BREAKDOWN ---")
    print(importance.groupby('category')['importance'].agg(['mean', 'count']))
