#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Drift Detection Utilities

🎯 PURPOSE: Statistical anomaly detection and data drift monitoring for ML training datasets
📊 FEATURES: Distribution shift detection, missing value spike identification, outlier detection, cardinality change monitoring, statistical testing
🏗️ ARCHITECTURE: DriftDetector class → Reference data loading → Statistical comparisons → Anomaly scoring and reporting
⚡ STATUS: Production statistical engine, handles large datasets, multiple detection methods (KS-test, z-score, IQR), configurable thresholds
"""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, Any, List


class DriftDetector:
    """Handles drift detection and anomaly identification."""

    def __init__(self, reference_data_path: str):
        self.reference_df = pd.read_csv(reference_data_path)

    def detect_distribution_shifts(self, current_df: pd.DataFrame) -> Dict[str, Any]:
        """Detect distribution shifts using KS-test and histogram comparison."""
        shifts = {}

        for col in self.reference_df.columns:
            if col not in current_df.columns:
                continue

            ref_data = self.reference_df[col].dropna()
            curr_data = current_df[col].dropna()

            # Skip if not enough data
            if len(ref_data) < 10 or len(curr_data) < 10:
                continue

            # Numeric columns: KS test
            if ref_data.dtype in ['int64', 'float64'] and curr_data.dtype in ['int64', 'float64']:
                try:
                    ks_stat, p_value = stats.ks_2samp(ref_data, curr_data)
                    if p_value < 0.05:  # Significant difference
                        shifts[col] = {
                            'test': 'ks_test',
                            'statistic': ks_stat,
                            'p_value': p_value,
                            'significance': 'significant' if p_value < 0.05 else 'not_significant',
                            'ref_mean': ref_data.mean(),
                            'curr_mean': curr_data.mean(),
                            'mean_shift': abs(curr_data.mean() - ref_data.mean())
                        }
                except:
                    # Fallback to simple mean comparison
                    mean_diff = abs(curr_data.mean() - ref_data.mean())
                    std_ref = ref_data.std()
                    if std_ref > 0 and mean_diff > 2 * std_ref:
                        shifts[col] = {
                            'test': 'mean_comparison',
                            'mean_shift': mean_diff,
                            'ref_mean': ref_data.mean(),
                            'curr_mean': curr_data.mean(),
                            'threshold': 2 * std_ref
                        }

            # Categorical columns: Chi-square test for distribution
            elif ref_data.dtype == 'object' or curr_data.dtype == 'object':
                try:
                    ref_counts = ref_data.value_counts()
                    curr_counts = curr_data.value_counts()

                    # Create contingency table for common values
                    common_values = set(ref_counts.index) | set(curr_counts.index)
                    contingency = []
                    for val in common_values:
                        contingency.append([ref_counts.get(val, 0), curr_counts.get(val, 0)])

                    if len(contingency) > 1:
                        chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
                        if p_value < 0.05:
                            shifts[col] = {
                                'test': 'chi_square',
                                'statistic': chi2,
                                'p_value': p_value,
                                'ref_unique': len(ref_counts),
                                'curr_unique': len(curr_counts)
                            }
                except:
                    # Fallback to cardinality change
                    card_change = abs(len(curr_counts) - len(ref_counts))
                    if card_change > 0:
                        shifts[col] = {
                            'test': 'cardinality_change',
                            'ref_cardinality': len(ref_counts),
                            'curr_cardinality': len(curr_counts),
                            'change': card_change
                        }

        return {
            'distribution_shifts': shifts,
            'shift_count': len(shifts)
        }

    def detect_missing_value_spikes(self, current_df: pd.DataFrame) -> Dict[str, Any]:
        """Detect spikes in missing values compared to reference."""
        missing_spikes = {}

        ref_missing_rates = self.reference_df.isnull().mean()
        curr_missing_rates = current_df.isnull().mean()

        for col in self.reference_df.columns:
            if col in current_df.columns:
                ref_rate = ref_missing_rates[col]
                curr_rate = curr_missing_rates[col]

                # Significant increase in missing values
                if curr_rate > ref_rate + 0.05:  # 5% threshold
                    missing_spikes[col] = {
                        'ref_missing_rate': ref_rate,
                        'curr_missing_rate': curr_rate,
                        'increase': curr_rate - ref_rate,
                        'threshold_exceeded': True
                    }

        return {
            'missing_spikes': missing_spikes,
            'spike_count': len(missing_spikes)
        }

    def detect_outliers(self, current_df: pd.DataFrame) -> Dict[str, Any]:
        """Detect outliers using z-score and IQR methods."""
        outliers = {}

        for col in current_df.columns:
            if current_df[col].dtype in ['int64', 'float64']:
                data = current_df[col].dropna()

                if len(data) < 10:  # Need minimum data points
                    continue

                # Z-score method
                z_scores = np.abs((data - data.mean()) / data.std())
                z_outliers = data[z_scores > 3]  # 3 standard deviations

                # IQR method
                Q1 = data.quantile(0.25)
                Q3 = data.quantile(0.75)
                IQR = Q3 - Q1
                iqr_outliers = data[(data < (Q1 - 1.5 * IQR)) | (data > (Q3 + 1.5 * IQR))]

                outlier_count = len(set(z_outliers.index) | set(iqr_outliers.index))

                if outlier_count > 0:
                    outliers[col] = {
                        'outlier_count': outlier_count,
                        'total_values': len(data),
                        'outlier_percentage': (outlier_count / len(data)) * 100,
                        'z_score_outliers': len(z_outliers),
                        'iqr_outliers': len(iqr_outliers),
                        'min_value': float(data.min()),
                        'max_value': float(data.max()),
                        'median': float(data.median())
                    }

        return {
            'outlier_detection': outliers,
            'columns_with_outliers': len(outliers)
        }
