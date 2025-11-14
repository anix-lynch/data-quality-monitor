#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Data Validation Utilities

🎯 PURPOSE: General data validation helpers and DataFrame analysis utilities
📊 FEATURES: DataFrame structure validation, completeness metrics, basic statistics, type checking, error categorization
🏗️ ARCHITECTURE: ValidationUtils class → DataFrame analysis → Metric calculation → Structured validation results
⚡ STATUS: Production validation toolkit, handles various data types, fast statistical calculations, comprehensive data profiling
"""

import pandas as pd
from typing import Dict, Any, List
from datetime import datetime


class ValidationUtils:
    """Utility functions for data validation."""

    @staticmethod
    def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic information about a DataFrame."""
        return {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }

    @staticmethod
    def validate_dataframe_structure(df: pd.DataFrame) -> Dict[str, Any]:
        """Validate basic DataFrame structure."""
        issues = []

        # Check for empty DataFrame
        if df.empty:
            issues.append("DataFrame is empty")

        # Check for duplicate column names
        if df.columns.duplicated().any():
            issues.append("Duplicate column names found")

        # Check for reasonable row count
        if len(df) == 0:
            issues.append("No rows in DataFrame")

        return {
            'structure_valid': len(issues) == 0,
            'issues': issues
        }

    @staticmethod
    def calculate_data_completeness(df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate data completeness metrics."""
        null_counts = df.isnull().sum()
        total_rows = len(df)

        completeness = {}
        for col in df.columns:
            completeness[col] = {
                'null_count': int(null_counts[col]),
                'null_percentage': (null_counts[col] / total_rows) * 100 if total_rows > 0 else 0,
                'complete_count': total_rows - null_counts[col],
                'completeness_rate': ((total_rows - null_counts[col]) / total_rows) * 100 if total_rows > 0 else 0
            }

        overall_completeness = sum(c['completeness_rate'] for c in completeness.values()) / len(completeness) if completeness else 0

        return {
            'column_completeness': completeness,
            'overall_completeness': overall_completeness,
            'fully_complete_columns': [col for col, stats in completeness.items() if stats['null_count'] == 0]
        }
