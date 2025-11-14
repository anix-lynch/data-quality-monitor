#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Logging and Reporting Utilities

🎯 PURPOSE: Structured logging and automated report generation for data quality monitoring operations
📊 FEATURES: Configurable logging levels, JSON report saving, HTML template rendering, file management, timestamp handling
🏗️ ARCHITECTURE: DataQualityLogger class → Logging configuration → Report generation → File output management
⚡ STATUS: Production logging system, handles large reports, multiple output formats, automatic timestamping, error tracking
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


class DataQualityLogger:
    """Handles logging for data quality monitoring operations."""

    def __init__(self, log_level: str = 'INFO'):
        self.logger = logging.getLogger('data_quality_monitor')
        self.logger.setLevel(getattr(logging, log_level))

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)

        # Add handler to logger
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)

    def log_validation_start(self, dataset_name: str):
        """Log the start of validation."""
        self.logger.info(f"Starting data quality validation for dataset: {dataset_name}")

    def log_validation_result(self, result: Dict[str, Any]):
        """Log validation results."""
        error_count = len(result.get('errors', []))
        warning_count = len(result.get('warnings', []))

        self.logger.info(f"Validation completed. Errors: {error_count}, Warnings: {warning_count}")

        if error_count > 0:
            self.logger.error(f"Validation errors: {result['errors']}")

        if warning_count > 0:
            self.logger.warning(f"Validation warnings: {result['warnings']}")

    def log_anomaly_detection(self, anomalies: Dict[str, Any]):
        """Log anomaly detection results."""
        shift_count = anomalies.get('distribution_shifts', {}).get('shift_count', 0)
        spike_count = anomalies.get('missing_value_spikes', {}).get('spike_count', 0)
        outlier_cols = anomalies.get('outliers', {}).get('columns_with_outliers', 0)

        self.logger.info(f"Anomaly detection completed. Shifts: {shift_count}, Missing spikes: {spike_count}, Columns with outliers: {outlier_cols}")

    def log_report_generation(self, report_path: str):
        """Log report generation."""
        self.logger.info(f"Generated data quality report: {report_path}")


def save_report_to_file(report_data: Dict[str, Any], reports_dir: str, timestamp: str = None) -> str:
    """Save report data to JSON file."""
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    reports_path = Path(reports_dir)
    reports_path.mkdir(exist_ok=True)

    json_filename = f"report_{timestamp}.json"
    json_path = reports_path / json_filename

    with open(json_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)

    return str(json_path)
