#!/bin/bash

# Data Quality & Monitoring Dashboard - Quality Check Runner
#
# 🎯 PURPOSE: Automated execution of comprehensive data quality validation checks on training datasets
# 📊 FEATURES: Schema validation, anomaly detection, report generation, environment verification, automated testing
# 🏗️ ARCHITECTURE: Environment setup → Data loading → Quality monitoring → Report generation → Results display
# ⚡ STATUS: Production check runner, handles large datasets, comprehensive validation, error reporting, CLI-first design

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Data Quality Monitoring - Running Checks ==="
echo "Project root: $PROJECT_ROOT"
echo

# Check if required files exist
echo "Checking required files..."
if [ ! -f "data/schema.json" ]; then
    echo "❌ Error: schema.json not found in data/ directory"
    echo "Run './scripts/generate_sample_data.py' to generate sample data"
    exit 1
fi

if [ ! -f "data/historical_reference.csv" ]; then
    echo "❌ Error: historical_reference.csv not found in data/ directory"
    echo "Run './scripts/generate_sample_data.py' to generate sample data"
    exit 1
fi

if [ ! -f "data/messy_input.csv" ]; then
    echo "❌ Error: messy_input.csv not found in data/ directory"
    echo "Run './scripts/generate_sample_data.py' to generate sample data"
    exit 1
fi

echo "✅ All required data files found"
echo

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Run data quality checks
echo "Running data quality validation..."
python -c "
import pandas as pd
from src.data_quality_monitor import DataQualityMonitor

# Load data
df = pd.read_csv('data/messy_input.csv')
print(f'Loaded dataset with {len(df)} rows and {len(df.columns)} columns')

# Initialize monitor
monitor = DataQualityMonitor(
    schema_path='data/schema.json',
    reference_data_path='data/historical_reference.csv',
    reports_dir='reports'
)

# Generate report
print('Generating quality report...')
report = monitor.generate_report(df)

print('✅ Report generated successfully!')
print(f'JSON Report: {report[\"file_paths\"][\"json_report\"]}')
print(f'HTML Report: {report[\"file_paths\"][\"html_report\"]}')
print()
print('Summary:')
print(f'- Overall Quality Score: {report[\"summary\"][\"overall_quality_score\"]:.1f}/100')
print(f'- Critical Issues: {report[\"summary\"][\"critical_issues\"]}')
print(f'- Warnings: {report[\"summary\"][\"warnings\"]}')
print(f'- Anomaly Score: {report[\"summary\"][\"anomaly_score\"]}')
print(f'- Completeness Rate: {report[\"summary\"][\"completeness_rate\"]:.1f}%')
"

echo
echo "=== Checks Complete ==="
echo "View the generated reports in the 'reports/' directory"
