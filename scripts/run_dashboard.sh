#!/bin/bash

# Data Quality & Monitoring Dashboard - API Server Launcher
#
# 🎯 PURPOSE: FastAPI web service launcher for real-time data quality monitoring dashboard
# 📊 FEATURES: Health monitoring, API endpoint serving, automatic reload, port management, environment validation
# 🏗️ ARCHITECTURE: Environment verification → Uvicorn server → FastAPI routes → Data quality monitoring integration
# ⚡ STATUS: Production API launcher, handles concurrent requests, automatic error recovery, development-friendly reloading

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Data Quality Monitoring Dashboard ==="
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

# Check if port 8010 is available
if lsof -Pi :8010 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "⚠️  Warning: Port 8010 is already in use"
    echo "The dashboard might not start properly if another service is using this port"
    echo
fi

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

echo "Starting FastAPI dashboard server..."
echo "API will be available at: http://localhost:8010"
echo
echo "Available endpoints:"
echo "  GET  /health              - Health check"
echo "  GET  /quality_report      - Generate fresh quality report"
echo "  GET  /freshness           - Data freshness metrics"
echo "  GET  /drift_summary       - Data drift indicators"
echo "  GET  /docs                - Interactive API documentation"
echo
echo "Press Ctrl+C to stop the server"
echo

# Start the server
python -m uvicorn src.dashboard_api:app --host 0.0.0.0 --port 8010 --reload
