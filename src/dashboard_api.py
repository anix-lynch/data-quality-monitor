#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - FastAPI Web Service

🎯 PURPOSE: REST API dashboard for real-time data quality monitoring with automated report generation and freshness tracking
📊 FEATURES: Health checks, fresh quality reports, data freshness metrics, drift summary endpoints, interactive API docs, Docker-ready
🏗️ ARCHITECTURE: FastAPI server → DataQualityMonitor engine → JSON responses → Automated HTML reports
⚡ STATUS: Production API service, handles concurrent requests, sub-second responses, comprehensive error handling, Docker containerized
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import os

from .data_quality_monitor import DataQualityMonitor

app = FastAPI(
    title="Data Quality Monitoring Dashboard",
    description="API for monitoring data quality and accessing validation reports",
    version="1.0.0"
)

# Initialize monitor (would be configurable in production)
DATA_DIR = Path(__file__).parent.parent / "data"
REPORTS_DIR = Path(__file__).parent.parent / "reports"

# Global monitor instance (lazy initialization)
monitor: Optional[DataQualityMonitor] = None

def get_monitor() -> DataQualityMonitor:
    """Get or create the data quality monitor instance."""
    global monitor
    if monitor is None:
        schema_path = DATA_DIR / "schema.json"
        reference_path = DATA_DIR / "historical_reference.csv"

        if not schema_path.exists() or not reference_path.exists():
            raise HTTPException(
                status_code=500,
                detail="Required data files not found. Please ensure schema.json and historical_reference.csv exist."
            )

        monitor = DataQualityMonitor(
            schema_path=str(schema_path),
            reference_data_path=str(reference_path),
            reports_dir=str(REPORTS_DIR)
        )
    return monitor

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "data_quality_monitor"
    }

@app.get("/quality_report")
async def get_quality_report(dataset_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the latest quality report or generate a new one.

    Args:
        dataset_path: Optional path to dataset CSV file. If not provided, uses messy_input.csv
    """
    try:
        monitor = get_monitor()

        # Determine dataset path
        if dataset_path:
            data_path = Path(dataset_path)
        else:
            data_path = DATA_DIR / "messy_input.csv"

        if not data_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Dataset file not found: {data_path}"
            )

        # Load and validate data
        df = pd.read_csv(data_path)

        # Generate fresh report
        report = monitor.generate_report(df)

        return JSONResponse(content=report)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating quality report: {str(e)}")

@app.get("/freshness")
async def get_freshness_metrics() -> Dict[str, Any]:
    """Get data freshness metrics based on file modification timestamps."""
    try:
        data_files = {
            "messy_input": DATA_DIR / "messy_input.csv",
            "historical_reference": DATA_DIR / "historical_reference.csv",
            "schema": DATA_DIR / "schema.json"
        }

        freshness_info = {}

        for name, path in data_files.items():
            if path.exists():
                stat = path.stat()
                freshness_info[name] = {
                    "exists": True,
                    "modified_timestamp": stat.st_mtime,
                    "modified_datetime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "size_bytes": stat.st_size
                }
            else:
                freshness_info[name] = {
                    "exists": False,
                    "error": "File not found"
                }

        # Calculate relative freshness
        current_time = datetime.now().timestamp()
        for name, info in freshness_info.items():
            if info["exists"]:
                age_hours = (current_time - info["modified_timestamp"]) / 3600
                info["age_hours"] = age_hours
                info["freshness_status"] = "fresh" if age_hours < 24 else "stale" if age_hours < 168 else "very_stale"

        # Get latest report
        reports = list(REPORTS_DIR.glob("report_*.json"))
        if reports:
            latest_report = max(reports, key=lambda x: x.stat().st_mtime)
            report_stat = latest_report.stat()
            freshness_info["latest_report"] = {
                "path": str(latest_report),
                "modified_timestamp": report_stat.st_mtime,
                "modified_datetime": datetime.fromtimestamp(report_stat.st_mtime).isoformat(),
                "age_hours": (current_time - report_stat.st_mtime) / 3600
            }

        return JSONResponse(content={
            "timestamp": datetime.now().isoformat(),
            "data_freshness": freshness_info
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting freshness metrics: {str(e)}")

@app.get("/drift_summary")
async def get_drift_summary() -> Dict[str, Any]:
    """Get summary of data drift indicators."""
    try:
        monitor = get_monitor()

        # Load current data
        data_path = DATA_DIR / "messy_input.csv"
        if not data_path.exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")

        df = pd.read_csv(data_path)

        # Get anomaly detection results
        anomalies = monitor.detect_anomalies(df)

        # Create drift summary
        drift_summary = {
            "timestamp": datetime.now().isoformat(),
            "drift_indicators": {
                "distribution_shifts": {
                    "count": anomalies["distribution_shifts"]["shift_count"],
                    "affected_columns": list(anomalies["distribution_shifts"]["distribution_shifts"].keys()),
                    "severity": "high" if anomalies["distribution_shifts"]["shift_count"] > 3 else "medium" if anomalies["distribution_shifts"]["shift_count"] > 1 else "low"
                },
                "missing_value_spikes": {
                    "count": anomalies["missing_value_spikes"]["spike_count"],
                    "affected_columns": list(anomalies["missing_value_spikes"]["missing_spikes"].keys()),
                    "severity": "high" if anomalies["missing_value_spikes"]["spike_count"] > 2 else "medium" if anomalies["missing_value_spikes"]["spike_count"] > 0 else "low"
                },
                "outliers": {
                    "columns_affected": anomalies["outliers"]["columns_with_outliers"],
                    "severity": "high" if anomalies["outliers"]["columns_with_outliers"] > 3 else "medium" if anomalies["outliers"]["columns_with_outliers"] > 1 else "low"
                }
            },
            "overall_anomaly_score": anomalies["anomaly_score"],
            "overall_severity": anomalies["anomaly_severity"],
            "recommendations": []
        }

        # Add recommendations based on findings
        if drift_summary["drift_indicators"]["distribution_shifts"]["count"] > 0:
            drift_summary["recommendations"].append("Review data collection process for distribution shifts")

        if drift_summary["drift_indicators"]["missing_value_spikes"]["count"] > 0:
            drift_summary["recommendations"].append("Investigate source of increased missing values")

        if drift_summary["drift_indicators"]["outliers"]["columns_affected"] > 0:
            drift_summary["recommendations"].append("Review outlier handling in preprocessing pipeline")

        if drift_summary["overall_anomaly_score"] == 0:
            drift_summary["recommendations"].append("Data appears stable - continue monitoring")

        return JSONResponse(content=drift_summary)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting drift summary: {str(e)}")

@app.get("/reports/{report_type}/{filename}")
async def get_report_file(report_type: str, filename: str):
    """Serve report files (JSON or HTML)."""
    if report_type not in ["json", "html"]:
        raise HTTPException(status_code=400, detail="Report type must be 'json' or 'html'")

    file_path = REPORTS_DIR / filename

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Report file not found")

    # Basic security check - ensure file is in reports directory
    if not str(file_path).startswith(str(REPORTS_DIR)):
        raise HTTPException(status_code=403, detail="Access denied")

    media_type = "application/json" if report_type == "json" else "text/html"
    return FileResponse(path=file_path, media_type=media_type, filename=filename)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)
