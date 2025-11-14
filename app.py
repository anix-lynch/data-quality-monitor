#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Streamlit Web App

🎯 PURPOSE: Interactive web dashboard for real-time data quality monitoring with automated report generation and drift visualization
📊 FEATURES: Live quality metrics, interactive charts, automated report generation, data freshness tracking, drift indicators, professional UI
🏗️ ARCHITECTURE: Streamlit frontend → DataQualityMonitor engine → Interactive visualizations → Automated reports → Live data insights
⚡ STATUS: Production web application, handles large datasets, real-time updates, professional UX, deployment-ready
"""

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import time

from src.data_quality_monitor import DataQualityMonitor

# Page configuration
st.set_page_config(
    page_title="Data Quality Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .status-good {
        color: #28a745;
        font-weight: bold;
    }
    .status-warning {
        color: #ffc107;
        font-weight: bold;
    }
    .status-error {
        color: #dc3545;
        font-weight: bold;
    }
    .report-section {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

def load_data_quality_monitor():
    """Initialize the data quality monitor."""
    try:
        data_dir = Path(__file__).parent / "data"
        schema_path = data_dir / "schema.json"
        reference_path = data_dir / "historical_reference.csv"

        if not schema_path.exists() or not reference_path.exists():
            st.error("❌ Required data files not found. Please run the data generation script first.")
            return None

        monitor = DataQualityMonitor(
            schema_path=str(schema_path),
            reference_data_path=str(reference_path),
            reports_dir="reports"
        )
        return monitor
    except Exception as e:
        st.error(f"❌ Error initializing monitor: {str(e)}")
        return None

def display_quality_metrics(report_data):
    """Display key quality metrics in cards."""
    summary = report_data.get('summary', {})

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        score = summary.get('overall_quality_score', 0)
        color_class = "status-good" if score >= 80 else "status-warning" if score >= 60 else "status-error"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{score:.1f}%</div>
            <div class="metric-label">Overall Quality Score</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        critical_issues = summary.get('critical_issues', 0)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{critical_issues}</div>
            <div class="metric-label">Critical Issues</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        warnings = summary.get('warnings', 0)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{warnings}</div>
            <div class="metric-label">Warnings</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        completeness = summary.get('completeness_rate', 0)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{completeness:.1f}%</div>
            <div class="metric-label">Data Completeness</div>
        </div>
        """, unsafe_allow_html=True)

def display_data_completeness(completeness_data):
    """Display data completeness visualization."""
    st.subheader("📈 Data Completeness by Column")

    if completeness_data and 'column_completeness' in completeness_data:
        columns = list(completeness_data['column_completeness'].keys())
        completeness_rates = [data['completeness_rate'] for data in completeness_data['column_completeness'].values()]

        fig = px.bar(
            x=columns,
            y=completeness_rates,
            title="Column Completeness Rates",
            labels={'x': 'Column', 'y': 'Completeness (%)'},
            color=completeness_rates,
            color_continuous_scale=['red', 'yellow', 'green']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def display_anomaly_detection(anomaly_data):
    """Display anomaly detection results."""
    st.subheader("🔍 Anomaly Detection Results")

    if anomaly_data:
        col1, col2, col3 = st.columns(3)

        with col1:
            shift_count = anomaly_data.get('distribution_shifts', {}).get('shift_count', 0)
            st.metric("Distribution Shifts", shift_count)

        with col2:
            spike_count = anomaly_data.get('missing_value_spikes', {}).get('spike_count', 0)
            st.metric("Missing Value Spikes", spike_count)

        with col3:
            outlier_cols = anomaly_data.get('outliers', {}).get('columns_with_outliers', 0)
            st.metric("Columns with Outliers", outlier_cols)

        # Anomaly severity gauge
        severity = anomaly_data.get('anomaly_severity', 'LOW')
        severity_colors = {'LOW': 'green', 'MEDIUM': 'orange', 'HIGH': 'red'}
        severity_color = severity_colors.get(severity, 'gray')

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=anomaly_data.get('anomaly_score', 0),
            title={'text': f"Anomaly Severity: {severity}"},
            gauge={'axis': {'range': [0, 10]},
                   'bar': {'color': severity_color},
                   'steps': [
                       {'range': [0, 2], 'color': 'lightgreen'},
                       {'range': [2, 5], 'color': 'lightyellow'},
                       {'range': [5, 10], 'color': 'lightcoral'}
                   ]}
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

def display_validation_results(validation_data):
    """Display schema validation results."""
    st.subheader("✅ Schema Validation Results")

    if validation_data:
        # Errors and warnings
        errors = validation_data.get('errors', [])
        warnings = validation_data.get('warnings', [])

        col1, col2 = st.columns(2)

        with col1:
            if errors:
                st.error(f"❌ {len(errors)} Critical Issues")
                for error in errors[:5]:  # Show first 5
                    st.write(f"• {error}")
                if len(errors) > 5:
                    st.write(f"... and {len(errors) - 5} more")
            else:
                st.success("✅ No critical issues found")

        with col2:
            if warnings:
                st.warning(f"⚠️ {len(warnings)} Warnings")
                for warning in warnings[:5]:  # Show first 5
                    st.write(f"• {warning}")
                if len(warnings) > 5:
                    st.write(f"... and {len(warnings) - 5} more")
            else:
                st.success("✅ No warnings")

def display_drift_summary(drift_data):
    """Display drift summary information."""
    st.subheader("🌊 Data Drift Summary")

    if drift_data:
        severity = drift_data.get('overall_severity', 'LOW')
        severity_colors = {'LOW': '🟢', 'MEDIUM': '🟡', 'HIGH': '🔴'}

        st.write(f"**Overall Severity:** {severity_colors.get(severity, '⚪')} {severity}")

        # Drift indicators
        indicators = drift_data.get('drift_indicators', {})

        col1, col2, col3 = st.columns(3)

        with col1:
            shifts = indicators.get('distribution_shifts', {})
            st.metric("Distribution Shifts",
                     shifts.get('count', 0),
                     delta=f"{shifts.get('severity', 'low').title()} severity")

        with col2:
            spikes = indicators.get('missing_value_spikes', {})
            st.metric("Missing Value Spikes",
                     spikes.get('count', 0),
                     delta=f"{spikes.get('severity', 'low').title()} severity")

        with col3:
            outliers = indicators.get('outliers', {})
            st.metric("Columns with Outliers",
                     outliers.get('columns_affected', 0),
                     delta=f"{outliers.get('severity', 'low').title()} severity")

        # Recommendations
        recommendations = drift_data.get('recommendations', [])
        if recommendations:
            st.subheader("💡 Recommendations")
            for rec in recommendations:
                st.info(f"• {rec}")

def main():
    """Main Streamlit application."""
    # Header
    st.markdown('<h1 class="main-header">📊 Data Quality & Monitoring Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("*Real-time ML data quality monitoring and automated reporting*")

    # Sidebar
    st.sidebar.title("🔧 Controls")

    # Load monitor
    monitor = load_data_quality_monitor()
    if not monitor:
        return

    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 30 seconds", value=False)

    # Generate report button
    if st.sidebar.button("🔄 Generate Fresh Report", type="primary"):
        with st.spinner("Generating quality report..."):
            # Load current data
            data_path = Path(__file__).parent / "data" / "messy_input.csv"
            if data_path.exists():
                df = pd.read_csv(data_path)
                report_data = monitor.generate_report(df)
                st.session_state.report_data = report_data
                st.success("✅ Report generated successfully!")
            else:
                st.error("❌ Dataset file not found")

    # Load existing report if available
    if 'report_data' not in st.session_state:
        # Try to load latest report
        reports_dir = Path("reports")
        if reports_dir.exists():
            json_files = list(reports_dir.glob("report_*.json"))
            if json_files:
                latest_report = max(json_files, key=lambda x: x.stat().st_mtime)
                try:
                    with open(latest_report, 'r') as f:
                        st.session_state.report_data = json.load(f)
                except:
                    pass

    # Display report data
    if 'report_data' in st.session_state:
        report_data = st.session_state.report_data

        # Key metrics
        display_quality_metrics(report_data)

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Overview", "🔍 Validation", "🌊 Drift Analysis", "📈 Completeness"])

        with tab1:
            st.subheader("📋 Quality Report Overview")

            metadata = report_data.get('report_metadata', {})
            st.write(f"**Generated:** {metadata.get('generated_at', 'Unknown')}")
            st.write(f"**Dataset Size:** {metadata.get('dataset_rows', 0)} rows, {metadata.get('dataset_columns', 0)} columns")

            # Overall status
            summary = report_data.get('summary', {})
            if summary.get('critical_issues', 0) == 0:
                st.success("✅ Dataset passed all critical quality checks")
            else:
                st.error(f"❌ Dataset has {summary.get('critical_issues', 0)} critical issues requiring attention")

        with tab2:
            validation_data = report_data.get('schema_validation', {})
            display_validation_results(validation_data)

        with tab3:
            anomaly_data = report_data.get('anomaly_detection', {})
            display_anomaly_detection(anomaly_data)

            drift_data = report_data.get('drift_summary', anomaly_data)  # Fallback to anomaly data
            display_drift_summary(drift_data)

        with tab4:
            completeness_data = report_data.get('data_completeness', {})
            display_data_completeness(completeness_data)

            # Overall completeness
            overall = completeness_data.get('overall_completeness', 0)
            st.metric("Overall Data Completeness", f"{overall:.1f}%")

    else:
        st.info("👆 Click 'Generate Fresh Report' to analyze your data quality")

        # Sample data info
        st.subheader("📊 Sample Dataset Information")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            **Clean Reference Data:**
            - 1,000 rows
            - 5 columns (user_id, age, country, signup_date, spend)
            - 100% data completeness
            - No quality issues
            """)

        with col2:
            st.markdown("""
            **Messy Input Data:**
            - 800 rows with intentional issues
            - Type inconsistencies (strings in numeric fields)
            - Missing values (7-13% across columns)
            - Invalid data and outliers
            - Duplicate records
            """)

    # Footer
    st.markdown("---")
    st.markdown("*Built with ❤️ using Streamlit and automated data quality monitoring*")

    # Auto-refresh
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()
